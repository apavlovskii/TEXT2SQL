"""High-level experiment runner with ablation toggle support.

CLI usage::

    uv run python -m rag_snow_agent.eval.experiment_runner \
      --split_jsonl Spider2/spider2-snow/spider2-snow.jsonl \
      --credentials rag_snow_agent/snowflake_credentials.json \
      --experiment ablation_v1 \
      --limit 25 \
      --model gpt-4o-mini \
      --best_of_n 2 \
      --disable_memory \
      --disable_verifier
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "defaults.yaml"
DEFAULT_REPORTS_DIR = Path("reports/experiments")


def _git_commit_hash() -> str | None:
    """Best-effort retrieval of current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


def load_config(config_path: Path | None = None) -> dict:
    """Load YAML config, merging optional ablation preset over defaults."""
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def merge_config(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base* (returns new dict)."""
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = merge_config(merged[k], v)
        else:
            merged[k] = v
    return merged


def apply_cli_toggles(config: dict, args: argparse.Namespace) -> dict:
    """Apply CLI ablation flags to config."""
    features = config.setdefault("features", {})
    if args.disable_memory:
        features["memory"] = False
        config.setdefault("memory", {})["enabled"] = False
    if args.disable_verifier:
        features["verifier"] = False
        config.setdefault("verifier", {})["enabled"] = False
    if args.disable_best_of_n:
        features["best_of_n"] = False
        config.setdefault("agent", {})["best_of_n"] = 1
    if args.disable_repair:
        features["repair"] = False
        config.setdefault("agent", {})["max_repairs"] = 0
    if args.disable_verification:
        features["verification"] = False
        agent = config.setdefault("agent", {})
        verification = agent.setdefault("verification", {})
        verification["enable_fingerprinting"] = False
        verification["enable_metamorphic"] = False
    if args.disable_join_graph:
        features["join_graph"] = False
        config.setdefault("retrieval", {})["connectivity_mode"] = "heuristic"
    if args.disable_sample_records:
        features["sample_records"] = False
        config.setdefault("sample_records", {})["enabled"] = False

    # CLI overrides for model / best_of_n / max_repairs
    if args.model:
        config.setdefault("llm", {})["model"] = args.model
    if args.best_of_n is not None and not args.disable_best_of_n:
        config.setdefault("agent", {})["best_of_n"] = args.best_of_n
    if args.max_repairs is not None:
        config.setdefault("agent", {})["max_repairs"] = args.max_repairs

    return config


def write_manifest(
    experiment_dir: Path,
    config: dict,
    args: argparse.Namespace,
) -> Path:
    """Write manifest.json with config snapshot and metadata."""
    toggles = {
        "disable_memory": args.disable_memory,
        "disable_verifier": args.disable_verifier,
        "disable_best_of_n": args.disable_best_of_n,
        "disable_repair": args.disable_repair,
        "disable_verification": args.disable_verification,
        "disable_join_graph": args.disable_join_graph,
        "disable_sample_records": args.disable_sample_records,
    }
    manifest = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit_hash(),
        "experiment": args.experiment,
        "model": config.get("llm", {}).get("model", "unknown"),
        "limit": args.limit,
        "toggles": toggles,
        "config_snapshot": config,
    }
    experiment_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = experiment_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    log.info("Wrote manifest: %s", manifest_path)
    return manifest_path


def load_instances(split_jsonl: Path, limit: int | None = None) -> list[dict]:
    """Load instances from a JSONL file."""
    instances = []
    with open(split_jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            instances.append(json.loads(line))
            if limit and len(instances) >= limit:
                break
    return instances


def preflight_check(
    credentials_path: str,
    model: str | None = None,
    chroma_dir: str | None = None,
    db_ids: list[str] | None = None,
) -> None:
    """Verify Snowflake, OpenAI, and ChromaDB connectivity before starting.

    Loads .env (primary) or .env.example (fallback) for environment variables.
    Prints a full connectivity report.  Exits with code 1 on any failure.
    """
    import os

    from dotenv import load_dotenv

    # Load env vars from .env (primary) or .env.example (fallback)
    for env_file in [".env", ".env.example"]:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path, override=False)
            break

    passed = 0
    total = 3
    print("=" * 60)
    print("  PREFLIGHT SMOKE TEST")
    print("=" * 60)

    # --- 1. Snowflake ---
    print("\n[1/3] Snowflake connectivity... ", end="", flush=True)
    try:
        from ..snowflake.client import connect

        conn = connect(credentials_path)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER()")
        row = cur.fetchone()
        sf_version, sf_account, sf_user = row[0], row[1], row[2]
        cur.close()
        conn.close()
        print(f"OK")
        print(f"       Version:  {sf_version}")
        print(f"       Account:  {sf_account}")
        print(f"       User:     {sf_user}")
        passed += 1
    except Exception as exc:
        print(f"FAILED")
        print(f"       Error: {exc}", file=sys.stderr)
        print(f"       Fix: check snowflake_credentials.json and network access.", file=sys.stderr)

    # --- 2. OpenAI API ---
    print("\n[2/3] OpenAI API connectivity... ", end="", flush=True)
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("FAILED")
        print(f"       Error: OPENAI_API_KEY environment variable is not set.", file=sys.stderr)
        print(f"       Fix: export OPENAI_API_KEY=sk-... or add to .env.example", file=sys.stderr)
    else:
        try:
            from ..agent.llm_client import call_llm

            test_model = model or "gpt-4o-mini"
            reply = call_llm(
                messages=[{"role": "user", "content": "Reply with only the word OK"}],
                model=test_model,
                max_tokens=10,
            )
            print(f"OK")
            print(f"       Model:   {test_model}")
            print(f"       Reply:   {reply!r}")
            passed += 1
        except Exception as exc:
            print(f"FAILED")
            print(f"       Error: {exc}", file=sys.stderr)
            print(f"       Fix: check OPENAI_API_KEY and API quota.", file=sys.stderr)

    # --- 3. ChromaDB ---
    print("\n[3/3] ChromaDB index... ", end="", flush=True)
    try:
        from ..chroma.chroma_store import ChromaStore

        store = ChromaStore(persist_dir=chroma_dir)
        col = store.schema_collection()
        total_items = col.count()

        if total_items == 0:
            print(f"WARNING (collection empty)")
            print(f"       The schema_cards collection has 0 items.")
            print(f"       Fix: run build_index for required databases first.")
        else:
            # Count items per db_id
            all_meta = col.get(include=["metadatas"])
            metas = all_meta.get("metadatas") or []
            from collections import Counter
            db_counts: Counter = Counter()
            type_counts: Counter = Counter()
            for m in metas:
                db_counts[m.get("db_id", "?")] += 1
                type_counts[m.get("object_type", "?")] += 1

            print(f"OK ({total_items:,} items)")
            print(f"       Cards:   {', '.join(f'{t}={c}' for t, c in sorted(type_counts.items()))}")
            print(f"       DBs:     {', '.join(f'{db}({c})' for db, c in sorted(db_counts.items()))}")

            # Verify required db_ids are indexed
            if db_ids:
                missing = [d for d in db_ids if d not in db_counts]
                if missing:
                    print(f"       WARNING: missing indexes for: {', '.join(missing)}")
                    print(f"       Fix: run build_index for those databases.")
                else:
                    print(f"       All required DBs indexed: {', '.join(db_ids)}")

            # Quick search test
            test_results = col.query(
                query_texts=["revenue by month"],
                n_results=1,
                include=["metadatas"],
            )
            if test_results["ids"] and test_results["ids"][0]:
                hit_id = test_results["ids"][0][0]
                print(f"       Search:  OK (test query returned: {hit_id[:60]})")
            else:
                print(f"       Search:  WARNING (test query returned no results)")

            passed += 1
    except Exception as exc:
        print(f"FAILED")
        print(f"       Error: {exc}", file=sys.stderr)
        print(f"       Fix: check .chroma/ directory and run build_index.", file=sys.stderr)

    # --- Summary ---
    print()
    print("-" * 60)
    if passed == total:
        print(f"  RESULT: ALL {total} CHECKS PASSED")
    else:
        print(f"  RESULT: {passed}/{total} CHECKS PASSED, {total - passed} FAILED")
    print("-" * 60)

    if passed < total:
        print("\nAborting: fix the failures above before running the benchmark.\n")
        sys.exit(1)

    print()


def run_experiment(args: argparse.Namespace) -> Path:
    """Orchestrate a full experiment run. Returns the experiment directory."""
    # Validate split_jsonl
    split_path = Path(args.split_jsonl)
    if not split_path.exists():
        print(f"ERROR: split_jsonl not found: {split_path}", file=sys.stderr)
        sys.exit(1)

    # Load and merge config
    config = load_config()
    if args.ablation_preset:
        preset_path = Path(args.ablation_preset)
        if preset_path.exists():
            with open(preset_path) as f:
                preset = yaml.safe_load(f) or {}
            config = merge_config(config, preset)
    config = apply_cli_toggles(config, args)

    # Create experiment directory
    experiment_dir = DEFAULT_REPORTS_DIR / args.experiment
    experiment_dir.mkdir(parents=True, exist_ok=True)

    # Load instances (before preflight so we can check db_ids)
    instances = load_instances(split_path, args.limit)
    log.info("Loaded %d instances from %s", len(instances), split_path)

    # Preflight connectivity checks
    if not args.skip_preflight:
        required_dbs = sorted(set(inst.get("db_id", "") for inst in instances if inst.get("db_id")))
        preflight_check(
            credentials_path=args.credentials,
            model=config.get("llm", {}).get("model"),
            chroma_dir=args.chroma_dir,
            db_ids=required_dbs,
        )

    # Write manifest
    write_manifest(experiment_dir, config, args)

    # Run instances
    results_path = experiment_dir / "instance_results.jsonl"
    successes = 0
    failures = 0
    errors = 0

    with open(results_path, "w") as results_file:
        for i, instance in enumerate(instances, 1):
            instance_id = instance.get("instance_id", f"unknown_{i}")
            instruction = instance.get("instruction", "")
            db_id = instance.get("db_id", "")

            log.info("[%d/%d] Processing %s", i, len(instances), instance_id)
            t_start = time.monotonic()

            try:
                # Import here to avoid circular imports and to allow
                # the runner to work even without full dependencies in tests
                from ..agent.agent import solve_instance
                from ..chroma.chroma_store import ChromaStore
                from ..retrieval.debug_retrieve import build_schema_slice
                from ..retrieval.hybrid_retriever import HybridRetriever
                from ..snowflake.executor import SnowflakeExecutor

                # Create executor
                sf_cfg = config.get("snowflake", {})
                executor = SnowflakeExecutor(
                    credentials_path=args.credentials,
                    db_id=db_id,
                    statement_timeout_sec=sf_cfg.get("statement_timeout_sec", 120),
                    sample_rows=config.get("agent", {}).get("sample_rows", 20),
                )

                # Retrieve schema slice via ChromaDB
                ret_cfg = config.get("retrieval", {})
                store = ChromaStore(persist_dir=args.chroma_dir)
                collection = store.schema_collection()
                retriever = HybridRetriever(collection)

                schema_slice, _, _ = build_schema_slice(
                    retriever=retriever,
                    query=instruction,
                    db_id=db_id,
                    top_k_tables=ret_cfg.get("top_k_tables", 8),
                    top_k_columns=ret_cfg.get("top_k_columns", 25),
                    max_schema_tokens=ret_cfg.get("max_schema_tokens", 2500),
                )

                # Determine solve parameters from config
                agent_cfg = config.get("agent", {})
                llm_cfg = config.get("llm", {})
                sem_cfg = config.get("semantic_layer", {})
                bon = agent_cfg.get("best_of_n", 1)
                max_repairs = agent_cfg.get("max_repairs", 2)
                memory_enabled = config.get("memory", {}).get("enabled", True)
                model = llm_cfg.get("model", "gpt-4o-mini")
                max_tokens = llm_cfg.get("max_output_tokens", 4096)
                decompose = agent_cfg.get("decompose_questions", False)

                # Retrieve semantic context if semantic layer is enabled
                semantic_context = None
                if sem_cfg.get("enabled", False):
                    try:
                        from ..retrieval.semantic_retriever import retrieve_semantic_context
                        semantic_context = retrieve_semantic_context(
                            db_id=db_id,
                            instruction=instruction,
                            chroma_store=store,
                            top_k=sem_cfg.get("retrieval_top_k", 8),
                        )
                        if semantic_context:
                            log.info("Semantic context retrieved (%d chars) for %s", len(semantic_context), instance_id)
                        else:
                            log.warning("Semantic context is EMPTY for %s", instance_id)
                    except Exception as exc:
                        log.error("Semantic context retrieval FAILED for %s: %s", instance_id, exc, exc_info=True)
                        raise

                # Retrieve sample records context if enabled
                sample_context = None
                sample_cfg = config.get("sample_records", {})
                if sample_cfg.get("enabled", False):
                    try:
                        from ..chroma.sample_records import SampleRecordStore, build_sample_context

                        sample_store = SampleRecordStore(store)
                        table_fqns = [t.qualified_name for t in schema_slice.tables]
                        table_docs = sample_store.get_sample_context_for_tables(db_id, table_fqns)
                        sample_context = build_sample_context(
                            table_docs,
                            max_tokens=sample_cfg.get("max_prompt_tokens", 800),
                        )
                        if sample_context:
                            log.info("Sample records context retrieved (%d chars) for %s", len(sample_context), instance_id)
                        else:
                            log.info("No sample records found for %s tables", instance_id)
                    except Exception as exc:
                        log.warning("Sample records retrieval failed for %s: %s", instance_id, exc)

                log.info("Features: decompose=%s, semantic_context=%s chars, sample_context=%s chars, chroma_store=%s",
                         decompose, len(semantic_context) if semantic_context else 0,
                         len(sample_context) if sample_context else 0,
                         "YES" if store else "NO")

                result = solve_instance(
                    instance_id=instance_id,
                    instruction=instruction,
                    db_id=db_id,
                    schema_slice=schema_slice,
                    model=model,
                    executor=executor,
                    best_of_n=bon,
                    max_repairs=max_repairs,
                    max_tokens=max_tokens,
                    memory_enabled=memory_enabled,
                    chroma_dir=args.chroma_dir,
                    gold_dir=args.gold_dir,
                    max_same_error_type=args.max_same_error_type,
                    chroma_store=store,
                    semantic_context=semantic_context,
                    decompose=decompose,
                    sample_context=sample_context,
                )

                # Write Spider2 result.json
                from ..eval.write_results import write_spider2_result
                write_spider2_result(
                    experiment=args.experiment,
                    instance_id=instance_id,
                    sql=result.final_sql,
                    success=result.success,
                )

                executor.close()

                # Determine gold_matched field
                gold_matched = None
                if args.gold_dir:
                    gold_matched = result.success  # success implies gold match when gold_dir is set

                record = {
                    "instance_id": instance_id,
                    "db_id": db_id,
                    "success": result.success,
                    "final_sql": result.final_sql,
                    "llm_calls": result.llm_calls,
                    "repair_count": len(result.repair_trace),
                    "candidate_count": result.candidate_count,
                    "best_of_n_used": result.best_of_n_used,
                    "error_message": result.error_message,
                    "error_type": None,
                    "selection_reason": result.selection_reason,
                    "gold_matched": gold_matched,
                }
                if result.success:
                    successes += 1
                else:
                    failures += 1

            except Exception as exc:
                log.error("Instance %s failed with error: %s", instance_id, exc)
                record = {
                    "instance_id": instance_id,
                    "db_id": db_id,
                    "success": False,
                    "final_sql": "",
                    "llm_calls": 0,
                    "repair_count": 0,
                    "candidate_count": 0,
                    "best_of_n_used": False,
                    "error_message": str(exc),
                    "error_type": "runner_error",
                }
                errors += 1

            results_file.write(json.dumps(record) + "\n")
            results_file.flush()

            # Progress bar
            t_elapsed = time.monotonic() - t_start
            total = len(instances)
            pct = 100 * i / total
            bar_len = 30
            filled = int(bar_len * i / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            status = "✓" if record.get("success") else "✗"
            print(
                f"\r  [{bar}] {i}/{total} ({pct:.0f}%) "
                f"| {status} {instance_id} ({t_elapsed:.1f}s) "
                f"| ok={successes} fail={failures} err={errors}",
                flush=True,
            )

    # Summary
    total = len(instances)
    print(f"\n{'='*50}")
    print(f"Experiment: {args.experiment}")
    print(f"Total instances: {total}")
    print(f"Successes: {successes} ({100*successes/total:.1f}%)" if total else "No instances")
    print(f"Failures: {failures}")
    print(f"Errors: {errors}")
    print(f"Results: {results_path}")
    print(f"{'='*50}")

    return experiment_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ablation experiment")
    parser.add_argument("--split_jsonl", required=True, help="Path to spider2-snow JSONL split")
    parser.add_argument("--credentials", default="rag_snow_agent/snowflake_credentials.json")
    parser.add_argument("--experiment", required=True, help="Experiment name")
    parser.add_argument("--limit", type=int, default=None, help="Max instances to process")
    parser.add_argument("--model", default=None, help="LLM model override")
    parser.add_argument("--best_of_n", type=int, default=None, help="Best-of-N count")
    parser.add_argument("--max_repairs", type=int, default=None, help="Max repair iterations per candidate")
    parser.add_argument("--ablation_preset", default=None, help="Path to ablation preset YAML")
    parser.add_argument("--chroma_dir", default=None, help="ChromaDB persistence directory")

    # Ablation toggles
    parser.add_argument("--disable_memory", action="store_true")
    parser.add_argument("--disable_verifier", action="store_true")
    parser.add_argument("--disable_best_of_n", action="store_true")
    parser.add_argument("--disable_repair", action="store_true")
    parser.add_argument("--disable_verification", action="store_true")
    parser.add_argument("--disable_join_graph", action="store_true")
    parser.add_argument("--disable_sample_records", action="store_true")
    parser.add_argument("--skip_preflight", action="store_true", help="Skip Snowflake/OpenAI connectivity checks")
    parser.add_argument("--gold_dir", default=None, help="Path to gold evaluation directory (enables gold-match verification)")
    parser.add_argument("--max_same_error_type", type=int, default=3, help="Stop retrying after N same-type errors per candidate (default 3)")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    run_experiment(args)


if __name__ == "__main__":
    main()
