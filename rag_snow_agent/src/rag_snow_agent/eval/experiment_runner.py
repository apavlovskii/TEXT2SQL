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

    # CLI overrides for model / best_of_n
    if args.model:
        config.setdefault("llm", {})["model"] = args.model
    if args.best_of_n is not None and not args.disable_best_of_n:
        config.setdefault("agent", {})["best_of_n"] = args.best_of_n

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


def preflight_check(credentials_path: str, model: str | None = None) -> None:
    """Verify Snowflake and OpenAI connectivity before starting the run.

    Loads .env / .env.example if OPENAI_API_KEY is not already set.
    Exits with code 1 if either check fails.
    """
    import os

    from dotenv import load_dotenv

    # Load env vars from .env or .env.example if not already set
    if not os.environ.get("OPENAI_API_KEY"):
        for env_file in [".env", ".env.example"]:
            env_path = Path(env_file)
            if env_path.exists():
                load_dotenv(env_path, override=False)
                break

    print("=" * 50)
    print("Preflight connectivity checks")
    print("=" * 50)

    # --- Snowflake ---
    print("\n[1/2] Snowflake connectivity... ", end="", flush=True)
    try:
        from ..snowflake.client import connect

        conn = connect(credentials_path)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"OK (version {version})")
    except Exception as exc:
        print(f"FAILED")
        print(f"  Error: {exc}", file=sys.stderr)
        print("\nFix: check snowflake_credentials.json and network access.", file=sys.stderr)
        sys.exit(1)

    # --- OpenAI ---
    print("[2/2] OpenAI API connectivity... ", end="", flush=True)
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("FAILED")
        print("  Error: OPENAI_API_KEY environment variable is not set.", file=sys.stderr)
        print("\nFix: export OPENAI_API_KEY=sk-...", file=sys.stderr)
        sys.exit(1)

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        test_model = model or "gpt-4o-mini"
        response = client.chat.completions.create(
            model=test_model,
            messages=[{"role": "user", "content": "Reply with only the word OK"}],
            max_tokens=5,
        )
        reply = response.choices[0].message.content.strip()
        print(f"OK (model={test_model}, reply={reply!r})")
    except Exception as exc:
        print(f"FAILED")
        print(f"  Error: {exc}", file=sys.stderr)
        print("\nFix: check OPENAI_API_KEY and API quota.", file=sys.stderr)
        sys.exit(1)

    print("\nAll preflight checks passed.\n")


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

    # Preflight connectivity checks
    if not args.skip_preflight:
        preflight_check(
            credentials_path=args.credentials,
            model=config.get("llm", {}).get("model"),
        )

    # Write manifest
    write_manifest(experiment_dir, config, args)

    # Load instances
    instances = load_instances(split_path, args.limit)
    log.info("Loaded %d instances from %s", len(instances), split_path)

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
                bon = agent_cfg.get("best_of_n", 1)
                max_repairs = agent_cfg.get("max_repairs", 2)
                memory_enabled = config.get("memory", {}).get("enabled", True)
                model = config.get("llm", {}).get("model", "gpt-4o-mini")

                result = solve_instance(
                    instance_id=instance_id,
                    instruction=instruction,
                    db_id=db_id,
                    schema_slice=schema_slice,
                    model=model,
                    executor=executor,
                    best_of_n=bon,
                    max_repairs=max_repairs,
                    memory_enabled=memory_enabled,
                    chroma_dir=args.chroma_dir,
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
    parser.add_argument("--ablation_preset", default=None, help="Path to ablation preset YAML")
    parser.add_argument("--chroma_dir", default=None, help="ChromaDB persistence directory")

    # Ablation toggles
    parser.add_argument("--disable_memory", action="store_true")
    parser.add_argument("--disable_verifier", action="store_true")
    parser.add_argument("--disable_best_of_n", action="store_true")
    parser.add_argument("--disable_repair", action="store_true")
    parser.add_argument("--disable_verification", action="store_true")
    parser.add_argument("--disable_join_graph", action="store_true")
    parser.add_argument("--skip_preflight", action="store_true", help="Skip Snowflake/OpenAI connectivity checks")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    run_experiment(args)


if __name__ == "__main__":
    main()
