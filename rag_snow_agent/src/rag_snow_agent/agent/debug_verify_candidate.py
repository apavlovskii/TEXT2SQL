"""Debug CLI for semantic verification of Best-of-N candidates.

Usage:
    uv run python -m rag_snow_agent.agent.debug_verify_candidate \
        --db_id TESTDB --query "top selling product by month in 2017" \
        --model gpt-4o-mini --n 2 --top_k 50
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import yaml

from ..chroma.chroma_store import ChromaStore
from ..eval.write_results import write_spider2_result
from ..retrieval.debug_retrieve import build_schema_slice
from ..retrieval.hybrid_retriever import HybridRetriever
from ..snowflake.executor import SnowflakeExecutor
from .best_of_n import run_best_of_n

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "defaults.yaml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return yaml.safe_load(_CONFIG_PATH.read_text()) or {}
    return {}


def main(argv: list[str] | None = None) -> None:
    cfg = _load_config()
    ret_cfg = cfg.get("retrieval", {})
    llm_cfg = cfg.get("llm", {})
    agent_cfg = cfg.get("agent", {})
    sf_cfg = cfg.get("snowflake", {})
    verify_cfg = agent_cfg.get("verification", {})

    parser = argparse.ArgumentParser(
        description="Debug semantic verification of Best-of-N candidates"
    )
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--credentials", default="snowflake_credentials.json")
    parser.add_argument("--instance_id", default="debug_verify")
    parser.add_argument("--n", type=int, default=agent_cfg.get("best_of_n", 2))
    parser.add_argument("--top_k", type=int, default=ret_cfg.get("top_k_tables", 8))
    parser.add_argument("--top_k_columns", type=int, default=ret_cfg.get("top_k_columns", 25))
    parser.add_argument(
        "--max_schema_tokens", type=int, default=ret_cfg.get("max_schema_tokens", 2500)
    )
    parser.add_argument("--model", default=llm_cfg.get("model", "gpt-4o-mini"))
    parser.add_argument("--max_repairs", type=int, default=agent_cfg.get("max_repairs", 2))
    parser.add_argument(
        "--max_metamorphic_checks", type=int,
        default=verify_cfg.get("max_metamorphic_checks", 2),
    )
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("--experiment", default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # ── Retrieve schema slice ──────────────────────────────────────────
    store = ChromaStore(persist_dir=args.chroma_dir)
    collection = store.schema_collection()
    retriever = HybridRetriever(collection)

    schema_slice, _, _ = build_schema_slice(
        retriever=retriever,
        query=args.query,
        db_id=args.db_id,
        top_k_tables=args.top_k,
        top_k_columns=args.top_k_columns,
        max_schema_tokens=args.max_schema_tokens,
    )

    print(f"\n{'='*60}")
    print(f"Query:             {args.query}")
    print(f"DB:                {args.db_id}")
    print(f"Model:             {args.model}")
    print(f"Candidates:        {args.n}")
    print(f"Metamorphic checks: {args.max_metamorphic_checks}")
    print(f"{'='*60}")

    print(f"\n--- SchemaSlice ---")
    print(schema_slice.summary())

    # ── Create executor ────────────────────────────────────────────────
    executor = SnowflakeExecutor(
        credentials_path=args.credentials,
        db_id=args.db_id,
        statement_timeout_sec=sf_cfg.get("statement_timeout_sec", 120),
        sample_rows=agent_cfg.get("sample_rows", 20),
    )

    try:
        result = run_best_of_n(
            instance_id=args.instance_id,
            db_id=args.db_id,
            instruction=args.query,
            schema_slice=schema_slice,
            model=args.model,
            executor=executor,
            n=args.n,
            max_repairs=args.max_repairs,
            explain_first=agent_cfg.get("explain_first", True),
            stop_on_repeated_error=agent_cfg.get("stop_on_repeated_error", True),
            enable_fingerprinting=verify_cfg.get("enable_fingerprinting", True),
            enable_metamorphic=verify_cfg.get("enable_metamorphic", True),
            max_metamorphic_checks=args.max_metamorphic_checks,
        )
    finally:
        executor.close()

    # ── Print expected shape ──────────────────────────────────────────
    es = result.get("expected_shape", {})
    print(f"\n--- Expected Shape ---")
    for k, v in es.items():
        if v and k != "notes":
            print(f"  {k}: {v}")
    if es.get("notes"):
        for n in es["notes"]:
            print(f"  note: {n}")

    # ── Print candidates ──────────────────────────────────────────────
    print(f"\n--- Candidates ({len(result['candidates'])}) ---")
    for c in result["candidates"]:
        status = "OK" if c.get("execution_success") else "FAIL"
        print(
            f"\n  Candidate {c['candidate_id']} [{c['strategy']}] "
            f"score={c['score']:.1f} {status}"
        )
        print(f"    Final SQL: {c['final_sql'][:120]}...")
        print(f"    Rows: {c.get('row_count')}, Repairs: {c['repairs_count']}")

        # Fingerprint
        fp = c.get("result_fingerprint")
        if fp:
            print(f"    Fingerprint: cols={fp.get('column_count')}, "
                  f"columns={fp.get('column_names', [])[:5]}")
            if fp.get("numeric_stats"):
                for col, stats in list(fp["numeric_stats"].items())[:3]:
                    print(f"      {col}: {stats}")

        # Metamorphic
        meta = c.get("metamorphic", {})
        checks = meta.get("checks_run", [])
        if checks:
            print(f"    Metamorphic ({len(checks)} checks, delta={meta.get('score_delta', 0):.1f}):")
            for ch in checks:
                print(f"      {ch['check_type']}: {ch.get('notes', '')[:100]}")

        # Score breakdown
        bd = c.get("score_breakdown", {})
        if bd:
            print(f"    Score breakdown:")
            for k, v in bd.items():
                if k != "total" and v != 0:
                    print(f"      {k}: {v:+.1f}")

    # ── Selection ─────────────────────────────────────────────────────
    print(f"\n--- Selection ---")
    print(f"Best: candidate {result['best_candidate_id']}")
    print(f"Reason: {result['selection_reason']}")
    print(f"Success: {result['best_success']}")
    print(f"\n--- Best SQL ---")
    print(result["best_sql"])

    if args.experiment:
        path = write_spider2_result(
            experiment=args.experiment,
            instance_id=args.instance_id,
            sql=result["best_sql"],
            success=result["best_success"],
        )
        print(f"\nSpider2 result written: {path}")

    print()


if __name__ == "__main__":
    main()
