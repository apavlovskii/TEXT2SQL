"""Debug CLI for Best-of-N candidate generation and selection.

Usage:
    uv run python -m rag_snow_agent.agent.debug_best_of_n \
        --db_id TESTDB --query "average amount by month in 2014" \
        --credentials snowflake_credentials.json \
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

    parser = argparse.ArgumentParser(description="Debug Best-of-N candidate selection")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--credentials", default="snowflake_credentials.json")
    parser.add_argument("--instance_id", default="debug_bon")
    parser.add_argument("--n", type=int, default=agent_cfg.get("best_of_n", 2))
    parser.add_argument("--top_k", type=int, default=ret_cfg.get("top_k_tables", 8))
    parser.add_argument("--top_k_columns", type=int, default=ret_cfg.get("top_k_columns", 25))
    parser.add_argument(
        "--max_schema_tokens", type=int, default=ret_cfg.get("max_schema_tokens", 2500)
    )
    parser.add_argument("--model", default=llm_cfg.get("model", "gpt-4o-mini"))
    parser.add_argument("--max_repairs", type=int, default=agent_cfg.get("max_repairs", 2))
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("--experiment", default=None, help="If set, write Spider2 result.json")
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
    print(f"Query:      {args.query}")
    print(f"DB:         {args.db_id}")
    print(f"Model:      {args.model}")
    print(f"Candidates: {args.n}")
    print(f"Max repairs: {args.max_repairs}")
    print(f"{'='*60}")

    print(f"\n--- SchemaSlice ---")
    print(schema_slice.summary())
    for ts in schema_slice.tables:
        cols = ", ".join(c.name for c in ts.columns)
        print(f"  {ts.qualified_name}: [{cols}]")

    # ── Create executor ────────────────────────────────────────────────
    executor = SnowflakeExecutor(
        credentials_path=args.credentials,
        db_id=args.db_id,
        statement_timeout_sec=sf_cfg.get("statement_timeout_sec", 120),
        sample_rows=agent_cfg.get("sample_rows", 20),
    )

    strategies = agent_cfg.get("candidate_strategies")

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
            strategies=strategies,
        )
    finally:
        executor.close()

    # ── Print results ──────────────────────────────────────────────────
    print(f"\n--- Candidates ({len(result['candidates'])}) ---")
    for c in result["candidates"]:
        status = "OK" if c["execution_success"] else "FAIL"
        print(
            f"\n  Candidate {c['candidate_id']} [{c['strategy']}] "
            f"score={c['score']:.1f} {status}"
        )
        print(f"    Initial SQL: {c['initial_sql'][:120]}...")
        print(f"    Final SQL:   {c['final_sql'][:120]}...")
        print(f"    Repairs: {c['repairs_count']}, Rows: {c['row_count']}")
        if c["error_type"]:
            print(f"    Error: {c['error_type']}")
        if c["repair_trace"]:
            for rt in c["repair_trace"]:
                print(
                    f"      repair #{rt['attempt']}: {rt['error_type']} → {rt['repair_action']}"
                )

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
