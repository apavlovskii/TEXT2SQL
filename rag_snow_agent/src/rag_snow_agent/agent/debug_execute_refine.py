"""Debug CLI for the execution + repair loop.

Usage:
    uv run python -m rag_snow_agent.agent.debug_execute_refine \
        --db_id TESTDB --query "average amount by month in 2014" \
        --credentials snowflake_credentials.json \
        --top_k 50 --model gpt-4o-mini
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import yaml

from ..chroma.chroma_store import ChromaStore
from ..retrieval.debug_retrieve import build_schema_slice
from ..retrieval.hybrid_retriever import HybridRetriever
from ..snowflake.executor import SnowflakeExecutor
from .agent import solve_instance
from ..eval.write_results import write_spider2_result

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

    parser = argparse.ArgumentParser(description="Debug execution + repair loop")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--credentials", default="snowflake_credentials.json")
    parser.add_argument("--instance_id", default="debug_instance")
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
    print(f"Query:     {args.query}")
    print(f"DB:        {args.db_id}")
    print(f"Model:     {args.model}")
    print(f"Repairs:   max {args.max_repairs}")
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

    try:
        # ── Solve ──────────────────────────────────────────────────────
        result = solve_instance(
            instance_id=args.instance_id,
            instruction=args.query,
            db_id=args.db_id,
            schema_slice=schema_slice,
            model=args.model,
            executor=executor,
            max_repairs=args.max_repairs,
            explain_first=agent_cfg.get("explain_first", True),
            stop_on_repeated_error=agent_cfg.get("stop_on_repeated_error", True),
        )
    finally:
        executor.close()

    # ── Print results ──────────────────────────────────────────────────
    if result.pipeline_result:
        print(f"\n--- Initial SQL ---")
        print(result.pipeline_result.sql)

    if result.repair_trace:
        print(f"\n--- Repair Trace ({len(result.repair_trace)} attempts) ---")
        for item in result.repair_trace:
            print(f"\n  Attempt {item.attempt}:")
            print(f"    Error type: {item.error_type}")
            print(f"    Action:     {item.repair_action}")
            print(f"    Error:      {item.error_message[:120]}")
            print(f"    Output SQL: {item.output_sql[:200]}")
    else:
        print(f"\n--- No repairs needed ---")

    print(f"\n--- Final SQL ---")
    print(result.final_sql)

    print(f"\n--- Result ---")
    print(f"Success:    {result.success}")
    print(f"LLM calls:  {result.llm_calls}")
    if result.error_message:
        print(f"Error:      {result.error_message[:200]}")

    # ── Optionally write Spider2 result ────────────────────────────────
    if args.experiment:
        path = write_spider2_result(
            experiment=args.experiment,
            instance_id=args.instance_id,
            sql=result.final_sql,
            success=result.success,
        )
        print(f"\nSpider2 result written: {path}")

    print()


if __name__ == "__main__":
    main()
