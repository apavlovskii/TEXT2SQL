"""Debug CLI for the plan → SQL pipeline.

Usage:
    uv run python -m rag_snow_agent.prompting.debug_plan_sql \
        --db_id TESTDB --query "average amount by month" --top_k 50
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import yaml

from ..agent.plan_sql_pipeline import run_pipeline
from ..chroma.chroma_store import ChromaStore
from ..retrieval.debug_retrieve import build_schema_slice
from ..retrieval.hybrid_retriever import HybridRetriever

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

    parser = argparse.ArgumentParser(description="Debug plan → SQL pipeline")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--top_k", type=int, default=ret_cfg.get("top_k_tables", 8))
    parser.add_argument("--top_k_columns", type=int, default=ret_cfg.get("top_k_columns", 25))
    parser.add_argument(
        "--max_schema_tokens", type=int, default=ret_cfg.get("max_schema_tokens", 2500)
    )
    parser.add_argument("--model", default=llm_cfg.get("model", "gpt-4o-mini"))
    parser.add_argument("--temperature", type=float, default=llm_cfg.get("temperature", 0.2))
    parser.add_argument("--max_tokens", type=int, default=llm_cfg.get("max_output_tokens", 800))
    parser.add_argument("--use_llm_sql", action="store_true",
                        help="Use LLM to generate SQL instead of deterministic compiler")
    parser.add_argument("--chroma_dir", default=None)
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
    print(f"Query: {args.query}")
    print(f"DB:    {args.db_id}")
    print(f"Model: {args.model}")
    print(f"{'='*60}")

    print(f"\n--- SchemaSlice ---")
    print(schema_slice.summary())
    for ts in schema_slice.tables:
        cols = ", ".join(c.name for c in ts.columns)
        print(f"  {ts.qualified_name}: [{cols}]")

    # ── Run pipeline ───────────────────────────────────────────────────
    result = run_pipeline(
        db_id=args.db_id,
        instruction=args.query,
        schema_slice=schema_slice,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        plan_retry_limit=agent_cfg.get("plan_retry_limit", 1),
        validation_fix_limit=agent_cfg.get("validation_fix_limit", 1),
        use_llm_sql=args.use_llm_sql,
        retriever=retriever,
    )

    print(f"\n--- Plan JSON ---")
    if result.plan:
        print(json.dumps(result.plan.model_dump(), indent=2))
    else:
        print("(no valid plan)")
        print(f"Raw: {result.plan_json_raw[:500]}")

    print(f"\n--- Final SQL ---")
    print(result.sql)

    print(f"\n--- Validation ---")
    if result.validation:
        status = "PASS" if result.validation.valid else "FAIL"
        print(f"Status: {status}")
        for e in result.validation.errors:
            print(f"  ERROR: {e}")
        for w in result.validation.warnings:
            print(f"  WARN:  {w}")
    else:
        print("(no validation run)")

    if result.warnings:
        print(f"\n--- Pipeline Warnings ---")
        for w in result.warnings:
            print(f"  {w}")

    print(f"\nLLM calls: {result.llm_calls}")
    print()


if __name__ == "__main__":
    main()
