"""CLI to build the semantic layer for a database.

Usage:
    python -m rag_snow_agent.semantic_layer.build_semantic_layer \\
        --db_id GA360 --credentials snowflake_credentials.json
"""

from __future__ import annotations

import argparse
import logging
import sys

from ..chroma.chroma_store import ChromaStore
from ..snowflake.client import connect
from ..snowflake.metadata import extract_tables
from .infer_from_docs import infer_from_docs
from .infer_from_metadata import infer_from_metadata
from .infer_from_probes import infer_from_probes
from .infer_from_traces import infer_from_traces
from .merge import merge_semantic_facts, render_semantic_profile_for_prompt
from .store import SemanticLayerStore

log = logging.getLogger(__name__)


def build(
    db_id: str,
    credentials: str,
    chroma_dir: str | None = None,
    docs_dir: str | None = None,
    trace_store=None,
    max_probe_budget: int = 10,
) -> dict:
    """Build the semantic layer for a database.

    Returns summary dict with counts.
    """
    # 1. Load metadata
    conn = connect(credentials)
    try:
        tables = extract_tables(conn, db_id)
    finally:
        conn.close()

    # 2. Run inference from metadata
    metadata_profile = infer_from_metadata(tables, db_id)
    metadata_facts = metadata_profile.all_facts()

    # 3. Run inference from docs
    doc_facts = infer_from_docs(db_id, docs_dir)

    # 4. Run inference from probes
    from ..snowflake.executor import SnowflakeExecutor

    executor = SnowflakeExecutor(
        credentials_path=credentials,
        db_id=db_id,
        statement_timeout_sec=30,
        sample_rows=5,
    )
    try:
        probe_facts = infer_from_probes(db_id, executor, tables, max_probe_budget)
    finally:
        executor.close()

    # 5. Run inference from traces
    trace_facts = infer_from_traces(db_id, trace_store)

    # 6. Merge
    profile = merge_semantic_facts(
        db_id, metadata_facts, doc_facts, probe_facts, trace_facts
    )

    # 7. Persist to Chroma
    chroma = ChromaStore(persist_dir=chroma_dir)
    sem_store = SemanticLayerStore(chroma)
    count = sem_store.upsert_semantic_profile(profile)

    # 8. Print summary
    rendered = render_semantic_profile_for_prompt(profile)
    summary = {
        "db_id": db_id,
        "total_facts": count,
        "time_columns": len(profile.time_columns),
        "metric_candidates": len(profile.metric_candidates),
        "dimension_candidates": len(profile.dimension_candidates),
        "nested_field_patterns": len(profile.nested_field_patterns),
        "join_semantics": len(profile.join_semantics),
        "filter_value_hints": len(profile.filter_value_hints),
        "sample_rows": len(profile.sample_rows),
        "column_stats": len(profile.column_stats),
    }

    print(f"\n=== Semantic Layer Summary for {db_id} ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    if rendered:
        print(f"\nRendered prompt preview ({len(rendered)} chars):")
        print(rendered[:500])

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build semantic layer for a Snowflake database"
    )
    parser.add_argument("--db_id", required=True, help="Database identifier")
    parser.add_argument(
        "--credentials", required=True, help="Path to snowflake credentials JSON"
    )
    parser.add_argument("--chroma_dir", default=None, help="ChromaDB persist directory")
    parser.add_argument("--docs_dir", default=None, help="Path to external docs")
    parser.add_argument(
        "--max_probe_budget", type=int, default=10, help="Max tables to probe"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    build(
        db_id=args.db_id,
        credentials=args.credentials,
        chroma_dir=args.chroma_dir,
        docs_dir=args.docs_dir,
        max_probe_budget=args.max_probe_budget,
    )


if __name__ == "__main__":
    main()
