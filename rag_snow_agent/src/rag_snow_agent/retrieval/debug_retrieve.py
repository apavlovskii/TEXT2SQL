"""Debug CLI for inspecting retrieval results.

Usage:
    uv run python -m rag_snow_agent.retrieval.debug_retrieve \
        --db_id TESTDB --query "total orders by month" \
        --top_k 50 --max_schema_tokens 800
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from pathlib import Path

import yaml

from ..chroma.chroma_store import ChromaStore
from .budget import classify_column, trim_to_budget
from .connectivity import expand_connectivity
from .hybrid_retriever import HybridRetriever, ScoredItem
from .schema_slice import ColumnSlice, SchemaSlice, TableSlice

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "defaults.yaml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return yaml.safe_load(_CONFIG_PATH.read_text()) or {}
    return {}


def build_schema_slice(
    retriever: HybridRetriever,
    query: str,
    db_id: str,
    top_k_tables: int,
    top_k_columns: int,
    max_schema_tokens: int,
    max_tables: int | None = None,
    max_columns_per_table: int | None = None,
    connectivity_rounds: int = 1,
) -> tuple[SchemaSlice, list[ScoredItem], list[ScoredItem]]:
    """Run full retrieval pipeline and return (slice, table_items, column_items)."""
    table_items = retriever.retrieve_tables(query, db_id, top_k=top_k_tables)
    column_items = retriever.retrieve_columns(query, db_id, top_k=top_k_columns)

    # Group columns by table
    cols_by_table: dict[str, list[ScoredItem]] = defaultdict(list)
    for ci in column_items:
        tqn = ci.metadata.get("table_qualified_name", "")
        cols_by_table[tqn].append(ci)

    # Build TableSlices for each retrieved table
    table_slices: list[TableSlice] = []
    for ti in table_items:
        qname = ti.qualified_name
        col_slices = []
        for ci in cols_by_table.get(qname, []):
            col_name = ci.qualified_name.rsplit(".", 1)[-1]
            cs = ColumnSlice(
                name=col_name,
                data_type=ci.metadata.get("data_type", "VARCHAR"),
                comment=None,
                token_estimate=ci.metadata.get("token_estimate", 5),
                fused_rank=ci.fused_rank,
            )
            classify_column(cs)
            col_slices.append(cs)

        # If no columns retrieved for this table, add columns from Chroma directly
        if not col_slices:
            all_cols = retriever.collection.get(
                where={
                    "$and": [
                        {"db_id": db_id},
                        {"object_type": "column"},
                        {"table_qualified_name": qname},
                    ]
                },
                include=["metadatas"],
            )
            for meta in all_cols["metadatas"] or []:
                col_name = meta.get("qualified_name", "").rsplit(".", 1)[-1]
                cs = ColumnSlice(
                    name=col_name,
                    data_type=meta.get("data_type", "VARCHAR"),
                    token_estimate=meta.get("token_estimate", 5),
                    fused_rank=999,
                )
                classify_column(cs)
                col_slices.append(cs)

        ts = TableSlice(
            qualified_name=qname,
            table_token_estimate=ti.metadata.get("token_estimate", 10),
            fused_rank=ti.fused_rank,
            columns=col_slices,
        )
        table_slices.append(ts)

    schema_slice = SchemaSlice(db_id=db_id, tables=table_slices)

    # Connectivity expansion
    if connectivity_rounds > 0:
        expand_connectivity(
            schema_slice, retriever.collection, max_rounds=connectivity_rounds
        )

    # Budget trimming
    trim_to_budget(
        schema_slice,
        max_schema_tokens=max_schema_tokens,
        max_tables=max_tables,
        max_columns_per_table=max_columns_per_table,
    )

    return schema_slice, table_items, column_items


def main(argv: list[str] | None = None) -> None:
    cfg = _load_config().get("retrieval", {})

    parser = argparse.ArgumentParser(description="Debug schema retrieval")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--top_k", type=int, default=cfg.get("top_k_tables", 8))
    parser.add_argument("--top_k_columns", type=int, default=cfg.get("top_k_columns", 25))
    parser.add_argument(
        "--max_schema_tokens", type=int, default=cfg.get("max_schema_tokens", 2500)
    )
    parser.add_argument("--max_tables", type=int, default=None)
    parser.add_argument("--max_columns_per_table", type=int, default=None)
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("--rrf_k", type=int, default=cfg.get("rrf_k", 60))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = ChromaStore(persist_dir=args.chroma_dir)
    collection = store.schema_collection()
    retriever = HybridRetriever(collection, rrf_k=args.rrf_k)

    schema_slice, table_items, column_items = build_schema_slice(
        retriever=retriever,
        query=args.query,
        db_id=args.db_id,
        top_k_tables=args.top_k,
        top_k_columns=args.top_k_columns,
        max_schema_tokens=args.max_schema_tokens,
        max_tables=args.max_tables,
        max_columns_per_table=args.max_columns_per_table,
    )

    # ── print results ─────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Query:   {args.query}")
    print(f"DB:      {args.db_id}")
    print(f"Budget:  {args.max_schema_tokens} tokens")
    print(f"{'='*60}")

    print(f"\n--- Top tables (retrieved {len(table_items)}) ---")
    for ti in table_items[:20]:
        print(
            f"  rank={ti.fused_rank:3d}  dense={ti.dense_rank:3d}  "
            f"lex={ti.lexical_rank:3d}  rrf={ti.rrf_score:.4f}  "
            f"{ti.qualified_name}"
        )

    print(f"\n--- SchemaSlice ---")
    print(schema_slice.summary())
    for ts in schema_slice.tables:
        print(f"\n  TABLE {ts.qualified_name}  (rank={ts.fused_rank}, ~{ts.token_estimate} tok)")
        for col in ts.columns:
            flags = []
            if col.is_join_key:
                flags.append("JK")
            if col.is_time_column:
                flags.append("T")
            flag_str = f" [{','.join(flags)}]" if flags else ""
            print(f"    {col.name:30s} {col.data_type:20s} rank={col.fused_rank}{flag_str}")

    print(f"\n--- Formatted prompt text ({schema_slice.token_estimate} tokens) ---")
    print(schema_slice.format_for_prompt())
    print()


if __name__ == "__main__":
    main()
