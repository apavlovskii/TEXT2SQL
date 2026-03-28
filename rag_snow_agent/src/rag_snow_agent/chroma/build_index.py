"""CLI: python -m rag_snow_agent.chroma.build_index --db_id <DB> --credentials <path>

Connects to Snowflake, extracts schema for db_id, and upserts
TableCard / ColumnCard into a local ChromaDB.
"""

from __future__ import annotations

import argparse
import logging
import sys

from ..snowflake.client import connect
from ..snowflake.metadata import JoinEdge, TableInfo, extract_join_edges, extract_tables
from .chroma_store import ChromaStore
from .schema_cards import ColumnCard, JoinCard, TableCard

log = logging.getLogger(__name__)

_TIME_TYPE_KEYWORDS = {"DATE", "TIME", "TIMESTAMP", "DATETIME"}


def _is_time_type(data_type: str) -> bool:
    upper = data_type.upper()
    return any(kw in upper for kw in _TIME_TYPE_KEYWORDS)


def build_table_card(table: TableInfo, db_id: str) -> TableCard:
    col_names = [c.column_name for c in table.columns]
    time_cols = [c.column_name for c in table.columns if _is_time_type(c.data_type)]
    join_key_hints = [
        c.column_name
        for c in table.columns
        if c.column_name.upper().endswith("_ID") or c.column_name.upper() == "ID"
    ]
    return TableCard(
        db_id=db_id,
        qualified_name=table.qualified_name,
        table_type=table.table_type,
        comment=table.comment,
        row_count=table.row_count,
        column_names=col_names,
        time_columns=time_cols,
        common_join_keys=join_key_hints,
    )


def build_column_card(table: TableInfo, col, db_id: str) -> ColumnCard:
    return ColumnCard(
        db_id=db_id,
        qualified_name=f"{table.qualified_name}.{col.column_name}",
        table_qualified_name=table.qualified_name,
        data_type=col.data_type,
        is_nullable=col.is_nullable,
        comment=col.comment,
    )


def build_join_card(edge: JoinEdge, db_id: str) -> JoinCard:
    return JoinCard(
        db_id=db_id,
        left_table=edge.left_table,
        left_column=edge.left_column,
        right_table=edge.right_table,
        right_column=edge.right_column,
        confidence=edge.confidence,
        source=edge.source,
    )


def run(db_id: str, credentials: str, chroma_dir: str | None = None) -> dict[str, int]:
    """Main entry point. Returns counts dict {table: N, column: M, join: J}."""
    conn = connect(credentials)
    try:
        tables = extract_tables(conn, db_id)
        join_edges = extract_join_edges(conn, db_id, tables)
    finally:
        conn.close()

    table_cards = [build_table_card(t, db_id) for t in tables]
    column_cards = [
        build_column_card(t, c, db_id) for t in tables for c in t.columns
    ]
    join_cards = [build_join_card(e, db_id) for e in join_edges]

    store = ChromaStore(persist_dir=chroma_dir)
    store.upsert_table_cards(table_cards)
    store.upsert_column_cards(column_cards)
    store.upsert_join_cards(join_cards)

    counts = store.count_by_type(db_id)
    log.info("Index build complete for %s: %s", db_id, counts)
    return counts


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Build ChromaDB index from Snowflake INFORMATION_SCHEMA"
    )
    parser.add_argument("--db_id", required=True, help="Snowflake database name")
    parser.add_argument(
        "--credentials",
        required=True,
        help="Path to snowflake_credentials.json",
    )
    parser.add_argument(
        "--chroma_dir",
        default=None,
        help="ChromaDB persistence directory (default: rag_snow_agent/.chroma)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    counts = run(args.db_id, args.credentials, args.chroma_dir)
    print(f"\nIndex for {args.db_id}:")
    for obj_type, count in sorted(counts.items()):
        print(f"  {obj_type}: {count}")


if __name__ == "__main__":
    main()
