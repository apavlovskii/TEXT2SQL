"""CLI: python -m rag_snow_agent.chroma.build_index --db_id <DB> --credentials <path>

Connects to Snowflake, extracts schema for db_id, and upserts
TableCard / ColumnCard into a local ChromaDB.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ..snowflake.client import connect
from ..snowflake.metadata import (
    ColumnInfo,
    JoinEdge,
    TableInfo,
    extract_join_edges,
    extract_tables,
    extract_variant_subfields,
)
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


def build_variant_column_card(col: ColumnInfo, db_id: str) -> ColumnCard:
    """Build a ColumnCard for a VARIANT sub-field discovered by sampling."""
    table_qname = f"{col.table_catalog}.{col.table_schema}.{col.table_name}"
    return ColumnCard(
        db_id=db_id,
        qualified_name=f"{table_qname}.{col.column_name}",
        table_qualified_name=table_qname,
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
        variant_subfields = extract_variant_subfields(conn, db_id, tables)
    finally:
        conn.close()

    table_cards = [build_table_card(t, db_id) for t in tables]
    column_cards = [
        build_column_card(t, c, db_id) for t in tables for c in t.columns
    ]
    # Add VARIANT sub-field column cards
    variant_cards = [build_variant_column_card(c, db_id) for c in variant_subfields]
    column_cards.extend(variant_cards)
    log.info("Added %d VARIANT sub-field cards to index", len(variant_cards))

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

    # Auto-update INDEX_CARD.md
    try:
        _update_index_card(ChromaStore(persist_dir=args.chroma_dir))
    except Exception:
        log.warning("Failed to update INDEX_CARD.md", exc_info=True)


def _update_index_card(store: ChromaStore) -> None:
    """Regenerate INDEX_CARD.md from current ChromaDB contents."""
    from collections import Counter
    from datetime import datetime, timezone

    col = store.schema_collection()
    all_meta = col.get(include=["metadatas"])
    metas = all_meta.get("metadatas") or []

    # Aggregate stats
    by_db: dict[str, dict[str, int]] = {}
    for m in metas:
        db = m.get("db_id", "?")
        otype = m.get("object_type", "?")
        if db not in by_db:
            by_db[db] = {"table": 0, "column": 0, "join": 0, "variant_field": 0}
        by_db[db][otype] = by_db[db].get(otype, 0) + 1
        if m.get("data_type") == "VARIANT_FIELD":
            by_db[db]["variant_field"] += 1

    # Trace memory count
    try:
        from .trace_memory import TraceMemoryStore
        tm = TraceMemoryStore(persist_dir=store.persist_dir)
        trace_count = tm.collection().count()
    except Exception:
        trace_count = 0

    total_items = sum(
        d["table"] + d["column"] + d["join"] for d in by_db.values()
    )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        "# ChromaDB Vector Database Index Card",
        "",
        "> Auto-generated by `build_index.py`. Updated each time a database is indexed.",
        f">",
        f"> **Last updated:** {now}",
        "",
        "---",
        "",
        "## Collections",
        "",
        "| Collection | Items | Purpose |",
        "|:-----------|------:|:--------|",
        f"| `schema_cards` | {total_items:,} | Tables, columns (incl. VARIANT sub-fields), and join edges extracted from Snowflake INFORMATION_SCHEMA |",
        f"| `trace_memory` | {trace_count:,} | Compact traces of successfully solved Spider2-Snow instances for few-shot retrieval |",
        "",
        "**Embedding model:** `text-embedding-3-large` (OpenAI, 3072 dimensions)",
        "**Persistence path:** `rag_snow_agent/.chroma/`",
        "**Similarity metric:** cosine",
        "",
        "---",
        "",
        "## Indexed Databases",
        "",
        "| Database | Tables | Columns | VARIANT Sub-fields | Join Edges | Total Cards |",
        "|:---------|-------:|--------:|-------------------:|-----------:|------------:|",
    ]

    grand = {"table": 0, "column": 0, "variant": 0, "join": 0, "total": 0}
    for db in sorted(by_db):
        d = by_db[db]
        t, c, v, j = d["table"], d["column"], d["variant_field"], d["join"]
        row_total = t + c + j
        lines.append(f"| {db} | {t:,} | {c:,} | {v:,} | {j:,} | {row_total:,} |")
        grand["table"] += t
        grand["column"] += c
        grand["variant"] += v
        grand["join"] += j
        grand["total"] += row_total

    lines.append(
        f"| **Total** | **{grand['table']:,}** | **{grand['column']:,}** "
        f"| **{grand['variant']:,}** | **{grand['join']:,}** | **{grand['total']:,}** |"
    )
    lines.append("")

    card_path = Path(__file__).parent / "INDEX_CARD.md"
    card_path.write_text("\n".join(lines) + "\n")
    log.info("Updated %s", card_path)


if __name__ == "__main__":
    main()
