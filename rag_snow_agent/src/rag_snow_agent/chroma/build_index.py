"""CLI: python -m rag_snow_agent.chroma.build_index --db_id <DB> --credentials <path>

Connects to Snowflake, extracts schema for db_id, and upserts
TableCard / ColumnCard into a local ChromaDB.
"""

from __future__ import annotations

import argparse
import copy
import logging
import re
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
_DATE_SUFFIX_RE = re.compile(r"^(.+?)_?(\d{6,8})$")


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


def _collapse_partition_tables(
    tables: list[TableInfo],
) -> tuple[list[TableInfo], dict[str, str]]:
    """Merge daily partition tables into one representative per schema group.

    GA360 has hundreds of tables like ``GA_SESSIONS_20170101`` …
    ``GA_SESSIONS_20170801`` that share the same column schema.  These are
    collapsed into a single representative named ``GA_SESSIONS`` (date suffix
    stripped) with a descriptive comment.

    Returns ``(collapsed_tables, rename_map)`` where *rename_map* maps each
    original ``qualified_name`` to the collapsed representative name so that
    VARIANT sub-field cards can be remapped.
    """
    # Group by (schema, base_name) — ignore column differences so that
    # partition tables with slightly varying schemas (e.g., GA360's Jul–Aug
    # group has an extra clientId column) still merge into one representative.
    groups: dict[str, list[TableInfo]] = {}
    non_partition: list[TableInfo] = []

    for t in tables:
        m = _DATE_SUFFIX_RE.match(t.table_name)
        if m:
            base = m.group(1).rstrip("_")
            sig = f"{t.table_schema}||{base}"
            groups.setdefault(sig, []).append(t)
        else:
            non_partition.append(t)

    result = list(non_partition)
    rename_map: dict[str, str] = {}

    for _sig, group in groups.items():
        if len(group) < 3:
            # Not a real partition pattern — keep all
            result.extend(group)
            continue

        # Sort by date suffix descending — pick the most recent as representative
        # (most likely to have data and be queryable)
        group.sort(
            key=lambda t: _DATE_SUFFIX_RE.match(t.table_name).group(2),  # type: ignore[union-attr]
            reverse=True,
        )
        rep = group[0]
        m = _DATE_SUFFIX_RE.match(rep.table_name)
        base_name = m.group(1).rstrip("_") if m else rep.table_name

        # Determine date range from group
        dates = sorted(
            _DATE_SUFFIX_RE.match(t.table_name).group(2)  # type: ignore[union-attr]
            for t in group
        )
        min_date, max_date = dates[0], dates[-1]
        total_rows = sum(t.row_count or 0 for t in group)

        # Keep the actual table name of the representative (it exists in
        # Snowflake), but add a comment explaining the partition pattern
        # so the LLM filters by the "date" column instead of picking tables.
        merged = copy.copy(rep)
        merged.row_count = total_rows
        merged.comment = (
            f"Partitioned: {len(group)} daily tables {base_name}_YYYYMMDD "
            f"({min_date}–{max_date}). All share the same schema. "
            f"Query this table and filter with WHERE \"date\" >= 'YYYYMMDD' "
            f"AND \"date\" < 'YYYYMMDD' to select date ranges."
        )

        # Build union of columns across all partition variants so no
        # columns are lost when schemas differ slightly.
        seen_cols: dict[str, object] = {}
        for t in group:
            for c in t.columns:
                if c.column_name not in seen_cols:
                    seen_cols[c.column_name] = copy.copy(c)
                    # Update table references to the representative
                    seen_cols[c.column_name].table_name = rep.table_name
        merged.columns = list(seen_cols.values())

        result.append(merged)

        # Build rename map: all original tables → the representative
        merged_qname = merged.qualified_name
        for t in group:
            rename_map[t.qualified_name] = merged_qname

        log.info(
            "Collapsed %d partition tables into %s (%s – %s, %s total rows)",
            len(group), merged_qname, min_date, max_date, f"{total_rows:,}",
        )

    return result, rename_map


def run(db_id: str, credentials: str, chroma_dir: str | None = None) -> dict[str, int]:
    """Main entry point. Returns counts dict {table: N, column: M, join: J}."""
    conn = connect(credentials)
    try:
        tables = extract_tables(conn, db_id)
        join_edges = extract_join_edges(conn, db_id, tables)
        variant_subfields = extract_variant_subfields(conn, db_id, tables)
    finally:
        conn.close()

    # Collapse daily partition tables (e.g. GA360 GA_SESSIONS_YYYYMMDD)
    tables, rename_map = _collapse_partition_tables(tables)
    if rename_map:
        log.info("Partition collapse: %d original tables mapped to %d representatives",
                 len(rename_map), len(set(rename_map.values())))

    # Remap VARIANT sub-field cards to use collapsed table names
    for vf in variant_subfields:
        old_qname = f"{vf.table_catalog}.{vf.table_schema}.{vf.table_name}"
        if old_qname in rename_map:
            new_qname = rename_map[old_qname]
            # Update the table_name to the collapsed name
            vf.table_name = new_qname.rsplit(".", 1)[-1]

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
