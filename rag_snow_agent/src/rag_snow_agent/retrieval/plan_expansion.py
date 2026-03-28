"""Plan-guided schema expansion: add tables/columns the plan references but the slice lacks."""

from __future__ import annotations

import logging
from collections import defaultdict

from ..prompting.plan_schema import QueryPlan
from .hybrid_retriever import HybridRetriever
from .schema_slice import ColumnSlice, SchemaSlice, TableSlice

log = logging.getLogger(__name__)


def _table_names_in_slice(schema_slice: SchemaSlice) -> set[str]:
    """Return the set of qualified table names already in the slice (upper-cased)."""
    return {ts.qualified_name.upper() for ts in schema_slice.tables}


def expand_schema_for_plan(
    schema_slice: SchemaSlice,
    plan: QueryPlan,
    retriever: HybridRetriever,
    db_id: str,
    max_added_tables: int = 3,
) -> SchemaSlice:
    """Add tables/columns referenced in *plan* but missing from *schema_slice*.

    Performs a single expansion round:
    1. Identifies tables in ``plan.selected_tables`` not present in the slice.
    2. Queries ChromaDB for those tables and their columns.
    3. Also checks ``plan.joins`` for columns missing from the slice.
    4. Appends up to *max_added_tables* new ``TableSlice`` entries.

    Returns the (mutated) *schema_slice*.
    """
    existing = _table_names_in_slice(schema_slice)
    added = 0

    # ── 1. Tables from plan.selected_tables ──────────────────────────────
    missing_tables: list[str] = []
    for tname in plan.selected_tables:
        if tname.upper() not in existing:
            missing_tables.append(tname)

    for tname in missing_tables:
        if added >= max_added_tables:
            break

        table_slice = _fetch_table_from_chroma(retriever, db_id, tname)
        if table_slice is not None:
            schema_slice.tables.append(table_slice)
            existing.add(table_slice.qualified_name.upper())
            added += 1
            log.info("Plan expansion: added table %s", table_slice.qualified_name)

    # ── 2. Tables from plan.joins ────────────────────────────────────────
    for join in plan.joins:
        if added >= max_added_tables:
            break
        for join_table in (join.left_table, join.right_table):
            if join_table.upper() not in existing:
                table_slice = _fetch_table_from_chroma(retriever, db_id, join_table)
                if table_slice is not None:
                    schema_slice.tables.append(table_slice)
                    existing.add(table_slice.qualified_name.upper())
                    added += 1
                    log.info("Plan expansion (join): added table %s", table_slice.qualified_name)
                if added >= max_added_tables:
                    break

    # ── 3. Check join columns exist in their table slices ────────────────
    _ensure_join_columns(schema_slice, plan, retriever, db_id)

    if added > 0:
        log.info("Plan expansion complete: added %d table(s)", added)
    else:
        log.debug("Plan expansion: no missing tables found")

    return schema_slice


def _fetch_table_from_chroma(
    retriever: HybridRetriever,
    db_id: str,
    table_name: str,
) -> TableSlice | None:
    """Look up a table by name in ChromaDB and return a TableSlice with its columns."""
    # Try to find the table card
    table_items = retriever.retrieve_tables(table_name, db_id, top_k=5)
    matched = None
    for item in table_items:
        if item.qualified_name.upper() == table_name.upper():
            matched = item
            break
        # Also check if the short name matches (last segment)
        if item.qualified_name.upper().endswith("." + table_name.upper()):
            matched = item
            break

    if matched is None and table_items:
        # Fall back to the best match
        matched = table_items[0]
        log.debug(
            "Plan expansion: exact match not found for %s, using best match %s",
            table_name,
            matched.qualified_name,
        )

    if matched is None:
        log.debug("Plan expansion: table %s not found in ChromaDB", table_name)
        return None

    qname = matched.qualified_name

    # Fetch columns for this table
    col_results = retriever.collection.get(
        where={
            "$and": [
                {"db_id": db_id},
                {"object_type": "column"},
                {"table_qualified_name": qname},
            ]
        },
        include=["metadatas"],
    )

    col_slices: list[ColumnSlice] = []
    for meta in col_results["metadatas"] or []:
        col_name = meta.get("qualified_name", "").rsplit(".", 1)[-1]
        cs = ColumnSlice(
            name=col_name,
            data_type=meta.get("data_type", "VARCHAR"),
            token_estimate=meta.get("token_estimate", 5),
            fused_rank=999,  # low priority since it's an expansion
        )
        col_slices.append(cs)

    return TableSlice(
        qualified_name=qname,
        table_token_estimate=matched.metadata.get("token_estimate", 10),
        fused_rank=999,
        columns=col_slices,
    )


def _ensure_join_columns(
    schema_slice: SchemaSlice,
    plan: QueryPlan,
    retriever: HybridRetriever,
    db_id: str,
) -> None:
    """Check that columns referenced in plan.joins exist in the slice; add missing ones."""
    # Build a lookup: table_name_upper -> TableSlice
    table_map: dict[str, TableSlice] = {}
    for ts in schema_slice.tables:
        table_map[ts.qualified_name.upper()] = ts

    for join in plan.joins:
        for tname, colname in [
            (join.left_table, join.left_column),
            (join.right_table, join.right_column),
        ]:
            ts = table_map.get(tname.upper())
            if ts is None:
                continue
            # Check if column already in slice
            existing_cols = {c.name.upper() for c in ts.columns}
            if colname.upper() in existing_cols:
                continue
            # Try to find the column in ChromaDB
            col_qname = f"{ts.qualified_name}.{colname}"
            col_results = retriever.collection.get(
                where={
                    "$and": [
                        {"db_id": db_id},
                        {"object_type": "column"},
                        {"table_qualified_name": ts.qualified_name},
                    ]
                },
                include=["metadatas"],
            )
            for meta in col_results["metadatas"] or []:
                found_col = meta.get("qualified_name", "").rsplit(".", 1)[-1]
                if found_col.upper() == colname.upper():
                    ts.columns.append(
                        ColumnSlice(
                            name=found_col,
                            data_type=meta.get("data_type", "VARCHAR"),
                            token_estimate=meta.get("token_estimate", 5),
                            fused_rank=999,
                            is_join_key=True,
                        )
                    )
                    log.debug("Plan expansion: added join column %s to %s", found_col, ts.qualified_name)
                    break
