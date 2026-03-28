"""Token-budget enforcement for SchemaSlice construction."""

from __future__ import annotations

import logging
import re

from .schema_slice import ColumnSlice, SchemaSlice, TableSlice

log = logging.getLogger(__name__)

_JOIN_RE = re.compile(r"(^ID$|_ID$|_KEY$)", re.IGNORECASE)
_TIME_RE = re.compile(r"(DATE|TIME|TIMESTAMP)", re.IGNORECASE)


def _is_protected(col: ColumnSlice) -> bool:
    """Join-ish or time-ish columns are protected from trimming."""
    return col.is_join_key or col.is_time_column


def classify_column(col: ColumnSlice) -> ColumnSlice:
    """Set is_join_key / is_time_column flags based on name and type."""
    if _JOIN_RE.search(col.name):
        col.is_join_key = True
    if _TIME_RE.search(col.data_type) or _TIME_RE.search(col.name):
        col.is_time_column = True
    return col


def trim_to_budget(
    schema_slice: SchemaSlice,
    max_schema_tokens: int,
    max_tables: int | None = None,
    max_columns_per_table: int | None = None,
) -> SchemaSlice:
    """Trim *schema_slice* in-place and return it.

    Trimming order:
    1. Drop lowest-ranked (highest fused_rank) **unprotected** columns first.
    2. If still over budget, drop lowest-ranked tables.
    Protected columns (join keys, time columns) are kept as long as possible.
    """
    # Classify all columns
    for ts in schema_slice.tables:
        for col in ts.columns:
            classify_column(col)

    # Cap tables
    if max_tables and len(schema_slice.tables) > max_tables:
        schema_slice.tables.sort(key=lambda t: t.fused_rank)
        schema_slice.tables = schema_slice.tables[:max_tables]

    # Cap columns per table
    if max_columns_per_table:
        for ts in schema_slice.tables:
            if len(ts.columns) > max_columns_per_table:
                # Keep protected first, then by rank
                protected = [c for c in ts.columns if _is_protected(c)]
                rest = [c for c in ts.columns if not _is_protected(c)]
                rest.sort(key=lambda c: c.fused_rank)
                budget_left = max_columns_per_table - len(protected)
                if budget_left > 0:
                    ts.columns = protected + rest[:budget_left]
                else:
                    # Even protected exceed cap; keep top-ranked protected
                    protected.sort(key=lambda c: c.fused_rank)
                    ts.columns = protected[:max_columns_per_table]

    # Token-budget trimming: iteratively drop worst unprotected columns
    while schema_slice.token_estimate > max_schema_tokens:
        # Collect all (table_idx, col_idx, col_fused_rank, table_fused_rank, protected)
        candidates: list[tuple[int, int, int, int, bool]] = []
        for ti, ts in enumerate(schema_slice.tables):
            for ci, col in enumerate(ts.columns):
                candidates.append((ti, ci, col.fused_rank, ts.fused_rank, _is_protected(col)))

        if not candidates:
            break

        # Prefer dropping unprotected with highest fused_rank;
        # break ties by preferring columns from worse-ranked tables
        unprotected = [c for c in candidates if not c[4]]
        pool = unprotected if unprotected else candidates  # last resort: protected
        # Sort: worst column rank first, then worst table rank first
        pool.sort(key=lambda c: (c[2], c[3]), reverse=True)
        ti, ci, _, _, _ = pool[0]
        dropped = schema_slice.tables[ti].columns.pop(ci)
        log.debug(
            "Budget trim: dropped column %s (rank %d, %d tokens)",
            dropped.name,
            dropped.fused_rank,
            dropped.token_estimate,
        )

        # If a table has no columns left, remove it
        if not schema_slice.tables[ti].columns:
            removed = schema_slice.tables.pop(ti)
            log.debug("Budget trim: removed empty table %s", removed.qualified_name)

        # Safety: if nothing to drop, bail
        if schema_slice.token_estimate <= 0:
            break

    # Still over budget? Drop lowest-ranked whole tables
    while schema_slice.token_estimate > max_schema_tokens and schema_slice.tables:
        schema_slice.tables.sort(key=lambda t: t.fused_rank, reverse=True)
        removed = schema_slice.tables.pop(0)
        log.debug("Budget trim: dropped table %s", removed.qualified_name)

    return schema_slice
