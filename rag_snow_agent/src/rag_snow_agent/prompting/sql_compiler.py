"""Deterministic QueryPlan → Snowflake SQL compiler."""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta

from ..retrieval.schema_slice import SchemaSlice
from .plan_schema import PlanCTE, PlanFlatten, PlanGeoJoin, QueryPlan

log = logging.getLogger(__name__)

# ── Date-shard rewriting ────────────────────────────────────────────────────

_DATE_SHARD_RE = re.compile(r"(\S+?)(\d{8})$")

# Old format: "Partitioned: 366 daily tables GA_SESSIONS_YYYYMMDD (20160801-20170801)"
_PARTITION_COMMENT_RE = re.compile(
    r"Partitioned:\s*(\d+)\s*daily\s*tables\s*\S+\s*\((\d{8})\s*.\s*(\d{8})"
)

# Current format: "Daily partitioned as GA_SESSIONS_YYYYMMDD (366 tables)"
_PARTITION_COMMENT_V2_RE = re.compile(
    r"Daily\s+partitioned\s+as\s+(\S+_)YYYYMMDD\s*\((\d+)\s*tables\)",
    re.IGNORECASE,
)

# Match >=/<= range: "date" >= '20170701' AND (optional alias.)"date" <= '20170731'
_DATE_GE_LE_RE = re.compile(
    r'"date"\s*>=\s*\'(\d{8})\'\s*AND\s*(?:\S+\.)?"date"\s*<=?\s*\'(\d{8})\'',
    re.IGNORECASE,
)

# Match BETWEEN: "date" BETWEEN '20170101' AND '20170331'
_DATE_BETWEEN_RE = re.compile(
    r'"date"\s*BETWEEN\s*\'(\d{8})\'\s*AND\s*\'(\d{8})\'',
    re.IGNORECASE,
)

# Match single date-like comparisons: "date" LIKE '201707%'
_DATE_LIKE_RE = re.compile(
    r'"date"\s+(?:I?LIKE)\s+\'(\d{4,6})%\'',
    re.IGNORECASE,
)


def _date_range_from_like(prefix: str) -> tuple[str, str] | None:
    """Convert a LIKE prefix '201707' into ('20170701', '20170731')."""
    if len(prefix) == 6:
        year, month = int(prefix[:4]), int(prefix[4:6])
        first = date(year, month, 1)
        if month == 12:
            last = date(year, 12, 31)
        else:
            last = date(year, month + 1, 1) - timedelta(days=1)
        return first.strftime("%Y%m%d"), last.strftime("%Y%m%d")
    elif len(prefix) == 4:
        return f"{prefix}0101", f"{prefix}1231"
    return None


def _generate_daily_tables(
    base: str,
    start: str,
    end: str,
    partition_start: str | None = None,
    partition_end: str | None = None,
) -> list[str]:
    """Generate list of daily table names for date range, optionally clamped."""
    start_d = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    end_d = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    if partition_start:
        ps = date(int(partition_start[:4]), int(partition_start[4:6]), int(partition_start[6:8]))
        start_d = max(start_d, ps)
    if partition_end:
        pe = date(int(partition_end[:4]), int(partition_end[4:6]), int(partition_end[6:8]))
        end_d = min(end_d, pe)
    tables = []
    d = start_d
    while d <= end_d:
        tables.append(f"{base}{d.strftime('%Y%m%d')}")
        d += timedelta(days=1)
    return tables


def rewrite_date_sharded_tables(sql: str, schema_slice: SchemaSlice | None) -> str:
    """Post-compilation rewrite: expand date-partitioned table references.

    Detects partitioned representative tables in the schema slice (via either
    of two comment formats) and any date-shard reference in the SQL with a
    matching base prefix. When the WHERE clause supplies a date range, replaces
    the reference with a CTE UNION ALL of all daily tables in that range.
    """
    if schema_slice is None:
        return sql

    # base_prefix -> (representative_qname_prefix, partition_start, partition_end)
    # partition_start/end may be None when comment doesn't supply them.
    partition_info: dict[str, tuple[str, str | None, str | None]] = {}
    for ts in schema_slice.tables:
        if not ts.comment:
            continue
        qname = ts.qualified_name
        sm = _DATE_SHARD_RE.match(qname)
        if not sm:
            continue
        # base_qname includes schema/db prefix, e.g. "GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS_"
        base_qname = sm.group(1)

        m = _PARTITION_COMMENT_RE.search(ts.comment)
        if m:
            partition_info[base_qname] = (base_qname, m.group(2), m.group(3))
            continue
        m2 = _PARTITION_COMMENT_V2_RE.search(ts.comment)
        if m2:
            partition_info[base_qname] = (base_qname, None, None)

    if not partition_info:
        return sql

    # Try to extract date range from the SQL
    date_range: tuple[str, str] | None = None
    rm = _DATE_GE_LE_RE.search(sql)
    if rm:
        date_range = (rm.group(1), rm.group(2))
    if not date_range:
        bm = _DATE_BETWEEN_RE.search(sql)
        if bm:
            date_range = (bm.group(1), bm.group(2))
    if not date_range:
        lm = _DATE_LIKE_RE.search(sql)
        if lm:
            date_range = _date_range_from_like(lm.group(1))

    if not date_range:
        log.debug("No date range found in SQL for date-shard rewriting")
        return sql

    start_date, end_date = date_range

    # For each partitioned base, find ANY shard reference in the SQL with that prefix
    # and rewrite it. Handles cases where the LLM picked a different date than the rep.
    for base_qname, (_rep, p_start, p_end) in partition_info.items():
        # Find all shard references in SQL: base_qname + 8 digits
        shard_re = re.compile(re.escape(base_qname) + r"(\d{8})")
        shards_in_sql = set(shard_re.findall(sql))
        if not shards_in_sql:
            continue

        daily_tables = _generate_daily_tables(base_qname, start_date, end_date, p_start, p_end)
        if len(daily_tables) <= 1:
            log.debug("Date range produces %d tables — skipping rewrite for %s", len(daily_tables), base_qname)
            continue

        # Skip rewrite when range is too broad (LIKE '2017%' → 365 days etc.):
        # Snowflake's planner times out on UNION ALL of >40 daily tables when
        # combined with LATERAL FLATTEN. Better to leave the SQL as-is and let
        # it fail naturally than waste minutes on EXPLAIN timeouts.
        if len(daily_tables) > 40:
            log.warning(
                "Date range produces %d tables for %s — too broad, skipping rewrite",
                len(daily_tables), base_qname,
            )
            continue

        # Build the UNION ALL CTE body with concrete table names (must NOT be
        # subject to the shard substitution below).
        cte_name = "_date_shard_union"
        union_parts = [f"SELECT * FROM {t}" for t in daily_tables]
        union_sql = " UNION ALL\n    ".join(union_parts)
        cte_block = f"{cte_name} AS (\n    {union_sql}\n)"

        # Substitute shard references in the original SQL body FIRST, then prepend
        # the CTE — this ensures the CTE body's concrete table names survive.
        body = shard_re.sub(cte_name, sql)
        if body.strip().upper().startswith("WITH "):
            sql = body.replace("WITH ", f"WITH {cte_block},\n", 1)
        else:
            sql = f"WITH {cte_block}\n{body}"

        log.info(
            "Date-shard rewrite: expanded %s* (saw %d distinct shard refs) into UNION ALL of %d tables (%s — %s)",
            base_qname, len(shards_in_sql), len(daily_tables), daily_tables[0], daily_tables[-1],
        )

    return sql


def _alias(idx: int) -> str:
    """Stable alias: t1, t2, ..."""
    return f"t{idx + 1}"


def _build_column_case_map(
    schema_slice: SchemaSlice | None,
) -> dict[str, dict[str, str]]:
    """Return {table_qname: {UPPER_COL: original_col}} from the SchemaSlice.

    Used to restore exact column casing when generating SQL.
    """
    if schema_slice is None:
        return {}
    case_map: dict[str, dict[str, str]] = {}
    for ts in schema_slice.tables:
        col_map: dict[str, str] = {}
        for col in ts.columns:
            original = col.original_name if col.original_name else col.name
            col_map[col.name.upper()] = original
        case_map[ts.qualified_name] = col_map
    return case_map


def _resolve_column(
    table: str,
    column: str,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None = None,
) -> str:
    """Return alias."COLUMN" reference with double-quoted column name.

    If *case_map* is provided, uses the original casing from the SchemaSlice.
    """
    alias = alias_map.get(table, table)
    # Resolve original casing if available
    col_name = column
    if case_map and table in case_map:
        col_name = case_map[table].get(column.upper(), column)
    # Double-quote column to preserve case (Snowflake treats unquoted as uppercase)
    return f'{alias}."{col_name}"'


def _compile_flatten_from(
    flatten_ops: list[PlanFlatten],
    alias_map: dict[str, str],
) -> list[str]:
    """Generate LATERAL FLATTEN clause fragments for FROM."""
    parts: list[str] = []
    for f in flatten_ops:
        table_alias = alias_map.get(f.table, f.table)
        parts.append(
            f', LATERAL FLATTEN(input => {table_alias}."{f.variant_column}") {f.alias}'
        )
    return parts


def _resolve_column_or_flatten(
    table: str,
    column: str,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None = None,
    flatten_aliases: set[str] | None = None,
) -> str:
    """Resolve a column reference, handling FLATTEN alias.value:"field" syntax.

    If *table* matches a FLATTEN alias (e.g. "h") and *column* contains a dot
    (e.g. "page.pagePath"), emit ``h.value:"page":"pagePath"`` syntax.
    If *column* has no dot, emit ``h.value:"column"`` syntax.
    Otherwise fall through to normal resolution.
    """
    if flatten_aliases and table in flatten_aliases:
        # column might be "page.pagePath" or just "productRevenue"
        field_parts = column.split(".")
        path = "".join(f':"{p}"' for p in field_parts)
        return f"{table}.value{path}"
    return _resolve_column(table, column, alias_map, case_map)


def _compile_geo_joins(
    geo_joins: list[PlanGeoJoin],
    alias_map: dict[str, str],
) -> list[str]:
    """Generate spatial JOIN clauses with geospatial ON predicates."""
    parts: list[str] = []
    for gj in geo_joins:
        jtype = gj.join_type.upper()
        right_alias = alias_map.get(gj.right_table, gj.right_table)
        # Substitute table aliases into the ON expression
        on_expr = _substitute_aliases(gj.on_expression, alias_map)
        if jtype == "CROSS":
            parts.append(f"CROSS JOIN {gj.right_table} AS {right_alias}")
        else:
            parts.append(
                f"{jtype} JOIN {gj.right_table} AS {right_alias} "
                f"ON {on_expr}"
            )
    return parts


def _substitute_aliases(expression: str, alias_map: dict[str, str]) -> str:
    """Best-effort substitution of full table names with aliases in a raw expression.

    Replaces occurrences of qualified table names (e.g. ``DB.SCHEMA.TABLE``)
    with the corresponding compiler alias (e.g. ``t1``).  This allows the LLM
    to write geo expressions using full table names and have them shortened.
    """
    result = expression
    # Sort by length descending so longer names are replaced first
    for full_name, alias in sorted(alias_map.items(), key=lambda x: -len(x[0])):
        result = result.replace(full_name, alias)
    return result


def _compile_single_block(
    selected_tables: list[str],
    joins: list,
    geo_joins: list[PlanGeoJoin],
    flatten_ops: list[PlanFlatten],
    filters: list,
    geo_filters: list,
    group_by: list[str],
    aggregations: list,
    order_by: list,
    limit: int | None,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None,
) -> str:
    """Compile a single SELECT block (used for main query and each CTE)."""
    flatten_aliases: set[str] = {f.alias for f in flatten_ops}

    # ── FROM / JOIN clause ──────────────────────────────────────────────
    if not selected_tables:
        return "SELECT 1"
    primary = selected_tables[0]
    from_parts = [f"{primary} AS {alias_map.get(primary, primary)}"]

    for j in joins:
        jtype = j.join_type.upper()
        right_alias = alias_map.get(j.right_table, j.right_table)
        left_ref = _resolve_column(j.left_table, j.left_column, alias_map, case_map)
        right_ref = _resolve_column(j.right_table, j.right_column, alias_map, case_map)
        from_parts.append(
            f"{jtype} JOIN {j.right_table} AS {right_alias} "
            f"ON {left_ref} = {right_ref}"
        )

    # Geospatial JOIN clauses
    from_parts.extend(_compile_geo_joins(geo_joins, alias_map))

    # LATERAL FLATTEN clauses
    from_parts.extend(_compile_flatten_from(flatten_ops, alias_map))

    from_clause = "\n".join(from_parts)

    # ── SELECT clause ───────────────────────────────────────────────────
    select_parts: list[str] = []

    # Group-by columns first
    for gb in group_by:
        if "." in gb:
            # Check if the first segment is a flatten alias (e.g. "h.page.pagePath")
            first_seg = gb.split(".", 1)[0]
            if first_seg in flatten_aliases:
                table_part = first_seg
                col_part = gb.split(".", 1)[1]  # "page.pagePath"
            else:
                table_part, col_part = gb.rsplit(".", 1)
            resolved = _resolve_column_or_flatten(
                table_part, col_part, alias_map, case_map, flatten_aliases
            )
            select_parts.append(resolved)
        else:
            select_parts.append(gb)

    # Aggregations
    for agg in aggregations:
        col_ref = _resolve_column_or_flatten(
            agg.table, agg.column, alias_map, case_map, flatten_aliases
        )
        if agg.func.upper() == "COUNT_DISTINCT":
            expr = f"COUNT(DISTINCT {col_ref})"
        elif agg.func.upper() == "COUNT" and agg.column == "*":
            expr = "COUNT(*)"
        else:
            expr = f"{agg.func.upper()}({col_ref})"
        select_parts.append(f"{expr} AS {agg.alias}")

    if not select_parts:
        # Fallback: select all columns from first table
        first_alias = alias_map.get(primary, primary)
        select_parts.append(f"{first_alias}.*")

    select_clause = ",\n  ".join(select_parts)

    # ── WHERE clause ────────────────────────────────────────────────────
    where_parts: list[str] = []
    for f in filters:
        col_ref = _resolve_column_or_flatten(
            f.table, f.column, alias_map, case_map, flatten_aliases
        )
        op = f.op.upper()
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{col_ref} {op}")
        elif op == "IN" and f.value is not None:
            where_parts.append(f"{col_ref} IN ({f.value})")
        elif op == "BETWEEN" and f.value is not None:
            where_parts.append(f"{col_ref} BETWEEN {f.value}")
        elif f.value is not None:
            val = f.value
            where_parts.append(f"{col_ref} {op} {val}")
        else:
            where_parts.append(f"{col_ref} {op} NULL")

    # Geospatial WHERE predicates (emitted verbatim with alias substitution)
    for gf in geo_filters:
        where_parts.append(_substitute_aliases(gf.expression, alias_map))

    where_clause = " AND ".join(where_parts) if where_parts else ""

    # ── GROUP BY clause ─────────────────────────────────────────────────
    group_parts: list[str] = []
    for gb in group_by:
        if "." in gb:
            first_seg = gb.split(".", 1)[0]
            if first_seg in flatten_aliases:
                table_part = first_seg
                col_part = gb.split(".", 1)[1]
            else:
                table_part, col_part = gb.rsplit(".", 1)
            resolved = _resolve_column_or_flatten(
                table_part, col_part, alias_map, case_map, flatten_aliases
            )
            group_parts.append(resolved)
        else:
            group_parts.append(gb)

    group_clause = ", ".join(group_parts) if group_parts else ""

    # ── ORDER BY clause ─────────────────────────────────────────────────
    order_parts: list[str] = []
    for ob in order_by:
        direction = ob.direction.upper()
        order_parts.append(f"{ob.expr} {direction}")

    order_clause = ", ".join(order_parts) if order_parts else ""

    # ── Assemble ────────────────────────────────────────────────────────
    sql_lines = [f"SELECT\n  {select_clause}", f"FROM {from_clause}"]
    if where_clause:
        sql_lines.append(f"WHERE {where_clause}")
    if group_clause:
        sql_lines.append(f"GROUP BY {group_clause}")
    if order_clause:
        sql_lines.append(f"ORDER BY {order_clause}")
    if limit is not None:
        sql_lines.append(f"LIMIT {limit}")

    return "\n".join(sql_lines)


def compile_plan(plan: QueryPlan, schema_slice: SchemaSlice | None = None) -> str:
    """Compile a QueryPlan into a Snowflake SQL string.

    Supports LATERAL FLATTEN for VARIANT ARRAYs and multi-step CTEs.
    After compilation, applies date-shard rewriting for partitioned tables.
    """
    if not plan.selected_tables:
        return "SELECT 1"

    # Build alias map: qualified_name -> t1, t2, ...
    alias_map: dict[str, str] = {}
    for i, tname in enumerate(plan.selected_tables):
        alias_map[tname] = _alias(i)

    # Build column case map from SchemaSlice (for original casing)
    case_map = _build_column_case_map(schema_slice)

    def _finalize(sql: str) -> str:
        return rewrite_date_sharded_tables(sql, schema_slice)

    # ── CTE-based compilation ───────────────────────────────────────────
    if plan.ctes:
        cte_parts: list[str] = []
        for cte in plan.ctes:
            # Build local alias map for tables referenced inside this CTE.
            # CTE source can be an upstream CTE name (no alias needed for those)
            # or a real table from selected_tables (use existing alias).
            cte_alias_map = dict(alias_map)
            for tbl in cte.selected_tables:
                if tbl not in cte_alias_map:
                    # Upstream CTE name — reference directly
                    cte_alias_map[tbl] = tbl

            # Also add geo_join right tables to the alias map
            for gj in cte.geo_joins:
                if gj.right_table not in cte_alias_map:
                    idx = len(cte_alias_map)
                    cte_alias_map[gj.right_table] = _alias(idx)

            block = _compile_single_block(
                selected_tables=cte.selected_tables,
                joins=cte.joins,
                geo_joins=cte.geo_joins,
                flatten_ops=cte.flatten_ops,
                filters=cte.filters,
                geo_filters=cte.geo_filters,
                group_by=cte.group_by,
                aggregations=cte.aggregations,
                order_by=cte.order_by,
                limit=cte.limit,
                alias_map=cte_alias_map,
                case_map=case_map,
            )
            cte_parts.append(f"{cte.name} AS (\n{block}\n)")

        # Final SELECT: use the last CTE as source by default
        last_cte = plan.ctes[-1].name

        # If there are top-level aggregations/group_by/filters, build a final
        # SELECT from the last CTE. Otherwise just SELECT * FROM last_cte.
        if plan.aggregations or plan.group_by or plan.filters or plan.geo_filters:
            final_alias_map = {last_cte: last_cte}
            final_block = _compile_single_block(
                selected_tables=[last_cte],
                joins=[],
                geo_joins=[],
                flatten_ops=[],
                filters=plan.filters,
                geo_filters=plan.geo_filters,
                group_by=plan.group_by,
                aggregations=plan.aggregations,
                order_by=plan.order_by,
                limit=plan.limit,
                alias_map=final_alias_map,
                case_map=None,
            )
        else:
            final_parts = [f"SELECT *\nFROM {last_cte}"]
            if plan.order_by:
                ob = ", ".join(f"{o.expr} {o.direction.upper()}" for o in plan.order_by)
                final_parts.append(f"ORDER BY {ob}")
            if plan.limit is not None:
                final_parts.append(f"LIMIT {plan.limit}")
            final_block = "\n".join(final_parts)

        return _finalize("WITH " + ",\n".join(cte_parts) + "\n" + final_block)

    # Add geo_join right tables to alias_map
    for gj in plan.geo_joins:
        if gj.right_table not in alias_map:
            alias_map[gj.right_table] = _alias(len(alias_map))

    # ── Single-block compilation (no CTEs) ──────────────────────────────
    return _finalize(_compile_single_block(
        selected_tables=plan.selected_tables,
        joins=plan.joins,
        geo_joins=plan.geo_joins,
        flatten_ops=plan.flatten_ops,
        filters=plan.filters,
        geo_filters=plan.geo_filters,
        group_by=plan.group_by,
        aggregations=plan.aggregations,
        order_by=plan.order_by,
        limit=plan.limit,
        alias_map=alias_map,
        case_map=case_map,
    ))


def _try_resolve(table_part: str, col_part: str, alias_map: dict[str, str]) -> str:
    """Resolve table_part.col_part using alias_map, trying exact then suffix match."""
    if table_part in alias_map:
        return f'{alias_map[table_part]}."{col_part}"'
    # Try suffix match (e.g. "ORDERS" matching "DB.SCHEMA.ORDERS")
    for full_name, alias in alias_map.items():
        if full_name.endswith(f".{table_part}") or full_name == table_part:
            return f'{alias}."{col_part}"'
    return f'{table_part}."{col_part}"'
