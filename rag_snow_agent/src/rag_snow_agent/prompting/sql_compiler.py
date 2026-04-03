"""Deterministic QueryPlan → Snowflake SQL compiler."""

from __future__ import annotations

from ..retrieval.schema_slice import SchemaSlice
from .plan_schema import PlanCTE, PlanFlatten, QueryPlan


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


def _compile_single_block(
    selected_tables: list[str],
    joins: list,
    flatten_ops: list[PlanFlatten],
    filters: list,
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
    """
    if not plan.selected_tables:
        return "SELECT 1"

    # Build alias map: qualified_name -> t1, t2, ...
    alias_map: dict[str, str] = {}
    for i, tname in enumerate(plan.selected_tables):
        alias_map[tname] = _alias(i)

    # Build column case map from SchemaSlice (for original casing)
    case_map = _build_column_case_map(schema_slice)

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

            block = _compile_single_block(
                selected_tables=cte.selected_tables,
                joins=cte.joins,
                flatten_ops=cte.flatten_ops,
                filters=cte.filters,
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
        if plan.aggregations or plan.group_by or plan.filters:
            final_alias_map = {last_cte: last_cte}
            final_block = _compile_single_block(
                selected_tables=[last_cte],
                joins=[],
                flatten_ops=[],
                filters=plan.filters,
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

        return "WITH " + ",\n".join(cte_parts) + "\n" + final_block

    # ── Single-block compilation (no CTEs) ──────────────────────────────
    return _compile_single_block(
        selected_tables=plan.selected_tables,
        joins=plan.joins,
        flatten_ops=plan.flatten_ops,
        filters=plan.filters,
        group_by=plan.group_by,
        aggregations=plan.aggregations,
        order_by=plan.order_by,
        limit=plan.limit,
        alias_map=alias_map,
        case_map=case_map,
    )


def _try_resolve(table_part: str, col_part: str, alias_map: dict[str, str]) -> str:
    """Resolve table_part.col_part using alias_map, trying exact then suffix match."""
    if table_part in alias_map:
        return f'{alias_map[table_part]}."{col_part}"'
    # Try suffix match (e.g. "ORDERS" matching "DB.SCHEMA.ORDERS")
    for full_name, alias in alias_map.items():
        if full_name.endswith(f".{table_part}") or full_name == table_part:
            return f'{alias}."{col_part}"'
    return f'{table_part}."{col_part}"'
