"""Deterministic QueryPlan → Snowflake SQL compiler."""

from __future__ import annotations

from ..retrieval.schema_slice import SchemaSlice
from .plan_schema import QueryPlan


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


def compile_plan(plan: QueryPlan, schema_slice: SchemaSlice | None = None) -> str:
    """Compile a QueryPlan into a Snowflake SQL string.

    Uses CTE style with stable aliases t1, t2, ...
    """
    if not plan.selected_tables:
        return "SELECT 1"

    # Build alias map: qualified_name -> t1, t2, ...
    alias_map: dict[str, str] = {}
    for i, tname in enumerate(plan.selected_tables):
        alias_map[tname] = _alias(i)

    # Build column case map from SchemaSlice (for original casing)
    case_map = _build_column_case_map(schema_slice)

    # ── FROM / JOIN clause ──────────────────────────────────────────────
    primary = plan.selected_tables[0]
    from_parts = [f"{primary} AS {alias_map[primary]}"]

    for j in plan.joins:
        jtype = j.join_type.upper()
        right_alias = alias_map.get(j.right_table, j.right_table)
        left_ref = _resolve_column(j.left_table, j.left_column, alias_map, case_map)
        right_ref = _resolve_column(j.right_table, j.right_column, alias_map, case_map)
        from_parts.append(
            f"{jtype} JOIN {j.right_table} AS {right_alias} "
            f"ON {left_ref} = {right_ref}"
        )

    from_clause = "\n".join(from_parts)

    # ── SELECT clause ───────────────────────────────────────────────────
    select_parts: list[str] = []

    # Group-by columns first
    for gb in plan.group_by:
        if "." in gb:
            parts = gb.rsplit(".", 1)
            table_part, col_part = parts[0], parts[1]
            # table_part could be "TABLE" (short) or fully qualified
            resolved = _try_resolve(table_part, col_part, alias_map)
            select_parts.append(resolved)
        else:
            select_parts.append(gb)

    # Aggregations
    for agg in plan.aggregations:
        col_ref = _resolve_column(agg.table, agg.column, alias_map, case_map)
        if agg.func.upper() == "COUNT_DISTINCT":
            expr = f"COUNT(DISTINCT {col_ref})"
        elif agg.func.upper() == "COUNT" and agg.column == "*":
            expr = "COUNT(*)"
        else:
            expr = f"{agg.func.upper()}({col_ref})"
        select_parts.append(f"{expr} AS {agg.alias}")

    if not select_parts:
        # Fallback: select all columns from first table
        select_parts.append(f"{alias_map[primary]}.*")

    select_clause = ",\n  ".join(select_parts)

    # ── WHERE clause ────────────────────────────────────────────────────
    where_parts: list[str] = []
    for f in plan.filters:
        col_ref = _resolve_column(f.table, f.column, alias_map, case_map)
        op = f.op.upper()
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{col_ref} {op}")
        elif op == "IN" and f.value is not None:
            where_parts.append(f"{col_ref} IN ({f.value})")
        elif op == "BETWEEN" and f.value is not None:
            where_parts.append(f"{col_ref} BETWEEN {f.value}")
        elif f.value is not None:
            # Quote string values; leave numeric/function values unquoted
            val = f.value
            where_parts.append(f"{col_ref} {op} {val}")
        else:
            where_parts.append(f"{col_ref} {op} NULL")

    where_clause = " AND ".join(where_parts) if where_parts else ""

    # ── GROUP BY clause ─────────────────────────────────────────────────
    group_parts: list[str] = []
    for gb in plan.group_by:
        if "." in gb:
            parts = gb.rsplit(".", 1)
            resolved = _try_resolve(parts[0], parts[1], alias_map)
            group_parts.append(resolved)
        else:
            # Bare name (no dot) — likely an alias, don't double-quote
            group_parts.append(gb)

    group_clause = ", ".join(group_parts) if group_parts else ""

    # ── ORDER BY clause ─────────────────────────────────────────────────
    # ORDER BY expressions are aliases or positional — do NOT double-quote
    order_parts: list[str] = []
    for ob in plan.order_by:
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
    if plan.limit is not None:
        sql_lines.append(f"LIMIT {plan.limit}")

    return "\n".join(sql_lines)


def _try_resolve(table_part: str, col_part: str, alias_map: dict[str, str]) -> str:
    """Resolve table_part.col_part using alias_map, trying exact then suffix match."""
    if table_part in alias_map:
        return f'{alias_map[table_part]}."{col_part}"'
    # Try suffix match (e.g. "ORDERS" matching "DB.SCHEMA.ORDERS")
    for full_name, alias in alias_map.items():
        if full_name.endswith(f".{table_part}") or full_name == table_part:
            return f'{alias}."{col_part}"'
    return f'{table_part}."{col_part}"'
