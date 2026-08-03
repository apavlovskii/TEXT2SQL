"""Identifier and type validation against a SchemaSlice."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..prompting.plan_schema import QueryPlan
from ..retrieval.schema_slice import SchemaSlice

# Matches qualified identifiers: WORD.WORD or WORD.WORD.WORD or WORD.WORD.WORD.WORD
# Also matches alias.COLUMN patterns like t1.COL
_IDENT_RE = re.compile(
    r"\b([A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2})\b"
)

# SQL keywords that look like identifiers but aren't
_SQL_KEYWORDS = frozenset({
    "AS", "ON", "IN", "IS", "OR", "BY", "IF", "DO", "GO", "NO", "TO", "UP",
    "AND", "NOT", "SET", "ALL", "ANY", "ASC", "AVG", "END", "FOR", "KEY",
    "MAX", "MIN", "NEW", "OLD", "OUT", "ROW", "SUM", "TOP",
    "CASE", "CAST", "DATE", "DESC", "DROP", "EACH", "ELSE", "FROM", "FULL",
    "INTO", "JOIN", "LEFT", "LIKE", "NULL", "ONLY", "OPEN", "OVER", "PLAN",
    "ROWS", "THEN", "TRUE", "TYPE", "WHEN", "WITH", "WORK",
    "COUNT", "CROSS", "FALSE", "FETCH", "FLOAT", "GROUP", "HAVING", "INNER",
    "LIMIT", "ORDER", "OUTER", "RIGHT", "TABLE", "UNION", "USING", "WHERE",
    "WHILE", "ARRAY", "BEGIN", "CHECK", "CLOSE", "GRANT", "INDEX", "ALTER",
    "BETWEEN", "CASCADE", "CURRENT", "DEFAULT", "DISTINCT", "EXISTS", "FOREIGN",
    "PRIMARY", "REPLACE", "SELECT", "UPDATE", "VALUES", "INSERT", "DELETE",
    "CREATE", "NUMBER", "FLOAT", "VARCHAR", "BOOLEAN", "TIMESTAMP",
    "ILIKE", "QUALIFY", "FLATTEN", "LATERAL", "VARIANT", "OBJECT",
    "DATE_TRUNC", "TRY_TO_DATE", "TRY_TO_NUMBER", "COALESCE", "NVL",
    "CURRENT_DATE", "CURRENT_TIMESTAMP",
})


@dataclass
class ValidationResult:
    """Result of identifier validation."""

    valid: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def error_messages(self) -> list[str]:
        return self.errors


def _build_lookup(schema_slice: SchemaSlice) -> tuple[set[str], dict[str, set[str]]]:
    """Build (table_names_set, table->columns_set) from SchemaSlice.

    Returns table names as uppercase, and also their short forms (just TABLE_NAME).
    Column names are uppercase.
    """
    table_names: set[str] = set()
    table_columns: dict[str, set[str]] = {}

    for ts in schema_slice.tables:
        qname_upper = ts.qualified_name.upper()
        table_names.add(qname_upper)
        # Also add short name (last segment)
        short = qname_upper.rsplit(".", 1)[-1]
        table_names.add(short)

        cols = {c.name.upper() for c in ts.columns}
        table_columns[qname_upper] = cols
        table_columns[short] = cols

    return table_names, table_columns


_FLATTEN_ALIAS_RE = re.compile(r"LATERAL\s+FLATTEN\([^)]*\)\s+(\w+)", re.IGNORECASE)


_QUOTED_IDENT_RE = re.compile(r'"([A-Za-z_]\w*)"')


def validate_sql(sql: str, schema_slice: SchemaSlice) -> ValidationResult:
    """Check that SQL references only identifiers present in the SchemaSlice.

    This is a conservative regex-based check (not a full SQL parser).
    """
    result = ValidationResult()
    table_names, table_columns = _build_lookup(schema_slice)

    # All known column names (flat set for loose validation)
    all_columns: set[str] = set()
    for cols in table_columns.values():
        all_columns |= cols

    # alias.VALUE is real Snowflake syntax for reading a LATERAL FLATTEN
    # row's contents (see _compile_flatten_from), but only for aliases that
    # are actually flatten output — column named literally "value" on a real
    # table is a normal, checkable column reference and must not be
    # exempted just because it happens to share that name.
    flatten_aliases = {m.upper() for m in _FLATTEN_ALIAS_RE.findall(sql)}

    # The compiler always double-quotes column names (alias."column") and
    # single-quotes string literals — never the reverse — so it's safe to
    # strip identifier-shaped double quotes before matching: this can't
    # accidentally eat into a string literal's contents. Without this, every
    # column reference the compiler emits is invisible to _IDENT_RE (its
    # \.[A-Za-z_]\w* only matches an unquoted identifier immediately after
    # the dot), so this check would silently pass 100% of compiler output.
    normalized_sql = _QUOTED_IDENT_RE.sub(r"\1", sql)

    # Find dotted identifiers in the SQL
    for match in _IDENT_RE.finditer(normalized_sql):
        ident = match.group(1).upper()
        parts = ident.split(".")

        # Skip if any part is a SQL keyword (e.g. "COUNT.something" won't match)
        if any(p in _SQL_KEYWORDS for p in parts):
            continue

        if len(parts) == 2:
            table_or_alias, column = parts
            # Skip FLATTEN alias references (e.g. h.VALUE, ah.VALUE) — but
            # only for aliases this SQL actually defines via LATERAL FLATTEN.
            if column == "VALUE" and table_or_alias in flatten_aliases:
                continue
            # Skip alias references (t1, t2, ...) — they are compiler-generated
            if re.match(r"^T\d+$", table_or_alias):
                # Validate column exists in any selected table
                if column not in all_columns and column != "*":
                    result.valid = False
                    result.errors.append(
                        f"Column '{column}' (via alias {table_or_alias}) "
                        f"not found in SchemaSlice"
                    )
            elif table_or_alias in table_names:
                # Direct table.column reference
                known_cols = table_columns.get(table_or_alias, set())
                if column not in known_cols and column not in all_columns:
                    result.valid = False
                    result.errors.append(
                        f"Column '{table_or_alias}.{column}' not found in SchemaSlice"
                    )
            # else: could be schema.table — don't flag

        elif len(parts) >= 3:
            # Could be DB.SCHEMA.TABLE or DB.SCHEMA.TABLE.COLUMN
            # Check if last 3 form a known table
            candidate_table = ".".join(parts[:3])
            if candidate_table in table_names:
                if len(parts) == 4:
                    column = parts[3]
                    known_cols = table_columns.get(candidate_table, set())
                    if column not in known_cols:
                        result.valid = False
                        result.errors.append(
                            f"Column '{candidate_table}.{column}' "
                            f"not found in SchemaSlice"
                        )
            else:
                # Check if it's a table reference we don't know
                short_table = parts[-1]
                if short_table not in table_names:
                    result.warnings.append(
                        f"Identifier '{ident}' not recognized in SchemaSlice"
                    )

    return result


# Matches JOIN ... ON patterns: table_or_alias.col = table_or_alias.col
_JOIN_ON_RE = re.compile(
    r"JOIN\s+(\S+)\s+.*?ON\s+(\S+)\.(\S+)\s*=\s*(\S+)\.(\S+)",
    re.IGNORECASE,
)


# Snowflake numeric type prefixes as they appear in ColumnSlice.data_type
# (e.g. "NUMBER(38,0)", "FLOAT", "DECIMAL(10,2)").
_NUMERIC_TYPE_PREFIXES = (
    "NUMBER", "INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL",
)

_DATE_SHAPED_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")

_COMPARISON_OPS = {"=", "!=", "<>", "<", ">", "<=", ">="}


def _is_number(token: str) -> bool:
    try:
        float(token)
        return True
    except ValueError:
        return False


def _build_type_lookup(schema_slice: SchemaSlice) -> dict[str, dict[str, str]]:
    """Return {qualified_table_name.upper(): {COLUMN_UPPER: data_type}}."""
    return {
        ts.qualified_name.upper(): {c.name.upper(): c.data_type for c in ts.columns}
        for ts in schema_slice.tables
    }


def _check_filter_value_type(
    table: str, column: str, col_type: str, op: str, value: str,
) -> str | None:
    """Return an error message if *value* is shape-inconsistent with a
    NUMBER-family column, or None if the check doesn't apply / passes.

    Deliberately narrow: this catches the concrete, high-confidence failure
    mode where a plan compares a NUMBER-typed column (often an epoch or
    YYYYMMDD-encoded date) to a plain date-shaped string literal — Snowflake
    rejects that with an implicit-cast error at execution time rather than
    catching it earlier. It does not attempt to validate DATE/TIMESTAMP or
    VARCHAR columns — those have too many legitimate literal shapes (formats,
    partial/fuzzy matches) to check without a high false-positive rate.
    """
    if op.upper() not in _COMPARISON_OPS:
        return None
    type_upper = col_type.upper()
    if not any(type_upper.startswith(p) for p in _NUMERIC_TYPE_PREFIXES):
        return None
    v = value.strip().strip("'\"")
    if _is_number(v) or v.upper() in ("TRUE", "FALSE", "NULL"):
        return None
    if _DATE_SHAPED_RE.match(v):
        return (
            f"{table}.{column} is type {col_type} but filter compares it to "
            f"date-shaped value '{value}' — if this column is epoch/YYYYMMDD-"
            f"encoded, convert the literal to match (e.g. an integer epoch or "
            f"YYYYMMDD), don't compare it directly to a date string."
        )
    return (
        f"{table}.{column} is type {col_type} but filter compares it to "
        f"non-numeric value '{value}'."
    )


def validate_plan_types(plan: QueryPlan, schema_slice: SchemaSlice) -> list[str]:
    """Return type-mismatch errors for filters in *plan* against real schema
    columns in *schema_slice*.

    Only checks filters whose *table* is a real schema table (not an
    upstream CTE — those are the plan's own inventions, not something the
    schema card can validate) and whose column is actually known on that
    table (column existence is validate_sql's job, not this one). See
    _check_filter_value_type for what's actually flagged and why the scope
    is narrow.
    """
    type_lookup = _build_type_lookup(schema_slice)
    errors: list[str] = []

    def _check_container(container) -> None:
        known_sources = set(container.selected_tables)
        for f in container.filters:
            if f.value is None or f.op.upper() in ("IS NULL", "IS NOT NULL", "IN"):
                continue
            # A filter value that's actually a reference to another column
            # in this block ("cte.column" or "db.schema.table.column",
            # mirroring sql_compiler._resolve_filter_value) compiles to a
            # real column-vs-column comparison, not a literal — nothing
            # here to type-check it against.
            v_stripped = f.value.strip()
            if "." in v_stripped and any(
                v_stripped.startswith(src + ".") and len(v_stripped) > len(src) + 1
                for src in known_sources
            ):
                continue
            table_types = type_lookup.get(f.table.upper())
            if not table_types:
                continue
            col_type = table_types.get(f.column.upper())
            if not col_type:
                continue
            values = (
                re.split(r"\s+AND\s+", f.value, maxsplit=1, flags=re.IGNORECASE)
                if f.op.upper() == "BETWEEN" else [f.value]
            )
            for v in values:
                msg = _check_filter_value_type(f.table, f.column, col_type, f.op, v)
                if msg:
                    errors.append(msg)

    for cte in plan.ctes:
        _check_container(cte)
    _check_container(plan)

    return errors


def validate_joins(sql: str, join_graph_edges: list[dict]) -> list[str]:
    """Return warnings for joins in *sql* not present in the join graph.

    *join_graph_edges* is a list of dicts with keys:
    left_table, left_column, right_table, right_column.
    """
    # Build a set of known join pairs (using short table names, uppercased)
    known_pairs: set[tuple[str, str, str, str]] = set()
    for e in join_graph_edges:
        lt = e.get("left_table", "").upper().rsplit(".", 1)[-1]
        lc = e.get("left_column", "").upper()
        rt = e.get("right_table", "").upper().rsplit(".", 1)[-1]
        rc = e.get("right_column", "").upper()
        known_pairs.add((lt, lc, rt, rc))
        known_pairs.add((rt, rc, lt, lc))  # bidirectional

    warnings: list[str] = []
    for match in _JOIN_ON_RE.finditer(sql):
        left_ref = match.group(2).upper().rsplit(".", 1)[-1]
        left_col = match.group(3).upper()
        right_ref = match.group(4).upper().rsplit(".", 1)[-1]
        right_col = match.group(5).upper()

        pair = (left_ref, left_col, right_ref, right_col)
        rev_pair = (right_ref, right_col, left_ref, left_col)
        if pair not in known_pairs and rev_pair not in known_pairs:
            warnings.append(
                f"Join {left_ref}.{left_col} = {right_ref}.{right_col} "
                f"not found in join graph"
            )

    return warnings
