"""Identifier validation against a SchemaSlice."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

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

    # Find dotted identifiers in the SQL
    for match in _IDENT_RE.finditer(sql):
        ident = match.group(1).upper()
        parts = ident.split(".")

        # Skip if any part is a SQL keyword (e.g. "COUNT.something" won't match)
        if any(p in _SQL_KEYWORDS for p in parts):
            continue

        if len(parts) == 2:
            table_or_alias, column = parts
            # Skip FLATTEN alias references (e.g. h.VALUE, ah.VALUE)
            if column == "VALUE":
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
