"""Pre-execution column validation against the ChromaDB index."""

from __future__ import annotations

import logging
import re

from ..chroma.chroma_store import ChromaStore

log = logging.getLogger(__name__)

# Matches dotted identifiers: alias.COL, TABLE.COL, DB.SCHEMA.TABLE.COL
_IDENT_RE = re.compile(
    r"\b([A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*){0,2})\b"
)

# Matches bare column names in SELECT / WHERE / GROUP BY / ORDER BY contexts.
# Conservative: only after SELECT, comma, WHERE, AND, OR, ON, BY, HAVING.
_BARE_COL_RE = re.compile(
    r"(?:SELECT|,|WHERE|AND|OR|ON|BY|HAVING|SET)\s+([A-Za-z_]\w*)\b",
    re.IGNORECASE,
)

# SQL keywords / functions to skip
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
    "CREATE", "NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP",
    "ILIKE", "QUALIFY", "FLATTEN", "LATERAL", "VARIANT", "OBJECT",
    "DATE_TRUNC", "TRY_TO_DATE", "TRY_TO_NUMBER", "COALESCE", "NVL",
    "CURRENT_DATE", "CURRENT_TIMESTAMP", "FIRST_VALUE", "LAST_VALUE",
    "ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "NTILE",
    "LISTAGG", "ARRAY_AGG", "OBJECT_CONSTRUCT", "PARSE_JSON",
    "TO_DATE", "TO_CHAR", "TO_NUMBER", "TO_TIMESTAMP", "TO_VARIANT",
    "DATEDIFF", "DATEADD", "EXTRACT", "YEAR", "MONTH", "DAY", "HOUR",
    "MINUTE", "SECOND", "TRIM", "UPPER", "LOWER", "LENGTH", "SUBSTR",
    "CONCAT", "REPLACE", "SPLIT", "IFF", "DECODE", "GREATEST", "LEAST",
    "ABS", "ROUND", "CEIL", "FLOOR", "MOD", "POWER", "SQRT", "LN", "LOG",
    "APPROX_COUNT_DISTINCT", "MEDIAN", "PERCENTILE_CONT", "STDDEV",
    "VARIANCE", "CORR", "REGR_SLOPE", "RATIO_TO_REPORT",
    "OBJECT_KEYS", "ARRAY_SIZE", "GET_PATH", "TRY_PARSE_JSON",
})


def _load_known_identifiers(
    db_id: str,
    chroma_store: ChromaStore,
) -> tuple[set[str], set[str]]:
    """Batch-load all column and table names for *db_id* from ChromaDB.

    Returns ``(known_columns, known_tables)`` — both upper-cased.
    """
    col = chroma_store.schema_collection()

    # Load columns
    col_results = col.get(
        where={"$and": [{"db_id": db_id}, {"object_type": "column"}]},
        include=["metadatas"],
    )
    known_columns: set[str] = set()
    for meta in col_results["metadatas"] or []:
        qname = meta.get("qualified_name", "")
        # qualified_name is DB.SCHEMA.TABLE.COLUMN — extract the column part
        parts = qname.split(".")
        if len(parts) >= 4:
            col_name = ".".join(parts[3:])  # handles names with colons
            known_columns.add(col_name.upper())
        # Also store the full qualified name
        known_columns.add(qname.upper())

    # Load table names (so we don't flag them as invalid columns)
    tbl_results = col.get(
        where={"$and": [{"db_id": db_id}, {"object_type": "table"}]},
        include=["metadatas"],
    )
    known_tables: set[str] = set()
    for meta in tbl_results["metadatas"] or []:
        qname = meta.get("qualified_name", "")
        known_tables.add(qname.upper())
        # Also add the short table name (last segment)
        parts = qname.split(".")
        if parts:
            known_tables.add(parts[-1].upper())

    return known_columns, known_tables


def _find_similar(name: str, known: set[str], max_suggestions: int = 3) -> list[str]:
    """Find similar column names using prefix/suffix overlap."""
    name_upper = name.upper()
    suggestions: list[tuple[float, str]] = []

    for k in known:
        # Skip very long qualified names for suggestions
        if k.count(".") >= 3:
            continue
        # Common prefix length
        prefix_len = 0
        for a, b in zip(name_upper, k):
            if a == b:
                prefix_len += 1
            else:
                break
        # Common suffix length
        suffix_len = 0
        for a, b in zip(reversed(name_upper), reversed(k)):
            if a == b:
                suffix_len += 1
            else:
                break
        max_len = max(len(name_upper), len(k))
        if max_len == 0:
            continue
        score = (prefix_len + suffix_len) / max_len
        if score > 0.4:
            suggestions.append((score, k))

    suggestions.sort(key=lambda x: x[0], reverse=True)
    return [s[1] for s in suggestions[:max_suggestions]]


def _extract_column_refs(sql: str) -> set[str]:
    """Extract column-like references from SQL."""
    refs: set[str] = set()

    # Dotted references (alias.COL, TABLE.COL etc.)
    for match in _IDENT_RE.finditer(sql):
        ident = match.group(1)
        parts = ident.split(".")
        # Skip if any part is a SQL keyword
        if any(p.upper() in _SQL_KEYWORDS for p in parts):
            continue
        # Extract the column part (last segment)
        col_part = parts[-1]
        if col_part.upper() not in _SQL_KEYWORDS and col_part != "*":
            refs.add(col_part)

    # Bare column references
    for match in _BARE_COL_RE.finditer(sql):
        bare = match.group(1)
        if bare.upper() not in _SQL_KEYWORDS and bare != "*":
            refs.add(bare)

    return refs


def validate_columns_against_index(
    sql: str,
    db_id: str,
    chroma_store: ChromaStore,
) -> tuple[bool, list[str], list[str]]:
    """Validate column references in SQL against ChromaDB index.

    Returns ``(is_valid, errors, suggestions)``.

    - Parse SQL for column-like references (TABLE.COLUMN, alias.COLUMN, bare COLUMN)
    - Look up each reference in the schema_cards collection
    - For failures, try to find similar column names and suggest corrections
    """
    known_columns, known_tables = _load_known_identifiers(db_id, chroma_store)
    if not known_columns:
        log.warning("No columns found in ChromaDB for db_id=%s; skipping validation", db_id)
        return True, [], []

    refs = _extract_column_refs(sql)
    errors: list[str] = []
    suggestions: list[str] = []

    for ref in sorted(refs):
        ref_upper = ref.upper()
        # Check: exact match in known columns
        if ref_upper in known_columns:
            continue
        # Check: is a known table name — not a column reference
        if ref_upper in known_tables:
            continue
        # Check: could be a table alias (t1, t2, etc.) — skip
        if re.match(r"^T\d+$", ref_upper):
            continue
        # Check: single-char identifiers often aliases
        if len(ref) <= 2:
            continue
        # Not found — flag it
        similar = _find_similar(ref, known_columns)
        errors.append(f"Column '{ref}' not found in schema index for {db_id}")
        if similar:
            suggestions.append(
                f"'{ref}' -> did you mean: {', '.join(similar)}?"
            )

    is_valid = len(errors) == 0
    return is_valid, errors, suggestions
