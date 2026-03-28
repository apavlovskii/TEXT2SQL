"""Classify Snowflake error messages into a structured taxonomy."""

from __future__ import annotations

import re

# ── Error categories ─────────────────────────────────────────────────────────

OBJECT_NOT_FOUND = "object_not_found"
NOT_AUTHORIZED = "not_authorized"
INVALID_IDENTIFIER = "invalid_identifier"
AMBIGUOUS_COLUMN = "ambiguous_column"
SQL_SYNTAX_ERROR = "sql_syntax_error"
AGGREGATION_ERROR = "aggregation_error"
TYPE_MISMATCH = "type_mismatch"
UNKNOWN_FUNCTION = "unknown_function"
OTHER_EXECUTION_ERROR = "other_execution_error"

# ── Patterns (order matters: more specific first) ────────────────────────────

_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Object not found (must precede NOT_AUTHORIZED — "does not exist or not authorized" has both)
    (re.compile(r"does not exist or not authorized", re.IGNORECASE), OBJECT_NOT_FOUND),
    (re.compile(r"Object '.*' does not exist", re.IGNORECASE), OBJECT_NOT_FOUND),
    # Authorization
    (re.compile(r"Insufficient privileges", re.IGNORECASE), NOT_AUTHORIZED),
    (re.compile(r"access denied|not authorized|permission denied", re.IGNORECASE), NOT_AUTHORIZED),
    (re.compile(r"Table '.*' does not exist", re.IGNORECASE), OBJECT_NOT_FOUND),
    (re.compile(r"Database '.*' does not exist", re.IGNORECASE), OBJECT_NOT_FOUND),
    (re.compile(r"Schema '.*' does not exist", re.IGNORECASE), OBJECT_NOT_FOUND),
    (re.compile(r"object does not exist", re.IGNORECASE), OBJECT_NOT_FOUND),
    # Invalid identifier
    (re.compile(r"invalid identifier", re.IGNORECASE), INVALID_IDENTIFIER),
    (re.compile(r"unknown column", re.IGNORECASE), INVALID_IDENTIFIER),
    (re.compile(r"column '.*' is not present", re.IGNORECASE), INVALID_IDENTIFIER),
    # Ambiguous
    (re.compile(r"ambiguous column", re.IGNORECASE), AMBIGUOUS_COLUMN),
    (re.compile(r"Column '.*' is ambiguous", re.IGNORECASE), AMBIGUOUS_COLUMN),
    # Aggregation / grouping
    (re.compile(r"not in GROUP BY", re.IGNORECASE), AGGREGATION_ERROR),
    (re.compile(r"is not an aggregate expression", re.IGNORECASE), AGGREGATION_ERROR),
    (re.compile(r"not a valid group by expression", re.IGNORECASE), AGGREGATION_ERROR),
    (re.compile(r"grouping error", re.IGNORECASE), AGGREGATION_ERROR),
    # Type mismatch
    (re.compile(r"type mismatch", re.IGNORECASE), TYPE_MISMATCH),
    (re.compile(r"cannot (be )?cast", re.IGNORECASE), TYPE_MISMATCH),
    (re.compile(r"Numeric value '.*' is not recognized", re.IGNORECASE), TYPE_MISMATCH),
    (re.compile(r"Invalid data type", re.IGNORECASE), TYPE_MISMATCH),
    (re.compile(r"Date '.*' is not recognized", re.IGNORECASE), TYPE_MISMATCH),
    # Unknown function
    (re.compile(r"Unknown function", re.IGNORECASE), UNKNOWN_FUNCTION),
    (re.compile(r"Unsupported subquery type", re.IGNORECASE), UNKNOWN_FUNCTION),
    # Syntax
    (re.compile(r"SQL compilation error", re.IGNORECASE), SQL_SYNTAX_ERROR),
    (re.compile(r"syntax error", re.IGNORECASE), SQL_SYNTAX_ERROR),
    (re.compile(r"unexpected '", re.IGNORECASE), SQL_SYNTAX_ERROR),
    (re.compile(r"parse error", re.IGNORECASE), SQL_SYNTAX_ERROR),
]

# ── Extraction patterns ─────────────────────────────────────────────────────

_IDENT_RE = re.compile(r"invalid identifier '([^']+)'", re.IGNORECASE)
_OBJECT_RE = re.compile(
    r"(?:Object|Table|Database|Schema|View) '([^']+)' does not exist", re.IGNORECASE
)
_COLUMN_RE = re.compile(r"Column '([^']+)'", re.IGNORECASE)


def classify_snowflake_error(error_message: str) -> str:
    """Map a Snowflake error message to a category string."""
    for pattern, category in _PATTERNS:
        if pattern.search(error_message):
            return category
    return OTHER_EXECUTION_ERROR


def extract_offending_identifier(error_message: str) -> str | None:
    """Extract the offending identifier from an error message, if present."""
    m = _IDENT_RE.search(error_message)
    if m:
        return m.group(1)
    m = _COLUMN_RE.search(error_message)
    if m:
        return m.group(1)
    return None


def extract_offending_object(error_message: str) -> str | None:
    """Extract the offending object name from an error message, if present."""
    m = _OBJECT_RE.search(error_message)
    if m:
        return m.group(1)
    return None
