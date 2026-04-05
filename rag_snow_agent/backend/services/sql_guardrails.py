"""Read-only SQL validation guardrails."""

from __future__ import annotations

import re


class SQLValidationError(Exception):
    """Raised when SQL fails read-only validation."""


_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE|"
    r"EXEC|EXECUTE|CALL|COPY|PUT|GET|REMOVE)\b",
    re.IGNORECASE,
)


def validate_read_only(sql: str) -> str:
    """Validate SQL is read-only. Returns cleaned SQL or raises SQLValidationError."""
    cleaned = sql.strip().rstrip(";").strip()

    if not cleaned:
        raise SQLValidationError("Empty SQL statement")

    # Reject multiple statements
    # Simple heuristic: check for semicolons not inside strings
    in_string = False
    quote_char = None
    for ch in cleaned:
        if in_string:
            if ch == quote_char:
                in_string = False
        else:
            if ch in ("'", '"'):
                in_string = True
                quote_char = ch
            elif ch == ";":
                raise SQLValidationError("Multiple SQL statements are not allowed")

    # Check first keyword is SELECT or WITH
    first_word = re.match(r"\s*(\w+)", cleaned, re.IGNORECASE)
    if not first_word or first_word.group(1).upper() not in ("SELECT", "WITH"):
        raise SQLValidationError(
            f"Only SELECT queries are allowed. Got: {first_word.group(1) if first_word else 'empty'}"
        )

    # Scan for forbidden keywords
    match = _FORBIDDEN_RE.search(cleaned)
    if match:
        raise SQLValidationError(
            f"Forbidden SQL keyword detected: {match.group(0).upper()}"
        )

    return cleaned
