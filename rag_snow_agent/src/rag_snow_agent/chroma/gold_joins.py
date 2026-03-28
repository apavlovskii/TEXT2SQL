"""Extract JOIN conditions from gold SQL files."""

from __future__ import annotations

import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)

# Pattern to match JOIN ... ON conditions.
# Handles both quoted and unquoted identifiers, with optional DB.SCHEMA.TABLE qualification.
_JOIN_RE = re.compile(
    r"""
    \bJOIN\s+
    (?P<right_full>
        "?[\w]+"?\."?[\w]+"?\."?[\w]+"?   # DB.SCHEMA.TABLE (quoted or unquoted)
    )
    \s+(?:AS\s+)?(?P<right_alias>"?[\w]+"?)  # optional alias
    \s+ON\s+
    (?P<on_clause>[^;]+?)                    # ON clause (greedy up to next JOIN/WHERE/GROUP/ORDER/LIMIT/;)
    (?=\s+(?:JOIN|LEFT|RIGHT|FULL|CROSS|INNER|WHERE|GROUP|ORDER|LIMIT|HAVING|UNION|QUALIFY|\)|$))
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# Simpler JOIN pattern that also handles the case where table has no alias
_JOIN_SIMPLE_RE = re.compile(
    r"""
    \bJOIN\s+
    (?P<right_full>
        "?[\w]+"?(?:\."?[\w]+"?){0,2}       # 1-3 part name
    )
    (?:\s+(?:AS\s+)?(?P<right_alias>"?[\w]+"?))?
    \s+ON\s+
    (?P<on_clause>.+?)
    (?=\s+(?:JOIN|LEFT|RIGHT|FULL|CROSS|INNER|WHERE|GROUP|ORDER|LIMIT|HAVING|UNION|QUALIFY|\)|;|$))
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)

# Pattern for ON condition: "alias"."col" = "alias"."col"
_ON_EQ_RE = re.compile(
    r"""
    "?(?P<left_ref>[\w]+)"?\."?(?P<left_col>[\w]+)"?
    \s*=\s*
    "?(?P<right_ref>[\w]+)"?\."?(?P<right_col>[\w]+)"?
    """,
    re.VERBOSE,
)

# Pattern to extract FROM/JOIN table aliases
_TABLE_ALIAS_RE = re.compile(
    r"""
    (?:FROM|JOIN)\s+
    (?P<full_name>"?[\w]+"?(?:\."?[\w]+"?){0,2})
    (?:\s+(?:AS\s+)?(?P<alias>"?[\w]+"?))?
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _strip_quotes(name: str) -> str:
    """Remove surrounding double quotes from an identifier."""
    return name.strip('"')


def _build_alias_map(sql: str) -> dict[str, str]:
    """Build a mapping from alias -> fully qualified table name."""
    alias_map: dict[str, str] = {}
    for m in _TABLE_ALIAS_RE.finditer(sql):
        full_name = _strip_quotes_from_parts(m.group("full_name"))
        alias = m.group("alias")
        if alias:
            alias = _strip_quotes(alias)
            # Skip SQL keywords that might be mis-parsed as aliases
            if alias.upper() in {"ON", "WHERE", "AS", "SET", "AND", "OR", "LEFT", "RIGHT",
                                  "INNER", "OUTER", "CROSS", "FULL", "JOIN", "GROUP",
                                  "ORDER", "HAVING", "LIMIT", "UNION"}:
                continue
            alias_map[alias.upper()] = full_name
        # Also map the short table name to the full name
        parts = full_name.split(".")
        if parts:
            short = parts[-1]
            alias_map[short.upper()] = full_name
    return alias_map


def _strip_quotes_from_parts(name: str) -> str:
    """Strip quotes from each part of a dotted name."""
    parts = name.split(".")
    return ".".join(_strip_quotes(p) for p in parts)


def extract_joins_from_gold_sqls(gold_sql_dir: str | Path) -> list[dict]:
    """Parse gold SQL files to extract JOIN conditions.

    Returns list of dicts with keys:
    ``left_table``, ``left_column``, ``right_table``, ``right_column``, ``source_file``
    """
    gold_dir = Path(gold_sql_dir)
    if not gold_dir.is_dir():
        log.warning("Gold SQL directory does not exist: %s", gold_dir)
        return []

    joins: list[dict] = []
    for sql_file in sorted(gold_dir.glob("*.sql")):
        try:
            sql_text = sql_file.read_text(encoding="utf-8")
        except Exception:
            log.warning("Could not read %s", sql_file, exc_info=True)
            continue

        file_joins = _extract_joins_from_sql(sql_text, sql_file.name)
        joins.extend(file_joins)

    log.info("Extracted %d join(s) from %s", len(joins), gold_dir)
    return joins


def _extract_joins_from_sql(sql: str, source_file: str) -> list[dict]:
    """Extract join conditions from a single SQL string."""
    alias_map = _build_alias_map(sql)
    results: list[dict] = []

    # Find all JOIN ... ON clauses
    for m in _JOIN_SIMPLE_RE.finditer(sql):
        on_clause = m.group("on_clause")
        right_full = _strip_quotes_from_parts(m.group("right_full"))
        right_alias = m.group("right_alias")
        if right_alias:
            right_alias = _strip_quotes(right_alias)
            if right_alias.upper() not in {"ON", "WHERE", "AS"}:
                alias_map[right_alias.upper()] = right_full

        # Parse ON equalities
        for eq_match in _ON_EQ_RE.finditer(on_clause):
            left_ref = _strip_quotes(eq_match.group("left_ref"))
            left_col = _strip_quotes(eq_match.group("left_col"))
            right_ref = _strip_quotes(eq_match.group("right_ref"))
            right_col = _strip_quotes(eq_match.group("right_col"))

            # Resolve aliases to full table names
            left_table = alias_map.get(left_ref.upper(), left_ref)
            right_table = alias_map.get(right_ref.upper(), right_ref)

            results.append({
                "left_table": left_table,
                "left_column": left_col,
                "right_table": right_table,
                "right_column": right_col,
                "source_file": source_file,
            })

    return results
