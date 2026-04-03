"""Post-SQL verification of decomposition subgoals."""

from __future__ import annotations

import re

from .question_decomposition import QuestionDecomposition

# Date-like patterns in SQL: date literals, BETWEEN with dates, comparison with dates
_DATE_PATTERN = re.compile(
    r"""(?ix)
      \d{4}[-/]\d{2}[-/]\d{2}      # ISO date literals
    | '\d{4}[-/]\d{2}[-/]\d{2}'    # quoted date literals
    | '\d{8}'                       # YYYYMMDD string
    | DATE\s*\(                     # DATE() function
    | DATE_TRUNC\s*\(               # DATE_TRUNC function
    | TO_DATE\s*\(                  # TO_DATE function
    | TRY_TO_DATE\s*\(              # TRY_TO_DATE function
    | YEAR\s*\(                     # YEAR() extraction
    | EXTRACT\s*\(                  # EXTRACT(YEAR FROM ...)
    | DATEADD\s*\(                  # DATEADD function
    | DATEDIFF\s*\(                 # DATEDIFF function
    | BETWEEN\b                     # BETWEEN (may be date range)
    """,
)

# Aggregation function patterns
_AGG_PATTERNS: dict[str, re.Pattern[str]] = {
    "COUNT": re.compile(r"\bCOUNT\s*\(", re.IGNORECASE),
    "SUM": re.compile(r"\bSUM\s*\(", re.IGNORECASE),
    "AVG": re.compile(r"\bAVG\s*\(", re.IGNORECASE),
    "MIN": re.compile(r"\bMIN\s*\(", re.IGNORECASE),
    "MAX": re.compile(r"\bMAX\s*\(", re.IGNORECASE),
    "COUNT DISTINCT": re.compile(r"\bCOUNT\s*\(\s*DISTINCT\b", re.IGNORECASE),
    "COUNT_DISTINCT": re.compile(r"\bCOUNT\s*\(\s*DISTINCT\b", re.IGNORECASE),
}

_GROUP_BY_PATTERN = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
_ORDER_BY_PATTERN = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)


def validate_sql_against_decomposition(
    sql: str,
    decomp: QuestionDecomposition,
) -> tuple[bool, list[str]]:
    """Check if SQL satisfies the decomposition subgoals.

    Returns (all_passed, list_of_warnings).
    """
    warnings: list[str] = []

    # Check temporal scope
    if decomp.temporal_scope:
        if not _DATE_PATTERN.search(sql):
            warnings.append(
                f"Temporal scope '{decomp.temporal_scope}' specified but no "
                f"date-related WHERE clause found in SQL"
            )

    # Check measures / aggregations
    for measure in decomp.measures:
        measure_upper = measure.upper()
        matched = False
        for agg_name, agg_re in _AGG_PATTERNS.items():
            if agg_name in measure_upper and agg_re.search(sql):
                matched = True
                break
        # Also check if the raw measure text appears in the SQL
        if not matched:
            # Try a loose check: any aggregation keyword from the measure
            for agg_name, agg_re in _AGG_PATTERNS.items():
                if agg_re.search(sql) and agg_name in measure_upper:
                    matched = True
                    break
        if not matched:
            warnings.append(
                f"Measure '{measure}' specified but no matching aggregation "
                f"found in SQL"
            )

    # Check grouping
    if decomp.grouping:
        if not _GROUP_BY_PATTERN.search(sql):
            warnings.append(
                f"Grouping specified ({decomp.grouping}) but no GROUP BY "
                f"clause found in SQL"
            )

    # Check ranking
    if decomp.ranking:
        ranking_lower = decomp.ranking.lower()
        if "top" in ranking_lower or "highest" in ranking_lower or "lowest" in ranking_lower:
            has_order = _ORDER_BY_PATTERN.search(sql)
            has_limit = _LIMIT_PATTERN.search(sql)
            if not (has_order and has_limit):
                warnings.append(
                    f"Ranking '{decomp.ranking}' specified but SQL is missing "
                    f"ORDER BY + LIMIT"
                )

    all_passed = len(warnings) == 0
    return all_passed, warnings
