"""Expected output shape inference from natural-language instructions.

Heuristic rules:
  - "top", "highest", "lowest", "most"  → expect_small_result
  - "monthly", "per month", "by month"  → expect_time_series, grain="month"
  - "daily", "per day", "by day"        → expect_time_series, grain="day"
  - "weekly", "per week", "by week"     → expect_time_series, grain="week"
  - "yearly", "per year", "by year"     → expect_time_series, grain="year"
  - "for each", "grouped by", "by <dim>"→ expect_grouped_output
  - "how many", "count", "total number" → expect_aggregate_output
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class ExpectedShape:
    expect_small_result: bool = False
    expect_grouped_output: bool = False
    expect_aggregate_output: bool = False
    expect_time_series: bool = False
    expected_time_grain: str | None = None
    notes: list[str] = field(default_factory=list)


_SMALL_RE = re.compile(
    r"\b(top\s+\d+|highest|lowest|most|least|best|worst|largest|smallest)\b",
    re.IGNORECASE,
)
_MONTHLY_RE = re.compile(r"\b(monthly|per month|by month|each month)\b", re.IGNORECASE)
_DAILY_RE = re.compile(r"\b(daily|per day|by day|each day)\b", re.IGNORECASE)
_WEEKLY_RE = re.compile(r"\b(weekly|per week|by week|each week)\b", re.IGNORECASE)
_YEARLY_RE = re.compile(r"\b(yearly|per year|by year|each year|annually)\b", re.IGNORECASE)
_GROUPED_RE = re.compile(
    r"\b(for each|grouped by|by\s+\w+|per\s+\w+|breakdown)\b", re.IGNORECASE
)
_AGGREGATE_RE = re.compile(
    r"\b(how many|count|total number|sum of|average|avg)\b", re.IGNORECASE
)


def infer_expected_shape(instruction: str) -> ExpectedShape:
    """Infer expected output shape from the natural language instruction."""
    shape = ExpectedShape()

    if _SMALL_RE.search(instruction):
        shape.expect_small_result = True
        shape.notes.append("Small result expected (top/highest/lowest/...)")

    if _MONTHLY_RE.search(instruction):
        shape.expect_time_series = True
        shape.expected_time_grain = "month"
        shape.notes.append("Monthly time series expected")
    elif _DAILY_RE.search(instruction):
        shape.expect_time_series = True
        shape.expected_time_grain = "day"
        shape.notes.append("Daily time series expected")
    elif _WEEKLY_RE.search(instruction):
        shape.expect_time_series = True
        shape.expected_time_grain = "week"
        shape.notes.append("Weekly time series expected")
    elif _YEARLY_RE.search(instruction):
        shape.expect_time_series = True
        shape.expected_time_grain = "year"
        shape.notes.append("Yearly time series expected")

    if _GROUPED_RE.search(instruction):
        shape.expect_grouped_output = True
        shape.notes.append("Grouped output expected")

    if _AGGREGATE_RE.search(instruction):
        shape.expect_aggregate_output = True
        shape.notes.append("Aggregate output expected")

    return shape
