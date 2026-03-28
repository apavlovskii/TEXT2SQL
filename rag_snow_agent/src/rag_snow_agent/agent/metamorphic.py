"""Lightweight metamorphic / counterfactual checks for candidate SQL.

v1 checks:
  - limit_expansion: if SQL has LIMIT, try with a larger LIMIT
  - shape_consistency: if grouped output expected but row_count==1, penalize

All checks are optional and bounded. If a derived SQL cannot be safely
constructed, the check is skipped.
"""

from __future__ import annotations

import logging
import re

from .shape_inference import ExpectedShape

log = logging.getLogger(__name__)

_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)\b", re.IGNORECASE)


def _try_limit_expansion(
    sql: str,
    executor,
    original_row_count: int | None,
) -> dict | None:
    """If SQL contains LIMIT N, try with LIMIT N*2 and verify it still runs."""
    match = _LIMIT_RE.search(sql)
    if not match:
        return None

    original_limit = int(match.group(1))
    expanded_limit = original_limit * 2
    derived_sql = sql[: match.start()] + f"LIMIT {expanded_limit}" + sql[match.end() :]

    try:
        result = executor.execute(derived_sql, sample_rows=1)
        return {
            "check_type": "limit_expansion",
            "success": result.success,
            "notes": (
                f"Expanded LIMIT {original_limit} → {expanded_limit}, "
                f"derived_rows={result.row_count}"
            ),
            "derived_sql": derived_sql,
            "derived_row_count": result.row_count,
        }
    except Exception as exc:
        log.debug("limit_expansion check failed: %s", exc)
        return {
            "check_type": "limit_expansion",
            "success": False,
            "notes": f"Execution error: {str(exc)[:100]}",
            "derived_sql": derived_sql,
            "derived_row_count": None,
        }


def _shape_consistency_check(
    row_count: int | None,
    expected_shape: ExpectedShape,
) -> dict | None:
    """Check if result shape is consistent with expected shape.

    Does not require SQL rewriting — pure data check.
    """
    notes_parts: list[str] = []
    penalty = 0.0

    if row_count is None:
        return None

    if expected_shape.expect_grouped_output and row_count == 1:
        notes_parts.append("Grouped output expected but only 1 row returned")
        penalty -= 15.0

    if expected_shape.expect_aggregate_output and not expected_shape.expect_grouped_output:
        if row_count == 1:
            notes_parts.append("Aggregate output confirmed: 1 row")
            penalty += 10.0
        elif row_count > 1:
            notes_parts.append(
                f"Aggregate expected but {row_count} rows returned"
            )
            penalty -= 5.0

    if expected_shape.expect_time_series:
        grain = expected_shape.expected_time_grain
        if grain == "month" and 6 <= row_count <= 24:
            notes_parts.append(f"Monthly time series plausible ({row_count} rows)")
            penalty += 10.0
        elif grain == "day" and row_count >= 7:
            notes_parts.append(f"Daily time series plausible ({row_count} rows)")
            penalty += 5.0
        elif grain == "year" and 2 <= row_count <= 30:
            notes_parts.append(f"Yearly time series plausible ({row_count} rows)")
            penalty += 5.0

    if expected_shape.expect_small_result and row_count <= 5:
        notes_parts.append(f"Small result confirmed ({row_count} rows)")
        penalty += 5.0

    if not notes_parts:
        return None

    return {
        "check_type": "shape_consistency",
        "success": True,
        "notes": "; ".join(notes_parts),
        "derived_sql": None,
        "derived_row_count": None,
        "score_delta": penalty,
    }


def run_metamorphic_checks(
    instruction: str,
    sql: str,
    executor,
    expected_shape: ExpectedShape | None = None,
    row_count: int | None = None,
    max_checks: int = 2,
) -> dict:
    """Run lightweight metamorphic/counterfactual checks.

    Returns {checks_run: [...], score_delta: float}.
    """
    if expected_shape is None:
        from .shape_inference import infer_expected_shape
        expected_shape = infer_expected_shape(instruction)

    checks: list[dict] = []
    total_delta = 0.0

    # Check 1: shape consistency (cheap, no SQL execution)
    if len(checks) < max_checks:
        sc = _shape_consistency_check(row_count, expected_shape)
        if sc:
            total_delta += sc.pop("score_delta", 0.0)
            checks.append(sc)

    # Check 2: limit expansion (requires executor)
    if len(checks) < max_checks and executor is not None:
        lc = _try_limit_expansion(sql, executor, row_count)
        if lc:
            checks.append(lc)
            # If limit expansion fails, slight penalty
            if not lc["success"]:
                total_delta -= 5.0

    return {"checks_run": checks, "score_delta": total_delta}
