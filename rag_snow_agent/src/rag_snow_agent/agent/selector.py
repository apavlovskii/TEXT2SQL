"""Candidate selection logic for Best-of-N (v2 with semantic signals).

Scoring rules:
  +100  if execution succeeds
  -10   per repair attempt
  -20   if result empty and instruction likely implies non-empty
  +10   if expected small result and row_count <= 5
  -15   if expected grouped output but row_count == 1
  +10   if expected aggregate output and row_count == 1
  +10   if expected time series and row_count in plausible range
  -30   if final error type is object_not_found / invalid_identifier
  -15   if final error type is aggregation_error
  + score_delta from metamorphic checks
  + verifier score (currently stub = 0.0)
"""

from __future__ import annotations

from .shape_inference import ExpectedShape, infer_expected_shape

# Default scoring parameters (can be overridden via config)
DEFAULT_SCORING = {
    "success_bonus": 100,
    "repair_penalty": 10,
    "empty_result_penalty": 20,
    "small_output_bonus": 10,
    "grouped_output_bonus": 5,
    "grouped_single_row_penalty": 15,
    "aggregate_single_row_bonus": 10,
    "time_series_bonus": 10,
    "object_not_found_penalty": 30,
    "invalid_identifier_penalty": 30,
    "aggregation_error_penalty": 15,
    "verifier_weight": 20.0,
}


def score_candidate(
    instruction: str,
    candidate_result: dict,
    scoring: dict | None = None,
) -> float:
    """Score a candidate result. Higher is better.

    *candidate_result* may have keys:
      execution_success, repairs_count, row_count, error_type,
      metamorphic (dict with score_delta), verifier_score
    """
    s = {**DEFAULT_SCORING, **(scoring or {})}
    breakdown = _build_breakdown(instruction, candidate_result, s)
    return float(sum(breakdown.values()))


def explain_candidate_score(
    instruction: str,
    candidate_result: dict,
    scoring: dict | None = None,
) -> dict:
    """Return a score breakdown dict for debugging."""
    s = {**DEFAULT_SCORING, **(scoring or {})}
    breakdown = _build_breakdown(instruction, candidate_result, s)
    breakdown["total"] = float(sum(v for k, v in breakdown.items() if k != "total"))
    return breakdown


def _build_breakdown(
    instruction: str,
    candidate_result: dict,
    s: dict,
) -> dict[str, float]:
    """Compute score breakdown."""
    bd: dict[str, float] = {}

    # Execution success
    if candidate_result.get("execution_success"):
        bd["execution_success"] = s["success_bonus"]

    # Repair penalty
    repairs = candidate_result.get("repairs_count", 0)
    if repairs:
        bd["repair_penalty"] = -repairs * s["repair_penalty"]

    # Shape-based scoring
    shape_dict = candidate_result.get("expected_shape")
    if shape_dict and isinstance(shape_dict, dict):
        shape = ExpectedShape(**shape_dict)
    elif shape_dict and isinstance(shape_dict, ExpectedShape):
        shape = shape_dict
    else:
        shape = infer_expected_shape(instruction)

    row_count = candidate_result.get("row_count")

    if row_count is not None and row_count == 0:
        bd["empty_result"] = -s["empty_result_penalty"]

    if row_count is not None and shape.expect_small_result and row_count <= 5:
        bd["small_output_bonus"] = s["small_output_bonus"]

    if row_count is not None and shape.expect_grouped_output:
        if row_count > 1:
            bd["grouped_output_bonus"] = s["grouped_output_bonus"]
        elif row_count == 1:
            bd["grouped_single_row_penalty"] = -s["grouped_single_row_penalty"]

    if row_count is not None and shape.expect_aggregate_output:
        if not shape.expect_grouped_output and row_count == 1:
            bd["aggregate_single_row_bonus"] = s["aggregate_single_row_bonus"]

    if row_count is not None and shape.expect_time_series:
        grain = shape.expected_time_grain
        plausible = False
        if grain == "month" and 6 <= row_count <= 24:
            plausible = True
        elif grain == "day" and row_count >= 7:
            plausible = True
        elif grain == "week" and 4 <= row_count <= 53:
            plausible = True
        elif grain == "year" and 2 <= row_count <= 30:
            plausible = True
        if plausible:
            bd["time_series_bonus"] = s["time_series_bonus"]

    # Error-type penalties
    error_type = candidate_result.get("error_type")
    if error_type in ("object_not_found", "invalid_identifier"):
        bd["error_penalty"] = -s["object_not_found_penalty"]
    elif error_type == "aggregation_error":
        bd["error_penalty"] = -s["aggregation_error_penalty"]

    # Metamorphic score delta
    metamorphic = candidate_result.get("metamorphic")
    if metamorphic and isinstance(metamorphic, dict):
        delta = metamorphic.get("score_delta", 0.0)
        if delta != 0.0:
            bd["metamorphic_delta"] = delta

    # Verifier score (weighted by verifier_weight)
    verifier = candidate_result.get("verifier_score", 0.0)
    if verifier != 0.0:
        bd["verifier_score"] = verifier * s["verifier_weight"]

    return bd
