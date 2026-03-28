"""Extract lightweight tabular features from a candidate record for the learned verifier."""

from __future__ import annotations

import re

from .shape_inference import infer_expected_shape


# The 8 error categories from error_classifier plus "other_execution_error"
_ERROR_TYPES = [
    "object_not_found",
    "not_authorized",
    "invalid_identifier",
    "ambiguous_column",
    "sql_syntax_error",
    "aggregation_error",
    "type_mismatch",
    "unknown_function",
    "other_execution_error",
]


def _row_count_bucket(row_count) -> int:
    """Bucket row count: 0=none, 1=1, 2=2-5, 3=6-20, 4=21-100, 5=100+."""
    if row_count is None:
        return 0
    rc = int(row_count)
    if rc <= 0:
        return 0
    if rc == 1:
        return 1
    if rc <= 5:
        return 2
    if rc <= 20:
        return 3
    if rc <= 100:
        return 4
    return 5


def _shape_alignment(instruction: str, row_count) -> float:
    """Score how well the row count aligns with the expected shape."""
    shape = infer_expected_shape(instruction)
    rc = row_count if row_count is not None else 0
    score = 0.0

    if shape.expect_small_result and rc <= 5 and rc > 0:
        score += 1.0
    if shape.expect_grouped_output and rc > 1:
        score += 1.0
    if shape.expect_aggregate_output and not shape.expect_grouped_output and rc == 1:
        score += 1.0
    if shape.expect_time_series:
        grain = shape.expected_time_grain
        if grain == "month" and 6 <= rc <= 24:
            score += 1.0
        elif grain == "day" and rc >= 7:
            score += 1.0
        elif grain == "week" and 4 <= rc <= 53:
            score += 1.0
        elif grain == "year" and 2 <= rc <= 30:
            score += 1.0

    return score


def extract_candidate_features(candidate_record: dict, instruction: str) -> dict:
    """Extract lightweight tabular features from a candidate record.

    Returns a dict of numeric features suitable for tabular ML.
    """
    sql = candidate_record.get("final_sql", "") or ""
    sql_upper = sql.upper()

    # Basic features
    execution_success = 1 if candidate_record.get("execution_success") else 0
    repairs_count = int(candidate_record.get("repairs_count", 0))

    # Error type one-hot
    error_type = candidate_record.get("error_type")
    error_features = {}
    for et in _ERROR_TYPES:
        error_features[f"error_type_{et}"] = 1 if error_type == et else 0

    # Row count bucket
    row_count = candidate_record.get("row_count")
    row_count_bucket = _row_count_bucket(row_count)

    # Shape alignment
    shape_alignment = _shape_alignment(instruction, row_count)

    # Metamorphic score delta
    metamorphic = candidate_record.get("metamorphic")
    if metamorphic and isinstance(metamorphic, dict):
        metamorphic_score_delta = float(metamorphic.get("score_delta", 0.0))
    else:
        metamorphic_score_delta = 0.0

    # SQL complexity features
    sql_length = len(sql)
    join_count = len(re.findall(r"\bJOIN\b", sql_upper))
    group_by_count = len(re.findall(r"\bGROUP\s+BY\b", sql_upper))
    cte_count = len(re.findall(r"\bWITH\b", sql_upper))

    # Heuristic score from existing selector
    heuristic_score = float(candidate_record.get("score", 0.0))

    features: dict = {
        "execution_success": execution_success,
        "repairs_count": repairs_count,
        **error_features,
        "row_count_bucket": row_count_bucket,
        "shape_alignment": shape_alignment,
        "metamorphic_score_delta": metamorphic_score_delta,
        "sql_length": sql_length,
        "join_count": join_count,
        "group_by_count": group_by_count,
        "cte_count": cte_count,
        "heuristic_score": heuristic_score,
    }

    return features
