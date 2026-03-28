"""Tests for verifier_features.extract_candidate_features."""

from rag_snow_agent.agent.verifier_features import extract_candidate_features


def _make_candidate(**overrides):
    base = {
        "execution_success": True,
        "repairs_count": 1,
        "error_type": None,
        "row_count": 10,
        "final_sql": "SELECT a, b FROM t GROUP BY a",
        "metamorphic": {"score_delta": 2.5},
        "score": 95.0,
    }
    base.update(overrides)
    return base


def test_all_expected_keys_present():
    record = _make_candidate()
    features = extract_candidate_features(record, "total sessions by month")
    expected_keys = {
        "execution_success",
        "repairs_count",
        "row_count_bucket",
        "shape_alignment",
        "metamorphic_score_delta",
        "sql_length",
        "join_count",
        "group_by_count",
        "cte_count",
        "heuristic_score",
        # Error type one-hots
        "error_type_object_not_found",
        "error_type_not_authorized",
        "error_type_invalid_identifier",
        "error_type_ambiguous_column",
        "error_type_sql_syntax_error",
        "error_type_aggregation_error",
        "error_type_type_mismatch",
        "error_type_unknown_function",
        "error_type_other_execution_error",
    }
    assert expected_keys.issubset(features.keys()), (
        f"Missing keys: {expected_keys - features.keys()}"
    )


def test_all_values_numeric():
    record = _make_candidate()
    features = extract_candidate_features(record, "count of users")
    for key, val in features.items():
        assert isinstance(val, (int, float)), f"Feature {key!r} has non-numeric value: {val!r}"


def test_execution_success_encoding():
    features_ok = extract_candidate_features(_make_candidate(execution_success=True), "test")
    features_fail = extract_candidate_features(_make_candidate(execution_success=False), "test")
    assert features_ok["execution_success"] == 1
    assert features_fail["execution_success"] == 0


def test_error_type_one_hot():
    record = _make_candidate(error_type="aggregation_error")
    features = extract_candidate_features(record, "test")
    assert features["error_type_aggregation_error"] == 1
    # All others should be 0
    for key, val in features.items():
        if key.startswith("error_type_") and key != "error_type_aggregation_error":
            assert val == 0, f"{key} should be 0"


def test_row_count_bucket():
    assert extract_candidate_features(_make_candidate(row_count=None), "t")["row_count_bucket"] == 0
    assert extract_candidate_features(_make_candidate(row_count=0), "t")["row_count_bucket"] == 0
    assert extract_candidate_features(_make_candidate(row_count=1), "t")["row_count_bucket"] == 1
    assert extract_candidate_features(_make_candidate(row_count=3), "t")["row_count_bucket"] == 2
    assert extract_candidate_features(_make_candidate(row_count=15), "t")["row_count_bucket"] == 3
    assert extract_candidate_features(_make_candidate(row_count=50), "t")["row_count_bucket"] == 4
    assert extract_candidate_features(_make_candidate(row_count=200), "t")["row_count_bucket"] == 5


def test_sql_complexity_features():
    sql = "WITH cte AS (SELECT a FROM t1 JOIN t2 ON t1.id = t2.id GROUP BY a) SELECT * FROM cte"
    features = extract_candidate_features(_make_candidate(final_sql=sql), "test")
    assert features["join_count"] == 1
    assert features["group_by_count"] == 1
    assert features["cte_count"] == 1
    assert features["sql_length"] == len(sql)


def test_metamorphic_delta():
    record = _make_candidate(metamorphic={"score_delta": -5.0})
    features = extract_candidate_features(record, "test")
    assert features["metamorphic_score_delta"] == -5.0


def test_no_metamorphic():
    record = _make_candidate(metamorphic=None)
    features = extract_candidate_features(record, "test")
    assert features["metamorphic_score_delta"] == 0.0
