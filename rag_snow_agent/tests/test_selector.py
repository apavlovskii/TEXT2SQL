"""Tests for candidate scoring and shape inference."""

from rag_snow_agent.agent.selector import score_candidate
from rag_snow_agent.agent.shape_inference import infer_expected_shape


# ── Shape inference ──────────────────────────────────────────────────────────


def test_infer_small_output():
    shape = infer_expected_shape("Show the top 5 customers by revenue")
    assert shape.expect_small_result


def test_infer_monthly():
    shape = infer_expected_shape("total sales by month in 2023")
    assert shape.expect_time_series
    assert shape.expected_time_grain == "month"


def test_infer_grouped():
    shape = infer_expected_shape("revenue for each product category")
    assert shape.expect_grouped_output


def test_infer_aggregate():
    shape = infer_expected_shape("how many orders were placed")
    assert shape.expect_aggregate_output


def test_infer_no_special():
    shape = infer_expected_shape("list all customers in California")
    assert not shape.expect_small_result
    assert not shape.expect_time_series


# ── Scoring ──────────────────────────────────────────────────────────────────


def test_success_dominates():
    """A successful candidate should score much higher than a failed one."""
    good = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 10,
        "error_type": None,
    }
    bad = {
        "execution_success": False,
        "repairs_count": 0,
        "row_count": None,
        "error_type": "sql_syntax_error",
    }
    assert score_candidate("list orders", good) > score_candidate("list orders", bad)


def test_fewer_repairs_preferred():
    """Among successful candidates, fewer repairs = higher score."""
    few = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 5,
        "error_type": None,
    }
    many = {
        "execution_success": True,
        "repairs_count": 2,
        "row_count": 5,
        "error_type": None,
    }
    assert score_candidate("list orders", few) > score_candidate("list orders", many)


def test_repair_penalty():
    base = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 10,
        "error_type": None,
    }
    repaired = {
        "execution_success": True,
        "repairs_count": 3,
        "row_count": 10,
        "error_type": None,
    }
    diff = score_candidate("x", base) - score_candidate("x", repaired)
    assert diff == 30  # 3 repairs * 10 penalty


def test_empty_result_penalty():
    """Empty result on a query that expects data should be penalized."""
    empty = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 0,
        "error_type": None,
    }
    nonempty = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 5,
        "error_type": None,
    }
    s_empty = score_candidate("show all orders", empty)
    s_nonempty = score_candidate("show all orders", nonempty)
    assert s_nonempty > s_empty


def test_small_output_bonus():
    """Small row_count should get a bonus when 'top' is in the instruction."""
    small = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 3,
        "error_type": None,
    }
    large = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 100,
        "error_type": None,
    }
    s_small = score_candidate("top 5 products by revenue", small)
    s_large = score_candidate("top 5 products by revenue", large)
    assert s_small > s_large


def test_object_not_found_penalty():
    failed = {
        "execution_success": False,
        "repairs_count": 1,
        "row_count": None,
        "error_type": "object_not_found",
    }
    generic_fail = {
        "execution_success": False,
        "repairs_count": 1,
        "row_count": None,
        "error_type": "other_execution_error",
    }
    # object_not_found should score lower
    assert score_candidate("x", failed) < score_candidate("x", generic_fail)
