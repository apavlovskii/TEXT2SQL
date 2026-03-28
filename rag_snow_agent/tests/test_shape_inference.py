"""Tests for expected output shape inference."""

from rag_snow_agent.agent.shape_inference import infer_expected_shape


def test_small_result_top():
    shape = infer_expected_shape("Show the top 5 customers by revenue")
    assert shape.expect_small_result
    assert not shape.expect_time_series


def test_monthly_time_series():
    shape = infer_expected_shape("total sales by month in 2023")
    assert shape.expect_time_series
    assert shape.expected_time_grain == "month"


def test_daily_time_series():
    shape = infer_expected_shape("orders per day last week")
    assert shape.expect_time_series
    assert shape.expected_time_grain == "day"


def test_yearly_time_series():
    shape = infer_expected_shape("revenue by year")
    assert shape.expect_time_series
    assert shape.expected_time_grain == "year"


def test_grouped_output():
    shape = infer_expected_shape("revenue for each product category")
    assert shape.expect_grouped_output


def test_aggregate_output():
    shape = infer_expected_shape("how many orders were placed")
    assert shape.expect_aggregate_output


def test_no_special():
    shape = infer_expected_shape("list all customers in California")
    assert not shape.expect_small_result
    assert not shape.expect_time_series
    assert not shape.expect_aggregate_output


def test_notes_populated():
    shape = infer_expected_shape("top 10 products by month")
    assert len(shape.notes) >= 1
    assert any("Small" in n for n in shape.notes)
