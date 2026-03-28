"""Minimal tests for metamorphic checks."""

from unittest.mock import MagicMock

from rag_snow_agent.agent.metamorphic import run_metamorphic_checks
from rag_snow_agent.agent.shape_inference import ExpectedShape
from rag_snow_agent.snowflake.executor import ExecutionResult


def _mock_executor(success: bool = True, row_count: int = 20):
    executor = MagicMock()
    executor.execute.return_value = ExecutionResult(
        success=success, sql="derived", row_count=row_count
    )
    return executor


def test_limit_expansion_runs():
    """SQL with LIMIT should trigger limit_expansion check."""
    executor = _mock_executor()
    result = run_metamorphic_checks(
        instruction="top 5 products",
        sql="SELECT * FROM t ORDER BY x DESC LIMIT 5",
        executor=executor,
        row_count=5,
        max_checks=2,
    )
    check_types = [c["check_type"] for c in result["checks_run"]]
    assert "limit_expansion" in check_types


def test_no_limit_no_expansion():
    """SQL without LIMIT should not trigger limit_expansion."""
    executor = _mock_executor()
    result = run_metamorphic_checks(
        instruction="all orders",
        sql="SELECT * FROM orders",
        executor=executor,
        row_count=100,
        max_checks=2,
    )
    check_types = [c["check_type"] for c in result["checks_run"]]
    assert "limit_expansion" not in check_types


def test_shape_consistency_grouped_single_row():
    """Grouped output expected but 1 row → negative delta."""
    shape = ExpectedShape(expect_grouped_output=True)
    result = run_metamorphic_checks(
        instruction="revenue for each category",
        sql="SELECT category, SUM(amount) FROM t GROUP BY category",
        executor=None,
        expected_shape=shape,
        row_count=1,
        max_checks=2,
    )
    assert result["score_delta"] < 0


def test_shape_consistency_aggregate_confirmed():
    """Aggregate output expected and 1 row → positive delta."""
    shape = ExpectedShape(expect_aggregate_output=True)
    result = run_metamorphic_checks(
        instruction="how many orders total",
        sql="SELECT COUNT(*) FROM orders",
        executor=None,
        expected_shape=shape,
        row_count=1,
        max_checks=2,
    )
    assert result["score_delta"] > 0


def test_monthly_time_series_plausible():
    """Monthly time series with 12 rows → positive delta."""
    shape = ExpectedShape(expect_time_series=True, expected_time_grain="month")
    result = run_metamorphic_checks(
        instruction="sales by month",
        sql="SELECT month, SUM(amount) FROM t GROUP BY month",
        executor=None,
        expected_shape=shape,
        row_count=12,
        max_checks=2,
    )
    assert result["score_delta"] > 0


def test_max_checks_respected():
    """Should not run more checks than max_checks."""
    shape = ExpectedShape(expect_time_series=True, expected_time_grain="month")
    executor = _mock_executor()
    result = run_metamorphic_checks(
        instruction="sales by month",
        sql="SELECT month, SUM(amount) FROM t GROUP BY month LIMIT 12",
        executor=executor,
        expected_shape=shape,
        row_count=12,
        max_checks=1,
    )
    assert len(result["checks_run"]) <= 1
