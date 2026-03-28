"""Tests for result fingerprinting."""

from rag_snow_agent.agent.result_fingerprint import (
    ResultFingerprint,
    build_result_fingerprint,
)
from rag_snow_agent.snowflake.executor import ExecutionResult


def test_failed_execution_partial():
    er = ExecutionResult(success=False, sql="bad", error_message="fail")
    fp = build_result_fingerprint(er)
    assert fp.row_count is None
    assert fp.column_count is None


def test_success_with_tuples():
    er = ExecutionResult(
        success=True,
        sql="ok",
        row_count=3,
        column_names=["ID", "AMOUNT", "NAME"],
        rows_sample=[(1, 100.0, "Alice"), (2, 200.0, None), (3, 150.0, "Bob")],
    )
    fp = build_result_fingerprint(er)
    assert fp.row_count == 3
    assert fp.column_count == 3
    assert fp.column_names == ["ID", "AMOUNT", "NAME"]


def test_null_ratios():
    er = ExecutionResult(
        success=True,
        sql="ok",
        row_count=4,
        column_names=["A", "B"],
        rows_sample=[(1, None), (2, None), (3, 10), (4, None)],
    )
    fp = build_result_fingerprint(er)
    assert fp.null_ratios["A"] == 0.0
    assert fp.null_ratios["B"] == 0.75


def test_numeric_stats():
    er = ExecutionResult(
        success=True,
        sql="ok",
        row_count=3,
        column_names=["VAL"],
        rows_sample=[(10,), (20,), (30,)],
    )
    fp = build_result_fingerprint(er)
    assert "VAL" in fp.numeric_stats
    assert fp.numeric_stats["VAL"]["min"] == 10.0
    assert fp.numeric_stats["VAL"]["max"] == 30.0
    assert fp.numeric_stats["VAL"]["mean"] == 20.0


def test_no_sample_rows():
    er = ExecutionResult(
        success=True,
        sql="ok",
        row_count=0,
        column_names=["X"],
        rows_sample=[],
    )
    fp = build_result_fingerprint(er)
    assert fp.row_count == 0
    assert fp.column_count == 1
    assert fp.null_ratios == {}


def test_mixed_types_skip_stats():
    """Non-numeric columns should not produce numeric stats."""
    er = ExecutionResult(
        success=True,
        sql="ok",
        row_count=2,
        column_names=["NAME", "COUNT"],
        rows_sample=[("Alice", 5), ("Bob", 10)],
    )
    fp = build_result_fingerprint(er)
    assert "NAME" not in fp.numeric_stats
    assert "COUNT" in fp.numeric_stats
