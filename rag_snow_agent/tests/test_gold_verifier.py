"""Tests for gold_verifier comparison logic."""

from __future__ import annotations

import json
import math
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rag_snow_agent.eval.gold_verifier import (
    GoldMatchResult,
    _compare_multi,
    _compare_tables,
    load_eval_standards,
    verify_against_gold,
)
from rag_snow_agent.snowflake.executor import ExecutionResult


# ── _compare_tables tests ────────────────────────────────────────────────────


def test_compare_tables_matching():
    """Matching DataFrames should return True."""
    pred = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    gold = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert _compare_tables(pred, gold) is True


def test_compare_tables_non_matching():
    """Non-matching DataFrames should return False."""
    pred = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    gold = pd.DataFrame({"a": [7, 8, 9], "b": [10, 11, 12]})
    assert _compare_tables(pred, gold) is False


def test_compare_tables_float_tolerance():
    """Close float values should match within tolerance (1e-2)."""
    pred = pd.DataFrame({"x": [1.001, 2.005, 3.009]})
    gold = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    assert _compare_tables(pred, gold) is True


def test_compare_tables_float_beyond_tolerance():
    """Float values beyond tolerance should not match."""
    pred = pd.DataFrame({"x": [1.02, 2.0, 3.0]})
    gold = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    assert _compare_tables(pred, gold) is False


def test_compare_tables_with_nan():
    """NaN values should be normalized to 0 and compared."""
    pred = pd.DataFrame({"a": [1, float("nan"), 3]})
    gold = pd.DataFrame({"a": [1, float("nan"), 3]})
    assert _compare_tables(pred, gold) is True


def test_compare_tables_ignore_order():
    """With ignore_order=True, row order should not matter."""
    pred = pd.DataFrame({"a": [3, 1, 2]})
    gold = pd.DataFrame({"a": [1, 2, 3]})
    assert _compare_tables(pred, gold, ignore_order=True) is True


def test_compare_tables_condition_cols():
    """Only specified condition columns should be checked."""
    pred = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    gold = pd.DataFrame({"x": [1, 2, 3], "y": [99, 99, 99]})
    # Only check column 0 of gold (which is [1,2,3])
    assert _compare_tables(pred, gold, condition_cols=[0]) is True


def test_compare_tables_different_lengths():
    """DataFrames with different row counts should not match."""
    pred = pd.DataFrame({"a": [1, 2]})
    gold = pd.DataFrame({"a": [1, 2, 3]})
    assert _compare_tables(pred, gold) is False


# ── _compare_multi tests ─────────────────────────────────────────────────────


def test_compare_multi_first_matches():
    """Multi-compare returns True if first gold matches."""
    pred = pd.DataFrame({"a": [1, 2]})
    gold1 = pd.DataFrame({"a": [1, 2]})
    gold2 = pd.DataFrame({"a": [9, 9]})
    assert _compare_multi(pred, [gold1, gold2]) is True


def test_compare_multi_second_matches():
    """Multi-compare returns True if second gold matches."""
    pred = pd.DataFrame({"a": [1, 2]})
    gold1 = pd.DataFrame({"a": [9, 9]})
    gold2 = pd.DataFrame({"a": [1, 2]})
    assert _compare_multi(pred, [gold1, gold2]) is True


def test_compare_multi_none_match():
    """Multi-compare returns False if no gold matches."""
    pred = pd.DataFrame({"a": [1, 2]})
    gold1 = pd.DataFrame({"a": [9, 9]})
    gold2 = pd.DataFrame({"a": [8, 8]})
    assert _compare_multi(pred, [gold1, gold2]) is False


# ── verify_against_gold tests ────────────────────────────────────────────────


def test_verify_no_eval_standard():
    """Returns error when instance_id not in eval standards."""
    executor = MagicMock()
    result = verify_against_gold(
        instance_id="missing_id",
        sql="SELECT 1",
        db_id="TESTDB",
        executor=executor,
        gold_dir="/nonexistent",
        eval_standards={},
    )
    assert result.matched is False
    assert result.error == "no_eval_standard"


def test_verify_no_gold_file():
    """Returns error when gold directory has no matching CSV."""
    with tempfile.TemporaryDirectory() as tmpdir:
        gold_dir = Path(tmpdir)
        exec_result_dir = gold_dir / "exec_result"
        exec_result_dir.mkdir()

        eval_standards = {
            "test_001": {"instance_id": "test_001", "condition_cols": [], "ignore_order": False}
        }
        executor = MagicMock()
        result = verify_against_gold(
            instance_id="test_001",
            sql="SELECT 1",
            db_id="TESTDB",
            executor=executor,
            gold_dir=gold_dir,
            eval_standards=eval_standards,
        )
        assert result.matched is False
        assert result.error == "no_gold_file"


def test_verify_empty_result():
    """Returns empty_result error when execution returns no rows."""
    with tempfile.TemporaryDirectory() as tmpdir:
        gold_dir = Path(tmpdir)
        exec_result_dir = gold_dir / "exec_result"
        exec_result_dir.mkdir()
        pd.DataFrame({"a": [1]}).to_csv(exec_result_dir / "test_001.csv", index=False)

        eval_standards = {
            "test_001": {"instance_id": "test_001", "condition_cols": [], "ignore_order": False}
        }
        executor = MagicMock()
        executor.execute.return_value = ExecutionResult(
            success=True, sql="SELECT 1", row_count=0,
            rows_sample=[], column_names=["a"],
        )
        result = verify_against_gold(
            instance_id="test_001",
            sql="SELECT 1",
            db_id="TESTDB",
            executor=executor,
            gold_dir=gold_dir,
            eval_standards=eval_standards,
        )
        assert result.matched is False
        assert result.error == "empty_result"


def test_verify_matching_result():
    """Returns matched=True when results match gold."""
    with tempfile.TemporaryDirectory() as tmpdir:
        gold_dir = Path(tmpdir)
        exec_result_dir = gold_dir / "exec_result"
        exec_result_dir.mkdir()
        pd.DataFrame({"a": [1, 2, 3]}).to_csv(exec_result_dir / "test_001.csv", index=False)

        eval_standards = {
            "test_001": {"instance_id": "test_001", "condition_cols": [], "ignore_order": False}
        }
        executor = MagicMock()
        executor.execute.return_value = ExecutionResult(
            success=True, sql="SELECT a FROM t",
            row_count=3,
            rows_sample=[(1,), (2,), (3,)],
            column_names=["a"],
        )
        result = verify_against_gold(
            instance_id="test_001",
            sql="SELECT a FROM t",
            db_id="TESTDB",
            executor=executor,
            gold_dir=gold_dir,
            eval_standards=eval_standards,
        )
        assert result.matched is True
        assert result.pred_rows == 3
        assert result.gold_rows == 3


def test_verify_mismatching_result():
    """Returns matched=False with result_mismatch when results differ."""
    with tempfile.TemporaryDirectory() as tmpdir:
        gold_dir = Path(tmpdir)
        exec_result_dir = gold_dir / "exec_result"
        exec_result_dir.mkdir()
        pd.DataFrame({"a": [1, 2, 3]}).to_csv(exec_result_dir / "test_001.csv", index=False)

        eval_standards = {
            "test_001": {"instance_id": "test_001", "condition_cols": [], "ignore_order": False}
        }
        executor = MagicMock()
        executor.execute.return_value = ExecutionResult(
            success=True, sql="SELECT a FROM t",
            row_count=3,
            rows_sample=[(10,), (20,), (30,)],
            column_names=["a"],
        )
        result = verify_against_gold(
            instance_id="test_001",
            sql="SELECT a FROM t",
            db_id="TESTDB",
            executor=executor,
            gold_dir=gold_dir,
            eval_standards=eval_standards,
        )
        assert result.matched is False
        assert result.error == "result_mismatch"


def test_load_eval_standards():
    """Loads eval standards from a JSONL file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "eval.jsonl"
        with open(jsonl_path, "w") as f:
            f.write(json.dumps({"instance_id": "sf001", "condition_cols": [0], "ignore_order": True}) + "\n")
            f.write(json.dumps({"instance_id": "sf002", "condition_cols": [], "ignore_order": False}) + "\n")

        standards = load_eval_standards(jsonl_path)
        assert len(standards) == 2
        assert standards["sf001"]["ignore_order"] is True
        assert standards["sf002"]["condition_cols"] == []


def test_load_eval_standards_missing_file():
    """Returns empty dict for missing file."""
    standards = load_eval_standards(Path("/nonexistent/path.jsonl"))
    assert standards == {}
