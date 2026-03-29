"""Verify SQL results against gold CSVs from Spider2 evaluation suite."""

from __future__ import annotations

import json
import logging
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Default gold directories
DEFAULT_GOLD_DIR = Path("Spider2/spider2-snow/evaluation_suite/gold")
DEFAULT_EVAL_JSONL = DEFAULT_GOLD_DIR / "spider2snow_eval.jsonl"


@dataclass
class GoldMatchResult:
    matched: bool
    instance_id: str
    error: str | None = None  # e.g. "empty_result", "result_mismatch", "no_gold_file"
    pred_rows: int | None = None
    gold_rows: int | None = None
    details: str | None = None  # brief mismatch description


def load_eval_standards(eval_jsonl: Path) -> dict:
    """Load evaluation standards (condition_cols, ignore_order per instance)."""
    standards: dict = {}
    if not eval_jsonl.exists():
        return standards
    with open(eval_jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            standards[item["instance_id"]] = item
    return standards


def _normalize(value):
    """Normalize NaN/None to 0, matching Spider2 evaluate.py logic."""
    if pd.isna(value):
        return 0
    return value


def _vectors_match(v1: list, v2: list, tol: float = 1e-2, ignore_order: bool = False) -> bool:
    """Compare two vectors with tolerance and optional order-ignoring."""
    v1 = [_normalize(x) for x in v1]
    v2 = [_normalize(x) for x in v2]
    if ignore_order:
        v1 = sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float))))
        v2 = sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float))))
    if len(v1) != len(v2):
        return False
    for a, b in zip(v1, v2):
        if pd.isna(a) and pd.isna(b):
            continue
        elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
            if not math.isclose(float(a), float(b), abs_tol=tol):
                return False
        elif a != b:
            return False
    return True


def _compare_tables(
    pred: pd.DataFrame,
    gold: pd.DataFrame,
    condition_cols: list | None = None,
    ignore_order: bool = False,
) -> bool:
    """Compare predicted DataFrame against gold, using Spider2's column-transpose logic.

    Replicates ``compare_pandas_table`` from Spider2's evaluate.py:
    - If *condition_cols* is provided, only those gold columns are checked.
    - Each transposed gold column vector must match at least one transposed pred column vector.
    """
    tolerance = 1e-2

    if condition_cols is not None and condition_cols != []:
        if not isinstance(condition_cols, (list, tuple)):
            condition_cols = [condition_cols]
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold
    pred_cols = pred

    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()

    for gold_vec in t_gold_list:
        if not any(
            _vectors_match(gold_vec, pred_vec, tol=tolerance, ignore_order=ignore_order)
            for pred_vec in t_pred_list
        ):
            return False
    return True


def _compare_multi(
    pred: pd.DataFrame,
    gold_dfs: list[pd.DataFrame],
    condition_cols: list | None = None,
    ignore_order: bool = False,
) -> bool:
    """Compare pred against multiple gold DataFrames (any match wins).

    Replicates ``compare_multi_pandas_table`` from Spider2's evaluate.py.
    """
    if (
        condition_cols is None
        or condition_cols == []
        or condition_cols == [[]]
        or condition_cols == [None]
    ):
        multi_condition_cols = [[] for _ in range(len(gold_dfs))]
    elif len(gold_dfs) > 1 and not all(isinstance(sublist, list) for sublist in condition_cols):
        multi_condition_cols = [condition_cols for _ in range(len(gold_dfs))]
    else:
        multi_condition_cols = condition_cols  # type: ignore[assignment]

    for i, gold in enumerate(gold_dfs):
        cols = multi_condition_cols[i] if i < len(multi_condition_cols) else []  # type: ignore[index]
        if _compare_tables(pred, gold, cols, ignore_order):
            return True
    return False


def verify_against_gold(
    instance_id: str,
    sql: str,
    db_id: str,
    executor,  # SnowflakeExecutor
    gold_dir: Path | str | None = None,
    eval_standards: dict | None = None,
) -> GoldMatchResult:
    """Execute SQL and compare results against gold CSV.

    Returns GoldMatchResult with matched=True if results match gold.
    """
    gold_path = Path(gold_dir) if gold_dir else DEFAULT_GOLD_DIR
    exec_result_dir = gold_path / "exec_result"

    # Load eval standards if not provided
    if eval_standards is None:
        eval_standards = load_eval_standards(gold_path / "spider2snow_eval.jsonl")

    standard = eval_standards.get(instance_id)
    if standard is None:
        return GoldMatchResult(matched=False, instance_id=instance_id, error="no_eval_standard")

    # Find gold CSV(s)
    if not exec_result_dir.exists():
        return GoldMatchResult(matched=False, instance_id=instance_id, error="no_gold_file")

    pattern = re.compile(rf'^{re.escape(instance_id)}(_[a-z])?\.csv$')
    gold_csvs = sorted(f for f in os.listdir(exec_result_dir) if pattern.match(f))
    if not gold_csvs:
        return GoldMatchResult(matched=False, instance_id=instance_id, error="no_gold_file")

    # Execute SQL
    exec_result = executor.execute(sql, sample_rows=10000)
    if not exec_result.success:
        return GoldMatchResult(
            matched=False, instance_id=instance_id,
            error="execution_error", details=exec_result.error_message,
        )

    # Build pred DataFrame
    if not exec_result.rows_sample or not exec_result.column_names:
        return GoldMatchResult(
            matched=False, instance_id=instance_id,
            error="empty_result", pred_rows=0,
        )

    pred_df = pd.DataFrame(exec_result.rows_sample, columns=exec_result.column_names)
    if pred_df.empty:
        return GoldMatchResult(
            matched=False, instance_id=instance_id,
            error="empty_result", pred_rows=0,
        )

    # Compare against gold
    condition_cols = standard.get("condition_cols", [])
    ignore_order = standard.get("ignore_order", False)

    gold_dfs = [pd.read_csv(exec_result_dir / f) for f in gold_csvs]

    # Use compare logic from Spider2 evaluate.py
    matched = _compare_multi(pred_df, gold_dfs, condition_cols, ignore_order)

    gold_rows = gold_dfs[0].shape[0] if gold_dfs else None

    if matched:
        return GoldMatchResult(
            matched=True, instance_id=instance_id,
            pred_rows=pred_df.shape[0], gold_rows=gold_rows,
        )
    else:
        return GoldMatchResult(
            matched=False, instance_id=instance_id,
            error="result_mismatch", pred_rows=pred_df.shape[0],
            gold_rows=gold_rows,
            details=f"pred_shape={pred_df.shape}, gold_shape={gold_dfs[0].shape}",
        )
