"""Tests for observability.training_data.build_verifier_dataset."""

import json
import tempfile
from pathlib import Path

from rag_snow_agent.observability.training_data import build_verifier_dataset


def _make_record(**overrides):
    base = {
        "instruction": "total sessions by month",
        "execution_success": True,
        "is_best": False,
        "repairs_count": 0,
        "error_type": None,
        "row_count": 12,
        "final_sql": "SELECT month, SUM(sessions) FROM t GROUP BY month",
        "metamorphic": {"score_delta": 0.0},
        "score": 100.0,
    }
    base.update(overrides)
    return base


def test_build_from_single_jsonl():
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "experiment1.jsonl"
        records = [
            _make_record(is_best=True, execution_success=True),
            _make_record(is_best=False, execution_success=True),
            _make_record(is_best=False, execution_success=False),
        ]
        with open(jsonl_path, "w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        rows = build_verifier_dataset(tmpdir)

        assert len(rows) == 3
        # First record: is_best=True and exec_success=True => label=1
        assert rows[0]["label"] == 1
        # Second: not best => label=0
        assert rows[1]["label"] == 0
        # Third: not successful => label=0
        assert rows[2]["label"] == 0


def test_build_labels_require_both_best_and_success():
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "test.jsonl"
        records = [
            _make_record(is_best=True, execution_success=False),  # best but failed
            _make_record(is_best=False, execution_success=True),  # success but not best
        ]
        with open(jsonl_path, "w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        rows = build_verifier_dataset(tmpdir)
        assert rows[0]["label"] == 0  # best but failed
        assert rows[1]["label"] == 0  # success but not best


def test_empty_dir_returns_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        rows = build_verifier_dataset(tmpdir)
        assert rows == []


def test_skips_blank_lines_and_bad_json():
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "messy.jsonl"
        with open(jsonl_path, "w") as fh:
            fh.write(json.dumps(_make_record()) + "\n")
            fh.write("\n")  # blank line
            fh.write("not valid json\n")  # bad json
            fh.write(json.dumps(_make_record(is_best=True)) + "\n")

        rows = build_verifier_dataset(tmpdir)
        assert len(rows) == 2


def test_all_features_numeric():
    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "test.jsonl"
        with open(jsonl_path, "w") as fh:
            fh.write(json.dumps(_make_record()) + "\n")

        rows = build_verifier_dataset(tmpdir)
        assert len(rows) == 1
        for key, val in rows[0].items():
            assert isinstance(val, (int, float)), f"Feature {key!r} is not numeric: {val!r}"
