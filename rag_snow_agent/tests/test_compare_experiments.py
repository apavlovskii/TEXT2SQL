"""Tests for experiment comparison."""

import json
import tempfile
from pathlib import Path

from rag_snow_agent.eval.compare_experiments import build_comparison_table, load_metrics


def _write_metrics(tmp_dir: Path, name: str, metrics: dict) -> Path:
    """Write a fake metrics.json in a sub-directory."""
    exp_dir = tmp_dir / name
    exp_dir.mkdir(parents=True, exist_ok=True)
    path = exp_dir / "metrics.json"
    path.write_text(json.dumps(metrics) + "\n")
    return exp_dir


def test_comparison_table_includes_deltas():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        dir_a = _write_metrics(tmp_path, "baseline", {
            "total_instances": 25,
            "success_count": 10,
            "accuracy_pct": 40.0,
            "avg_llm_calls": 3.0,
            "median_llm_calls": 2.5,
            "p95_llm_calls": 6.0,
            "avg_repairs": 1.5,
        })
        dir_b = _write_metrics(tmp_path, "full_system", {
            "total_instances": 25,
            "success_count": 15,
            "accuracy_pct": 60.0,
            "avg_llm_calls": 5.0,
            "median_llm_calls": 4.0,
            "p95_llm_calls": 10.0,
            "avg_repairs": 2.0,
        })

        table = build_comparison_table([dir_a, dir_b])

        # Check accuracy delta: 60 - 40 = +20
        assert "+20.00" in table
        # Check avg_llm_calls delta: 5.0 - 3.0 = +2.0
        assert "+2.00" in table
        # Check column headers
        assert "baseline" in table
        assert "full_system" in table
        assert "Accuracy" in table


def test_comparison_table_negative_delta():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        dir_a = _write_metrics(tmp_path, "high", {
            "total_instances": 10,
            "success_count": 8,
            "accuracy_pct": 80.0,
            "avg_llm_calls": 5.0,
            "median_llm_calls": 4.0,
            "p95_llm_calls": 8.0,
            "avg_repairs": 2.0,
        })
        dir_b = _write_metrics(tmp_path, "low", {
            "total_instances": 10,
            "success_count": 4,
            "accuracy_pct": 40.0,
            "avg_llm_calls": 3.0,
            "median_llm_calls": 2.0,
            "p95_llm_calls": 5.0,
            "avg_repairs": 1.0,
        })

        table = build_comparison_table([dir_a, dir_b])

        # Accuracy delta: 40 - 80 = -40
        assert "-40.00" in table


def test_comparison_needs_two_experiments():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        dir_a = _write_metrics(tmp_path, "single", {
            "total_instances": 10,
            "accuracy_pct": 50.0,
        })

        table = build_comparison_table([dir_a])
        assert "Need at least 2" in table


def test_load_metrics_missing_file():
    with tempfile.TemporaryDirectory() as tmp:
        result = load_metrics(Path(tmp))
        assert result == {}


def test_comparison_three_experiments():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        dirs = []
        for name, acc in [("a", 30.0), ("b", 50.0), ("c", 70.0)]:
            d = _write_metrics(tmp_path, name, {
                "total_instances": 10,
                "success_count": int(acc / 10),
                "accuracy_pct": acc,
                "avg_llm_calls": 3.0,
                "median_llm_calls": 3.0,
                "p95_llm_calls": 5.0,
                "avg_repairs": 1.0,
            })
            dirs.append(d)

        table = build_comparison_table(dirs)
        # Delta is last - first: 70 - 30 = +40
        assert "+40.00" in table
