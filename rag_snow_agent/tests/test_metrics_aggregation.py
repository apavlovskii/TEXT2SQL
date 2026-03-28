"""Tests for metrics aggregation from instance_results.jsonl."""

import json
import tempfile
from pathlib import Path

from rag_snow_agent.eval.aggregate_metrics import compute_metrics, write_metrics


def _write_fake_results(tmp_dir: Path, records: list[dict]) -> Path:
    """Write fake instance_results.jsonl."""
    path = tmp_dir / "instance_results.jsonl"
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return path


def test_compute_metrics_basic():
    records = [
        {"instance_id": "a", "success": True, "llm_calls": 3, "repair_count": 1, "candidate_count": 2},
        {"instance_id": "b", "success": True, "llm_calls": 5, "repair_count": 2, "candidate_count": 2},
        {"instance_id": "c", "success": False, "llm_calls": 4, "repair_count": 2, "candidate_count": 1, "error_type": "timeout"},
        {"instance_id": "d", "success": False, "llm_calls": 2, "repair_count": 0, "candidate_count": 1, "error_type": "syntax"},
    ]
    metrics = compute_metrics(records)

    assert metrics["total_instances"] == 4
    assert metrics["success_count"] == 2
    assert metrics["accuracy_pct"] == 50.0
    assert metrics["avg_llm_calls"] == 3.5
    assert metrics["avg_repairs"] == 1.25


def test_compute_metrics_empty():
    metrics = compute_metrics([])
    assert metrics["total_instances"] == 0
    assert metrics["accuracy_pct"] == 0.0


def test_compute_metrics_all_success():
    records = [
        {"instance_id": "x", "success": True, "llm_calls": 2, "repair_count": 0, "candidate_count": 1},
        {"instance_id": "y", "success": True, "llm_calls": 4, "repair_count": 1, "candidate_count": 1},
    ]
    metrics = compute_metrics(records)
    assert metrics["accuracy_pct"] == 100.0
    assert metrics["success_count"] == 2
    assert metrics["failure_taxonomy"] == {}


def test_compute_metrics_failure_taxonomy():
    records = [
        {"instance_id": "a", "success": False, "llm_calls": 1, "repair_count": 0, "error_type": "timeout"},
        {"instance_id": "b", "success": False, "llm_calls": 1, "repair_count": 0, "error_type": "timeout"},
        {"instance_id": "c", "success": False, "llm_calls": 1, "repair_count": 0, "error_type": "syntax"},
    ]
    metrics = compute_metrics(records)
    assert metrics["failure_taxonomy"]["timeout"] == 2
    assert metrics["failure_taxonomy"]["syntax"] == 1


def test_compute_metrics_p95():
    records = [
        {"instance_id": str(i), "success": True, "llm_calls": i, "repair_count": 0, "candidate_count": 1}
        for i in range(1, 21)
    ]
    metrics = compute_metrics(records)
    # 20 items, p95 index = int(0.95 * 20) = 19, value = 20
    assert metrics["p95_llm_calls"] == 20.0


def test_compute_metrics_candidate_distribution():
    records = [
        {"instance_id": "a", "success": True, "llm_calls": 2, "repair_count": 0, "candidate_count": 1},
        {"instance_id": "b", "success": True, "llm_calls": 4, "repair_count": 1, "candidate_count": 2},
        {"instance_id": "c", "success": True, "llm_calls": 3, "repair_count": 0, "candidate_count": 2},
    ]
    metrics = compute_metrics(records)
    assert metrics["candidate_count_distribution"]["1"] == 1
    assert metrics["candidate_count_distribution"]["2"] == 2


def test_write_metrics_creates_file():
    with tempfile.TemporaryDirectory() as tmp:
        exp_dir = Path(tmp)
        metrics = {"total_instances": 5, "accuracy_pct": 80.0}
        path = write_metrics(exp_dir, metrics)

        assert path.exists()
        data = json.loads(path.read_text())
        assert data["total_instances"] == 5
        assert data["accuracy_pct"] == 80.0


def test_median_llm_calls():
    records = [
        {"instance_id": "a", "success": True, "llm_calls": 1, "repair_count": 0},
        {"instance_id": "b", "success": True, "llm_calls": 3, "repair_count": 0},
        {"instance_id": "c", "success": True, "llm_calls": 10, "repair_count": 0},
    ]
    metrics = compute_metrics(records)
    assert metrics["median_llm_calls"] == 3.0
