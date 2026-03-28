"""Aggregate metrics from instance_results.jsonl.

CLI usage::

    uv run python -m rag_snow_agent.eval.aggregate_metrics \
      --experiment_dir reports/experiments/ablation_v1
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import Counter
from pathlib import Path

log = logging.getLogger(__name__)


def load_instance_results(experiment_dir: Path) -> list[dict]:
    """Load instance_results.jsonl from an experiment directory."""
    path = experiment_dir / "instance_results.jsonl"
    if not path.exists():
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def compute_metrics(records: list[dict]) -> dict:
    """Compute aggregate metrics from instance result records."""
    total = len(records)
    if total == 0:
        return {
            "total_instances": 0,
            "success_count": 0,
            "accuracy_pct": 0.0,
            "avg_llm_calls": 0.0,
            "median_llm_calls": 0.0,
            "p95_llm_calls": 0.0,
            "avg_repairs": 0.0,
            "failure_taxonomy": {},
            "candidate_count_distribution": {},
            "memory_hit_rate": None,
        }

    success_count = sum(1 for r in records if r.get("success"))
    accuracy_pct = round(100.0 * success_count / total, 2)

    llm_calls = [r.get("llm_calls", 0) for r in records]
    repair_counts = [r.get("repair_count", 0) for r in records]

    avg_llm = round(statistics.mean(llm_calls), 2) if llm_calls else 0.0
    median_llm = round(statistics.median(llm_calls), 2) if llm_calls else 0.0

    # p95
    sorted_llm = sorted(llm_calls)
    p95_idx = int(0.95 * len(sorted_llm))
    p95_idx = min(p95_idx, len(sorted_llm) - 1)
    p95_llm = float(sorted_llm[p95_idx]) if sorted_llm else 0.0

    avg_repairs = round(statistics.mean(repair_counts), 2) if repair_counts else 0.0

    # Failure taxonomy
    error_types = [
        r.get("error_type", "unknown")
        for r in records
        if not r.get("success")
    ]
    failure_taxonomy = dict(Counter(error_types))

    # Candidate count distribution
    candidate_counts = [r.get("candidate_count", 1) for r in records]
    candidate_dist = dict(Counter(str(c) for c in candidate_counts))

    # Memory hit rate
    memory_hits = [r.get("memory_hit") for r in records if r.get("memory_hit") is not None]
    memory_hit_rate = (
        round(sum(1 for h in memory_hits if h) / len(memory_hits), 3)
        if memory_hits
        else None
    )

    return {
        "total_instances": total,
        "success_count": success_count,
        "accuracy_pct": accuracy_pct,
        "avg_llm_calls": avg_llm,
        "median_llm_calls": median_llm,
        "p95_llm_calls": p95_llm,
        "avg_repairs": avg_repairs,
        "failure_taxonomy": failure_taxonomy,
        "candidate_count_distribution": candidate_dist,
        "memory_hit_rate": memory_hit_rate,
    }


def write_metrics(experiment_dir: Path, metrics: dict) -> Path:
    """Write metrics.json to experiment directory."""
    path = experiment_dir / "metrics.json"
    path.write_text(json.dumps(metrics, indent=2) + "\n")
    log.info("Wrote metrics: %s", path)
    return path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Aggregate experiment metrics")
    parser.add_argument("--experiment_dir", required=True, help="Path to experiment directory")
    args = parser.parse_args()

    experiment_dir = Path(args.experiment_dir)
    records = load_instance_results(experiment_dir)
    metrics = compute_metrics(records)
    write_metrics(experiment_dir, metrics)

    print(f"Metrics for {experiment_dir.name}:")
    print(f"  Total: {metrics['total_instances']}")
    print(f"  Accuracy: {metrics['accuracy_pct']}%")
    print(f"  Avg LLM calls: {metrics['avg_llm_calls']}")
    print(f"  Avg repairs: {metrics['avg_repairs']}")


if __name__ == "__main__":
    main()
