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


def _percentile(sorted_vals: list, pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(pct * len(sorted_vals))
    idx = min(idx, len(sorted_vals) - 1)
    return float(sorted_vals[idx])


def _tele(r: dict) -> dict:
    return r.get("telemetry") or {}


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
            "total_tokens": 0,
            "avg_total_tokens": 0.0,
            "median_total_tokens": 0.0,
            "p95_total_tokens": 0.0,
            "avg_wall_clock_sec": 0.0,
            "component_activation_rate": {},
        }

    success_count = sum(1 for r in records if r.get("success"))
    accuracy_pct = round(100.0 * success_count / total, 2)

    llm_calls = [r.get("llm_calls", 0) for r in records]
    repair_counts = [r.get("repair_count", 0) for r in records]

    avg_llm = round(statistics.mean(llm_calls), 2) if llm_calls else 0.0
    median_llm = round(statistics.median(llm_calls), 2) if llm_calls else 0.0

    sorted_llm = sorted(llm_calls)
    p95_llm = _percentile(sorted_llm, 0.95)

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

    # Memory hit rate (legacy field; telemetry block also carries memory_hit)
    memory_hits = [r.get("memory_hit") for r in records if r.get("memory_hit") is not None]
    memory_hit_rate = (
        round(sum(1 for h in memory_hits if h) / len(memory_hits), 3)
        if memory_hits else None
    )

    # ── Token + cost telemetry ─────────────────────────────────────────
    total_tokens_per = [_tele(r).get("total_tokens", 0) for r in records]
    prompt_tokens_per = [_tele(r).get("prompt_tokens", 0) for r in records]
    completion_tokens_per = [_tele(r).get("completion_tokens", 0) for r in records]
    wall_clock_per = [_tele(r).get("wall_clock_sec", 0.0) for r in records]

    total_tokens_sum = sum(total_tokens_per)
    avg_total_tokens = round(statistics.mean(total_tokens_per), 2) if total_tokens_per else 0.0
    median_total_tokens = round(statistics.median(total_tokens_per), 2) if total_tokens_per else 0.0
    p95_total_tokens = _percentile(sorted(total_tokens_per), 0.95)
    avg_wall_clock = round(statistics.mean(wall_clock_per), 2) if wall_clock_per else 0.0

    tokens_per_success = round(total_tokens_sum / success_count, 1) if success_count else 0.0

    # ── Component activation rates ─────────────────────────────────────
    flags_to_count = [
        "best_of_n_used",
        "semantic_used",
        "sample_used",
        "external_knowledge_injected",
        "verifier_used",
        "date_shard_rewrite_used",
        "join_graph_used",
        "geo_routed",
        "memory_hit",
    ]
    activation = {}
    for fn in flags_to_count:
        # best_of_n_used is on the top-level record; others are inside telemetry
        if fn == "best_of_n_used":
            hits = sum(1 for r in records if r.get(fn))
        else:
            hits = sum(1 for r in records if _tele(r).get(fn))
        activation[fn] = {
            "count": hits,
            "rate": round(hits / total, 3),
        }

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
        "total_tokens": total_tokens_sum,
        "total_prompt_tokens": sum(prompt_tokens_per),
        "total_completion_tokens": sum(completion_tokens_per),
        "avg_total_tokens": avg_total_tokens,
        "median_total_tokens": median_total_tokens,
        "p95_total_tokens": p95_total_tokens,
        "tokens_per_success": tokens_per_success,
        "avg_wall_clock_sec": avg_wall_clock,
        "total_wall_clock_sec": round(sum(wall_clock_per), 1),
        "component_activation_rate": activation,
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
