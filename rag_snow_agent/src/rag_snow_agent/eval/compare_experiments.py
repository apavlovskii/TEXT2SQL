"""Compare metrics across experiments.

CLI usage::

    uv run python -m rag_snow_agent.eval.compare_experiments \
      --experiments reports/experiments/baseline reports/experiments/full_system
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def load_metrics(experiment_dir: Path) -> dict:
    """Load metrics.json from an experiment directory."""
    path = experiment_dir / "metrics.json"
    if not path.exists():
        print(f"WARNING: metrics.json not found in {experiment_dir}", file=sys.stderr)
        return {}
    with open(path) as f:
        return json.load(f)


def format_delta(value: float) -> str:
    """Format a delta value with sign."""
    if value > 0:
        return f"+{value:.2f}"
    return f"{value:.2f}"


def build_comparison_table(experiment_dirs: list[Path]) -> str:
    """Build a markdown comparison table from experiment directories."""
    all_metrics = []
    names = []
    for d in experiment_dirs:
        m = load_metrics(d)
        all_metrics.append(m)
        names.append(d.name)

    if len(all_metrics) < 2:
        return "Need at least 2 experiments to compare."

    # Header
    cols = ["Metric"] + names + ["Delta (last - first)"]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"

    rows = []
    comparison_keys = [
        ("accuracy_pct", "Accuracy (%)", "{:.2f}"),
        ("avg_llm_calls", "Avg LLM Calls", "{:.2f}"),
        ("median_llm_calls", "Median LLM Calls", "{:.2f}"),
        ("p95_llm_calls", "P95 LLM Calls", "{:.1f}"),
        ("avg_repairs", "Avg Repairs", "{:.2f}"),
        ("total_instances", "Total Instances", "{:.0f}"),
        ("success_count", "Success Count", "{:.0f}"),
    ]

    for key, label, fmt in comparison_keys:
        vals = [m.get(key, 0) for m in all_metrics]
        formatted = [fmt.format(v) for v in vals]
        delta = vals[-1] - vals[0]
        row = f"| {label} | " + " | ".join(formatted) + f" | {format_delta(delta)} |"
        rows.append(row)

    return "\n".join([header, sep] + rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Compare experiment metrics")
    parser.add_argument(
        "--experiments",
        nargs="+",
        required=True,
        help="Paths to experiment directories",
    )
    args = parser.parse_args()

    dirs = [Path(d) for d in args.experiments]
    table = build_comparison_table(dirs)
    print(table)


if __name__ == "__main__":
    main()
