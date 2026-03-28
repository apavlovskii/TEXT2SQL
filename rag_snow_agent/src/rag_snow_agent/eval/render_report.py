"""Render a markdown report from experiment results.

CLI usage::

    uv run python -m rag_snow_agent.eval.render_report \
      --experiment_dir reports/experiments/ablation_v1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def render_report(experiment_dir: Path) -> str:
    """Generate REPORT.md content for an experiment."""
    manifest = _load_json(experiment_dir / "manifest.json")
    metrics = _load_json(experiment_dir / "metrics.json")
    instances = _load_jsonl(experiment_dir / "instance_results.jsonl")

    lines: list[str] = []
    lines.append(f"# Experiment Report: {experiment_dir.name}")
    lines.append("")

    # Config snapshot
    if manifest:
        lines.append("## Configuration")
        lines.append("")
        lines.append(f"- **Timestamp**: {manifest.get('timestamp', 'N/A')}")
        lines.append(f"- **Git Commit**: `{manifest.get('git_commit', 'N/A')}`")
        lines.append(f"- **Model**: {manifest.get('model', 'N/A')}")
        lines.append(f"- **Limit**: {manifest.get('limit', 'N/A')}")
        lines.append("")
        toggles = manifest.get("toggles", {})
        if toggles:
            lines.append("### Ablation Toggles")
            lines.append("")
            for k, v in toggles.items():
                lines.append(f"- `{k}`: {v}")
            lines.append("")

    # Summary metrics
    if metrics:
        lines.append("## Summary Metrics")
        lines.append("")
        lines.append(f"| Metric | Value |")
        lines.append(f"| --- | --- |")
        lines.append(f"| Total Instances | {metrics.get('total_instances', 0)} |")
        lines.append(f"| Successes | {metrics.get('success_count', 0)} |")
        lines.append(f"| Accuracy | {metrics.get('accuracy_pct', 0):.2f}% |")
        lines.append(f"| Avg LLM Calls | {metrics.get('avg_llm_calls', 0):.2f} |")
        lines.append(f"| Median LLM Calls | {metrics.get('median_llm_calls', 0):.2f} |")
        lines.append(f"| P95 LLM Calls | {metrics.get('p95_llm_calls', 0):.1f} |")
        lines.append(f"| Avg Repairs | {metrics.get('avg_repairs', 0):.2f} |")
        lines.append("")

    # Top token-consuming queries
    if instances:
        sorted_by_llm = sorted(instances, key=lambda r: r.get("llm_calls", 0), reverse=True)
        top_n = sorted_by_llm[:5]
        if top_n and any(r.get("llm_calls", 0) > 0 for r in top_n):
            lines.append("## Top Token-Consuming Queries")
            lines.append("")
            lines.append("| Instance | LLM Calls | Repairs | Success |")
            lines.append("| --- | --- | --- | --- |")
            for r in top_n:
                lines.append(
                    f"| {r.get('instance_id', 'N/A')} "
                    f"| {r.get('llm_calls', 0)} "
                    f"| {r.get('repair_count', 0)} "
                    f"| {'Yes' if r.get('success') else 'No'} |"
                )
            lines.append("")

    # Failure categories
    if metrics and metrics.get("failure_taxonomy"):
        lines.append("## Failure Categories")
        lines.append("")
        lines.append("| Error Type | Count |")
        lines.append("| --- | --- |")
        for error_type, count in sorted(
            metrics["failure_taxonomy"].items(), key=lambda x: x[1], reverse=True
        ):
            lines.append(f"| {error_type} | {count} |")
        lines.append("")

    # Note about Spider2 evaluation
    lines.append("## Evaluation")
    lines.append("")
    lines.append(
        "For official accuracy numbers, run the Spider2 evaluation suite "
        "against the generated result.json files. See `README_SPIDER2.md` for details."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Render experiment report")
    parser.add_argument("--experiment_dir", required=True, help="Path to experiment directory")
    args = parser.parse_args()

    experiment_dir = Path(args.experiment_dir)
    if not experiment_dir.exists():
        print(f"ERROR: {experiment_dir} not found", file=sys.stderr)
        sys.exit(1)

    report = render_report(experiment_dir)
    report_path = experiment_dir / "REPORT.md"
    report_path.write_text(report)
    print(f"Wrote report: {report_path}")


if __name__ == "__main__":
    main()
