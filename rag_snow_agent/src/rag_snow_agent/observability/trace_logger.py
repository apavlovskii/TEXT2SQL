"""Log candidate records to JSONL files for later verifier training."""

from __future__ import annotations

import json
from pathlib import Path


def log_candidate_records(
    experiment: str,
    candidates: list[dict],
    base_dir: str = "reports/candidate_logs",
) -> Path:
    """Append candidate records to a JSONL file for later training.

    Writes to ``base_dir/experiment.jsonl``, one JSON line per candidate.
    Returns the path to the written file.
    """
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)

    out_path = base / f"{experiment}.jsonl"
    with open(out_path, "a") as fh:
        for candidate in candidates:
            line = json.dumps(candidate, default=str)
            fh.write(line + "\n")

    return out_path
