"""Build structured dataset from run artifacts for verifier training."""

from __future__ import annotations

import json
from pathlib import Path

from ..agent.verifier_features import extract_candidate_features


def build_verifier_dataset(run_dir: str | Path) -> list[dict]:
    """Scan JSONL candidate logs in *run_dir* and extract training rows.

    Each JSONL line is expected to be a candidate record dict with at least:
      - ``instruction`` (str)
      - ``execution_success`` (bool)
      - ``is_best`` (bool) -- whether this candidate was selected & successful
      - ``final_sql`` (str)

    Returns a list of feature dicts, each with a ``label`` key (1 if the
    candidate was best+successful, 0 otherwise).
    """
    run_path = Path(run_dir)
    rows: list[dict] = []

    jsonl_files = sorted(run_path.glob("*.jsonl"))
    for jsonl_file in jsonl_files:
        with open(jsonl_file) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                instruction = record.get("instruction", "")
                features = extract_candidate_features(record, instruction)

                # Label: 1 if candidate was the best AND execution succeeded
                is_best = record.get("is_best", False)
                exec_ok = record.get("execution_success", False)
                features["label"] = 1 if (is_best and exec_ok) else 0

                rows.append(features)

    return rows
