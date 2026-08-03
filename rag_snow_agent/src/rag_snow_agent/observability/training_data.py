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


def _load_instructions(instructions_jsonl: str | Path) -> dict[str, str]:
    """Build an {instance_id: instruction} lookup from a Spider2 split file.

    instance_results.jsonl (written by experiment_runner) doesn't carry the
    instruction text itself, only instance_id — so it has to be joined back
    against the question bank it was solved from.
    """
    lookup: dict[str, str] = {}
    path = Path(instructions_jsonl)
    if not path.exists():
        return lookup
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            iid = item.get("instance_id")
            if iid:
                lookup[iid] = item.get("instruction", "")
    return lookup


def build_verifier_dataset_from_experiment_results(
    experiments_dir: str | Path,
    instructions_jsonl: str | Path,
) -> list[dict]:
    """Scan ``reports/experiments/*/instance_results.jsonl`` for labeled candidates.

    Each instance_results.jsonl line has a nested ``candidates`` list; when the
    run was scored with ``--eval_gold_dir``, each candidate carries a
    ``gold_matched`` field verified against real gold execution results. That
    is a strictly better training label than ``is_best`` (used by
    :func:`build_verifier_dataset`): ``is_best`` only reflects what the
    current heuristic selector already picked, so training on it would just
    teach the model to imitate the heuristic it's meant to improve on.
    Candidates without a ``gold_matched`` verdict (``None`` — no
    ``--eval_gold_dir`` was set for that run) are skipped rather than guessed.

    *instructions_jsonl* is the Spider2 question-bank split (e.g.
    ``spider2-snow.jsonl``) used to look up each instance_id's instruction
    text, since instance_results.jsonl doesn't store it.

    Returns a list of feature dicts, each with a ``label`` key (1 if
    ``gold_matched`` is True, 0 if False).
    """
    experiments_path = Path(experiments_dir)
    instructions = _load_instructions(instructions_jsonl)
    rows: list[dict] = []

    for jsonl_file in sorted(experiments_path.glob("*/instance_results.jsonl")):
        with open(jsonl_file) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    instance = json.loads(line)
                except json.JSONDecodeError:
                    continue

                instruction = instructions.get(instance.get("instance_id", ""), "")
                for candidate in instance.get("candidates") or []:
                    gold_matched = candidate.get("gold_matched")
                    if gold_matched is None:
                        continue
                    record = {**candidate, "execution_success": candidate.get("success")}
                    features = extract_candidate_features(record, instruction)
                    features["label"] = 1 if gold_matched else 0
                    rows.append(features)

    return rows
