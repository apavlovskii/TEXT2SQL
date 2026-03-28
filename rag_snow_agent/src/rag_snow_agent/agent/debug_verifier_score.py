"""CLI to debug verifier scores for a single candidate.

Usage:
    python -m rag_snow_agent.agent.debug_verifier_score \
        --candidate_json path/to/candidate.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .verifier_features import extract_candidate_features
from .verifier import load_verifier, score_candidate_semantics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Debug verifier score for a candidate")
    parser.add_argument("--candidate_json", required=True, help="Path to candidate JSON file")
    parser.add_argument("--model_path", default=None, help="Path to verifier model (optional)")
    args = parser.parse_args(argv)

    # Load candidate record
    candidate_path = Path(args.candidate_json)
    if not candidate_path.exists():
        print(f"ERROR: File not found: {candidate_path}", file=sys.stderr)
        sys.exit(1)

    with open(candidate_path) as fh:
        record = json.load(fh)

    instruction = record.get("instruction", "")

    # Extract features
    features = extract_candidate_features(record, instruction)
    print("Extracted features:")
    for name in sorted(features.keys()):
        print(f"  {name:35s} = {features[name]}")

    # Score
    score = score_candidate_semantics(
        instruction=instruction,
        sql=record.get("final_sql", ""),
        candidate_record=record,
        model_path=args.model_path,
    )
    print(f"\nVerifier score: {score:.4f}")

    # Model info
    model = load_verifier(args.model_path)
    if model is None:
        print("(No trained model found -- score defaults to 0.0)")
    else:
        print("(Using trained model)")


if __name__ == "__main__":
    main()
