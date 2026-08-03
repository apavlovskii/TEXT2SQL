"""CLI to train a learned verifier model from candidate run logs.

Usage (candidate-log JSONL directory — is_best-labeled, legacy format):
    python -m rag_snow_agent.agent.train_verifier \
        --run_dir reports/candidate_logs \
        --output_model rag_snow_agent/models/verifier.joblib

Usage (accumulated experiment_runner history — gold_matched-labeled, preferred):
    python -m rag_snow_agent.agent.train_verifier \
        --experiments_dir reports/experiments \
        --instructions_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
        --output_model rag_snow_agent/models/verifier.joblib
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from ..observability.training_data import (
    build_verifier_dataset,
    build_verifier_dataset_from_experiment_results,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Train verifier model from candidate logs")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run_dir", help="Directory with legacy is_best-labeled candidate-log JSONL files")
    source.add_argument(
        "--experiments_dir",
        help="reports/experiments-style directory of run subfolders, each with an "
        "instance_results.jsonl; trains on real gold_matched labels instead of "
        "the is_best heuristic (requires --instructions_jsonl)",
    )
    parser.add_argument(
        "--instructions_jsonl",
        help="Spider2 question-bank split (e.g. spider2-snow.jsonl) to look up instruction "
        "text by instance_id; required with --experiments_dir",
    )
    parser.add_argument(
        "--output_model",
        default="rag_snow_agent/models/verifier.joblib",
        help="Path to save the trained model",
    )
    parser.add_argument("--test_size", type=float, default=0.2, help="Test split fraction")
    parser.add_argument("--random_state", type=int, default=42, help="Random seed")
    args = parser.parse_args(argv)

    if args.experiments_dir and not args.instructions_jsonl:
        parser.error("--experiments_dir requires --instructions_jsonl")

    # Build dataset
    if args.experiments_dir:
        rows = build_verifier_dataset_from_experiment_results(
            args.experiments_dir, args.instructions_jsonl
        )
        source_desc = f"--experiments_dir for *.jsonl under {args.experiments_dir}"
    else:
        rows = build_verifier_dataset(args.run_dir)
        source_desc = f"--run_dir {args.run_dir}"
    if not rows:
        print(f"ERROR: No training rows found. Check {source_desc}", file=sys.stderr)
        sys.exit(1)

    print(f"Built dataset with {len(rows)} rows")

    # Separate features and labels
    labels = [r.pop("label") for r in rows]
    feature_names = sorted(rows[0].keys())
    X = [[r[f] for f in feature_names] for r in rows]
    y = labels

    # Split
    if len(X) < 4:
        print("WARNING: Very small dataset, skipping train/test split")
        X_train, X_test, y_train, y_test = X, X, y, y
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=args.test_size, random_state=args.random_state
        )

    print(f"Train: {len(X_train)}, Test: {len(X_test)}")

    # Train
    model = LogisticRegression(max_iter=5000, random_state=args.random_state)
    model.fit(X_train, y_train)

    # Evaluate
    train_acc = accuracy_score(y_train, model.predict(X_train))
    test_acc = accuracy_score(y_test, model.predict(X_test))
    print(f"Train accuracy: {train_acc:.4f}")
    print(f"Test accuracy:  {test_acc:.4f}")

    # Feature importances
    print("\nFeature weights:")
    for name, coef in sorted(zip(feature_names, model.coef_[0]), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {name:35s} {coef:+.4f}")

    # Save
    output_path = Path(args.output_model)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save model and feature names together
    artifact = {"model": model, "feature_names": feature_names}
    joblib.dump(artifact, output_path)
    print(f"\nModel saved to {output_path}")


if __name__ == "__main__":
    main()
