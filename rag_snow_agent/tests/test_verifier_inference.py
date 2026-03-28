"""Tests for verifier.score_candidate_semantics with and without a model."""

import tempfile
from pathlib import Path

import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression

from rag_snow_agent.agent.verifier import (
    load_verifier,
    reset_verifier_cache,
    score_candidate_semantics,
)
from rag_snow_agent.agent.verifier_features import extract_candidate_features


def _make_candidate(**overrides):
    base = {
        "execution_success": True,
        "repairs_count": 0,
        "error_type": None,
        "row_count": 5,
        "final_sql": "SELECT x FROM t",
        "metamorphic": {"score_delta": 0.0},
        "score": 100.0,
    }
    base.update(overrides)
    return base


def test_returns_zero_when_no_model():
    reset_verifier_cache()
    score = score_candidate_semantics(
        instruction="count users",
        sql="SELECT COUNT(*) FROM users",
        candidate_record=_make_candidate(),
        model_path="/nonexistent/path/model.joblib",
    )
    assert score == 0.0


def test_returns_zero_when_no_candidate_record():
    reset_verifier_cache()
    score = score_candidate_semantics(
        instruction="count users",
        sql="SELECT COUNT(*) FROM users",
        candidate_record=None,
    )
    assert score == 0.0


def test_works_with_tiny_trained_model():
    reset_verifier_cache()

    # Build tiny training data
    instruction = "count of orders"
    records = [
        _make_candidate(execution_success=True, row_count=1, score=100),
        _make_candidate(execution_success=True, row_count=5, score=80),
        _make_candidate(execution_success=False, row_count=0, score=0),
        _make_candidate(execution_success=False, row_count=None, score=0),
    ]
    labels = [1, 0, 0, 0]

    features_list = [extract_candidate_features(r, instruction) for r in records]
    feature_names = sorted(features_list[0].keys())
    X = [[f[n] for n in feature_names] for f in features_list]

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X, labels)

    artifact = {"model": model, "feature_names": feature_names}

    with tempfile.NamedTemporaryFile(suffix=".joblib", delete=False) as tmp:
        joblib.dump(artifact, tmp.name)
        tmp_path = tmp.name

    try:
        reset_verifier_cache()
        score = score_candidate_semantics(
            instruction=instruction,
            sql="SELECT COUNT(*) FROM orders",
            candidate_record=_make_candidate(execution_success=True, row_count=1, score=100),
            model_path=tmp_path,
        )
        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0
    finally:
        reset_verifier_cache()
        Path(tmp_path).unlink(missing_ok=True)
