"""Semantic verifier — learned model or 0.0 fallback.

v1: rule-based stub returning 0.0.
v2 (Milestone 9): trained LogisticRegression loaded from joblib.
Falls back to 0.0 when no model is available.
"""

from __future__ import annotations

import logging
from pathlib import Path

import joblib

from .verifier_features import extract_candidate_features

log = logging.getLogger(__name__)

_DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[4] / "models" / "verifier.joblib"
_cached_model = None


def load_verifier(model_path=None):
    """Load trained model artifact. Returns None if not found."""
    global _cached_model
    p = Path(model_path) if model_path else _DEFAULT_MODEL_PATH
    if _cached_model is not None:
        return _cached_model
    if not p.exists():
        return None
    try:
        _cached_model = joblib.load(p)
        log.info("Loaded verifier model from %s", p)
    except Exception:
        log.warning("Failed to load verifier model from %s", p, exc_info=True)
        return None
    return _cached_model


def reset_verifier_cache() -> None:
    """Clear the cached model (useful for testing)."""
    global _cached_model
    _cached_model = None


def score_candidate_semantics(
    instruction: str,
    sql: str,
    schema_slice=None,
    fingerprint=None,
    candidate_record: dict | None = None,
    model_path: str | None = None,
) -> float:
    """Score semantic plausibility. Uses trained model if available, else 0.0."""
    model_artifact = load_verifier(model_path)
    if model_artifact is None or candidate_record is None:
        return 0.0

    features = extract_candidate_features(candidate_record, instruction)

    # Use stored feature names for consistent ordering
    if isinstance(model_artifact, dict) and "model" in model_artifact:
        model = model_artifact["model"]
        feature_names = model_artifact.get("feature_names", sorted(features.keys()))
    else:
        # Bare model (legacy)
        model = model_artifact
        feature_names = sorted(features.keys())

    X = [[features.get(f, 0.0) for f in feature_names]]
    try:
        proba = model.predict_proba(X)
        return float(proba[0][1])  # probability of class 1
    except Exception:
        log.warning("Verifier predict_proba failed", exc_info=True)
        return 0.0
