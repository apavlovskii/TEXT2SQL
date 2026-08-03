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

_DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[3] / "models" / "verifier.joblib"
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


def _mark_verifier_used() -> None:
    """Best-effort telemetry mark; never raises into the scoring path.

    Marked for both the trained-model and heuristic-fallback paths, so on
    its own it only means "score_candidate_semantics ran" — not "the trained
    model produced this score". Use ``_mark_verifier_model_used`` to tell
    the two apart.
    """
    try:
        from ..observability.instance_telemetry import telemetry
        telemetry.mark("verifier_used")
        telemetry.increment("verifier_scores_computed")
    except Exception:
        pass


def _mark_verifier_model_used() -> None:
    """Best-effort telemetry mark for the trained-model path specifically."""
    try:
        from ..observability.instance_telemetry import telemetry
        telemetry.mark("verifier_model_used")
    except Exception:
        pass


def _heuristic_verifier_score(features: dict) -> float:
    """Deterministic plausibility score in [0, 1] derived from candidate features.

    Used when no trained model is available. Rewards candidates that execute
    successfully and whose result shape aligns with the inferred expectation;
    penalizes execution failures and empty results. This is intentionally a
    transparent, reproducible rule (no learned weights) so the verifier is a
    real, firing component for ablation without requiring a training step.
    """
    score = 0.5  # neutral prior

    # Execution success dominates: a candidate that ran is far more plausible.
    if features.get("execution_success"):
        score += 0.30
    else:
        score -= 0.30

    # Shape alignment (0..~2): result row-count consistent with the question.
    score += min(float(features.get("shape_alignment", 0.0)), 2.0) * 0.10

    # Metamorphic agreement nudges plausibility either way (already small).
    delta = float(features.get("metamorphic_score_delta", 0.0))
    score += max(-0.10, min(0.10, delta))

    # An empty result from a query that *did* run is mildly suspicious.
    if features.get("execution_success") and features.get("row_count_bucket", 0) == 0:
        score -= 0.10

    return max(0.0, min(1.0, score))


def score_candidate_semantics(
    instruction: str,
    sql: str,
    schema_slice=None,
    fingerprint=None,
    candidate_record: dict | None = None,
    model_path: str | None = None,
) -> float:
    """Score semantic plausibility of a candidate in [0, 1].

    Uses the trained model when one is available; otherwise falls back to a
    deterministic heuristic (:func:`_heuristic_verifier_score`). Returns 0.0
    only when there is no candidate to score. Marks ``verifier_used`` telemetry
    whenever a real score is computed (model or heuristic).
    """
    if candidate_record is None:
        return 0.0

    features = extract_candidate_features(candidate_record, instruction)

    model_artifact = load_verifier(model_path)
    if model_artifact is None:
        # No learned model on disk — use the transparent heuristic instead of
        # silently returning 0.0 (which left the verifier dormant historically).
        score = _heuristic_verifier_score(features)
        _mark_verifier_used()
        return score

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
        score = float(proba[0][1])  # probability of class 1
        _mark_verifier_used()
        _mark_verifier_model_used()
        return score
    except Exception:
        log.warning("Verifier predict_proba failed; using heuristic", exc_info=True)
        score = _heuristic_verifier_score(features)
        _mark_verifier_used()
        return score
