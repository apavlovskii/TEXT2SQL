"""Tests for upgraded selector with semantic scoring signals."""

from rag_snow_agent.agent.selector import explain_candidate_score, score_candidate


def _base_candidate(**overrides) -> dict:
    c = {
        "execution_success": True,
        "repairs_count": 0,
        "row_count": 10,
        "error_type": None,
        "metamorphic": {"checks_run": [], "score_delta": 0.0},
        "verifier_score": 0.0,
    }
    c.update(overrides)
    return c


def test_grouped_single_row_penalty():
    """Expected grouped output but only 1 row → penalty."""
    c = _base_candidate(row_count=1)
    score_multi = score_candidate("revenue for each category", _base_candidate(row_count=5))
    score_single = score_candidate("revenue for each category", c)
    assert score_multi > score_single


def test_aggregate_single_row_bonus():
    """Expected aggregate and got 1 row → bonus."""
    c1 = _base_candidate(row_count=1)
    c10 = _base_candidate(row_count=10)
    s1 = score_candidate("how many orders total", c1)
    s10 = score_candidate("how many orders total", c10)
    assert s1 > s10


def test_time_series_bonus():
    """Monthly time series with plausible row count → bonus."""
    c12 = _base_candidate(row_count=12)
    c1 = _base_candidate(row_count=1)
    s12 = score_candidate("total by month", c12)
    s1 = score_candidate("total by month", c1)
    assert s12 > s1


def test_metamorphic_delta_positive():
    """Positive metamorphic delta should improve score."""
    c_pos = _base_candidate(metamorphic={"checks_run": [], "score_delta": 10.0})
    c_zero = _base_candidate(metamorphic={"checks_run": [], "score_delta": 0.0})
    assert score_candidate("test", c_pos) > score_candidate("test", c_zero)


def test_metamorphic_delta_negative():
    """Negative metamorphic delta should reduce score."""
    c_neg = _base_candidate(metamorphic={"checks_run": [], "score_delta": -15.0})
    c_zero = _base_candidate()
    assert score_candidate("test", c_neg) < score_candidate("test", c_zero)


def test_explain_has_total():
    c = _base_candidate()
    bd = explain_candidate_score("how many orders", c)
    assert "total" in bd
    assert isinstance(bd["total"], float)


def test_explain_shows_components():
    c = _base_candidate(repairs_count=2, row_count=1)
    bd = explain_candidate_score("how many orders total", c)
    assert "execution_success" in bd
    assert "repair_penalty" in bd
    assert bd["repair_penalty"] < 0


def test_small_result_bonus():
    c_small = _base_candidate(row_count=3)
    c_large = _base_candidate(row_count=100)
    s_small = score_candidate("top 5 products", c_small)
    s_large = score_candidate("top 5 products", c_large)
    assert s_small > s_large
