"""Tests for subgoal validator — SQL verification against decomposition."""

from __future__ import annotations

from rag_snow_agent.prompting.question_decomposition import QuestionDecomposition
from rag_snow_agent.prompting.subgoal_validator import validate_sql_against_decomposition


# ── Temporal scope checks ──────────────────────────────────────────────────


def test_temporal_scope_with_iso_date():
    d = QuestionDecomposition(temporal_scope="year 2020")
    sql = "SELECT * FROM sales WHERE sale_date >= '2020-01-01'"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_with_date_trunc():
    d = QuestionDecomposition(temporal_scope="monthly in 2021")
    sql = "SELECT DATE_TRUNC('MONTH', created_at) AS m, COUNT(*) FROM orders GROUP BY m"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_with_extract():
    d = QuestionDecomposition(temporal_scope="year 2019")
    sql = "SELECT * FROM events WHERE EXTRACT(YEAR FROM event_date) = 2019"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_with_year_function():
    d = QuestionDecomposition(temporal_scope="year 2018")
    sql = "SELECT * FROM orders WHERE YEAR(order_date) = 2018"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_with_to_date():
    d = QuestionDecomposition(temporal_scope="January 2021")
    sql = "SELECT * FROM logs WHERE ts >= TO_DATE('2021-01-01')"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_with_between():
    d = QuestionDecomposition(temporal_scope="Q1 2022")
    sql = "SELECT * FROM orders WHERE order_date BETWEEN '2022-01-01' AND '2022-03-31'"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_temporal_scope_missing_warns():
    d = QuestionDecomposition(temporal_scope="year 2017")
    sql = "SELECT COUNT(*) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert len(warnings) == 1
    assert "Temporal scope" in warnings[0]


def test_no_temporal_scope_no_warning():
    d = QuestionDecomposition()
    sql = "SELECT COUNT(*) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


# ── Measure / aggregation checks ──────────────────────────────────────────


def test_count_measure_found():
    d = QuestionDecomposition(measures=["COUNT(*)"])
    sql = "SELECT COUNT(*) FROM users"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_sum_measure_found():
    d = QuestionDecomposition(measures=["SUM(revenue)"])
    sql = "SELECT SUM(revenue) FROM sales"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_avg_measure_found():
    d = QuestionDecomposition(measures=["AVG(price)"])
    sql = "SELECT AVG(price) FROM products"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_count_distinct_measure_found():
    d = QuestionDecomposition(measures=["COUNT DISTINCT"])
    sql = "SELECT COUNT(DISTINCT user_id) FROM sessions"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_multiple_measures_all_found():
    d = QuestionDecomposition(measures=["COUNT(*)", "SUM(amount)"])
    sql = "SELECT COUNT(*), SUM(amount) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_measure_missing_warns():
    d = QuestionDecomposition(measures=["SUM(revenue)"])
    sql = "SELECT * FROM sales"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert any("SUM(revenue)" in w for w in warnings)


def test_one_measure_missing_of_two():
    d = QuestionDecomposition(measures=["COUNT(*)", "AVG(score)"])
    sql = "SELECT COUNT(*) FROM results"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert len(warnings) == 1
    assert "AVG(score)" in warnings[0]


# ── GROUP BY checks ───────────────────────────────────────────────────────


def test_grouping_with_group_by():
    d = QuestionDecomposition(grouping=["by category"])
    sql = "SELECT category, COUNT(*) FROM products GROUP BY category"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_grouping_missing_group_by():
    d = QuestionDecomposition(grouping=["by region"])
    sql = "SELECT region, COUNT(*) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert any("GROUP BY" in w for w in warnings)


def test_no_grouping_no_warning():
    d = QuestionDecomposition()
    sql = "SELECT COUNT(*) FROM users"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


# ── Ranking checks (ORDER BY + LIMIT) ────────────────────────────────────


def test_ranking_top_with_order_limit():
    d = QuestionDecomposition(ranking="top 5")
    sql = "SELECT name, score FROM users ORDER BY score DESC LIMIT 5"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_ranking_highest_with_order_limit():
    d = QuestionDecomposition(ranking="highest")
    sql = "SELECT * FROM products ORDER BY price DESC LIMIT 1"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_ranking_lowest_with_order_limit():
    d = QuestionDecomposition(ranking="lowest")
    sql = "SELECT * FROM scores ORDER BY score ASC LIMIT 1"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_ranking_top_missing_limit():
    d = QuestionDecomposition(ranking="top 10")
    sql = "SELECT name FROM users ORDER BY score DESC"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert any("ORDER BY + LIMIT" in w for w in warnings)


def test_ranking_top_missing_order():
    d = QuestionDecomposition(ranking="top 3")
    sql = "SELECT name FROM users LIMIT 3"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert any("ORDER BY + LIMIT" in w for w in warnings)


def test_ranking_without_top_keyword_no_check():
    """Ranking without 'top', 'highest', or 'lowest' does not trigger check."""
    d = QuestionDecomposition(ranking="ordered by date")
    sql = "SELECT * FROM events"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_no_ranking_no_warning():
    d = QuestionDecomposition()
    sql = "SELECT * FROM users"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


# ── Combined checks ───────────────────────────────────────────────────────


def test_multiple_warnings():
    """Multiple unmet subgoals produce multiple warnings."""
    d = QuestionDecomposition(
        temporal_scope="year 2020",
        measures=["SUM(revenue)"],
        grouping=["by region"],
        ranking="top 5",
    )
    sql = "SELECT * FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert len(warnings) == 4  # temporal, measure, grouping, ranking


def test_all_subgoals_satisfied():
    """All subgoals met produces no warnings."""
    d = QuestionDecomposition(
        temporal_scope="year 2020",
        measures=["SUM(revenue)"],
        grouping=["by region"],
        ranking="top 5",
    )
    sql = (
        "SELECT region, SUM(revenue) FROM orders "
        "WHERE order_date >= '2020-01-01' AND order_date < '2021-01-01' "
        "GROUP BY region ORDER BY SUM(revenue) DESC LIMIT 5"
    )
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_empty_decomposition_always_passes():
    """An empty decomposition has no checks to fail."""
    d = QuestionDecomposition()
    sql = "SELECT 1"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_min_max_measures():
    d = QuestionDecomposition(measures=["MIN(price)", "MAX(price)"])
    sql = "SELECT MIN(price), MAX(price) FROM products"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []
