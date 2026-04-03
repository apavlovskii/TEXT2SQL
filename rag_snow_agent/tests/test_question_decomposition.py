"""Tests for question decomposition and rendering."""

from __future__ import annotations

import json
from unittest.mock import patch

from rag_snow_agent.prompting.question_decomposition import (
    QuestionDecomposition,
    decompose_question,
    render_decomposition_for_prompt,
)
from rag_snow_agent.prompting.subgoal_validator import validate_sql_against_decomposition


# ── QuestionDecomposition model tests ─────────────────────────────────────


def test_decomposition_model_defaults():
    """Empty decomposition has sensible defaults."""
    d = QuestionDecomposition()
    assert d.temporal_scope is None
    assert d.temporal_grain is None
    assert d.filters == []
    assert d.measures == []
    assert d.grouping == []
    assert d.notes == []


def test_decomposition_model_with_values():
    """Model accepts all fields."""
    d = QuestionDecomposition(
        temporal_scope="year 2017",
        temporal_grain="month",
        filters=["campaign = 'Data Share'"],
        cohort_conditions=["visitors with transactions"],
        target_entity="distinct visitors",
        target_grain="per visitor",
        measures=["COUNT DISTINCT", "SUM(revenue)"],
        set_operations=["MINUS"],
        ranking="top 5",
        grouping=["by month", "by source"],
        nested_fields=["trafficSource.source"],
        expected_shape="single number",
        notes=["ambiguous date format"],
    )
    assert d.temporal_scope == "year 2017"
    assert len(d.measures) == 2
    assert d.ranking == "top 5"


# ── render_decomposition_for_prompt tests ─────────────────────────────────


def test_render_empty_decomposition():
    """Empty decomposition renders to empty string."""
    d = QuestionDecomposition()
    result = render_decomposition_for_prompt(d)
    assert result == ""


def test_render_temporal_scope():
    d = QuestionDecomposition(temporal_scope="year 2017")
    result = render_decomposition_for_prompt(d)
    assert "Question decomposition:" in result
    assert "Temporal: year 2017" in result


def test_render_temporal_with_grain():
    d = QuestionDecomposition(temporal_scope="year 2017", temporal_grain="month")
    result = render_decomposition_for_prompt(d)
    assert "Temporal: year 2017 (grain: month)" in result


def test_render_measures():
    d = QuestionDecomposition(measures=["SUM(revenue)", "COUNT DISTINCT"])
    result = render_decomposition_for_prompt(d)
    assert "Measure: SUM(revenue)" in result
    assert "Measure: COUNT DISTINCT" in result


def test_render_filters():
    d = QuestionDecomposition(filters=["status = 'active'"])
    result = render_decomposition_for_prompt(d)
    assert "Filter: status = 'active'" in result


def test_render_grouping():
    d = QuestionDecomposition(grouping=["by month"])
    result = render_decomposition_for_prompt(d)
    assert "Grouping: by month" in result


def test_render_ranking():
    d = QuestionDecomposition(ranking="top 5")
    result = render_decomposition_for_prompt(d)
    assert "Ranking: top 5" in result


def test_render_expected_shape():
    d = QuestionDecomposition(expected_shape="single number")
    result = render_decomposition_for_prompt(d)
    assert "Output: single number" in result


def test_render_cohort():
    d = QuestionDecomposition(cohort_conditions=["visitors who made at least one transaction"])
    result = render_decomposition_for_prompt(d)
    assert "Cohort: visitors who made at least one transaction" in result


def test_render_set_operations():
    d = QuestionDecomposition(set_operations=["MINUS"])
    result = render_decomposition_for_prompt(d)
    assert "Set operation: MINUS" in result


def test_render_nested_fields():
    d = QuestionDecomposition(nested_fields=["trafficSource.source"])
    result = render_decomposition_for_prompt(d)
    assert "Nested field: trafficSource.source" in result


def test_render_notes():
    d = QuestionDecomposition(notes=["ambiguous date format"])
    result = render_decomposition_for_prompt(d)
    assert "Note: ambiguous date format" in result


def test_render_target_entity():
    d = QuestionDecomposition(target_entity="distinct visitors", target_grain="per visitor")
    result = render_decomposition_for_prompt(d)
    assert "Target entity: distinct visitors (per visitor)" in result


def test_render_full_decomposition():
    """Full decomposition renders all parts."""
    d = QuestionDecomposition(
        temporal_scope="year 2017",
        measures=["SUM(revenue)"],
        filters=["visitors who made at least one transaction"],
        grouping=["by traffic source"],
        expected_shape="single row with source name + revenue difference",
    )
    result = render_decomposition_for_prompt(d)
    assert "Temporal: year 2017" in result
    assert "Measure: SUM(revenue)" in result
    assert "Filter: visitors who made at least one transaction" in result
    assert "Grouping: by traffic source" in result
    assert "Output: single row" in result


# ── decompose_question with mock LLM ─────────────────────────────────────


def test_decompose_question_success():
    """decompose_question parses valid LLM response."""
    mock_response = json.dumps({
        "temporal_scope": "year 2020",
        "measures": ["COUNT(*)"],
        "grouping": ["by category"],
    })

    with patch("rag_snow_agent.prompting.question_decomposition.call_llm", return_value=mock_response):
        result = decompose_question("How many items per category in 2020?")

    assert result.temporal_scope == "year 2020"
    assert result.measures == ["COUNT(*)"]
    assert result.grouping == ["by category"]


def test_decompose_question_with_markdown_fences():
    """decompose_question strips markdown fences."""
    mock_response = "```json\n" + json.dumps({
        "temporal_scope": "Q1 2021",
        "measures": ["SUM(revenue)"],
    }) + "\n```"

    with patch("rag_snow_agent.prompting.question_decomposition.call_llm", return_value=mock_response):
        result = decompose_question("Total revenue in Q1 2021?")

    assert result.temporal_scope == "Q1 2021"
    assert result.measures == ["SUM(revenue)"]


def test_decompose_question_fallback_on_error():
    """decompose_question returns minimal decomposition on failure."""
    with patch("rag_snow_agent.prompting.question_decomposition.call_llm", side_effect=Exception("API error")):
        result = decompose_question("Some question")

    assert result.notes == ["Some question"]
    assert result.temporal_scope is None


def test_decompose_question_invalid_json_fallback():
    """decompose_question handles invalid JSON gracefully."""
    with patch("rag_snow_agent.prompting.question_decomposition.call_llm", return_value="not valid json at all"):
        result = decompose_question("Another question")

    assert result.notes == ["Another question"]


# ── validate_sql_against_decomposition (basic checks in this file) ────────


def test_validate_sql_with_date_filter_passes():
    d = QuestionDecomposition(temporal_scope="year 2017")
    sql = "SELECT * FROM orders WHERE created_at >= '2017-01-01' AND created_at < '2018-01-01'"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_validate_sql_without_date_warns():
    d = QuestionDecomposition(temporal_scope="year 2017")
    sql = "SELECT COUNT(*) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert not passed
    assert any("Temporal scope" in w for w in warnings)


def test_validate_sql_with_group_by_passes():
    d = QuestionDecomposition(grouping=["by category"])
    sql = "SELECT category, COUNT(*) FROM products GROUP BY category"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []


def test_validate_sql_count_matches_measure():
    d = QuestionDecomposition(measures=["COUNT(*)"])
    sql = "SELECT COUNT(*) FROM orders"
    passed, warnings = validate_sql_against_decomposition(sql, d)
    assert passed
    assert warnings == []
