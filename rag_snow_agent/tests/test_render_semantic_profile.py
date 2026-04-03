"""Tests for render_semantic_profile_for_prompt: token budget respected."""

from __future__ import annotations

import tiktoken

from rag_snow_agent.semantic_layer.models import SemanticFact, SemanticProfile
from rag_snow_agent.semantic_layer.merge import render_semantic_profile_for_prompt

_enc = tiktoken.get_encoding("cl100k_base")


def _fact(
    fact_type: str = "primary_time_column",
    subject: str = "TESTDB.PUBLIC.T.COL",
    value: str = "DATE",
    confidence: float = 0.8,
) -> SemanticFact:
    return SemanticFact(
        fact_type=fact_type,
        subject=subject,
        value=value,
        confidence=confidence,
        source=["metadata"],
    )


class TestRenderBasic:
    def test_empty_profile_returns_empty_string(self):
        profile = SemanticProfile(db_id="TESTDB")
        result = render_semantic_profile_for_prompt(profile)
        assert result == ""

    def test_single_fact_renders(self):
        profile = SemanticProfile(
            db_id="TESTDB",
            time_columns=[_fact()],
        )
        result = render_semantic_profile_for_prompt(profile)
        assert "Semantic context:" in result
        assert "primary_time_column" in result
        assert "TESTDB.PUBLIC.T.COL" in result

    def test_facts_sorted_by_confidence(self):
        profile = SemanticProfile(
            db_id="TESTDB",
            time_columns=[_fact(confidence=0.5)],
            metric_candidates=[_fact(fact_type="metric_candidate", confidence=0.9)],
        )
        result = render_semantic_profile_for_prompt(profile)
        lines = result.strip().split("\n")
        # The metric_candidate (0.9) should appear before time_column (0.5)
        metric_line_idx = None
        time_line_idx = None
        for i, line in enumerate(lines):
            if "metric_candidate" in line:
                metric_line_idx = i
            if "primary_time_column" in line:
                time_line_idx = i
        assert metric_line_idx is not None
        assert time_line_idx is not None
        assert metric_line_idx < time_line_idx


class TestTokenBudget:
    def test_respects_small_budget(self):
        # Create many facts
        facts = [
            _fact(
                subject=f"TESTDB.PUBLIC.TABLE_{i}.COL_{i}",
                confidence=0.8 - i * 0.01,
            )
            for i in range(50)
        ]
        profile = SemanticProfile(db_id="TESTDB", time_columns=facts)

        # Very small budget
        result = render_semantic_profile_for_prompt(profile, max_tokens=50)
        token_count = len(_enc.encode(result))
        assert token_count <= 50

    def test_respects_medium_budget(self):
        facts = [
            _fact(
                subject=f"TESTDB.PUBLIC.TABLE_{i}.COL_{i}",
                confidence=0.8 - i * 0.01,
            )
            for i in range(50)
        ]
        profile = SemanticProfile(db_id="TESTDB", time_columns=facts)

        result = render_semantic_profile_for_prompt(profile, max_tokens=200)
        token_count = len(_enc.encode(result))
        assert token_count <= 200

    def test_default_budget_800(self):
        facts = [
            _fact(
                subject=f"TESTDB.PUBLIC.TABLE_{i}.COL_{i}",
                confidence=0.8 - i * 0.01,
            )
            for i in range(100)
        ]
        profile = SemanticProfile(db_id="TESTDB", time_columns=facts)

        result = render_semantic_profile_for_prompt(profile)
        token_count = len(_enc.encode(result))
        assert token_count <= 800


class TestValueRendering:
    def test_dict_value_rendered(self):
        fact = _fact(value={"min": "2020-01-01", "max": "2023-12-31"})
        profile = SemanticProfile(db_id="TESTDB", time_columns=[fact])
        result = render_semantic_profile_for_prompt(profile)
        assert "min=" in result or "max=" in result

    def test_list_value_rendered(self):
        fact = _fact(value=["active", "inactive", "pending"])
        profile = SemanticProfile(db_id="TESTDB", time_columns=[fact])
        result = render_semantic_profile_for_prompt(profile)
        assert "active" in result

    def test_string_value_rendered(self):
        fact = _fact(value="TIMESTAMP_NTZ")
        profile = SemanticProfile(db_id="TESTDB", time_columns=[fact])
        result = render_semantic_profile_for_prompt(profile)
        assert "TIMESTAMP_NTZ" in result
