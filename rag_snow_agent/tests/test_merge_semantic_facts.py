"""Tests for merge_semantic_facts: multi-source merge + confidence."""

from __future__ import annotations

from rag_snow_agent.semantic_layer.models import SemanticFact
from rag_snow_agent.semantic_layer.merge import merge_semantic_facts


def _fact(
    fact_type: str = "primary_time_column",
    subject: str = "TESTDB.PUBLIC.T.COL",
    value: str = "DATE",
    confidence: float = 0.8,
    source: list[str] | None = None,
    evidence: list[str] | None = None,
) -> SemanticFact:
    return SemanticFact(
        fact_type=fact_type,
        subject=subject,
        value=value,
        confidence=confidence,
        source=source or ["metadata"],
        evidence=evidence or ["test evidence"],
    )


class TestBasicMerge:
    def test_single_source_preserved(self):
        facts = [_fact()]
        profile = merge_semantic_facts("TESTDB", facts, [], [], [])
        assert len(profile.time_columns) == 1
        assert profile.time_columns[0].confidence == 0.8

    def test_empty_sources(self):
        profile = merge_semantic_facts("TESTDB", [], [], [], [])
        assert profile.db_id == "TESTDB"
        assert len(profile.all_facts()) == 0

    def test_db_id_set(self):
        profile = merge_semantic_facts("MYDB", [], [], [], [])
        assert profile.db_id == "MYDB"


class TestMultiSourceAgreement:
    def test_same_fact_from_two_sources_uses_max_confidence(self):
        metadata_fact = _fact(confidence=0.7, source=["metadata"])
        probe_fact = _fact(confidence=0.9, source=["probes"])
        profile = merge_semantic_facts(
            "TESTDB", [metadata_fact], [], [probe_fact], []
        )
        assert len(profile.time_columns) == 1
        assert profile.time_columns[0].confidence == 0.9

    def test_merged_sources_list(self):
        metadata_fact = _fact(source=["metadata"])
        probe_fact = _fact(source=["probes"])
        profile = merge_semantic_facts(
            "TESTDB", [metadata_fact], [], [probe_fact], []
        )
        assert "metadata" in profile.time_columns[0].source
        assert "probes" in profile.time_columns[0].source

    def test_merged_evidence_list(self):
        f1 = _fact(evidence=["evidence A"])
        f2 = _fact(evidence=["evidence B"], source=["probes"])
        profile = merge_semantic_facts("TESTDB", [f1], [], [f2], [])
        combined_evidence = profile.time_columns[0].evidence
        assert "evidence A" in combined_evidence
        assert "evidence B" in combined_evidence


class TestFactTypeCategorization:
    def test_metric_candidate_goes_to_metric_candidates(self):
        fact = _fact(fact_type="metric_candidate")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.metric_candidates) == 1

    def test_dimension_candidate_goes_to_dimension_candidates(self):
        fact = _fact(fact_type="dimension_candidate")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.dimension_candidates) == 1

    def test_nested_container_goes_to_nested_field_patterns(self):
        fact = _fact(fact_type="nested_container_column")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.nested_field_patterns) == 1

    def test_identifier_goes_to_join_semantics(self):
        fact = _fact(fact_type="identifier_column")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.join_semantics) == 1

    def test_filter_value_hints_categorized(self):
        fact = _fact(fact_type="filter_value_hints")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.filter_value_hints) == 1

    def test_sample_rows_categorized(self):
        fact = _fact(fact_type="sample_rows")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.sample_rows) == 1

    def test_column_stats_categorized(self):
        fact = _fact(fact_type="column_stats")
        profile = merge_semantic_facts("TESTDB", [fact], [], [], [])
        assert len(profile.column_stats) == 1


class TestDifferentSubjects:
    def test_different_subjects_not_merged(self):
        f1 = _fact(subject="TESTDB.PUBLIC.T.COL_A")
        f2 = _fact(subject="TESTDB.PUBLIC.T.COL_B")
        profile = merge_semantic_facts("TESTDB", [f1, f2], [], [], [])
        assert len(profile.time_columns) == 2

    def test_all_four_source_types(self):
        metadata = [_fact(source=["metadata"], evidence=["m"])]
        docs = [_fact(fact_type="field_definition", source=["docs"], evidence=["d"])]
        probes = [_fact(fact_type="filter_value_hints", source=["probes"], evidence=["p"])]
        traces = [_fact(fact_type="frequently_used_table", source=["traces"], evidence=["t"])]
        profile = merge_semantic_facts("TESTDB", metadata, docs, probes, traces)
        assert len(profile.all_facts()) == 4
