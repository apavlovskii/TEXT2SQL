"""Pydantic models for the semantic layer."""

from __future__ import annotations

from pydantic import BaseModel


class SemanticFact(BaseModel):
    """A single semantic fact about a database object."""

    fact_type: str  # primary_time_column, metric_candidate, dimension_candidate, etc.
    subject: str  # qualified column name or table name
    value: str | dict | list  # the semantic value/hint
    confidence: float  # 0.0 to 1.0
    evidence: list[str] = []
    source: list[str] = []  # metadata, docs, probes, traces


class SemanticCard(BaseModel):
    """Semantic fact card for Chroma storage."""

    db_id: str
    fact_type: str
    subject: str
    confidence: float
    source_types: list[str]

    @property
    def document(self) -> str:
        """Text representation used as the ChromaDB document (embedded)."""
        parts = [
            f"Semantic: {self.fact_type}",
            f"Subject: {self.subject}",
            f"Confidence: {self.confidence}",
            f"Sources: {', '.join(self.source_types)}",
        ]
        return "\n".join(parts)

    def chroma_id(self) -> str:
        return f"semantic:{self.db_id}:{self.fact_type}:{self.subject}"

    def chroma_metadata(self) -> dict:
        return {
            "db_id": self.db_id,
            "object_type": "semantic",
            "fact_type": self.fact_type,
            "subject": self.subject,
            "confidence": self.confidence,
            "source_types": ",".join(self.source_types),
        }


class SemanticProfile(BaseModel):
    """Aggregated semantic profile for a database."""

    db_id: str
    time_columns: list[SemanticFact] = []
    metric_candidates: list[SemanticFact] = []
    dimension_candidates: list[SemanticFact] = []
    nested_field_patterns: list[SemanticFact] = []
    join_semantics: list[SemanticFact] = []
    filter_value_hints: list[SemanticFact] = []
    sample_rows: list[SemanticFact] = []
    column_stats: list[SemanticFact] = []

    def all_facts(self) -> list[SemanticFact]:
        """Return all facts across all categories."""
        return (
            self.time_columns
            + self.metric_candidates
            + self.dimension_candidates
            + self.nested_field_patterns
            + self.join_semantics
            + self.filter_value_hints
            + self.sample_rows
            + self.column_stats
        )
