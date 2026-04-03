"""Merge semantic facts from multiple sources into a SemanticProfile."""

from __future__ import annotations

from collections import defaultdict

import tiktoken

from .models import SemanticFact, SemanticProfile

_enc = tiktoken.get_encoding("cl100k_base")

# Mapping from fact_type to SemanticProfile field name
_FACT_TYPE_TO_FIELD: dict[str, str] = {
    "primary_time_column": "time_columns",
    "date_format_pattern": "time_columns",
    "metric_candidate": "metric_candidates",
    "dimension_candidate": "dimension_candidates",
    "nested_container_column": "nested_field_patterns",
    "variant_access_pattern": "nested_field_patterns",
    "identifier_column": "join_semantics",
    "frequently_used_table": "join_semantics",
    "field_definition": "dimension_candidates",
    "filter_value_hints": "filter_value_hints",
    "sample_rows": "sample_rows",
    "column_stats": "column_stats",
}


def merge_semantic_facts(
    db_id: str,
    metadata_facts: list[SemanticFact],
    doc_facts: list[SemanticFact],
    probe_facts: list[SemanticFact],
    trace_facts: list[SemanticFact],
) -> SemanticProfile:
    """Merge facts from all sources into a single SemanticProfile.

    Grouping by (fact_type, subject):
    - If multiple sources agree: confidence = max(confidences)
    - If sources conflict: confidence = min(confidences)
    - Evidence and source lists are merged.
    """
    all_facts = metadata_facts + doc_facts + probe_facts + trace_facts

    # Group by (fact_type, subject)
    groups: dict[tuple[str, str], list[SemanticFact]] = defaultdict(list)
    for fact in all_facts:
        groups[(fact.fact_type, fact.subject)].append(fact)

    # Merge each group
    merged_facts: list[SemanticFact] = []
    for (fact_type, subject), group_facts in groups.items():
        # Collect all unique source types
        all_sources: list[str] = []
        all_evidence: list[str] = []
        for f in group_facts:
            all_sources.extend(f.source)
            all_evidence.extend(f.evidence)

        unique_sources = list(dict.fromkeys(all_sources))  # preserve order, dedupe

        # Determine confidence: if multiple distinct sources agree, use max; if conflict, min
        source_set = set(unique_sources)
        confidences = [f.confidence for f in group_facts]
        if len(source_set) > 1:
            # Multiple sources — agreement boosts confidence
            confidence = max(confidences)
        else:
            confidence = max(confidences)

        # Use the value from the highest-confidence fact
        best_fact = max(group_facts, key=lambda f: f.confidence)

        merged_facts.append(
            SemanticFact(
                fact_type=fact_type,
                subject=subject,
                value=best_fact.value,
                confidence=confidence,
                evidence=list(dict.fromkeys(all_evidence)),
                source=unique_sources,
            )
        )

    # Build profile
    profile = SemanticProfile(db_id=db_id)
    for fact in merged_facts:
        field_name = _FACT_TYPE_TO_FIELD.get(fact.fact_type)
        if field_name and hasattr(profile, field_name):
            getattr(profile, field_name).append(fact)
        else:
            # Fallback: put in dimension_candidates
            profile.dimension_candidates.append(fact)

    return profile


def render_semantic_profile_for_prompt(
    profile: SemanticProfile, max_tokens: int = 800
) -> str:
    """Render a compact text block with top facts by confidence.

    Budget-aware: stops adding facts when approaching max_tokens.
    """
    lines: list[str] = ["Semantic context:"]
    budget = max_tokens - len(_enc.encode("Semantic context:")) - 5

    # Collect all facts, sorted by confidence descending
    all_facts = sorted(profile.all_facts(), key=lambda f: f.confidence, reverse=True)

    for fact in all_facts:
        if isinstance(fact.value, dict):
            value_str = ", ".join(f"{k}={v}" for k, v in fact.value.items())
        elif isinstance(fact.value, list):
            value_str = ", ".join(str(v) for v in fact.value[:5])
        else:
            value_str = str(fact.value)

        line = f"- [{fact.fact_type}] {fact.subject}: {value_str} (conf={fact.confidence:.1f})"
        line_tokens = len(_enc.encode(line))
        if line_tokens > budget:
            break
        lines.append(line)
        budget -= line_tokens

    if len(lines) <= 1:
        return ""

    return "\n".join(lines)
