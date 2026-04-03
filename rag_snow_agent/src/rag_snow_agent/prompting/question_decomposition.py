"""Decompose a natural-language question into semantic subgoals."""

from __future__ import annotations

import json
import logging

from pydantic import BaseModel

from ..agent.llm_client import call_llm

log = logging.getLogger(__name__)


class QuestionDecomposition(BaseModel):
    """Structured breakdown of a natural-language question."""

    temporal_scope: str | None = None  # "year 2017", "January 7, 2021"
    temporal_grain: str | None = None  # "month", "day", "year"
    filters: list[str] = []  # "campaign name contains 'Data Share'"
    cohort_conditions: list[str] = []  # "visitors who made at least one transaction"
    target_entity: str | None = None  # "distinct pseudo users", "products"
    target_grain: str | None = None  # "per visitor", "per product"
    measures: list[str] = []  # "COUNT DISTINCT", "SUM(revenue)"
    set_operations: list[str] = []  # "MINUS", "excluding", "but not"
    ranking: str | None = None  # "top 5", "highest"
    grouping: list[str] = []  # "by month", "for each category"
    nested_fields: list[str] = []  # "trafficSource.source", "hits.product"
    expected_shape: str | None = None  # "single number", "table with 12 rows"
    notes: list[str] = []  # ambiguities or special requirements


_DECOMPOSE_SYSTEM = """\
You are a SQL query analyst. Given a natural-language question about a database, \
decompose it into structured semantic subgoals. Return ONLY valid JSON matching \
the schema below. No markdown, no explanation.

JSON schema:
{
  "temporal_scope": "string or null — time period mentioned (e.g. 'year 2017')",
  "temporal_grain": "string or null — time granularity (e.g. 'month', 'day', 'year')",
  "filters": ["list of filter conditions mentioned"],
  "cohort_conditions": ["list of cohort/subset conditions"],
  "target_entity": "string or null — what is being counted/measured",
  "target_grain": "string or null — per-what granularity",
  "measures": ["list of aggregation functions needed"],
  "set_operations": ["list of set operations like MINUS, EXCEPT, excluding"],
  "ranking": "string or null — ranking requirement like 'top 5'",
  "grouping": ["list of grouping dimensions"],
  "nested_fields": ["list of nested/variant field paths"],
  "expected_shape": "string or null — expected output shape",
  "notes": ["any ambiguities or special requirements"]
}\
"""

_DECOMPOSE_USER = """\
Question: {instruction}

Decompose this question into semantic subgoals. Return JSON only.\
"""


def _strip_markdown_fences(text: str) -> str:
    """Remove ```json ... ``` or ``` ... ``` wrappers."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def decompose_question(
    instruction: str,
    model: str = "gpt-4o-mini",
    max_tokens: int | None = None,
) -> QuestionDecomposition:
    """Use LLM to decompose a question into semantic subgoals."""
    messages = [
        {"role": "system", "content": _DECOMPOSE_SYSTEM},
        {"role": "user", "content": _DECOMPOSE_USER.format(instruction=instruction)},
    ]
    try:
        raw = call_llm(messages, model=model, temperature=0.0, max_tokens=max_tokens)
        cleaned = _strip_markdown_fences(raw)
        data = json.loads(cleaned)
        return QuestionDecomposition.model_validate(data)
    except Exception:
        log.warning("Question decomposition failed, returning minimal decomposition", exc_info=True)
        return QuestionDecomposition(notes=[instruction])


def render_decomposition_for_prompt(
    decomp: QuestionDecomposition,
    semantic_context: str | None = None,
) -> str:
    """Render decomposition as a structured block for the plan prompt."""
    lines: list[str] = ["Question decomposition:"]

    if decomp.temporal_scope:
        entry = f"  Temporal: {decomp.temporal_scope}"
        if decomp.temporal_grain:
            entry += f" (grain: {decomp.temporal_grain})"
        lines.append(entry)
    elif decomp.temporal_grain:
        lines.append(f"  Temporal grain: {decomp.temporal_grain}")

    for m in decomp.measures:
        lines.append(f"  Measure: {m}")

    if decomp.target_entity:
        entry = f"  Target entity: {decomp.target_entity}"
        if decomp.target_grain:
            entry += f" ({decomp.target_grain})"
        lines.append(entry)
    elif decomp.target_grain:
        lines.append(f"  Target grain: {decomp.target_grain}")

    for f in decomp.filters:
        lines.append(f"  Filter: {f}")

    for c in decomp.cohort_conditions:
        lines.append(f"  Cohort: {c}")

    for g in decomp.grouping:
        lines.append(f"  Grouping: {g}")

    for s in decomp.set_operations:
        lines.append(f"  Set operation: {s}")

    if decomp.ranking:
        lines.append(f"  Ranking: {decomp.ranking}")

    for nf in decomp.nested_fields:
        lines.append(f"  Nested field: {nf}")
        lines.append(f"    → Use flatten_ops for this field (LATERAL FLATTEN on the parent VARIANT ARRAY column)")

    if decomp.expected_shape:
        lines.append(f"  Output: {decomp.expected_shape}")

    for n in decomp.notes:
        lines.append(f"  Note: {n}")

    # ── Strategic hints based on decomposition complexity ───────────────
    needs_ctes = bool(
        decomp.set_operations
        or (decomp.ranking and len(decomp.filters) + len(decomp.cohort_conditions) > 1)
        or len(decomp.cohort_conditions) > 1
    )
    if needs_ctes:
        step_count = 1 + len(decomp.set_operations) + len(decomp.cohort_conditions)
        lines.append(
            f"  Planning hint: This question requires ~{step_count} intermediate steps. "
            f"Use the 'ctes' array to build a multi-step pipeline."
        )

    if decomp.nested_fields:
        lines.append(
            "  Planning hint: Nested fields detected — add flatten_ops for each "
            "VARIANT ARRAY column referenced. Do NOT access arrays directly."
        )

    # If only the header line was added, there's nothing useful to render
    if len(lines) <= 1:
        return ""

    return "\n".join(lines)
