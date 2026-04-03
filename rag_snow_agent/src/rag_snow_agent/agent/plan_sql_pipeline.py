"""Plan → SQL pipeline: generate plan, compile SQL, validate identifiers."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from pydantic import ValidationError

from ..prompting.constraints import ValidationResult, validate_sql
from ..prompting.plan_schema import QueryPlan
from ..prompting.prompt_builder import (
    build_fix_json_prompt,
    build_fix_plan_prompt,
    build_plan_prompt,
    build_sql_prompt,
)
from ..prompting.question_decomposition import (
    QuestionDecomposition,
    decompose_question,
    render_decomposition_for_prompt,
)
from ..prompting.sql_compiler import compile_plan
from ..prompting.subgoal_validator import validate_sql_against_decomposition
from ..retrieval.hybrid_retriever import HybridRetriever
from ..retrieval.plan_expansion import expand_schema_for_plan
from ..retrieval.schema_slice import SchemaSlice
from .llm_client import call_llm

log = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Output of the plan→SQL pipeline."""

    sql: str
    plan: QueryPlan | None = None
    plan_json_raw: str = ""
    compiled_sql: str = ""
    llm_sql: str = ""
    validation: ValidationResult | None = None
    warnings: list[str] = field(default_factory=list)
    llm_calls: int = 0


def _strip_markdown_fences(text: str) -> str:
    """Remove ```json ... ``` or ``` ... ``` wrappers."""
    text = text.strip()
    if text.startswith("```"):
        # Remove first line (```json or ```)
        lines = text.split("\n")
        lines = lines[1:]  # drop opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _parse_plan(raw: str) -> QueryPlan:
    """Parse raw LLM output into a QueryPlan."""
    cleaned = _strip_markdown_fences(raw)
    data = json.loads(cleaned)
    return QueryPlan.model_validate(data)


def _extract_sql(raw: str) -> str:
    """Extract SQL from LLM output, removing markdown fences if present."""
    text = _strip_markdown_fences(raw)
    # Also strip ```sql fences
    if text.upper().startswith("SQL"):
        text = text[3:].strip()
    return text.strip().rstrip(";") + ";"


def run_pipeline(
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_tokens: int = 800,
    plan_retry_limit: int = 1,
    validation_fix_limit: int = 1,
    use_llm_sql: bool = False,
    memory_context: str | None = None,
    retriever: HybridRetriever | None = None,
    semantic_context: str | None = None,
    decompose: bool = False,
    decomposition_model: str | None = None,
    sample_context: str | None = None,
) -> PipelineResult:
    """Execute the full plan → SQL pipeline.

    Steps:
    1. Generate plan JSON via LLM
    2. Parse into QueryPlan (retry once if invalid JSON)
    3. Compile plan into SQL deterministically
    4. Validate identifiers
    5. If validation fails, fix plan via LLM and recompile
    """
    result = PipelineResult(sql="")

    # ── Step 0 (optional): Decompose question ──────────────────────────
    decomp: QuestionDecomposition | None = None
    decomposition_context: str | None = None
    if decompose:
        decomp_model = decomposition_model or model
        decomp = decompose_question(instruction, model=decomp_model)
        result.llm_calls += 1
        decomposition_context = render_decomposition_for_prompt(
            decomp, semantic_context=semantic_context,
        ) or None

    # ── Step 1: Generate plan ───────────────────────────────────────────
    plan_messages = build_plan_prompt(
        instruction, schema_slice, memory_context=memory_context,
        semantic_context=semantic_context,
        decomposition_context=decomposition_context,
        sample_context=sample_context,
    )
    plan_raw = call_llm(plan_messages, model=model, temperature=temperature, max_tokens=max_tokens)
    result.plan_json_raw = plan_raw
    result.llm_calls += 1

    # ── Step 2: Parse plan ──────────────────────────────────────────────
    plan: QueryPlan | None = None
    for attempt in range(1 + plan_retry_limit):
        try:
            plan = _parse_plan(plan_raw)
            break
        except (json.JSONDecodeError, ValidationError) as exc:
            error_msg = str(exc)
            log.warning("Plan parse failed (attempt %d): %s", attempt + 1, error_msg)
            if attempt < plan_retry_limit:
                fix_messages = build_fix_json_prompt(plan_raw, error_msg)
                plan_raw = call_llm(fix_messages, model=model, temperature=0.0, max_tokens=max_tokens)
                result.plan_json_raw = plan_raw
                result.llm_calls += 1

    if plan is None:
        result.warnings.append("Failed to parse plan JSON after retries")
        result.sql = "SELECT 1 /* plan parse failed */"
        return result

    result.plan = plan

    # ── Step 2b: Plan-guided schema expansion ──────────────────────────
    if retriever is not None:
        try:
            expand_schema_for_plan(schema_slice, plan, retriever, db_id)
        except Exception:
            log.warning("Plan-guided schema expansion failed", exc_info=True)

    # ── Step 3: Compile plan into SQL ───────────────────────────────────
    if use_llm_sql:
        sql_messages = build_sql_prompt(plan, schema_slice)
        llm_sql_raw = call_llm(sql_messages, model=model, temperature=temperature, max_tokens=max_tokens)
        result.llm_calls += 1
        result.llm_sql = _extract_sql(llm_sql_raw)
        result.compiled_sql = result.llm_sql
    else:
        result.compiled_sql = compile_plan(plan, schema_slice)

    result.sql = result.compiled_sql

    # ── Step 4: Validate identifiers ────────────────────────────────────
    validation = validate_sql(result.sql, schema_slice)
    result.validation = validation

    # ── Step 4b: Validate against decomposition subgoals ──────────────
    if decomp is not None:
        _, decomp_warnings = validate_sql_against_decomposition(result.sql, decomp)
        result.warnings.extend(decomp_warnings)

    if validation.valid:
        return result

    # ── Step 5: Fix plan if validation fails ────────────────────────────
    for fix_attempt in range(validation_fix_limit):
        log.info(
            "Validation failed (%d errors), attempting fix %d/%d",
            len(validation.errors),
            fix_attempt + 1,
            validation_fix_limit,
        )
        result.warnings.extend(validation.errors)

        fix_messages = build_fix_plan_prompt(plan, schema_slice, validation.errors)
        fixed_raw = call_llm(fix_messages, model=model, temperature=0.0, max_tokens=max_tokens)
        result.llm_calls += 1

        try:
            plan = _parse_plan(fixed_raw)
            result.plan = plan
        except (json.JSONDecodeError, ValidationError) as exc:
            result.warnings.append(f"Fix-plan parse failed: {exc}")
            break

        if use_llm_sql:
            sql_messages = build_sql_prompt(plan, schema_slice)
            llm_sql_raw = call_llm(sql_messages, model=model, temperature=temperature, max_tokens=max_tokens)
            result.llm_calls += 1
            result.compiled_sql = _extract_sql(llm_sql_raw)
        else:
            result.compiled_sql = compile_plan(plan, schema_slice)

        result.sql = result.compiled_sql
        validation = validate_sql(result.sql, schema_slice)
        result.validation = validation

        if validation.valid:
            return result

    # Return best-effort result
    if not validation.valid:
        result.warnings.extend(validation.errors)

    return result
