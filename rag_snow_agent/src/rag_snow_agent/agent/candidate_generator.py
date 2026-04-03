"""Generate multiple diverse SQL candidates for one instruction."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from pydantic import ValidationError

from ..prompting.plan_schema import QueryPlan
from ..prompting.prompt_builder import (
    build_fix_json_prompt,
    build_plan_prompt_with_strategy,
    build_sql_prompt,
)
from ..prompting.sql_compiler import compile_plan
from ..retrieval.hybrid_retriever import HybridRetriever
from ..retrieval.plan_expansion import expand_schema_for_plan
from ..retrieval.schema_slice import SchemaSlice
from .llm_client import call_llm

log = logging.getLogger(__name__)

# Strategy rotation order (cycled for n > len).
# flatten_first and cte_first are placed early so they are used
# even with small best_of_n values (e.g. n=3).
STRATEGIES = [
    "default",
    "flatten_first",
    "cte_first",
    "join_first",
    "metric_first",
    "time_first",
]


@dataclass
class CandidateItem:
    candidate_id: int
    strategy: str
    plan: QueryPlan | None = None
    sql: str = ""
    generation_notes: dict = field(default_factory=dict)


def _strip_markdown_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def _try_parse_plan(
    raw: str,
    model: str,
    max_tokens: int,
) -> tuple[QueryPlan | None, str]:
    """Attempt to parse plan JSON; retry once on failure. Returns (plan, raw_used)."""
    cleaned = _strip_markdown_fences(raw)
    try:
        data = json.loads(cleaned)
        return QueryPlan.model_validate(data), cleaned
    except (json.JSONDecodeError, ValidationError) as exc:
        log.warning("Plan parse failed, attempting fix: %s", str(exc)[:120])
        fix_msgs = build_fix_json_prompt(raw, str(exc))
        fixed = call_llm(fix_msgs, model=model, temperature=0.0, max_tokens=max_tokens)
        cleaned2 = _strip_markdown_fences(fixed)
        try:
            data2 = json.loads(cleaned2)
            return QueryPlan.model_validate(data2), cleaned2
        except (json.JSONDecodeError, ValidationError) as exc2:
            log.warning("Plan fix also failed: %s", str(exc2)[:120])
            return None, raw


def generate_candidate_sqls(
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_tokens: int = 800,
    n: int = 2,
    strategies: list[str] | None = None,
    retriever: HybridRetriever | None = None,
    semantic_context: str | None = None,
    decompose: bool = False,
    sample_context: str | None = None,
) -> list[CandidateItem]:
    """Produce *n* candidate SQLs using diverse prompt strategies.

    Each candidate uses a different planning strategy to encourage diversity.
    """
    if strategies is None:
        strategies = STRATEGIES

    candidates: list[CandidateItem] = []

    # Build decomposition context once (reused across all candidates)
    decomp_ctx = None
    if decompose:
        try:
            from ..prompting.question_decomposition import (
                decompose_question,
                render_decomposition_for_prompt,
            )
            decomp = decompose_question(instruction, model=model, max_tokens=max_tokens)
            decomp_ctx = render_decomposition_for_prompt(decomp, semantic_context)
            log.info("Question decomposition: %d chars", len(decomp_ctx) if decomp_ctx else 0)
        except Exception as exc:
            log.error("Question decomposition FAILED: %s", exc, exc_info=True)
            raise

    if semantic_context:
        log.info("Semantic context injected: %d chars", len(semantic_context))

    for i in range(n):
        strategy = strategies[i % len(strategies)]
        log.info("Generating candidate %d/%d with strategy '%s'", i + 1, n, strategy)

        messages = build_plan_prompt_with_strategy(
            instruction, schema_slice, strategy,
            semantic_context=semantic_context,
            decomposition_context=decomp_ctx,
            sample_context=sample_context,
        )
        # Slightly vary temperature for non-default strategies to encourage diversity
        temp = temperature if i == 0 else min(temperature + 0.1, 0.8)
        plan_raw = call_llm(messages, model=model, temperature=temp, max_tokens=max_tokens)

        plan, _ = _try_parse_plan(plan_raw, model, max_tokens)

        # Plan-guided schema expansion (optional)
        if plan is not None and retriever is not None:
            try:
                expand_schema_for_plan(schema_slice, plan, retriever, db_id)
            except Exception as exc:
                log.warning("Plan expansion FAILED for candidate %d: %s", i + 1, exc, exc_info=True)

        if plan is not None:
            sql = compile_plan(plan, schema_slice)
            # If compile produced SELECT 1 (empty selected_tables), retry
            # with feedback so the LLM can fix the plan
            if sql.strip() == "SELECT 1" and plan.selected_tables == []:
                log.warning(
                    "Candidate %d: plan has empty selected_tables, retrying with feedback",
                    i + 1,
                )
                fix_messages = build_plan_prompt_with_strategy(
                    instruction, schema_slice, strategy,
                    semantic_context=semantic_context,
                    decomposition_context=decomp_ctx,
                    sample_context=sample_context,
                )
                fix_messages.append({
                    "role": "assistant",
                    "content": plan_raw,
                })
                fix_messages.append({
                    "role": "user",
                    "content": (
                        "Your plan has empty selected_tables. "
                        "You MUST include at least one table from the schema in selected_tables. "
                        "Return the corrected plan JSON only."
                    ),
                })
                retry_raw = call_llm(fix_messages, model=model, temperature=0.0, max_tokens=max_tokens)
                retry_plan, _ = _try_parse_plan(retry_raw, model, max_tokens)
                if retry_plan is not None and retry_plan.selected_tables:
                    plan = retry_plan
                    sql = compile_plan(plan, schema_slice)
                    log.info("Retry produced plan with %d tables", len(plan.selected_tables))

            # If compiler still produces SELECT 1, fall back to LLM SQL generation
            if sql.strip().startswith("SELECT 1"):
                log.warning(
                    "Candidate %d: compiler produced SELECT 1, falling back to LLM SQL generation",
                    i + 1,
                )
                sql_messages = build_sql_prompt(plan, schema_slice)
                raw_sql = call_llm(sql_messages, model=model, temperature=temp, max_tokens=max_tokens)
                fallback_sql = _strip_markdown_fences(raw_sql)
                if fallback_sql and not fallback_sql.startswith("SELECT 1"):
                    sql = fallback_sql
                    log.info("LLM SQL fallback produced %d chars of SQL", len(sql))
        else:
            sql = "SELECT 1 /* plan parse failed */"

        candidates.append(
            CandidateItem(
                candidate_id=i + 1,
                strategy=strategy,
                plan=plan,
                sql=sql,
                generation_notes={
                    "temperature": temp,
                    "plan_parsed": plan is not None,
                },
            )
        )

    return candidates
