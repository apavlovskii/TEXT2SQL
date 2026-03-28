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
)
from ..prompting.sql_compiler import compile_plan
from ..retrieval.schema_slice import SchemaSlice
from .llm_client import call_llm

log = logging.getLogger(__name__)

# Strategy rotation order (cycled for n > len)
STRATEGIES = ["default", "join_first", "metric_first", "time_first"]


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
) -> list[CandidateItem]:
    """Produce *n* candidate SQLs using diverse prompt strategies.

    Each candidate uses a different planning strategy to encourage diversity.
    """
    if strategies is None:
        strategies = STRATEGIES

    candidates: list[CandidateItem] = []
    for i in range(n):
        strategy = strategies[i % len(strategies)]
        log.info("Generating candidate %d/%d with strategy '%s'", i + 1, n, strategy)

        messages = build_plan_prompt_with_strategy(instruction, schema_slice, strategy)
        # Slightly vary temperature for non-default strategies to encourage diversity
        temp = temperature if i == 0 else min(temperature + 0.1, 0.8)
        plan_raw = call_llm(messages, model=model, temperature=temp, max_tokens=max_tokens)

        plan, _ = _try_parse_plan(plan_raw, model, max_tokens)

        if plan is not None:
            sql = compile_plan(plan, schema_slice)
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
