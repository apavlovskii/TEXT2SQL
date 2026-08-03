"""Generate multiple diverse SQL candidates for one instruction."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from pydantic import ValidationError

from ..prompting.plan_schema import QueryPlan
from ..prompting.prompt_builder import (
    build_fix_json_prompt,
    build_plan_prompt_with_strategy,
    build_raw_sql_prompt_with_strategy,
    build_sql_prompt,
)
from ..prompting.sql_compiler import (
    compile_plan,
    rewrite_date_sharded_tables,
    rewrite_listagg_nullif,
)
from ..prompting.sql_correction import (
    build_fix_sql_prompt,
    check_type_mismatches_raw,
    validate_raw_sql,
)
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
    "geo_first",
]

_CHANGE_WORD_RE = re.compile(
    r"\b(change|difference|delta|variation)\b", re.IGNORECASE
)
_DIRECTIONAL_QUALIFIER_RE = re.compile(
    r"\b(increase[ds]?|decrease[ds]?|growth|grew|declin(?:e|ed|es)|ris(?:e|es|en)|rose|"
    r"drop(?:s|ped)?|gain(?:s|ed)?|loss(?:es)?|lost|uptick|downtick|upswing|downswing|"
    r"positive|negative)\b",
    re.IGNORECASE,
)

_SIGNED_CHANGE_HINT = (
    "\nInterpretation directive (this candidate only): the question's wording "
    "around \"change\"/\"difference\" has no explicit direction (no \"increase\"/"
    "\"decrease\"/etc.), which is genuinely ambiguous. For THIS candidate, compute "
    "it as a SIGNED value: (later value - earlier value), which can be negative. "
    "Do not take an absolute value."
)
_ABSOLUTE_CHANGE_HINT = (
    "\nInterpretation directive (this candidate only): the question's wording "
    "around \"change\"/\"difference\" has no explicit direction (no \"increase\"/"
    "\"decrease\"/etc.), which is genuinely ambiguous. For THIS candidate, compute "
    "it as an ABSOLUTE MAGNITUDE: ABS(later value - earlier value), which is always "
    "non-negative — i.e. treat a big decrease as just as large a \"change\" as a big "
    "increase."
)


def _detect_unqualified_change_ambiguity(instruction: str) -> bool:
    """True if the question uses "change"/"difference"/"delta"/"variation"
    without any wording that would pin down whether a signed (can be
    negative) or absolute-magnitude (always non-negative) value is meant.

    This is a real, recurring failure mode (confirmed on sf_local056 —
    "highest average monthly change in payment amounts": gold computed
    ABS(delta), the pipeline computed a signed delta, both are defensible
    readings of the same English sentence) — not a hypothetical. Rather than
    guess which reading is "right" (there's no way to know without the
    gold answer), both interpretations are deliberately represented among
    the Best-of-N candidates so self-consistency/verification/tie-break
    gets a real choice instead of 8 independent rolls that can all land on
    the same guess.
    """
    return bool(_CHANGE_WORD_RE.search(instruction)) and not _DIRECTIONAL_QUALIFIER_RE.search(
        instruction
    )


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


@dataclass
class PendingCandidateRequest:
    """One not-yet-generated candidate — a built prompt, no LLM call made yet."""

    candidate_id: int
    strategy: str
    messages: list[dict[str, str]]
    temperature: float


def build_raw_sql_candidate_requests(
    instruction: str,
    schema_slice: SchemaSlice,
    model: str,
    max_tokens: int,
    n: int = 2,
    strategies: list[str] | None = None,
    temperature: float = 0.2,
    semantic_context: str | None = None,
    decompose: bool = False,
    sample_context: str | None = None,
    exploration_context: str | None = None,
    plan_context: str | None = None,
) -> list[PendingCandidateRequest]:
    """Build every raw-SQL candidate prompt for one instance, without calling
    the LLM for candidate generation — the batch-generation counterpart to
    generate_candidate_sqls's per-candidate loop (see assemble_candidates_
    from_batch_results for the other half). Question decomposition, if
    enabled, still runs synchronously here: it's a single per-instance call
    whose result feeds every candidate's prompt, not one of the N mutually
    independent per-strategy calls this function batches.
    """
    if strategies is None:
        strategies = STRATEGIES

    decomp_ctx = None
    if decompose:
        try:
            from ..prompting.question_decomposition import (
                decompose_question,
                render_decomposition_for_prompt,
            )
            decomp = decompose_question(instruction, model=model, max_tokens=max_tokens)
            decomp_ctx = render_decomposition_for_prompt(decomp, semantic_context)
        except Exception as exc:
            log.error("Question decomposition FAILED: %s", exc, exc_info=True)
            raise

    change_ambiguous = _detect_unqualified_change_ambiguity(instruction)

    pending: list[PendingCandidateRequest] = []
    for i in range(n):
        strategy = strategies[i % len(strategies)]
        temp = temperature if i == 0 else min(temperature + 0.1, 0.8)

        # See generate_candidate_sqls for the matching sync-path logic and
        # _detect_unqualified_change_ambiguity's docstring for rationale.
        candidate_decomp_ctx = decomp_ctx
        if change_ambiguous and n >= 2:
            if i == 0:
                candidate_decomp_ctx = (decomp_ctx or "") + _SIGNED_CHANGE_HINT
            elif i == 1:
                candidate_decomp_ctx = (decomp_ctx or "") + _ABSOLUTE_CHANGE_HINT

        messages = _build_raw_sql_messages(
            instruction, schema_slice, strategy,
            semantic_context, candidate_decomp_ctx, sample_context, exploration_context, plan_context,
        )
        pending.append(PendingCandidateRequest(
            candidate_id=i + 1, strategy=strategy, messages=messages, temperature=temp,
        ))
    return pending


def assemble_candidates_from_batch_results(
    pending: list[PendingCandidateRequest],
    batch_results: dict[str, str],
    custom_id_prefix: str,
    schema_slice: SchemaSlice,
    model: str,
    max_tokens: int,
) -> list[CandidateItem]:
    """Turn one instance's batch-generation results back into CandidateItems,
    applying the same post-processing _generate_raw_sql_candidate uses for
    the synchronous path (see _postprocess_raw_sql_candidate) — so a batched
    run and a synchronous run handle validation/repair identically.

    A candidate whose custom_id is missing from batch_results (the request
    failed or expired inside the batch job) becomes a failed placeholder
    candidate rather than raising — Best-of-N already tolerates individual
    candidate failures (see generate_candidate_sqls's "SELECT 1" fallback),
    so one bad batch entry doesn't sink the whole instance.
    """
    candidates: list[CandidateItem] = []
    for req in pending:
        custom_id = f"{custom_id_prefix}::{req.candidate_id}"
        raw = batch_results.get(custom_id)
        if raw is None:
            log.warning("Batch candidate %s missing from results (failed/expired)", custom_id)
            candidates.append(CandidateItem(
                candidate_id=req.candidate_id,
                strategy=req.strategy,
                plan=None,
                sql="SELECT 1 /* batch request failed */",
                generation_notes={"temperature": req.temperature, "batch_failed": True},
            ))
            continue
        _, sql, notes = _postprocess_raw_sql_candidate(raw, schema_slice, model, max_tokens)
        candidates.append(CandidateItem(
            candidate_id=req.candidate_id,
            strategy=req.strategy,
            plan=None,
            sql=sql,
            generation_notes={"temperature": req.temperature, **notes},
        ))
    return candidates


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
    exploration_context: str | None = None,
    plan_context: str | None = None,
    use_deterministic_compiler: bool = False,
) -> list[CandidateItem]:
    """Produce *n* candidate SQLs using diverse prompt strategies.

    Each candidate uses a different planning strategy to encourage diversity.

    *use_deterministic_compiler* selects the SQL-generation mechanism:
    - False (default): the LLM writes SQL directly (build_raw_sql_prompt_with_strategy),
      followed by a deterministic correction layer (date-shard rewriting,
      schema-card column/type validation, one repair attempt) — see
      sql_correction.py. No QueryPlan is produced; CandidateItem.plan is None.
    - True: the legacy path — LLM produces a structured QueryPlan, which
      compile_plan() assembles deterministically into SQL. Kept for
      comparison/ablation; measured to underperform the default on the same
      instances (~70% execute / ~10% gold-match vs. ~95% / ~35% for raw SQL).
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

    change_ambiguous = _detect_unqualified_change_ambiguity(instruction)

    for i in range(n):
        strategy = strategies[i % len(strategies)]
        log.info("Generating candidate %d/%d with strategy '%s'", i + 1, n, strategy)
        # Slightly vary temperature for non-default strategies to encourage diversity
        temp = temperature if i == 0 else min(temperature + 0.1, 0.8)

        # For an unqualified "change" question, force candidates 1 and 2
        # (of n>=2) to each commit to a different interpretation (signed vs
        # absolute magnitude) rather than leaving it to chance which one (if
        # either) the model's natural sampling happens to produce — see
        # _detect_unqualified_change_ambiguity's docstring.
        candidate_decomp_ctx = decomp_ctx
        if change_ambiguous and n >= 2:
            if i == 0:
                candidate_decomp_ctx = (decomp_ctx or "") + _SIGNED_CHANGE_HINT
            elif i == 1:
                candidate_decomp_ctx = (decomp_ctx or "") + _ABSOLUTE_CHANGE_HINT

        if use_deterministic_compiler:
            plan, sql, notes = _generate_plan_compiled_candidate(
                db_id, instruction, schema_slice, strategy, model, temp, max_tokens,
                i, retriever, semantic_context, candidate_decomp_ctx, sample_context,
                exploration_context, plan_context,
            )
        else:
            plan, sql, notes = _generate_raw_sql_candidate(
                instruction, schema_slice, strategy, model, temp, max_tokens,
                semantic_context, candidate_decomp_ctx, sample_context,
                exploration_context, plan_context,
            )

        candidates.append(
            CandidateItem(
                candidate_id=i + 1,
                strategy=strategy,
                plan=plan,
                sql=sql,
                generation_notes={"temperature": temp, **notes},
            )
        )

    return candidates


def _build_raw_sql_messages(
    instruction: str,
    schema_slice: SchemaSlice,
    strategy: str,
    semantic_context: str | None,
    decomp_ctx: str | None,
    sample_context: str | None,
    exploration_context: str | None,
    plan_context: str | None,
) -> list[dict[str, str]]:
    """Build the raw-SQL-generation prompt for one strategy — no LLM call.

    Split out from _generate_raw_sql_candidate so a batch-generation caller
    (see build_raw_sql_candidate_requests) can build every candidate's prompt
    up front, submit them all as one OpenAI Batch API job, and only then run
    _postprocess_raw_sql_candidate on each result — the exact same
    post-processing the synchronous path uses.
    """
    return build_raw_sql_prompt_with_strategy(
        instruction, schema_slice, strategy,
        semantic_context=semantic_context,
        decomposition_context=decomp_ctx,
        sample_context=sample_context,
        exploration_context=exploration_context,
        plan_context=plan_context,
    )


def _postprocess_raw_sql_candidate(
    raw: str,
    schema_slice: SchemaSlice,
    model: str,
    max_tokens: int,
) -> tuple[None, str, dict]:
    """Deterministic correction layer applied to raw LLM SQL output — date-shard
    rewriting, schema-card validation, one repair attempt (a synchronous LLM
    call; validation failures are rare enough — see sql_correction.py — that
    this isn't worth batching). Shared by both the synchronous and
    batch-generation candidate paths so they apply identical post-processing.
    """
    sql = rewrite_date_sharded_tables(_strip_markdown_fences(raw), schema_slice)

    id_result = validate_raw_sql(sql, schema_slice)
    type_errors = check_type_mismatches_raw(sql, schema_slice)
    all_errors = list(id_result.errors) + type_errors
    repair_attempted = False
    repair_succeeded = False

    if all_errors:
        repair_attempted = True
        log.info("Raw SQL validation failed (%d errors), attempting repair", len(all_errors))
        fix_messages = build_fix_sql_prompt(sql, schema_slice.format_for_prompt(), all_errors)
        fixed_raw = call_llm(fix_messages, model=model, temperature=0.0, max_tokens=max_tokens)
        fixed_sql = rewrite_date_sharded_tables(_strip_markdown_fences(fixed_raw), schema_slice)
        fixed_id_result = validate_raw_sql(fixed_sql, schema_slice)
        fixed_type_errors = check_type_mismatches_raw(fixed_sql, schema_slice)
        if not fixed_id_result.errors and not fixed_type_errors:
            sql = fixed_sql
            repair_succeeded = True
        else:
            log.warning("Raw SQL repair still invalid — keeping original SQL")

    # Applied last, after validation/repair settle on a final SQL string —
    # a pure textual wrap that can't affect identifier/type validation.
    sql = rewrite_listagg_nullif(sql)

    return None, sql, {
        "validation_failed": bool(all_errors),
        "repair_attempted": repair_attempted,
        "repair_succeeded": repair_succeeded,
    }


def _generate_raw_sql_candidate(
    instruction: str,
    schema_slice: SchemaSlice,
    strategy: str,
    model: str,
    temp: float,
    max_tokens: int,
    semantic_context: str | None,
    decomp_ctx: str | None,
    sample_context: str | None,
    exploration_context: str | None,
    plan_context: str | None,
) -> tuple[None, str, dict]:
    """Default candidate path: LLM writes SQL directly, then a deterministic
    correction layer (date-shard rewriting, schema-card validation, one
    repair attempt) fixes evident schema-driven errors. See sql_correction.py.
    """
    messages = _build_raw_sql_messages(
        instruction, schema_slice, strategy,
        semantic_context, decomp_ctx, sample_context, exploration_context, plan_context,
    )
    raw = call_llm(messages, model=model, temperature=temp, max_tokens=max_tokens)
    return _postprocess_raw_sql_candidate(raw, schema_slice, model, max_tokens)


def _generate_plan_compiled_candidate(
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    strategy: str,
    model: str,
    temp: float,
    max_tokens: int,
    candidate_idx: int,
    retriever: HybridRetriever | None,
    semantic_context: str | None,
    decomp_ctx: str | None,
    sample_context: str | None,
    exploration_context: str | None,
    plan_context: str | None,
) -> tuple[QueryPlan | None, str, dict]:
    """Legacy candidate path: LLM produces a structured QueryPlan, compiled
    deterministically via compile_plan(). Kept for comparison/ablation —
    measured to underperform _generate_raw_sql_candidate on the same
    instances (~70% execute / ~10% gold-match vs. ~95% / ~35%).
    """
    messages = build_plan_prompt_with_strategy(
        instruction, schema_slice, strategy,
        semantic_context=semantic_context,
        decomposition_context=decomp_ctx,
        sample_context=sample_context,
        exploration_context=exploration_context,
        plan_context=plan_context,
    )
    plan_raw = call_llm(messages, model=model, temperature=temp, max_tokens=max_tokens)
    plan, _ = _try_parse_plan(plan_raw, model, max_tokens)

    if plan is not None and retriever is not None:
        try:
            expand_schema_for_plan(schema_slice, plan, retriever, db_id)
        except Exception as exc:
            log.warning("Plan expansion FAILED for candidate %d: %s", candidate_idx + 1, exc, exc_info=True)

    if plan is None:
        return None, "SELECT 1 /* plan parse failed */", {"plan_parsed": False}

    sql = compile_plan(plan, schema_slice)
    if sql.strip() == "SELECT 1" and plan.selected_tables == []:
        log.warning(
            "Candidate %d: plan has empty selected_tables, retrying with feedback",
            candidate_idx + 1,
        )
        fix_messages = build_plan_prompt_with_strategy(
            instruction, schema_slice, strategy,
            semantic_context=semantic_context,
            decomposition_context=decomp_ctx,
            sample_context=sample_context,
        )
        fix_messages.append({"role": "assistant", "content": plan_raw})
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

    if sql.strip().startswith("SELECT 1"):
        log.warning(
            "Candidate %d: compiler produced SELECT 1, falling back to LLM SQL generation",
            candidate_idx + 1,
        )
        sql_messages = build_sql_prompt(plan, schema_slice)
        raw_sql = call_llm(sql_messages, model=model, temperature=temp, max_tokens=max_tokens)
        fallback_sql = _strip_markdown_fences(raw_sql)
        if fallback_sql and not fallback_sql.startswith("SELECT 1"):
            sql = rewrite_date_sharded_tables(fallback_sql, schema_slice)
            log.info("LLM SQL fallback produced %d chars of SQL", len(sql))

    return plan, sql, {"plan_parsed": True}
