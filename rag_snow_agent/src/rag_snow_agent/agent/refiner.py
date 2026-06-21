"""Bounded execution-guided repair loop."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from ..chroma.chroma_store import ChromaStore
from ..prompting.sql_compiler import rewrite_date_encoding, rewrite_date_sharded_tables
from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import ExecutionResult, SnowflakeExecutor
from ..snowflake.probes import probe_column_exists
from .column_validator import validate_columns_against_index
from .error_classifier import (
    AGGREGATION_ERROR,
    EMPTY_RESULT,
    INVALID_IDENTIFIER,
    NOT_AUTHORIZED,
    OBJECT_NOT_FOUND,
    RESULT_MISMATCH,
    SELF_CRITIQUE,
    classify_snowflake_error,
    extract_offending_identifier,
    extract_offending_object,
)
from .llm_client import call_llm

log = logging.getLogger(__name__)

# ── Snowflake guidance (compact, for repair prompts) ─────────────────────────

_SF_RULES = (
    "Snowflake dialect. SQL only. No markdown. No explanation. "
    "Use only identifiers from the schema provided. "
    "Prefer CTEs. Use DATE_TRUNC for date grouping. "
    'ALWAYS double-quote column names: "colName". Use LATERAL FLATTEN for VARIANT arrays.'
)


# ── Repair prompt builders (minimal growth) ──────────────────────────────────


def _build_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
    extra_guidance: str = "",
) -> list[dict[str, str]]:
    """Build a minimal repair prompt — no conversation history."""
    system = (
        "You fix broken Snowflake SQL queries. "
        "Return ONLY the corrected SQL. No markdown, no explanation.\n"
        f"{_SF_RULES}"
    )
    if extra_guidance:
        system += f"\n{extra_guidance}"

    user = (
        f"Schema:\n{schema_text}\n\n"
        f"Question: {instruction}\n\n"
        f"Previous SQL (failed):\n{previous_sql}\n\n"
        f"Error:\n{error_message}\n\n"
        "Return the corrected SQL only."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _build_identifier_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
    offending: str | None,
) -> list[dict[str, str]]:
    extra = ""
    if offending:
        extra = (
            f"The identifier '{offending}' is invalid. "
            "Check column/table names in the schema and use exact matches."
        )
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _build_object_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
    offending: str | None,
) -> list[dict[str, str]]:
    extra = "Use ONLY tables from the schema provided. Verify database/schema qualification."
    if offending:
        extra += f" The object '{offending}' does not exist."
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _build_aggregation_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
) -> list[dict[str, str]]:
    extra = (
        "Rewrite using CTEs. Ensure every non-aggregated column is in GROUP BY. "
        "Aggregate only measures, not dimensions."
    )
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _build_column_validation_repair_prompt(
    instruction: str,
    sql: str,
    errors: list[str],
    suggestions: list[str],
    schema_text: str,
) -> list[dict[str, str]]:
    """Build a targeted repair prompt for column validation failures."""
    system = (
        "You fix broken Snowflake SQL queries. "
        "Return ONLY the corrected SQL. No markdown, no explanation.\n"
        f"{_SF_RULES}"
    )
    error_block = "\n".join(errors[:10])
    suggestion_block = "\n".join(suggestions[:10]) if suggestions else "No suggestions available."
    user = (
        f"Schema:\n{schema_text}\n\n"
        f"Question: {instruction}\n\n"
        f"SQL:\n{sql}\n\n"
        f"Invalid column references:\n{error_block}\n\n"
        f"Suggested replacements:\n{suggestion_block}\n\n"
        "Fix only the invalid column references. Return SQL only."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _build_result_mismatch_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
) -> list[dict[str, str]]:
    extra = (
        "The SQL executed successfully but returned WRONG RESULTS. "
        "Re-read the question carefully. Check: "
        "1) Are the correct tables and columns used? "
        "2) Are JOINs correct? "
        "3) Are WHERE filters matching the question's conditions exactly? "
        "4) Are aggregations (GROUP BY, COUNT, SUM) correct? "
        "5) Is the date/time filtering correct? "
        "Return a corrected SQL that answers the question accurately."
    )
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _build_recursive_cte_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
) -> list[dict[str, str]]:
    extra = (
        "The recursive CTE is invalid in Snowflake. Rewrite it to this exact shape:\n"
        "WITH RECURSIVE name (col1, col2, ...) AS (\n"
        "  -- ANCHOR: a non-recursive SELECT that does NOT reference `name`\n"
        "  SELECT ...\n"
        "  UNION ALL\n"
        "  -- RECURSIVE: SELECT ... FROM name JOIN <table> ON ...\n"
        "  SELECT ...\n"
        ")\n"
        "Rules: (1) `WITH RECURSIVE` is required. (2) The anchor must NOT reference the CTE "
        "name and must define the column list. (3) Anchor and recursive arms must have the "
        "SAME number and types of columns. (4) Pick the correct anchor set (e.g. true root "
        "rows — those not appearing as a child). (5) Reference the CTE name unqualified in the "
        "recursive arm. Return one corrected SQL only."
    )
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _build_empty_result_repair_prompt(
    instruction: str,
    previous_sql: str,
    error_message: str,
    schema_text: str,
) -> list[dict[str, str]]:
    extra = (
        "The SQL executed successfully but returned ZERO ROWS (empty result). "
        "Since output is empty, please simplify some conditions. Consider: "
        "1) Relaxing date range filters — check if dates are stored as NUMBER (YYYYMMDD) or VARCHAR, not DATE type. "
        "2) Removing restrictive WHERE clauses that may filter out all rows. "
        "3) Checking if column values match expected format (e.g., country_code='US' vs 'United States'). "
        "4) Using ILIKE instead of = for string matching. "
        "5) Verifying VARIANT field access paths — ensure colon syntax is correct. "
        "Return a corrected SQL that produces non-empty results."
    )
    return _build_repair_prompt(instruction, previous_sql, error_message, schema_text, extra)


def _strip_sql_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    if text.upper().startswith("SQL"):
        text = text[3:].strip()
    return text.strip()


def expand_schema_slice_for_error(
    schema_slice: SchemaSlice,
    error_type: str,
    error_message: str,
) -> SchemaSlice:
    """Placeholder for schema re-retrieval on object-not-found errors.

    TODO: In a future milestone, this should query ChromaDB for additional
    tables/columns that match the offending object name.
    """
    log.debug(
        "expand_schema_slice_for_error called (no-op): error_type=%s", error_type
    )
    return schema_slice


def _run_column_probes(
    executor: SnowflakeExecutor,
    errors: list[str],
    suggestions: list[str],
    schema_slice: SchemaSlice,
    max_probes: int = 2,
) -> None:
    """Run micro-probes for columns flagged as invalid, enriching error/suggestion lists.

    Modifies *errors* and *suggestions* in place. Runs at most *max_probes* probes.
    """
    import re as _re

    probes_run = 0
    # Extract column names from error messages like "Column 'FOO' not found..."
    col_pattern = _re.compile(r"Column '(\w+)' not found")
    for i, err in enumerate(list(errors)):
        if probes_run >= max_probes:
            break
        m = col_pattern.search(err)
        if not m:
            continue
        col_name = m.group(1)
        # Try probing against each table in the schema slice
        confirmed_missing = True
        for ts in schema_slice.tables:
            probes_run += 1
            if probe_column_exists(executor, ts.qualified_name, col_name):
                confirmed_missing = False
                log.debug(
                    "Probe confirmed column %s exists in %s",
                    col_name, ts.qualified_name,
                )
                break
            if probes_run >= max_probes:
                break

        if confirmed_missing:
            errors[i] = f"{err} (confirmed missing by live probe)"
            log.debug("Probe confirmed column %s is missing", col_name)


@dataclass
class RepairTraceItem:
    attempt: int
    input_sql: str
    error_type: str
    error_message: str
    repair_action: str
    output_sql: str


def refine_sql(
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    sql: str,
    executor: SnowflakeExecutor,
    model: str = "gpt-4o-mini",
    temperature: float = 0.0,
    max_tokens: int = 800,
    max_repairs: int = 2,
    explain_first: bool = True,
    stop_on_repeated_error: bool = True,
    chroma_store: ChromaStore | None = None,
    gold_dir: str | Path | None = None,
    eval_standards: dict | None = None,
    instance_id: str | None = None,
    max_same_error_type: int = 3,
    sample_context: str | None = None,
    enable_self_critic: bool = False,
    self_critic_max: int = 1,
    date_encodings: dict | None = None,
) -> tuple[str, list[RepairTraceItem], ExecutionResult | None]:
    """Run EXPLAIN → execute → repair loop.

    Returns (final_sql, repair_trace, last_execution_result).
    """
    trace: list[RepairTraceItem] = []
    last_error: str | None = None
    last_result: ExecutionResult | None = None

    def _maybe_rewrite_shards(s: str) -> str:
        """Deterministic rewrites on generated SQL: date-shard unions + epoch date
        encoding (so a wrong YYYYMMDD comparison on an epoch column is always fixed,
        regardless of what the LLM emitted). Shard rewrite is idempotent."""
        if "_date_shard_union" not in s:
            s = rewrite_date_sharded_tables(s, schema_slice)
        if date_encodings:
            s = rewrite_date_encoding(s, date_encodings)
        return s

    # Apply the deterministic rewrites to the initial SQL too (not just repairs).
    current_sql = _maybe_rewrite_shards(sql)

    schema_text = schema_slice.format_for_prompt()
    if sample_context:
        schema_text = schema_text + "\n\n" + sample_context

    # ── Pre-execution column validation ──────────────────────────────
    if chroma_store is not None:
        try:
            is_valid, errors, suggestions = validate_columns_against_index(
                current_sql, db_id, chroma_store
            )
            if not is_valid:
                log.info(
                    "Column validation found %d issue(s); attempting pre-repair",
                    len(errors),
                )
                # ── Micro-probe to double-check invalid columns ──────
                _run_column_probes(
                    executor, errors, suggestions, schema_slice, max_probes=2,
                )
                repair_prompt = _build_column_validation_repair_prompt(
                    instruction, current_sql, errors, suggestions, schema_text,
                )
                raw = call_llm(
                    repair_prompt, model=model,
                    temperature=temperature, max_tokens=max_tokens,
                )
                repaired = _maybe_rewrite_shards(_strip_sql_fences(raw))
                trace.append(RepairTraceItem(
                    attempt=0,
                    input_sql=current_sql,
                    error_type="column_validation",
                    error_message="; ".join(errors[:5]),
                    repair_action="pre_validate_columns",
                    output_sql=repaired,
                ))
                current_sql = repaired
        except Exception:
            log.debug("Column validation failed; proceeding without it", exc_info=True)

    # Track error type frequency for early termination on hopeless repairs
    error_type_counts: dict[str, int] = {}

    for attempt in range(1 + max_repairs):
        # ── EXPLAIN phase ────────────────────────────────────────────
        if explain_first:
            explain_result = executor.explain(current_sql)
            if not explain_result.success:
                error_msg = explain_result.error_message or "EXPLAIN failed"
                error_type = classify_snowflake_error(error_msg)
                log.info(
                    "EXPLAIN failed (attempt %d): %s → %s",
                    attempt + 1, error_type, error_msg[:120],
                )

                if stop_on_repeated_error and error_msg == last_error:
                    log.info("Repeated error, stopping repair loop")
                    last_result = explain_result
                    break

                # Early termination: same error TYPE seen 3+ times
                error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
                if error_type_counts.get(error_type, 0) >= max_same_error_type:
                    log.info(
                        "Error type '%s' occurred %d times, stopping repair loop",
                        error_type, error_type_counts[error_type],
                    )
                    last_result = explain_result
                    break

                last_error = error_msg

                if attempt >= max_repairs:
                    last_result = explain_result
                    break

                repaired = _maybe_rewrite_shards(_attempt_repair(
                    instruction, current_sql, error_msg, error_type,
                    schema_text, schema_slice, model, temperature, max_tokens,
                    chroma_store=chroma_store,
                ))
                trace.append(RepairTraceItem(
                    attempt=attempt + 1,
                    input_sql=current_sql,
                    error_type=error_type,
                    error_message=error_msg[:500],
                    repair_action=_action_for_type(error_type),
                    output_sql=repaired,
                ))
                current_sql = repaired
                continue

        # ── EXECUTE phase ────────────────────────────────────────────
        exec_result = executor.execute(current_sql)
        last_result = exec_result

        if exec_result.success:
            # Check gold match if gold data available
            if gold_dir and instance_id:
                from ..eval.gold_verifier import verify_against_gold

                gold_result = verify_against_gold(
                    instance_id, current_sql, db_id, executor, gold_dir, eval_standards,
                )
                if gold_result.matched:
                    log.info("Gold match PASSED (attempt %d)", attempt + 1)
                    return current_sql, trace, exec_result
                else:
                    # Treat as error and repair
                    error_msg = f"SQL executed but results don't match gold: {gold_result.error}"
                    if gold_result.details:
                        error_msg += f" ({gold_result.details})"
                    error_type = gold_result.error or RESULT_MISMATCH
                    log.info("Gold match FAILED (attempt %d): %s", attempt + 1, error_msg[:120])

                    # Track for early termination
                    error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
                    if error_type_counts.get(error_type, 0) >= max_same_error_type:
                        log.info(
                            "Error type '%s' occurred %d times, stopping repair loop",
                            error_type, error_type_counts[error_type],
                        )
                        last_result = ExecutionResult(
                            success=False, sql=current_sql,
                            error_message=error_msg, error_type=error_type,
                            row_count=exec_result.row_count,
                        )
                        break

                    if attempt >= max_repairs:
                        # Mark as failed even though execution succeeded
                        last_result = ExecutionResult(
                            success=False, sql=current_sql,
                            error_message=error_msg, error_type=error_type,
                            row_count=exec_result.row_count,
                        )
                        break

                    # Repair: tell LLM results were wrong
                    repaired = _maybe_rewrite_shards(_attempt_repair(
                        instruction, current_sql, error_msg, error_type,
                        schema_text, schema_slice, model, temperature, max_tokens,
                        chroma_store=chroma_store,
                    ))
                    trace.append(RepairTraceItem(
                        attempt=attempt + 1,
                        input_sql=current_sql,
                        error_type=error_type,
                        error_message=error_msg[:500],
                        repair_action=_action_for_type(error_type),
                        output_sql=repaired,
                    ))
                    current_sql = repaired
                    continue
            else:
                # ── No-gold path ─────────────────────────────────────
                # Gold-free correctness signal: an LLM self-critique inspects
                # the executed result and may flag a likely-wrong answer. This
                # is the production-realistic substitute for the gold-driven
                # RESULT_MISMATCH repair above — it lets the loop keep fixing
                # toward correctness instead of accepting the first SQL that
                # merely executes. It never marks the result as failed; if the
                # critique budget is spent the current (successful) SQL is kept.
                if enable_self_critic and self_critic_max > 0:
                    critique = _self_critique(
                        instruction, current_sql, exec_result,
                        schema_text, model, max_tokens,
                    )
                    if critique.get("problem"):
                        error_type = SELF_CRITIQUE
                        reason = critique.get("reason", "")
                        error_msg = f"Self-critique flagged the result as likely wrong: {reason}"
                        error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
                        # Bound critique-driven repairs and respect global budget;
                        # on exhaustion accept the current successful execution.
                        if (error_type_counts[error_type] > self_critic_max
                                or attempt >= max_repairs):
                            log.info(
                                "Self-critique flagged but budget reached; keeping current SQL"
                            )
                            return current_sql, trace, exec_result
                        log.info("Self-critique (attempt %d): %s", attempt + 1, reason[:120])
                        repaired = _maybe_rewrite_shards(_attempt_repair(
                            instruction, current_sql, error_msg, error_type,
                            schema_text, schema_slice, model, temperature, max_tokens,
                            chroma_store=chroma_store,
                        ))
                        trace.append(RepairTraceItem(
                            attempt=attempt + 1,
                            input_sql=current_sql,
                            error_type=error_type,
                            error_message=error_msg[:500],
                            repair_action=_action_for_type(error_type),
                            output_sql=repaired,
                        ))
                        current_sql = repaired
                        continue
                # No gold data, no critique problem → accept.
                log.info("Execution succeeded (attempt %d)", attempt + 1)
                return current_sql, trace, exec_result

        error_msg = exec_result.error_message or "Execution failed"
        error_type = classify_snowflake_error(error_msg)
        log.info(
            "Execution failed (attempt %d): %s → %s",
            attempt + 1, error_type, error_msg[:120],
        )

        if stop_on_repeated_error and error_msg == last_error:
            log.info("Repeated error, stopping repair loop")
            break

        # Early termination: same error TYPE seen 3+ times
        error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
        if error_type_counts.get(error_type, 0) >= max_same_error_type:
            log.info(
                "Error type '%s' occurred %d times, stopping repair loop",
                error_type, error_type_counts[error_type],
            )
            break

        last_error = error_msg

        if attempt >= max_repairs:
            break

        repaired = _maybe_rewrite_shards(_attempt_repair(
            instruction, current_sql, error_msg, error_type,
            schema_text, schema_slice, model, temperature, max_tokens,
            chroma_store=chroma_store,
        ))
        trace.append(RepairTraceItem(
            attempt=attempt + 1,
            input_sql=current_sql,
            error_type=error_type,
            error_message=error_msg[:500],
            repair_action=_action_for_type(error_type),
            output_sql=repaired,
        ))
        current_sql = repaired

    return current_sql, trace, last_result


def _action_for_type(error_type: str) -> str:
    return {
        INVALID_IDENTIFIER: "patch_identifier",
        OBJECT_NOT_FOUND: "fix_object_reference",
        NOT_AUTHORIZED: "fix_object_reference",
        AGGREGATION_ERROR: "rewrite_aggregation",
        RESULT_MISMATCH: "fix_wrong_results",
        SELF_CRITIQUE: "fix_wrong_results",
        EMPTY_RESULT: "fix_empty_results",
    }.get(error_type, "general_repair")


def _format_result_preview(exec_result, max_rows: int = 8, max_cell: int = 60) -> str:
    """Compact textual preview of an execution result for self-critique."""
    cols = exec_result.column_names or []
    rows = exec_result.rows_sample or []
    lines = [f"row_count={exec_result.row_count}, columns={cols}"]
    for row in rows[:max_rows]:
        if isinstance(row, dict):
            vals = [row.get(c) for c in cols] if cols else list(row.values())
        else:
            vals = list(row)
        cells = []
        for v in vals:
            s = "NULL" if v is None else str(v)
            cells.append(s[:max_cell])
        lines.append(" | ".join(cells))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more sampled rows)")
    return "\n".join(lines)


def _self_critique(
    instruction: str,
    sql: str,
    exec_result,
    schema_text: str,
    model: str,
    max_tokens: int,
) -> dict:
    """Gold-free correctness check: ask an LLM whether the executed result
    plausibly answers the question. Conservative by design — it should only flag
    a problem when there is a *clear* mismatch (wrong aggregation/grain, missing
    or wrong filter, wrong/extra columns, suspicious empty/degenerate result),
    not on stylistic doubts. Returns {"problem": bool, "reason": str}; any
    parsing/LLM failure returns {"problem": False} so the loop fails safe.
    """
    preview = _format_result_preview(exec_result)
    system = (
        "You are a meticulous SQL reviewer. You are given a natural-language "
        "question, the SQL that was run, and a preview of its actual result. "
        "Decide whether the result PLAUSIBLY answers the question. Be "
        "conservative: only report a problem when you are confident there is a "
        "concrete error — wrong aggregation or time grain, a missing/incorrect "
        "filter or join, wrong or extra/missing columns, or a degenerate result "
        "(e.g. empty, all-NULL, single row when many are expected). Do NOT flag "
        "mere style. Respond with STRICT JSON only: "
        '{"problem": true|false, "reason": "<short concrete reason or empty>"}.'
    )
    user = (
        f"Question:\n{instruction}\n\n"
        f"Schema (subset):\n{schema_text[:2500]}\n\n"
        f"SQL:\n{sql}\n\n"
        f"Result preview:\n{preview}\n\n"
        "Does this result plausibly and correctly answer the question? "
        "Return strict JSON."
    )
    try:
        raw = call_llm(
            [{"role": "system", "content": system},
             {"role": "user", "content": user}],
            model=model, temperature=0.0, max_tokens=max_tokens,
        )
    except Exception:
        log.debug("Self-critique LLM call failed; treating as no problem", exc_info=True)
        return {"problem": False}

    text = (raw or "").strip()
    # Strip code fences / locate the JSON object.
    if "```" in text:
        text = text.split("```")[1] if len(text.split("```")) > 1 else text
        text = text.removeprefix("json").strip()
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start:end + 1]
    try:
        data = json.loads(text)
        return {
            "problem": bool(data.get("problem", False)),
            "reason": str(data.get("reason", "") or ""),
        }
    except (ValueError, TypeError):
        log.debug("Self-critique returned unparseable output: %r", raw)
        return {"problem": False}


def _get_syntax_guidance(error_msg: str, sql: str, chroma_store: ChromaStore | None) -> str:
    """Query snowflake_syntax collection for relevant guidance."""
    if chroma_store is None:
        return ""
    try:
        from ..chroma.snowflake_syntax import SnowflakeSyntaxStore

        syntax_store = SnowflakeSyntaxStore(chroma_store)
        # Build query from error context
        query = f"{error_msg[:100]} {sql[:100]}"
        results = syntax_store.query(query, top_k=2)
        if not results:
            return ""
        snippets = []
        for r in results:
            content = r.get("content", "")[:300]
            snippets.append(content)
        return "\nSnowflake syntax reference:\n" + "\n---\n".join(snippets)
    except Exception:
        return ""


def _attempt_repair(
    instruction: str,
    current_sql: str,
    error_msg: str,
    error_type: str,
    schema_text: str,
    schema_slice: SchemaSlice,
    model: str,
    temperature: float,
    max_tokens: int,
    chroma_store: ChromaStore | None = None,
) -> str:
    """Dispatch to error-specific repair strategy and return fixed SQL."""
    if "recursive" in (error_msg or "").lower():
        # Recursive-CTE syntax errors are common and dialect-specific; route to a
        # targeted recipe regardless of the classifier's error_type.
        messages = _build_recursive_cte_repair_prompt(
            instruction, current_sql, error_msg, schema_text
        )
    elif error_type == EMPTY_RESULT:
        messages = _build_empty_result_repair_prompt(
            instruction, current_sql, error_msg, schema_text
        )
    elif error_type in (RESULT_MISMATCH, SELF_CRITIQUE):
        messages = _build_result_mismatch_repair_prompt(
            instruction, current_sql, error_msg, schema_text
        )
    elif error_type == INVALID_IDENTIFIER:
        offending = extract_offending_identifier(error_msg)
        messages = _build_identifier_repair_prompt(
            instruction, current_sql, error_msg, schema_text, offending
        )
    elif error_type in (OBJECT_NOT_FOUND, NOT_AUTHORIZED):
        offending = extract_offending_object(error_msg)
        # Attempt schema expansion (placeholder)
        expand_schema_slice_for_error(schema_slice, error_type, error_msg)
        messages = _build_object_repair_prompt(
            instruction, current_sql, error_msg, schema_text, offending
        )
    elif error_type == AGGREGATION_ERROR:
        messages = _build_aggregation_repair_prompt(
            instruction, current_sql, error_msg, schema_text
        )
    else:
        # General repair for type_mismatch, unknown_function, syntax, etc.
        messages = _build_repair_prompt(
            instruction, current_sql, error_msg, schema_text
        )

    # Append syntax reference guidance from ChromaDB if available
    syntax_guidance = _get_syntax_guidance(error_msg, current_sql, chroma_store)
    if syntax_guidance:
        # Append to the system message's content
        messages[0]["content"] += syntax_guidance

    raw = call_llm(messages, model=model, temperature=temperature, max_tokens=max_tokens)
    return _strip_sql_fences(raw)
