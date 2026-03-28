"""Bounded execution-guided repair loop."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import ExecutionResult, SnowflakeExecutor
from .error_classifier import (
    AGGREGATION_ERROR,
    INVALID_IDENTIFIER,
    NOT_AUTHORIZED,
    OBJECT_NOT_FOUND,
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
    "Avoid double-quoting identifiers."
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
) -> tuple[str, list[RepairTraceItem], ExecutionResult | None]:
    """Run EXPLAIN → execute → repair loop.

    Returns (final_sql, repair_trace, last_execution_result).
    """
    trace: list[RepairTraceItem] = []
    current_sql = sql
    last_error: str | None = None
    last_result: ExecutionResult | None = None

    schema_text = schema_slice.format_for_prompt()

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

                last_error = error_msg

                if attempt >= max_repairs:
                    last_result = explain_result
                    break

                repaired = _attempt_repair(
                    instruction, current_sql, error_msg, error_type,
                    schema_text, schema_slice, model, temperature, max_tokens,
                )
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

        last_error = error_msg

        if attempt >= max_repairs:
            break

        repaired = _attempt_repair(
            instruction, current_sql, error_msg, error_type,
            schema_text, schema_slice, model, temperature, max_tokens,
        )
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
    }.get(error_type, "general_repair")


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
) -> str:
    """Dispatch to error-specific repair strategy and return fixed SQL."""
    if error_type == INVALID_IDENTIFIER:
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

    raw = call_llm(messages, model=model, temperature=temperature, max_tokens=max_tokens)
    return _strip_sql_fences(raw)
