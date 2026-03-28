"""Build LLM prompts for plan generation and SQL generation."""

from __future__ import annotations

import json

import tiktoken

from ..retrieval.schema_slice import SchemaSlice
from .plan_schema import QueryPlan

_mem_enc = tiktoken.get_encoding("cl100k_base")

# ── Snowflake guidance (compact) ────────────────────────────────────────────

_SNOWFLAKE_GUIDANCE = """\
Snowflake SQL rules:
- Use DATE_TRUNC('MONTH', col) for monthly aggregation, not EXTRACT+GROUP BY.
- Use TRY_TO_DATE / TRY_TO_NUMBER for safe casting.
- String comparison is case-sensitive by default; use ILIKE for case-insensitive.
- Use :: for casting (e.g. col::DATE).
- Prefer CTEs (WITH ... AS) over nested subqueries.
- Do NOT use LIMIT without ORDER BY.
- QUALIFY is supported for window-function filtering.
- ALWAYS double-quote column names to preserve case: "fullVisitorId", "trafficSource", "publication_number".
- For VARIANT/ARRAY columns, use LATERAL FLATTEN:
  SELECT f.value:"field"::STRING FROM table, LATERAL FLATTEN(input => table."variant_col") f
- Access VARIANT nested fields with colon: "col":"field"::TYPE
- Snowflake treats unquoted identifiers as UPPERCASE — always quote mixed-case or lowercase columns.\
"""

# ── Plan prompt ─────────────────────────────────────────────────────────────

_PLAN_SYSTEM = """\
You are a SQL query planner for Snowflake databases.
Given a natural-language question and a schema, produce a structured JSON plan.

Rules:
- Use ONLY tables and columns listed in the schema below.
- Return ONLY valid JSON matching the schema specification. No markdown, no explanation.
- The plan must be sufficient to generate a correct Snowflake SQL query.

{snowflake_guidance}

Plan JSON schema:
{{
  "selected_tables": ["DB.SCHEMA.TABLE", ...],
  "joins": [{{"left_table": "...", "left_column": "...", "right_table": "...", "right_column": "...", "join_type": "INNER"}}],
  "filters": [{{"table": "...", "column": "...", "op": "=", "value": "..."}}],
  "group_by": ["table.column", ...],
  "aggregations": [{{"func": "COUNT", "table": "...", "column": "...", "alias": "..."}}],
  "order_by": [{{"expr": "...", "direction": "ASC"}}],
  "limit": null,
  "notes": null
}}\
"""

_PLAN_USER = """\
Schema:
{schema_text}

Question: {instruction}

Return the plan as JSON only.\
"""

# ── SQL prompt ──────────────────────────────────────────────────────────────

_SQL_SYSTEM = """\
You are a Snowflake SQL generator.
Given a query plan and schema, produce a single SQL statement.

Rules:
- Use ONLY tables and columns listed in the schema.
- Return ONLY the SQL statement. No markdown, no explanation, no comments.
- Snowflake dialect only.
- Prefer CTEs (WITH ... AS) for readability.

{snowflake_guidance}\
"""

_SQL_USER = """\
Schema:
{schema_text}

Plan:
{plan_json}

Write the SQL query.\
"""

# ── Fix-plan prompt ─────────────────────────────────────────────────────────

_FIX_PLAN_SYSTEM = """\
You are a SQL query planner for Snowflake databases.
The previous plan used invalid identifiers. Fix the plan to use only columns present in the schema.
Return ONLY valid JSON matching the plan schema. No markdown, no explanation.\
"""

_FIX_PLAN_USER = """\
Schema:
{schema_text}

Previous plan:
{plan_json}

Validation errors:
{errors}

Fix the plan JSON. Use only tables and columns from the schema above.\
"""

# ── Fix-JSON prompt ─────────────────────────────────────────────────────────

_FIX_JSON_SYSTEM = "You fix malformed JSON. Return ONLY valid JSON. No markdown, no explanation."

_FIX_JSON_USER = """\
The following text should be valid JSON but failed to parse:
{raw_text}

Error: {error}

Return the corrected JSON only.\
"""


def build_memory_context(
    traces: list[dict], max_memory_tokens: int = 800
) -> str:
    """Format retrieved traces into a compact few-shot prompt section.

    Returns an empty string if no traces are provided.
    """
    if not traces:
        return ""
    header = "Prior successful queries on this database:\n"
    parts = [header]
    budget = max_memory_tokens - len(_mem_enc.encode(header)) - 10  # footer margin
    for t in traces:
        # Extract SQL if available from the document
        doc_text = t.get("document", "")
        tables = t.get("metadata", {}).get("tables_used", "")
        sql_preview = t.get("metadata", {}).get("sql_preview", "")
        entry = f"- [{t.get('trace_id', '')}] {doc_text}"
        if tables:
            entry += f" | tables: {tables}"
        if sql_preview:
            entry += f" | sql: {sql_preview[:120]}"
        entry += "\n"
        entry_tokens = len(_mem_enc.encode(entry))
        if entry_tokens > budget:
            break
        parts.append(entry)
        budget -= entry_tokens
    if len(parts) <= 1:
        return ""
    return "".join(parts).rstrip("\n")


def _format_join_hints(join_hints: list[str]) -> str:
    """Format join hints into a prompt section."""
    if not join_hints:
        return ""
    lines = ["\nKnown join relationships:"]
    for hint in join_hints:
        lines.append(f"  - {hint}")
    return "\n".join(lines)


def build_plan_prompt(
    instruction: str,
    schema_slice: SchemaSlice,
    memory_context: str | None = None,
    join_hints: list[str] | None = None,
) -> list[dict[str, str]]:
    """Return messages list for the plan-generation LLM call."""
    schema_text = schema_slice.format_for_prompt()
    if join_hints:
        schema_text += _format_join_hints(join_hints)
    user_content = _PLAN_USER.format(
        schema_text=schema_text, instruction=instruction
    )
    if memory_context:
        user_content = memory_context + "\n\n" + user_content
    return [
        {
            "role": "system",
            "content": _PLAN_SYSTEM.format(snowflake_guidance=_SNOWFLAKE_GUIDANCE),
        },
        {
            "role": "user",
            "content": user_content,
        },
    ]


def build_sql_prompt(
    plan: QueryPlan,
    schema_slice: SchemaSlice,
) -> list[dict[str, str]]:
    """Return messages list for the SQL-generation LLM call."""
    schema_text = schema_slice.format_for_prompt()
    plan_json = json.dumps(plan.model_dump(), indent=2)
    return [
        {
            "role": "system",
            "content": _SQL_SYSTEM.format(snowflake_guidance=_SNOWFLAKE_GUIDANCE),
        },
        {
            "role": "user",
            "content": _SQL_USER.format(
                schema_text=schema_text, plan_json=plan_json
            ),
        },
    ]


def build_fix_plan_prompt(
    plan: QueryPlan,
    schema_slice: SchemaSlice,
    errors: list[str],
) -> list[dict[str, str]]:
    """Return messages for fixing a plan that failed identifier validation."""
    schema_text = schema_slice.format_for_prompt()
    plan_json = json.dumps(plan.model_dump(), indent=2)
    return [
        {"role": "system", "content": _FIX_PLAN_SYSTEM},
        {
            "role": "user",
            "content": _FIX_PLAN_USER.format(
                schema_text=schema_text,
                plan_json=plan_json,
                errors="\n".join(f"- {e}" for e in errors),
            ),
        },
    ]


# ── Strategy-specific plan prompts ──────────────────────────────────────────

_STRATEGY_HINTS: dict[str, str] = {
    "default": "",
    "join_first": (
        "\nPlanning priority: START by identifying the correct JOIN relationships "
        "between tables. Build the query outward from the joins. "
        "Ensure all join keys are valid columns."
    ),
    "metric_first": (
        "\nPlanning priority: START by identifying the target metric/aggregation "
        "(COUNT, SUM, AVG, etc.) and the column it applies to. "
        "Then determine which tables and joins are needed to compute it."
    ),
    "time_first": (
        "\nPlanning priority: START by identifying any date/time filters or "
        "time-based grouping the question implies. Locate the relevant "
        "DATE/TIMESTAMP columns first, then build the rest of the query around them."
    ),
}


def build_plan_prompt_with_strategy(
    instruction: str,
    schema_slice: SchemaSlice,
    strategy: str = "default",
    memory_context: str | None = None,
    join_hints: list[str] | None = None,
) -> list[dict[str, str]]:
    """Return plan-generation messages with an optional strategy hint."""
    hint = _STRATEGY_HINTS.get(strategy, "")
    schema_text = schema_slice.format_for_prompt()
    if join_hints:
        schema_text += _format_join_hints(join_hints)
    system_content = _PLAN_SYSTEM.format(snowflake_guidance=_SNOWFLAKE_GUIDANCE)
    if hint:
        system_content += hint
    user_content = _PLAN_USER.format(
        schema_text=schema_text, instruction=instruction
    )
    if memory_context:
        user_content = memory_context + "\n\n" + user_content
    return [
        {"role": "system", "content": system_content},
        {
            "role": "user",
            "content": user_content,
        },
    ]


def build_fix_json_prompt(raw_text: str, error: str) -> list[dict[str, str]]:
    """Return messages for fixing malformed JSON output."""
    return [
        {"role": "system", "content": _FIX_JSON_SYSTEM},
        {
            "role": "user",
            "content": _FIX_JSON_USER.format(raw_text=raw_text, error=error),
        },
    ]
