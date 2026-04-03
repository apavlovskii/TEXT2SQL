"""Trace record creation and summarization for memory."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict


@dataclass
class TraceRecord:
    trace_id: str
    instance_id: str
    db_id: str
    instruction: str
    instruction_summary: str
    schema_slice_summary: str
    plan_summary: str | None = None
    final_sql: str = ""
    repair_summary: str | None = None
    verification_summary: str | None = None
    tables_used: list[str] = field(default_factory=list)
    key_columns_used: list[str] = field(default_factory=list)
    join_conditions: list[str] = field(default_factory=list)
    column_access_patterns: list[str] = field(default_factory=list)
    token_estimate: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def summarize_schema_slice(schema_slice) -> str:
    parts = [f"DB: {schema_slice.db_id}"]
    for ts in schema_slice.tables:
        cols = ", ".join(c.name for c in ts.columns[:8])
        parts.append(f"  {ts.qualified_name}: [{cols}]")
    return "\n".join(parts)


def summarize_plan(plan) -> str:
    if plan is None:
        return ""
    parts = []
    if plan.selected_tables:
        parts.append(
            f"Tables: {', '.join(t.rsplit('.', 1)[-1] for t in plan.selected_tables)}"
        )
    if plan.joins:
        parts.append(f"Joins: {len(plan.joins)}")
    if plan.aggregations:
        aggs = ", ".join(f"{a.func}({a.column})" for a in plan.aggregations)
        parts.append(f"Aggs: {aggs}")
    if plan.group_by:
        parts.append(f"Group: {', '.join(plan.group_by)}")
    if plan.filters:
        parts.append(f"Filters: {len(plan.filters)}")
    return "; ".join(parts)


def summarize_repair_trace(repair_trace) -> str:
    if not repair_trace:
        return "No repairs"
    parts = []
    for item in repair_trace:
        if hasattr(item, "error_type"):
            parts.append(f"{item.error_type}->{item.repair_action}")
        elif isinstance(item, dict):
            parts.append(
                f"{item.get('error_type', '')}->{item.get('repair_action', '')}"
            )
    return "; ".join(parts) if parts else "No repairs"


def summarize_verification(candidate_record) -> str:
    if not candidate_record:
        return ""
    parts = []
    if candidate_record.get("execution_success"):
        parts.append("exec:OK")
    row_count = candidate_record.get("row_count")
    if row_count is not None:
        parts.append(f"rows:{row_count}")
    meta = candidate_record.get("metamorphic", {})
    delta = meta.get("score_delta", 0)
    if delta:
        parts.append(f"meta_delta:{delta:+.1f}")
    return ", ".join(parts)


def make_trace_record(
    instance_id: str,
    db_id: str,
    instruction: str,
    schema_slice,
    plan=None,
    final_sql: str = "",
    repair_trace=None,
    candidate_record: dict | None = None,
) -> TraceRecord:
    trace_id = hashlib.sha256(
        f"{instance_id}:{db_id}:{final_sql[:100]}".encode()
    ).hexdigest()[:16]
    tables_used = []
    key_cols = []
    if schema_slice:
        for ts in schema_slice.tables:
            tables_used.append(ts.qualified_name)
            for c in ts.columns:
                if c.is_join_key or c.is_time_column:
                    key_cols.append(f"{ts.qualified_name}.{c.name}")
    # Extract join conditions from plan
    join_conditions = []
    if plan and hasattr(plan, "joins"):
        for j in plan.joins:
            join_conditions.append(
                f"{j.left_table}.{j.left_column} = {j.right_table}.{j.right_column}"
            )

    # Extract VARIANT access patterns from SQL (e.g., "trafficSource":"source"::STRING)
    import re
    column_access_patterns = []
    if final_sql:
        # Match "col":"field" or "col":"field"::TYPE patterns
        variant_re = re.compile(r'"(\w+)":"(\w+)"(?:::(\w+))?')
        for m in variant_re.finditer(final_sql):
            pattern = f'"{m.group(1)}":"{m.group(2)}"'
            if m.group(3):
                pattern += f"::{m.group(3)}"
            if pattern not in column_access_patterns:
                column_access_patterns.append(pattern)

    # Truncate SQL for storage
    sql_compact = final_sql[:500] if final_sql else ""
    return TraceRecord(
        trace_id=trace_id,
        instance_id=instance_id,
        db_id=db_id,
        instruction=instruction,
        instruction_summary=instruction[:200],
        schema_slice_summary=(
            summarize_schema_slice(schema_slice) if schema_slice else ""
        ),
        plan_summary=summarize_plan(plan) if plan else None,
        final_sql=sql_compact,
        repair_summary=(
            summarize_repair_trace(repair_trace) if repair_trace else None
        ),
        verification_summary=(
            summarize_verification(candidate_record) if candidate_record else None
        ),
        tables_used=tables_used,
        key_columns_used=key_cols,
        join_conditions=join_conditions,
        column_access_patterns=column_access_patterns,
    )
