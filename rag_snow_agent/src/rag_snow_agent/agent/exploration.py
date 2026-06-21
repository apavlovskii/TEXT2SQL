"""Online value/entity exploration (#1) + structured Information Aggregation plan (#2).

A gold-free pre-generation phase, inspired by ReFoRCE (column exploration) and
DSR-SQL (information aggregation):

  explore()                -> run read-only probe SQLs to resolve the question's
                              entities to REAL database values and discover nested
                              structure; return compact evidence.
  build_information_plan()  -> one LLM call that turns the schema + exploration
                              evidence into a structured plan (entity->column map,
                              resolved literals, join/FLATTEN paths, formulas, grain).

Both outputs feed the candidate-generation prompts as context. Everything here is
gold-free and best-effort: any failure degrades to empty context, never raising.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import SnowflakeExecutor
from .llm_client import LLMQuotaExhausted, call_llm

log = logging.getLogger(__name__)

_SQL_BLOCK_RE = re.compile(r"```sql\s*(.*?)```", re.DOTALL | re.IGNORECASE)
# Anything that mutates state or moves data is rejected (read-only guard).
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|CREATE|ALTER|GRANT|REVOKE|"
    r"CALL|COPY|PUT|GET|REMOVE|UNLOAD)\b",
    re.IGNORECASE,
)


@dataclass
class ExplorationResult:
    evidence_text: str = ""
    probes: list[dict] = field(default_factory=list)
    n_probes: int = 0
    n_ok: int = 0
    date_encodings: dict = field(default_factory=dict)  # col -> "micros"/"millis"/"seconds"


_DATE_NAME_RE = re.compile(r"(date|time|created|modified|timestamp|_at$|_dt$|_ts$)", re.I)
_NATIVE_DATE_TYPES = ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "DATETIME")


_KIND_DIRECTIVE = {
    "micros": 'epoch MICROSECONDS — to filter, use TO_TIMESTAMP("{c}"/1000000) (do NOT treat as YYYYMMDD)',
    "millis": 'epoch MILLISECONDS — to filter, use TO_TIMESTAMP("{c}"/1000) (do NOT treat as YYYYMMDD)',
    "seconds": 'epoch SECONDS — to filter, use TO_TIMESTAMP("{c}") (do NOT treat as YYYYMMDD)',
    "yyyymmdd": "integer YYYYMMDD — compare as integers",
}


def _encoding_kind(v) -> str | None:
    """Classify a numeric date column's encoding from its magnitude (deterministic)."""
    try:
        x = abs(float(v))
    except (TypeError, ValueError):
        return None
    if 1e14 <= x < 1e17:
        return "micros"
    if 1e11 <= x < 1e14:
        return "millis"
    if 1e8 <= x < 1e11:
        return "seconds"
    if 19000101 <= x <= 99991231:
        return "yyyymmdd"
    return None


def profile_date_columns(schema_slice: SchemaSlice, executor: SnowflakeExecutor,
                         max_cols: int = 8) -> tuple[list[str], dict[str, str]]:
    """Deterministically profile date/time columns.

    Returns (facts, encodings) where ``encodings`` maps exact column name -> epoch
    kind ("micros"/"millis"/"seconds") for deterministic SQL rewriting. The model's
    prior often overrides soft evidence, so we classify encodings in code (from raw
    MIN/MAX magnitude) and both (a) emit authoritative facts and (b) enable a rewrite.
    """
    facts: list[str] = []
    encodings: dict[str, str] = {}
    probed = 0
    for ts in schema_slice.tables:
        for col in (ts.columns or []):
            name = (col.original_name or col.name or "").strip('"')
            dtype = (col.data_type or "").upper()
            if not (getattr(col, "is_time_column", False) or _DATE_NAME_RE.search(name)):
                continue
            tq = ts.qualified_name
            if dtype in _NATIVE_DATE_TYPES:
                facts.append(f'{tq}."{name}": native {dtype} — compare to DATE/TIMESTAMP '
                             f'literals (do NOT TO_DATE(...,\'YYYYMMDD\') or compare to an int).')
                encodings[name] = "native"   # enable deterministic rewrite of wrong YYYYMMDD usage
                continue
            if probed >= max_cols:
                continue
            probed += 1
            try:
                r = executor.execute(f'SELECT MIN("{name}"), MAX("{name}") FROM {tq}', sample_rows=2)
            except Exception:
                continue
            if not r.success or not r.rows_sample:
                continue
            mn, mx = r.rows_sample[0][0], r.rows_sample[0][1]
            kind = _encoding_kind(mx)
            if kind:
                facts.append(f'{tq}."{name}": {_KIND_DIRECTIVE[kind].format(c=name)} '
                             f'(observed min={mn}, max={mx}).')
                if kind in ("micros", "millis", "seconds"):
                    encodings[name] = kind   # enable deterministic rewrite
    return facts, encodings


def _extract_sql_blocks(text: str) -> list[str]:
    blocks = [b.strip() for b in _SQL_BLOCK_RE.findall(text or "")]
    return [b for b in blocks if b]


def _strip_sql_comments(sql: str) -> str:
    s = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)   # block comments
    s = re.sub(r"--[^\n]*", " ", s)                        # line comments
    return s


def _strip_string_literals(s: str) -> str:
    return re.sub(r"'(?:''|[^'])*'", "''", s)              # collapse '...' literals


def _is_safe_select(sql: str) -> bool:
    """True only for a single read-only SELECT/WITH statement.

    Evaluates the statement with comments and string literals stripped, so a
    leading `-- description` comment or a literal like `ILIKE '%drop%'` does not
    cause a false reject.
    """
    if not sql:
        return False
    code = _strip_sql_comments(sql).strip()
    if not code:
        return False
    no_str = _strip_string_literals(code)
    if ";" in no_str.rstrip(";").strip():                  # statement chaining
        return False
    low = no_str.lstrip("(").lstrip().lower()
    if not (low.startswith("select") or low.startswith("with")):
        return False
    if _FORBIDDEN.search(no_str):
        return False
    return True


def _format_rows(column_names, rows, max_rows: int, max_cell: int = 60) -> str:
    cols = column_names or []
    lines = [" | ".join(str(c) for c in cols)] if cols else []
    for row in (rows or [])[:max_rows]:
        vals = list(row.values()) if isinstance(row, dict) else list(row)
        lines.append(" | ".join(("NULL" if v is None else str(v))[:max_cell] for v in vals))
    if rows and len(rows) > max_rows:
        lines.append(f"... (+{len(rows) - max_rows} more sampled rows)")
    return "\n".join(lines) if lines else "(no rows)"


def _exploration_messages(question: str, schema_text: str, max_probes: int) -> list[dict]:
    system = (
        "You are a Snowflake data analyst preparing to answer a question. Before writing "
        "the final query, you explore the database to GROUND yourself in real values and "
        "nested structure. Output only read-only SELECT queries."
    )
    user = (
        f"Question:\n{question}\n\nSchema (subset):\n{schema_text[:6000]}\n\n"
        f"Write at most {max_probes} DISTINCT, **read-only** Snowflake SQL probes to ground "
        "the query in REAL data BEFORE writing it. Prioritize the columns you will FILTER, "
        "JOIN, or AGGREGATE on (probe by role, not generic sampling). Cover, in priority order:\n"
        "1. DATE/TIME columns you will filter on — inspect ACTUAL stored values with NO date "
        "filter (a filter assumes the format and a wrong guess returns 0 rows). Use "
        "`SELECT MIN(\"date_col\"), MAX(\"date_col\") FROM <t>` and "
        "`SELECT \"date_col\" FROM <t> WHERE \"date_col\" IS NOT NULL LIMIT 5`. Infer the encoding "
        "from the raw MAGNITUDE: ~1e9 = epoch seconds, ~1e12 = epoch millis, ~1e15 = epoch micros, "
        "an 8-digit value like 20210101 = integer YYYYMMDD, '2021-01-01' = DATE/string. "
        "(If any date-filtered query returns 0 rows, your format assumption is WRONG — re-inspect raw values.)\n"
        "2. Entity/literal resolution — map any value the question names to its EXACT stored "
        "form via fuzzy match: `SELECT DISTINCT \"col\" FROM <t> WHERE \"col\" ILIKE '%keyword%' LIMIT 20` "
        "(replace spaces with %).\n"
        "3. Categorical filter columns — list top values: `SELECT \"col\", COUNT(*) FROM <t> GROUP BY 1 ORDER BY 2 DESC LIMIT 10`.\n"
        "4. Nested VARIANT/ARRAY structure: `SELECT f.value FROM <t>, LATERAL FLATTEN(input => t.\"col\") f LIMIT 5`.\n"
        "Rules: SELECT only (no DML/DDL), one statement per block, double-quote identifiers, "
        "use real table names from the schema. "
        "Return each as a ```sql``` block beginning with a `-- <description>` comment."
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def explore(
    question: str,
    db_id: str,
    schema_slice: SchemaSlice,
    executor: SnowflakeExecutor,
    model: str,
    max_probes: int = 6,
    max_rows: int = 20,
    max_evidence_chars: int = 4000,
    max_tokens: int = 800,
) -> ExplorationResult:
    """Generate, safety-check, and execute read-only probes; return evidence."""
    schema_text = schema_slice.format_for_prompt()

    # Deterministic date-format profiling (authoritative facts the model must obey,
    # plus an encoding map that drives a deterministic SQL rewrite downstream).
    try:
        date_facts, date_encodings = profile_date_columns(schema_slice, executor)
    except Exception:
        log.debug("Date profiling failed", exc_info=True)
        date_facts, date_encodings = [], {}

    try:
        raw = call_llm(
            _exploration_messages(question, schema_text, max_probes),
            model=model, temperature=0.2, max_tokens=max_tokens,
        )
    except LLMQuotaExhausted:
        raise  # let the runner checkpoint + pause for clean resume
    except Exception:
        log.warning("Exploration probe-generation failed; skipping", exc_info=True)
        raw = ""

    blocks = _extract_sql_blocks(raw)[:max_probes]
    probes: list[dict] = []
    pieces: list[str] = []
    n_ok = 0
    for sql in blocks:
        # Pull a leading `-- description` if present.
        desc = ""
        m = re.match(r"\s*--\s*(.*)", sql)
        if m:
            desc = m.group(1).strip()[:120]
        if not _is_safe_select(sql):
            probes.append({"sql": sql, "ok": False, "note": "rejected (not read-only SELECT)"})
            continue
        try:
            r = executor.execute(sql, sample_rows=max_rows)
        except Exception as exc:
            probes.append({"sql": sql, "ok": False, "note": f"exec error: {str(exc)[:80]}"})
            continue
        if not r.success:
            probes.append({"sql": sql, "ok": False, "note": (r.error_message or "")[:80]})
            continue
        n_ok += 1
        body = _format_rows(r.column_names, r.rows_sample, max_rows)
        probes.append({"sql": sql, "ok": True, "rows": r.row_count})
        header = f"-- {desc}" if desc else "-- probe"
        pieces.append(f"{header}\n{sql}\n-> {body}")

    # Assemble evidence under the char budget.
    evidence = ""
    for p in pieces:
        if len(evidence) + len(p) + 2 > max_evidence_chars:
            break
        evidence += (p + "\n\n")
    evidence = evidence.strip()
    if evidence:
        evidence = "Exploration evidence (real values observed in the database):\n" + evidence

    # Prepend authoritative, deterministically-verified format facts (highest priority).
    if date_facts:
        facts_block = ("DATA FORMAT FACTS (verified against the database — you MUST obey "
                       "these and ignore any contrary assumption):\n"
                       + "\n".join(f"- {f}" for f in date_facts))
        evidence = facts_block + ("\n\n" + evidence if evidence else "")

    log.info("Exploration: %d/%d probes ok, %d date-facts, evidence %d chars",
             n_ok, len(blocks), len(date_facts), len(evidence))
    return ExplorationResult(evidence_text=evidence, probes=probes,
                             n_probes=len(blocks), n_ok=n_ok,
                             date_encodings=date_encodings)


def _plan_messages(question, schema_text, exploration_evidence, evidence) -> list[dict]:
    system = (
        "You are a senior analytics engineer. Produce a concise STRUCTURED PLAN for "
        "translating the question into one Snowflake SQL query. Do NOT write the SQL."
    )
    parts = [f"Question:\n{question}\n"]
    if evidence:
        parts.append(f"\nDomain knowledge:\n{evidence[:1500]}\n")
    if exploration_evidence:
        parts.append(f"\n{exploration_evidence[:3500]}\n")
    parts.append(f"\nSchema (subset):\n{schema_text[:5000]}\n")
    parts.append(
        "\nReturn a short plan with these sections:\n"
        "1. Tables & roles (which table provides what).\n"
        "2. CONSTRAINT CHECKLIST — enumerate EVERY filter/constraint the question states "
        "(named entities, specific codes/symbols, date ranges, exclusions, thresholds) and map "
        "each to its column + exact value. Do NOT drop any constraint the question names.\n"
        "3. Entity->column mapping: exact column + access path (incl. nested `t.\"col\":field` "
        "or LATERAL FLATTEN), and the EXACT literal value to filter on if exploration resolved it.\n"
        "4. FORMAT RECONCILIATION — for each date/value filter, infer the column's REAL stored "
        "format from the raw MIN/MAX MAGNITUDE in the exploration evidence (NOT from any "
        "filtered probe): ~1e15 = epoch micros, ~1e12 = millis, ~1e9 = seconds, 20210101 = "
        "integer YYYYMMDD, '2021-01-01' = DATE. Then express the condition accordingly "
        "(epoch micros: `TO_TIMESTAMP(col/1000000)` or compare to the epoch integer; DATE: "
        "`col <= DATE '2021-10-01'`; YYYYMMDD: integer compare). Never assume YYYYMMDD.\n"
        "5. Joins / FLATTEN steps needed.\n"
        "6. Derived metrics / formulas (note units, e.g. revenue in micros -> /1e6).\n"
        "7. Output grain & columns (and required ordering).\n"
        "Be specific and brief; this guides SQL generation but is not binding."
    )
    return [{"role": "system", "content": system},
            {"role": "user", "content": "".join(parts)}]


def build_information_plan(
    question: str,
    schema_slice: SchemaSlice,
    exploration_evidence: str,
    evidence: str | None,
    model: str,
    max_tokens: int = 1200,
    max_plan_chars: int = 2000,
) -> str:
    """One LLM call producing a structured pre-generation plan. Best-effort."""
    schema_text = schema_slice.format_for_prompt()
    try:
        raw = call_llm(
            _plan_messages(question, schema_text, exploration_evidence, evidence),
            model=model, temperature=0.0, max_tokens=max_tokens,
        )
    except LLMQuotaExhausted:
        raise  # let the runner checkpoint + pause for clean resume
    except Exception:
        log.warning("Information-plan generation failed; skipping", exc_info=True)
        return ""
    plan = (raw or "").strip()
    if len(plan) > max_plan_chars:
        plan = plan[:max_plan_chars].rstrip() + "…"
    if plan:
        plan = "Structured plan (guidance):\n" + plan
    log.info("Information plan: %d chars", len(plan))
    return plan
