"""Deterministic QueryPlan → Snowflake SQL compiler."""

from __future__ import annotations

import ast
import logging
import re
from datetime import date, timedelta

import sqlglot
import sqlglot.expressions as sqlglot_exp

from ..retrieval.schema_slice import SchemaSlice
from .plan_schema import PlanCTE, PlanComputedColumn, PlanFlatten, PlanGeoJoin, QueryPlan

log = logging.getLogger(__name__)


def _is_number(token: str) -> bool:
    try:
        float(token)
        return True
    except ValueError:
        return False


def _format_literal(value: str, force_string: bool = False) -> str:
    """Format a scalar filter value as a SQL literal.

    Numbers and TRUE/FALSE/NULL are left bare; everything else is quoted as a
    string (with embedded single quotes escaped) since the plan JSON only ever
    supplies unquoted values. *force_string* skips the numeric/keyword checks
    — needed for operators like LIKE/ILIKE, whose right-hand side is always a
    string pattern even when it happens to look like a number (e.g. "15825003").
    """
    v = value.strip()
    if not v:
        return "''"
    if len(v) >= 2 and v[0] == v[-1] == "'":
        return v  # already a quoted string literal
    if not force_string:
        if v.upper() in ("TRUE", "FALSE", "NULL"):
            return v.upper()
        if _is_number(v):
            return v
    return "'" + v.replace("'", "''") + "'"


def _resolve_filter_value(
    value: str,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None,
    flatten_aliases: set[str] | None,
) -> str | None:
    """Return a column reference for *value* if it's actually a reference to
    another column in this block ("cte_name.column" or a fully qualified
    "db.schema.table.column") rather than a literal.

    PlanFilter has no separate field for a column-vs-column comparison, so
    the LLM sometimes writes the right-hand column's path directly into
    *value* (e.g. "season_max_wins.max_wins") — formatting that as a string
    literal (the default for every other filter value) produces a
    type-mismatch at execution time instead of the intended join-style
    comparison. Returns None if *value* doesn't match any source this block
    actually reads from (via *alias_map*), so the caller falls back to
    treating it as a literal — the overwhelmingly common case.
    """
    v = value.strip()
    if "." not in v:
        return None
    # Longest source name first, so a 4-part "DB.SCHEMA.TABLE.column" isn't
    # shadowed by a coincidental shorter prefix match.
    for source in sorted(alias_map, key=len, reverse=True):
        prefix = source + "."
        if v.startswith(prefix) and len(v) > len(prefix):
            column = v[len(prefix):]
            if _IDENTIFIER_RE.match(column):
                return _resolve_column_or_flatten(source, column, alias_map, case_map, flatten_aliases)
    return None


def _format_in_list(value: str) -> str:
    """Format a filter value for ``IN (...)`` as a comma-separated list of literals."""
    v = value.strip()
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = ast.literal_eval(v)
            items = [str(x) for x in parsed]
        except (ValueError, SyntaxError):
            items = [x.strip().strip("'\"") for x in v[1:-1].split(",") if x.strip()]
    else:
        items = [x.strip().strip("'\"") for x in v.split(",") if x.strip()]
    return ", ".join(_format_literal(x) for x in items)


def _format_between(value: str) -> str:
    """Format a filter value for ``BETWEEN x AND y`` as two literals.

    Accepts either "x AND y" or a JSON/Python-list-style "[x, y]" — the LLM
    sometimes writes BETWEEN bounds in the same bracketed-list shape it uses
    for IN (see _format_in_list), especially for date ranges. Without this,
    a "[x, y]" value falls through to being quoted as one single garbled
    literal instead of two BETWEEN bounds.
    """
    v = value.strip()
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = ast.literal_eval(v)
            if len(parsed) == 2:
                return f"{_format_literal(str(parsed[0]))} AND {_format_literal(str(parsed[1]))}"
        except (ValueError, SyntaxError, TypeError):
            pass
    parts = re.split(r"\s+AND\s+", v, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return _format_literal(value)
    return f"{_format_literal(parts[0])} AND {_format_literal(parts[1])}"


# Real Snowflake aggregate functions the compiler knows how to wrap as FUNC(col).
# LLM plans occasionally invent a placeholder function name (e.g. "EXPR") to mean
# "this column value is already the complete expression, don't wrap it in
# anything" — an unrecognized func name is treated exactly that way.
_KNOWN_AGG_FUNCS = {
    "SUM", "AVG", "COUNT", "MIN", "MAX", "MEDIAN",
    "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "VARIANCE", "VAR_POP", "VAR_SAMP",
    "ARRAY_AGG", "LISTAGG", "ANY_VALUE", "BOOLOR_AGG", "BOOLAND_AGG", "MODE",
    "APPROX_COUNT_DISTINCT",
}

# ── Date-encoding enforcement ────────────────────────────────────────────────
# Deterministically fix the common failure where a date column is treated as
# integer/string YYYYMMDD. Handles epoch columns (micros/millis/seconds) AND native
# DATE/TIMESTAMP columns wrongly parsed via TO_DATE(...,'YYYYMMDD') or compared to a
# YYYYMMDD literal. Only unambiguously-wrong patterns are rewritten, so a correct
# query is never altered.

_DIVISOR = {"micros": 1000000, "millis": 1000, "seconds": 1}
# YYYYMMDD literal, either as bare int or quoted string: 20200101 or '20200101'
_LIT = r"(?:'(\d{8})'|(\d{8}))"


def _yyyymmdd_to_iso(token: str | None) -> str | None:
    if not token or len(token) != 8 or not token.isdigit():
        return None
    y, m, d = int(token[:4]), int(token[4:6]), int(token[6:8])
    if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
        return f"{y:04d}-{m:02d}-{d:02d}"
    return None


def rewrite_date_encoding(sql: str, encodings: dict[str, str] | None) -> str:
    """Rewrite date columns wrongly treated as YYYYMMDD.

    *encodings*: {exact_column_name: "micros"|"millis"|"seconds"|"native"}.
      - epoch kinds  -> wrap column as TO_TIMESTAMP(col/divisor)::DATE, literal -> DATE
      - "native"     -> column is already a DATE; strip TO_DATE(...,'YYYYMMDD'),
                        and convert YYYYMMDD literals (int or string) to DATE literals
    """
    if not sql or not encodings:
        return sql
    out = sql
    for col, kind in encodings.items():
        ref = r'((?:[A-Za-z_]\w*\.)?"' + re.escape(col) + r'")'
        if kind == "native":
            def colexpr(refexpr: str) -> str:
                return refexpr          # already a date
            cast = ""
        elif kind in _DIVISOR:
            div = _DIVISOR[kind]
            def colexpr(refexpr: str, div=div) -> str:
                inner = f"{refexpr}/{div}" if div != 1 else refexpr
                return f"TO_TIMESTAMP({inner})"
            cast = "::DATE"
        else:
            continue

        # P1: TO_DATE([TO_VARCHAR(|TO_CHAR(|CAST(] ref [)], 'YYYYMMDD')
        out = re.sub(
            r"(?:TRY_)?TO_DATE\s*\(\s*(?:TO_VARCHAR|TO_CHAR|CAST)?\s*\(?\s*" + ref
            + r"\s*(?:AS\s+\w+(?:\(\d+\))?)?\s*\)?\s*,\s*'YYYYMMDD'\s*\)",
            lambda m: f"{colexpr(m.group(1))}{cast}",
            out, flags=re.IGNORECASE,
        )
        # P2-BETWEEN: ref BETWEEN <lit> AND <lit>
        def _between(m):
            iso1 = _yyyymmdd_to_iso(m.group(2) or m.group(3))
            iso2 = _yyyymmdd_to_iso(m.group(4) or m.group(5))
            if not (iso1 and iso2):
                return m.group(0)
            return f"{colexpr(m.group(1))}{cast} BETWEEN DATE '{iso1}' AND DATE '{iso2}'"
        out = re.sub(ref + r"\s+BETWEEN\s+" + _LIT + r"\s+AND\s+" + _LIT, _between, out, flags=re.IGNORECASE)
        # P2-COMPARE: ref <op> <lit>
        def _cmp(m):
            iso = _yyyymmdd_to_iso(m.group(3) or m.group(4))
            if not iso:
                return m.group(0)
            return f"{colexpr(m.group(1))}{cast} {m.group(2)} DATE '{iso}'"
        out = re.sub(ref + r"\s*(<=|>=|<>|!=|<|>|=)\s*" + _LIT, _cmp, out, flags=re.IGNORECASE)
    return out


# ── Date-shard rewriting ────────────────────────────────────────────────────

_DATE_SHARD_RE = re.compile(r"(\S+?)(\d{8})$")

# Old format: "Partitioned: 366 daily tables GA_SESSIONS_YYYYMMDD (20160801-20170801)"
_PARTITION_COMMENT_RE = re.compile(
    r"Partitioned:\s*(\d+)\s*daily\s*tables\s*\S+\s*\((\d{8})\s*.\s*(\d{8})"
)

# Current format: "Daily partitioned as GA_SESSIONS_YYYYMMDD (366 tables)"
_PARTITION_COMMENT_V2_RE = re.compile(
    r"Daily\s+partitioned\s+as\s+(\S+_)YYYYMMDD\s*\((\d+)\s*tables\)",
    re.IGNORECASE,
)

# Match >=/<= range: "date" >= '20170701' AND (optional alias.)"date" <= '20170731'
_DATE_GE_LE_RE = re.compile(
    r'"date"\s*>=\s*\'(\d{8})\'\s*AND\s*(?:\S+\.)?"date"\s*<=?\s*\'(\d{8})\'',
    re.IGNORECASE,
)

# Match BETWEEN: "date" BETWEEN '20170101' AND '20170331'
_DATE_BETWEEN_RE = re.compile(
    r'"date"\s*BETWEEN\s*\'(\d{8})\'\s*AND\s*\'(\d{8})\'',
    re.IGNORECASE,
)

# Match single date-like comparisons: "date" LIKE '201707%'
_DATE_LIKE_RE = re.compile(
    r'"date"\s+(?:I?LIKE)\s+\'(\d{4,6})%\'',
    re.IGNORECASE,
)


def _date_range_from_like(prefix: str) -> tuple[str, str] | None:
    """Convert a LIKE prefix '201707' into ('20170701', '20170731')."""
    if len(prefix) == 6:
        year, month = int(prefix[:4]), int(prefix[4:6])
        first = date(year, month, 1)
        if month == 12:
            last = date(year, 12, 31)
        else:
            last = date(year, month + 1, 1) - timedelta(days=1)
        return first.strftime("%Y%m%d"), last.strftime("%Y%m%d")
    elif len(prefix) == 4:
        return f"{prefix}0101", f"{prefix}1231"
    return None


def _generate_daily_tables(
    base: str,
    start: str,
    end: str,
    partition_start: str | None = None,
    partition_end: str | None = None,
) -> list[str]:
    """Generate list of daily table names for date range, optionally clamped."""
    start_d = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
    end_d = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
    if partition_start:
        ps = date(int(partition_start[:4]), int(partition_start[4:6]), int(partition_start[6:8]))
        start_d = max(start_d, ps)
    if partition_end:
        pe = date(int(partition_end[:4]), int(partition_end[4:6]), int(partition_end[6:8]))
        end_d = min(end_d, pe)
    tables = []
    d = start_d
    while d <= end_d:
        tables.append(f"{base}{d.strftime('%Y%m%d')}")
        d += timedelta(days=1)
    return tables


def rewrite_date_sharded_tables(sql: str, schema_slice: SchemaSlice | None) -> str:
    """Post-compilation rewrite: expand date-partitioned table references.

    Detects partitioned representative tables in the schema slice (via either
    of two comment formats) and any date-shard reference in the SQL with a
    matching base prefix. When the WHERE clause supplies a date range, replaces
    the reference with a CTE UNION ALL of all daily tables in that range.
    """
    if schema_slice is None:
        return sql

    # base_prefix -> (representative_qname_prefix, partition_start, partition_end)
    # partition_start/end may be None when comment doesn't supply them.
    partition_info: dict[str, tuple[str, str | None, str | None]] = {}
    for ts in schema_slice.tables:
        if not ts.comment:
            continue
        qname = ts.qualified_name
        sm = _DATE_SHARD_RE.match(qname)
        if not sm:
            continue
        # base_qname includes schema/db prefix, e.g. "GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS_"
        base_qname = sm.group(1)

        m = _PARTITION_COMMENT_RE.search(ts.comment)
        if m:
            partition_info[base_qname] = (base_qname, m.group(2), m.group(3))
            continue
        m2 = _PARTITION_COMMENT_V2_RE.search(ts.comment)
        if m2:
            partition_info[base_qname] = (base_qname, None, None)

    if not partition_info:
        return sql

    # Try to extract date range from the SQL
    date_range: tuple[str, str] | None = None
    rm = _DATE_GE_LE_RE.search(sql)
    if rm:
        date_range = (rm.group(1), rm.group(2))
    if not date_range:
        bm = _DATE_BETWEEN_RE.search(sql)
        if bm:
            date_range = (bm.group(1), bm.group(2))
    if not date_range:
        lm = _DATE_LIKE_RE.search(sql)
        if lm:
            date_range = _date_range_from_like(lm.group(1))

    if not date_range:
        log.debug("No date range found in SQL for date-shard rewriting")
        return sql

    start_date, end_date = date_range

    # For each partitioned base, find ANY shard reference in the SQL with that prefix
    # and rewrite it. Handles cases where the LLM picked a different date than the rep.
    for base_qname, (_rep, p_start, p_end) in partition_info.items():
        # Find all shard references in SQL: base_qname + 8 digits
        shard_re = re.compile(re.escape(base_qname) + r"(\d{8})")
        shards_in_sql = set(shard_re.findall(sql))
        if not shards_in_sql:
            continue

        daily_tables = _generate_daily_tables(base_qname, start_date, end_date, p_start, p_end)
        if len(daily_tables) <= 1:
            log.debug("Date range produces %d tables — skipping rewrite for %s", len(daily_tables), base_qname)
            continue

        # Skip rewrite when range is too broad (LIKE '2017%' → 365 days etc.):
        # Snowflake's planner times out on UNION ALL of >40 daily tables when
        # combined with LATERAL FLATTEN. Better to leave the SQL as-is and let
        # it fail naturally than waste minutes on EXPLAIN timeouts.
        if len(daily_tables) > 40:
            log.warning(
                "Date range produces %d tables for %s — too broad, skipping rewrite",
                len(daily_tables), base_qname,
            )
            continue

        # Build the UNION ALL CTE body with concrete table names (must NOT be
        # subject to the shard substitution below).
        cte_name = "_date_shard_union"
        union_parts = [f"SELECT * FROM {t}" for t in daily_tables]
        union_sql = " UNION ALL\n    ".join(union_parts)
        cte_block = f"{cte_name} AS (\n    {union_sql}\n)"

        # Substitute shard references in the original SQL body FIRST, then prepend
        # the CTE — this ensures the CTE body's concrete table names survive.
        body = shard_re.sub(cte_name, sql)
        if body.strip().upper().startswith("WITH "):
            sql = body.replace("WITH ", f"WITH {cte_block},\n", 1)
        else:
            sql = f"WITH {cte_block}\n{body}"

        log.info(
            "Date-shard rewrite: expanded %s* (saw %d distinct shard refs) into UNION ALL of %d tables (%s — %s)",
            base_qname, len(shards_in_sql), len(daily_tables), daily_tables[0], daily_tables[-1],
        )
        try:
            from ..observability.instance_telemetry import telemetry
            telemetry.increment("date_shard_rewrites")
            telemetry.mark("date_shard_rewrite_used")
        except Exception:
            log.debug("Telemetry increment failed", exc_info=True)

    return sql


_LISTAGG_START_RE = re.compile(r"\bLISTAGG\s*\(", re.IGNORECASE)
_WITHIN_GROUP_RE = re.compile(r"\s*WITHIN\s+GROUP\s*\(", re.IGNORECASE)


def _find_matching_paren(sql: str, open_paren_idx: int) -> int | None:
    """Return the index of the ')' matching the '(' at *open_paren_idx*, or
    None if unbalanced. Ignores parens inside single-quoted string literals
    (Snowflake escapes an embedded quote by doubling it: '' inside a string).
    """
    depth = 0
    i = open_paren_idx
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            i += 1
            while i < n:
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    break
                i += 1
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def rewrite_listagg_nullif(sql: str) -> str:
    """Wrap every ``LISTAGG(...)`` (with its ``WITHIN GROUP (...)`` clause, if
    present) in ``NULLIF(..., '')``.

    Snowflake's LISTAGG returns an EMPTY STRING (not NULL) when every value
    being aggregated in a group is NULL — e.g. after a LEFT JOIN that found
    no match. Downstream NULL-based comparisons/filters (and gold-answer
    comparisons expecting NULL) then silently mismatch. This is a purely
    mechanical, always-safe rewrite: it changes '' results to NULL and
    otherwise leaves the value unchanged, so it can't turn a correct answer
    into a wrong one. A prompt-only instruction to do this was tried first
    and found unreliable — 0/8 candidates applied it across a live test even
    though it was present in every candidate's system prompt — so this
    replaces that reliance on LLM compliance with a deterministic guarantee.
    Idempotent: re-running on already-wrapped SQL just adds a harmless nested
    NULLIF.
    """
    out = []
    i = 0
    n = len(sql)
    while i < n:
        m = _LISTAGG_START_RE.search(sql, i)
        if not m:
            out.append(sql[i:])
            break
        out.append(sql[i:m.start()])
        open_paren_idx = m.end() - 1
        close_paren_idx = _find_matching_paren(sql, open_paren_idx)
        if close_paren_idx is None:
            # Unbalanced parens — leave the rest of the string untouched
            # rather than risk mangling it.
            out.append(sql[m.start():])
            break
        span_end = close_paren_idx + 1
        wg = _WITHIN_GROUP_RE.match(sql, span_end)
        if wg:
            wg_open_paren_idx = wg.end() - 1
            wg_close_paren_idx = _find_matching_paren(sql, wg_open_paren_idx)
            if wg_close_paren_idx is not None:
                span_end = wg_close_paren_idx + 1
        span = sql[m.start():span_end]
        out.append(f"NULLIF({span}, '')")
        i = span_end
    return "".join(out)


def _alias(idx: int) -> str:
    """Stable alias: t1, t2, ..."""
    return f"t{idx + 1}"


def _build_column_case_map(
    schema_slice: SchemaSlice | None,
) -> dict[str, dict[str, str]]:
    """Return {table_qname: {UPPER_COL: original_col}} from the SchemaSlice.

    Used to restore exact column casing when generating SQL.
    """
    if schema_slice is None:
        return {}
    case_map: dict[str, dict[str, str]] = {}
    for ts in schema_slice.tables:
        col_map: dict[str, str] = {}
        for col in ts.columns:
            original = col.original_name if col.original_name else col.name
            col_map[col.name.upper()] = original
        case_map[ts.qualified_name] = col_map
    return case_map


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _looks_like_expression(column: str) -> bool:
    """Return True if *column* is a SQL expression, not a plain identifier.

    LLM plans occasionally stuff a computed expression (CASE WHEN, arithmetic,
    function calls) into a field meant for a plain column name. Quoting the
    whole expression as an identifier corrupts the SQL, so it must be emitted
    verbatim instead.
    """
    return not _IDENTIFIER_RE.match(column.strip())


def _rewrite_expression_columns(
    expr_text: str,
    table: str,
    alias: str,
    case_map: dict[str, dict[str, str]] | None,
    alias_map: dict[str, str] | None = None,
    source_map: dict[str, str] | None = None,
) -> str:
    """Rewrite column references inside a computed expression.

    LLM-authored expressions (a formula stuffed into a column field) reference
    their columns without the alias/casing help the rest of the compiler
    gives every other column. Parses *expr_text* and, for any unqualified
    ``Column`` node, adds the table alias and restores original casing.
    Columns that are already qualified with a table/CTE name are left alone
    if their identifier is already quoted (trusted as intentional); if not
    quoted, the identifier is quoted as-given (not case-mapped, since that's
    only known for real schema tables) — every alias this compiler defines
    is quoted verbatim, so an unquoted qualified reference to one would
    otherwise be a silent case-sensitivity mismatch. Function names,
    keywords, and string literals are never touched. Falls back to the raw
    text unchanged if it doesn't parse as a SQL expression (never worse than
    not rewriting at all).

    *table*/*alias* are the plan-declared owner of the expression — the
    default for any column *source_map* can't place. A computed_column can
    only declare one such owner even though its expression may span every
    source the block joins (e.g. a ratio of a column from one joined CTE
    over a column from another): when *source_map* attributes an unqualified
    column to a different source, that source's alias is used instead. See
    _build_source_column_map.
    """
    try:
        tree = sqlglot.parse_one(expr_text, dialect="snowflake")
    except Exception:
        return expr_text

    for col in tree.find_all(sqlglot_exp.Column):
        if col.table:
            # The LLM sometimes qualifies a column with the real table's
            # full "DB.SCHEMA.TABLE" name (sqlglot splits this across
            # catalog/db/table) instead of the compiler-assigned alias
            # (t1, t2, ...) actually used in the FROM clause — that
            # qualifier must be re-mapped, or it's a dangling reference to
            # a table name that never appears in the generated SQL.
            full_qualifier = ".".join(p for p in (col.catalog, col.db, col.table) if p)
            if alias_map and full_qualifier in alias_map:
                col.set("catalog", None)
                col.set("db", None)
                col.set("table", sqlglot_exp.to_identifier(alias_map[full_qualifier], quoted=False))
            if not col.this.quoted:
                col.set("this", sqlglot_exp.to_identifier(col.name, quoted=True))
            continue
        col_upper = col.name.upper()
        resolved_table = table
        if source_map is not None and col_upper in source_map:
            resolved_table = source_map[col_upper]
        resolved_alias = (
            alias if resolved_table == table
            else (alias_map.get(resolved_table, resolved_table) if alias_map else resolved_table)
        )
        col_map = case_map.get(resolved_table, {}) if case_map else {}
        original = col_map.get(col_upper, col.name)
        col.set("this", sqlglot_exp.to_identifier(original, quoted=True))
        col.set("table", sqlglot_exp.to_identifier(resolved_alias, quoted=False))

    return tree.sql(dialect="snowflake")


def _build_source_column_map(
    selected_tables: list[str],
    plan: QueryPlan | None,
    cte_by_name: dict[str, PlanCTE] | None,
    case_map: dict[str, dict[str, str]] | None,
) -> dict[str, str]:
    """Map UPPER_COLUMN_NAME -> source name, for every source a block reads
    from (its own joined CTEs/tables). Used by _rewrite_expression_columns to
    route a bare column reference inside a computed_column expression to the
    source that actually exposes it, when the block joins more than one
    source and the expression spans columns from more than one of them.

    A column exposed by more than one source is left out of the map
    (genuinely ambiguous) rather than guessed at; callers fall back to the
    computed_column's declared *table* field in that case, same as when the
    map has nothing to say about a column at all.
    """
    seen_from: dict[str, str] = {}
    ambiguous: set[str] = set()
    for src in selected_tables:
        cols: set[str] | None = None
        if cte_by_name and src in cte_by_name and plan is not None:
            _, exposed = _self_exposure(cte_by_name[src], plan, cte_by_name)
            cols = exposed
        elif case_map and src in case_map:
            cols = set(case_map[src].keys())
        if not cols:
            continue
        for c in cols:
            if c in seen_from and seen_from[c] != src:
                ambiguous.add(c)
            else:
                seen_from[c] = src
    return {c: src for c, src in seen_from.items() if c not in ambiguous}


_QUALIFIED_IDENTIFIER_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$"
)


def _format_order_by(
    order_by: list,
    alias_map: dict[str, str] | None = None,
    case_map: dict[str, dict[str, str]] | None = None,
) -> str:
    """Render an ORDER BY clause body (no ``ORDER BY`` keyword), or "" if empty.

    A bare identifier is almost always a reference to a SELECT-list alias
    (e.g. an aggregation output), which is quoted at definition time — quote
    the reference here too so it still matches. A qualified "table.column"
    reference is resolved the same way any other column reference is
    (alias + exact casing). Anything more complex (an expression) is left
    verbatim.
    """
    parts: list[str] = []
    for ob in order_by:
        direction = ob.direction.upper()
        stripped = ob.expr.strip()
        qualified = _QUALIFIED_IDENTIFIER_RE.match(stripped)
        if _IDENTIFIER_RE.match(stripped):
            expr = f'"{stripped}"'
        elif qualified and alias_map is not None:
            expr = _resolve_column(qualified.group(1), qualified.group(2), alias_map, case_map)
        else:
            expr = ob.expr
        parts.append(f"{expr} {direction}")
    return ", ".join(parts)


def _extract_bare_identifiers(expr_text: str) -> set[str]:
    """Return the set of unqualified column names (original casing) referenced
    in a SQL expression fragment. Used to figure out which upstream CTE
    columns a computed expression actually depends on. Returns an empty set
    if the fragment doesn't parse (never raises).

    Casing is preserved rather than normalized: callers that thread a result
    into a *new* passthrough column reference (e.g. autofix_cte_scope) need
    it to match how the column was quoted at definition time elsewhere in
    the plan — every reference to a given alias must agree, or Snowflake
    sees two distinct quoted identifiers. Callers doing membership checks
    against an already-uppercased set should upper() at the comparison site
    instead of relying on this function to normalize.
    """
    try:
        tree = sqlglot.parse_one(expr_text, dialect="snowflake")
    except Exception:
        return set()
    return {col.name for col in tree.find_all(sqlglot_exp.Column) if not col.table}


def _expression_is_aggregating(expr_text: str) -> bool:
    """Return True if a computed_column's raw expression already contains a
    genuine (non-windowed) aggregate function call (e.g. "MAX(CASE WHEN
    ...)") — a legitimate pattern when agg_func is left null (the aggregate
    is embedded in the expression itself rather than wrapped around it).

    A windowed call (e.g. "LAG(x) OVER (PARTITION BY ...)") does NOT count —
    window functions operate per-row and never require or use GROUP BY, even
    though sqlglot classifies functions like LAG/SUM/... as AggFunc
    regardless of whether they're windowed. Returns False if the fragment
    doesn't parse (never raises).
    """
    try:
        tree = sqlglot.parse_one(expr_text, dialect="snowflake")
    except Exception:
        return False
    return any(
        not isinstance(agg.parent, sqlglot_exp.Window)
        for agg in tree.find_all(sqlglot_exp.AggFunc)
    )


def _resolve_column(
    table: str,
    column: str,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None = None,
) -> str:
    """Return alias."COLUMN" reference with double-quoted column name.

    If *case_map* is provided, uses the original casing from the SchemaSlice.
    If *column* looks like a SQL expression rather than a plain identifier,
    its internal column references are rewritten with proper alias/casing
    (see _rewrite_expression_columns) rather than quoting the whole thing as
    a single identifier.
    """
    if _looks_like_expression(column):
        alias = alias_map.get(table, table)
        return _rewrite_expression_columns(column, table, alias, case_map, alias_map=alias_map)
    alias = alias_map.get(table, table)
    # Resolve original casing if available
    col_name = column
    if case_map and table in case_map:
        col_name = case_map[table].get(column.upper(), column)
    # Double-quote column to preserve case (Snowflake treats unquoted as uppercase)
    return f'{alias}."{col_name}"'


def _compile_flatten_from(
    flatten_ops: list[PlanFlatten],
    alias_map: dict[str, str],
) -> list[str]:
    """Generate LATERAL FLATTEN clause fragments for FROM."""
    parts: list[str] = []
    for f in flatten_ops:
        table_alias = alias_map.get(f.table, f.table)
        parts.append(
            f', LATERAL FLATTEN(input => {table_alias}."{f.variant_column}") {f.alias}'
        )
    return parts


def _resolve_column_or_flatten(
    table: str,
    column: str,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None = None,
    flatten_aliases: set[str] | None = None,
) -> str:
    """Resolve a column reference, handling FLATTEN alias.value:"field" syntax.

    If *table* matches a FLATTEN alias (e.g. "h") and *column* contains a dot
    (e.g. "page.pagePath"), emit ``h.value:"page":"pagePath"`` syntax.
    If *column* has no dot, emit ``h.value:"column"`` syntax.
    Otherwise fall through to normal resolution.
    """
    if flatten_aliases and table in flatten_aliases:
        # column might be "page.pagePath" or just "productRevenue"
        field_parts = column.split(".")
        path = "".join(f':"{p}"' for p in field_parts)
        return f"{table}.value{path}"
    return _resolve_column(table, column, alias_map, case_map)


def _compile_geo_joins(
    geo_joins: list[PlanGeoJoin],
    alias_map: dict[str, str],
) -> list[str]:
    """Generate spatial JOIN clauses with geospatial ON predicates."""
    parts: list[str] = []
    for gj in geo_joins:
        jtype = gj.join_type.upper()
        right_alias = alias_map.get(gj.right_table, gj.right_table)
        # Substitute table aliases into the ON expression
        on_expr = _substitute_aliases(gj.on_expression, alias_map)
        if jtype == "CROSS":
            parts.append(f"CROSS JOIN {gj.right_table} AS {right_alias}")
        else:
            parts.append(
                f"{jtype} JOIN {gj.right_table} AS {right_alias} "
                f"ON {on_expr}"
            )
    return parts


def _substitute_aliases(expression: str, alias_map: dict[str, str]) -> str:
    """Best-effort substitution of full table names with aliases in a raw expression.

    Replaces occurrences of qualified table names (e.g. ``DB.SCHEMA.TABLE``)
    with the corresponding compiler alias (e.g. ``t1``).  This allows the LLM
    to write geo expressions using full table names and have them shortened.
    """
    result = expression
    # Sort by length descending so longer names are replaced first
    for full_name, alias in sorted(alias_map.items(), key=lambda x: -len(x[0])):
        result = result.replace(full_name, alias)
    return result


def _compile_single_block(
    selected_tables: list[str],
    joins: list,
    geo_joins: list[PlanGeoJoin],
    flatten_ops: list[PlanFlatten],
    filters: list,
    geo_filters: list,
    group_by: list[str],
    aggregations: list,
    order_by: list,
    limit: int | None,
    alias_map: dict[str, str],
    case_map: dict[str, dict[str, str]] | None,
    computed_columns: list | None = None,
    plan: QueryPlan | None = None,
    cte_by_name: dict[str, PlanCTE] | None = None,
) -> str:
    """Compile a single SELECT block (used for main query and each CTE)."""
    flatten_aliases: set[str] = {f.alias for f in flatten_ops}

    # For computed_column expressions that span more than one joined source
    # (e.g. a ratio of a column from one joined CTE over a column from
    # another) — see _build_source_column_map / _rewrite_expression_columns.
    source_map = (
        _build_source_column_map(selected_tables, plan, cte_by_name, case_map)
        if computed_columns else {}
    )

    # ── FROM / JOIN clause ──────────────────────────────────────────────
    if not selected_tables:
        return "SELECT 1"
    primary = selected_tables[0]
    from_parts = [f"{primary} AS {alias_map.get(primary, primary)}"]

    # Composite-key joins are expressed as multiple PlanJoin entries sharing
    # the same right_table (the schema has no field for a multi-column join
    # condition) — group and merge them into one JOIN clause with ANDed
    # conditions, or Snowflake rejects the same alias appearing twice.
    joins_by_table: dict[str, list] = {}
    for j in joins:
        joins_by_table.setdefault(j.right_table, []).append(j)

    for right_table, group in joins_by_table.items():
        jtype = group[0].join_type.upper()
        right_alias = alias_map.get(right_table, right_table)
        conditions = [
            f"{_resolve_column(j.left_table, j.left_column, alias_map, case_map)} = "
            f"{_resolve_column(j.right_table, j.right_column, alias_map, case_map)}"
            for j in group
        ]
        from_parts.append(
            f"{jtype} JOIN {right_table} AS {right_alias} "
            f"ON {' AND '.join(conditions)}"
        )

    # Geospatial JOIN clauses
    from_parts.extend(_compile_geo_joins(geo_joins, alias_map))

    # LATERAL FLATTEN clauses
    from_parts.extend(_compile_flatten_from(flatten_ops, alias_map))

    # Any other selected_tables entry not brought in by a join above (e.g.
    # two independent single-row aggregates meant to be combined — no join
    # key exists between them) still needs to be in FROM, or references to
    # it later in SELECT/WHERE point at a table that was never queried. But
    # cap how many of these get auto-cross-joined: a handful of unconnected
    # tables is a plausible "combine independent scalars" plan; dozens or
    # hundreds (e.g. one entry per date-sharded table, meant to be UNIONed,
    # not multiplied) is a different, dangerous shape — blindly cross
    # joining that is a combinatorial-explosion risk, not a reasonable
    # guess, so it's left alone (surfaces as a missing-table reference
    # instead of an enormous query).
    _MAX_AUTO_CROSS_JOIN_TABLES = 3
    already_joined = {primary} | set(joins_by_table) | {gj.right_table for gj in geo_joins}
    extra_tables = [t for t in selected_tables[1:] if t not in already_joined]
    if len(extra_tables) > _MAX_AUTO_CROSS_JOIN_TABLES:
        log.warning(
            "FROM clause: %d selected_tables entries have no join and exceed "
            "the auto-cross-join cap (%d) — likely a date-shard/UNION pattern "
            "the plan should express differently; left out of FROM rather "
            "than risking a combinatorial-explosion cross join.",
            len(extra_tables), _MAX_AUTO_CROSS_JOIN_TABLES,
        )
    else:
        for extra_table in extra_tables:
            from_parts.append(
                f"CROSS JOIN {extra_table} AS {alias_map.get(extra_table, extra_table)}"
            )
            already_joined.add(extra_table)

    from_clause = "\n".join(from_parts)

    # ── SELECT clause ───────────────────────────────────────────────────
    select_parts: list[str] = []

    # Aliases that aggregations/computed_columns will independently produce
    # in this same block — a bare group_by entry with the same name is a
    # redundant re-statement of that same output (LLM plans occasionally
    # list a computed alias in group_by too), not a separate raw column, so
    # it must not be added as its own (broken, unresolvable) SELECT item.
    known_aliases = {agg.alias.upper() for agg in aggregations} | {
        cc.alias.upper() for cc in (computed_columns or [])
    }

    # Group-by columns first
    for gb in group_by:
        if "." in gb:
            # Check if the first segment is a flatten alias (e.g. "h.page.pagePath")
            first_seg = gb.split(".", 1)[0]
            if first_seg in flatten_aliases:
                table_part = first_seg
                col_part = gb.split(".", 1)[1]  # "page.pagePath"
            else:
                table_part, col_part = gb.rsplit(".", 1)
            resolved = _resolve_column_or_flatten(
                table_part, col_part, alias_map, case_map, flatten_aliases
            )
            select_parts.append(resolved)
        elif gb.upper() in known_aliases:
            continue  # already produced below via its aggregation/computed_column
        else:
            # A bare passthrough reference (e.g. to an upstream CTE's own
            # column) — quote it for the same reason every other reference
            # is quoted: the source was itself quoted at definition time.
            select_parts.append(f'"{gb}"')

    # Aggregations
    for agg in aggregations:
        col_ref = _resolve_column_or_flatten(
            agg.table, agg.column, alias_map, case_map, flatten_aliases
        )
        func_upper = agg.func.upper()
        if func_upper == "COUNT_DISTINCT":
            expr = f"COUNT(DISTINCT {col_ref})"
        elif func_upper == "COUNT" and agg.column == "*":
            expr = "COUNT(*)"
        elif func_upper not in _KNOWN_AGG_FUNCS:
            # Unrecognized/invented function name — the column is already the
            # full expression to select, don't wrap it in anything.
            expr = col_ref
        else:
            expr = f"{func_upper}({col_ref})"
        # Quote the alias so it matches how downstream references to this
        # column are resolved: _resolve_column always double-quotes column
        # references (including references to a prior CTE's own output
        # columns). Leaving this alias unquoted would make Snowflake store it
        # uppercased, while every later reference to it is quoted-lowercase —
        # a case-sensitivity mismatch ("invalid identifier").
        select_parts.append(f'{expr} AS "{agg.alias}"')

    # Computed/derived columns (formulas, CASE pivots, geo distance, etc.)
    for cc in computed_columns or []:
        cc_alias = alias_map.get(cc.table, cc.table)
        rewritten = _rewrite_expression_columns(
            cc.expression, cc.table, cc_alias, case_map,
            alias_map=alias_map, source_map=source_map,
        )
        agg_func_upper = (cc.agg_func or "").upper()
        if agg_func_upper == "COUNT_DISTINCT":
            expr = f"COUNT(DISTINCT {rewritten})"
        elif agg_func_upper in _KNOWN_AGG_FUNCS:
            expr = f"{agg_func_upper}({rewritten})"
        else:
            # No (recognized) agg_func — the expression is already the full
            # value to select (a per-row formula, or one with its own
            # aggregate calls already inside it).
            expr = rewritten
        select_parts.append(f'{expr} AS "{cc.alias}"')

    if not select_parts:
        # Fallback: select all columns from first table
        first_alias = alias_map.get(primary, primary)
        select_parts.append(f"{first_alias}.*")

    select_clause = ",\n  ".join(select_parts)

    # ── WHERE clause ────────────────────────────────────────────────────
    where_parts: list[str] = []
    for f in filters:
        col_ref = _resolve_column_or_flatten(
            f.table, f.column, alias_map, case_map, flatten_aliases
        )
        op = f.op.upper()
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{col_ref} {op}")
        elif op == "IN" and f.value is not None:
            where_parts.append(f"{col_ref} IN ({_format_in_list(f.value)})")
        elif op == "BETWEEN" and f.value is not None:
            where_parts.append(f"{col_ref} BETWEEN {_format_between(f.value)}")
        elif f.value is not None:
            rhs = _resolve_filter_value(f.value, alias_map, case_map, flatten_aliases)
            if rhs is None:
                force_string = op in ("LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE")
                rhs = _format_literal(f.value, force_string)
            where_parts.append(f"{col_ref} {op} {rhs}")
        else:
            where_parts.append(f"{col_ref} {op} NULL")

    # Geospatial WHERE predicates (emitted verbatim with alias substitution)
    for gf in geo_filters:
        where_parts.append(_substitute_aliases(gf.expression, alias_map))

    where_clause = " AND ".join(where_parts) if where_parts else ""

    # ── GROUP BY clause ─────────────────────────────────────────────────
    group_parts: list[str] = []
    for gb in group_by:
        if "." in gb:
            first_seg = gb.split(".", 1)[0]
            if first_seg in flatten_aliases:
                table_part = first_seg
                col_part = gb.split(".", 1)[1]
            else:
                table_part, col_part = gb.rsplit(".", 1)
            resolved = _resolve_column_or_flatten(
                table_part, col_part, alias_map, case_map, flatten_aliases
            )
            group_parts.append(resolved)
        else:
            # Either a same-block alias or a bare passthrough reference —
            # both are quoted at definition time (see select_parts above),
            # so quote the GROUP BY reference the same way.
            group_parts.append(f'"{gb}"')

    # group_by entries are always SELECT-list items, but a GROUP BY clause is
    # only valid/intended when this block actually aggregates something —
    # otherwise a passthrough column would silently turn a per-row query into
    # a deduplicating one.
    cc_is_self_aggregating = {
        id(cc): (
            (cc.agg_func or "").upper() in _KNOWN_AGG_FUNCS
            or (cc.agg_func or "").upper() == "COUNT_DISTINCT"
            or _expression_is_aggregating(cc.expression)
        )
        for cc in (computed_columns or [])
    }
    is_aggregating = bool(aggregations) or any(cc_is_self_aggregating.values())

    if is_aggregating:
        # A computed_column or aggregation that does not itself aggregate —
        # a plain passthrough column, or a scalar formula over one (e.g. a
        # DATE_TRUNC bucketing transform) — is semantically identical to a
        # group_by entry once real aggregation is happening in the same
        # block, and Snowflake requires it to be in GROUP BY too, or it
        # rejects the query outright rather than silently doing the wrong
        # thing.
        for agg in aggregations:
            func_upper = agg.func.upper()
            if (
                func_upper not in _KNOWN_AGG_FUNCS
                and func_upper != "COUNT_DISTINCT"
                and not (func_upper == "COUNT" and agg.column == "*")
                and not _looks_like_expression(agg.column)
            ):
                ref = _resolve_column_or_flatten(
                    agg.table, agg.column, alias_map, case_map, flatten_aliases
                )
                if ref not in group_parts:
                    group_parts.append(ref)
        for cc in (computed_columns or []):
            if not cc.agg_func and not cc_is_self_aggregating[id(cc)]:
                cc_alias = alias_map.get(cc.table, cc.table)
                rewritten = _rewrite_expression_columns(
                    cc.expression, cc.table, cc_alias, case_map,
                    alias_map=alias_map, source_map=source_map,
                )
                if rewritten not in group_parts:
                    group_parts.append(rewritten)

    group_clause = ", ".join(group_parts) if (group_parts and is_aggregating) else ""

    # ── ORDER BY clause ─────────────────────────────────────────────────
    order_clause = _format_order_by(order_by, alias_map, case_map)

    # ── Assemble ────────────────────────────────────────────────────────
    sql_lines = [f"SELECT\n  {select_clause}", f"FROM {from_clause}"]
    if where_clause:
        sql_lines.append(f"WHERE {where_clause}")
    if group_clause:
        sql_lines.append(f"GROUP BY {group_clause}")
    if order_clause:
        sql_lines.append(f"ORDER BY {order_clause}")
    if limit is not None:
        sql_lines.append(f"LIMIT {limit}")

    return "\n".join(sql_lines)


def _self_exposure(
    start,
    plan: QueryPlan,
    cte_by_name: dict[str, PlanCTE],
):
    """Resolve what a CTE (or the top-level plan) actually exposes in its
    own SELECT list, following the compiler's implicit wildcard-passthrough
    fallback (``SELECT primary_table.*``) when the block has no explicit
    group_by/aggregations/computed_columns of its own — the same fallback
    ``_compile_single_block`` uses.

    Returns (owner, exposed_columns):
    - If *start* (or whatever it wildcards down to, transitively) has an
      explicit SELECT list, *owner* is that CTE/plan object (whose group_by
      should be extended if a column needs threading through) and
      *exposed_columns* is its uppercased alias/passthrough-column set.
    - Returns (None, None) if the chain bottoms out at a real table (an
      unknown, unbounded column set) or a plan with nothing to wildcard —
      callers must treat that as "cannot validate", not "exposes nothing".
    """
    seen: set[str] = set()
    current = start
    while True:
        if current.group_by or current.aggregations or current.computed_columns:
            cols: set[str] = set()
            for gb in current.group_by:
                cols.add(gb.rsplit(".", 1)[-1].upper())
            for agg in current.aggregations:
                cols.add(agg.alias.upper())
            for cc in current.computed_columns:
                cols.add(cc.alias.upper())
            return current, cols

        if current is plan:
            if not plan.ctes:
                return None, None
            next_name = plan.ctes[-1].name
        else:
            if not current.selected_tables:
                return None, None
            next_name = current.selected_tables[0]

        if next_name in seen or next_name not in cte_by_name:
            return None, None  # cycle guard, or wildcards a real table
        seen.add(next_name)
        current = cte_by_name[next_name]


def _find_source_table_for_column(
    cte: PlanCTE, column_upper: str, cte_by_name: dict[str, PlanCTE]
) -> str | None:
    """Search a CTE's own fields for a table attribution for *column_upper*
    — e.g. it's already referenced inside one of the CTE's own
    computed_column expressions, or filtered on directly — used to safely
    infer which real table a missing passthrough column belongs to.
    """
    for cc in cte.computed_columns:
        if column_upper in {i.upper() for i in _extract_bare_identifiers(cc.expression)}:
            return cc.table
    for f in cte.filters:
        if f.column.upper() == column_upper:
            return f.table
    for agg in cte.aggregations:
        if agg.column.upper() == column_upper:
            return agg.table
    # No existing usage of the column found anywhere in this CTE. If it
    # reads from exactly one source AND that source is a real table (an
    # unknown, unbounded column set), an unreferenced column plausibly still
    # belongs to it — a reasonable guess. But if the sole source is another
    # CTE, that CTE's exposed columns are already fully known; if the
    # missing column isn't among them, it definitely does not come from
    # there, so guessing it does would be a confirmed-wrong fix, not an
    # uncertain one.
    if len(cte.selected_tables) == 1:
        only_source = cte.selected_tables[0]
        if only_source not in cte_by_name:
            return only_source
    return None


def _consumer_references(container) -> list[tuple[str, str]]:
    """(table, column) pairs a CTE's (or the top-level plan's) own joins/
    filters/aggregations/group_by/computed_columns reference — checked
    against an upstream CTE's exposed columns. Computed-column expressions
    are expanded to every bare identifier they touch.
    """
    refs: list[tuple[str, str]] = []
    for j in container.joins:
        refs.append((j.left_table, j.left_column))
        refs.append((j.right_table, j.right_column))
    for f in container.filters:
        refs.append((f.table, f.column))
    for agg in container.aggregations:
        if _looks_like_expression(agg.column):
            for ident in _extract_bare_identifiers(agg.column):
                refs.append((agg.table, ident))
        else:
            refs.append((agg.table, agg.column))
    for cc in container.computed_columns:
        for ident in _extract_bare_identifiers(cc.expression):
            refs.append((cc.table, ident))
    for gb in container.group_by:
        if "." in gb:
            t, c = gb.rsplit(".", 1)
            refs.append((t, c))
    return refs


def _references_only_safe_columns(expr_text: str, allowed_upper: set[str]) -> bool:
    """True if every column *expr_text* references is a bare (unqualified)
    identifier in *allowed_upper*.

    Used to check whether a dependent computed_column can be safely moved
    into a wrapper CTE that only reads from the base CTE's own SELECT
    output: a QUALIFIED reference (e.g. to a real table.column) can't be —
    the wrapper doesn't join that table at all, so it's a dangling
    reference — and a bare reference to anything the base CTE doesn't
    actually expose (not one of its own aggregation/computed_column
    aliases, or a group_by passthrough column) is equally dangling. Returns
    False (unsafe) if the expression doesn't parse, rather than risk it.
    """
    try:
        tree = sqlglot.parse_one(expr_text, dialect="snowflake")
    except Exception:
        return False
    for col in tree.find_all(sqlglot_exp.Column):
        if col.table:
            return False
        if col.name.upper() not in allowed_upper:
            return False
    return True


def split_self_referencing_computed_columns(plan: QueryPlan) -> QueryPlan:
    """Split a CTE whose computed_columns reference a SIBLING aggregation's/
    computed_column's own alias into two chained CTEs.

    SQL cannot see a sibling SELECT-list alias within the same SELECT — e.g.
    a CASE WHEN referencing another computed_column's own output, or a
    per-row formula consuming a window-function computed_column defined in
    the same step. The LLM's plan sometimes expresses this dependency
    directly instead of as two CTE steps (despite being told not to — see
    the plan-generation prompt), which the compiler previously resolved by
    quoting the sibling alias as an ordinary (nonexistent) column on
    whatever real table the computed_column happened to declare, producing
    an "invalid identifier" error at execution time.

    Where it's SAFE to do so automatically, this rewrites the single CTE
    into ``<name>__base`` (everything except the dependent computed_columns)
    feeding ``<name>`` (a thin wrapper that adds them back as plain per-row
    references to the base CTE's own output columns) — downstream CTEs keep
    referencing ``<name>`` unchanged, since the wrapper keeps the original
    name.

    Deliberately narrow: only handles a dependent computed_column that is
    itself a plain per-row expression — no ``agg_func``, and no aggregate
    call embedded in its own expression text (checked via
    _expression_is_aggregating). A dependent computed_column that ALSO
    needs its own aggregation (e.g. one embedding a further MAX(...) over
    the sibling's per-row window-function output) implies a THIRD grouping
    level whose key isn't specified anywhere in the plan — guessing it
    risks silently wrong SQL, which is worse than the loud failure this
    produces today, so those cases are deliberately left alone. Also
    excluded: a dependent computed_column that references anything else the
    base CTE won't expose in its own SELECT output — e.g. a raw column from
    a table the base CTE joins but never aggregates/passes through (see
    _references_only_safe_columns) — the wrapper only reads from the base
    CTE's single output, so any such reference would be dangling there too.
    Likewise left alone if the CTE's own filters/geo_filters/group_by/
    order_by reference a dependent alias (would need moving to the wrapper
    too, not handled here).

    Operates on and returns a deep copy; the original plan is untouched.
    """
    if not plan.ctes:
        return plan

    plan = plan.model_copy(deep=True)
    cte_by_name = {c.name: c for c in plan.ctes}
    new_ctes: list[PlanCTE] = []

    for cte in plan.ctes:
        own_aliases = {a.alias.upper() for a in cte.aggregations} | {
            c.alias.upper() for c in cte.computed_columns
        }

        # A bare identifier that happens to share a sibling's alias NAME but
        # is ALSO independently reachable as a genuine passthrough/real
        # column (e.g. a computed_column that's just "expression": "year_num",
        # "alias": "year_num" — a same-named passthrough, not a computed
        # value) is not a same-step dependency at all: the normal resolver
        # (_build_source_column_map, used elsewhere for every other
        # computed_column) already finds it correctly. Only an identifier
        # that exists SOLELY as another entry's own alias — nowhere in this
        # CTE's actual joined/read sources — is a genuine dependency that
        # needs splitting. Getting this wrong forces an unnecessary split,
        # which for a window-function expression produces broken SQL
        # (Snowflake rejects a window function nested inside another).
        block_sources = list(dict.fromkeys(
            cte.selected_tables
            + [j.right_table for j in cte.joins]
            + [gj.right_table for gj in cte.geo_joins]
        ))
        real_source_map = _build_source_column_map(block_sources, plan, cte_by_name, None)

        group_by_names_upper = {gb.rsplit(".", 1)[-1].upper() for gb in cte.group_by}
        # What the base CTE's SELECT list will actually expose once split —
        # a dependent computed_column may only reference these plus its own
        # sibling alias(es); anything else (esp. a qualified reference into
        # a table the base CTE joins but doesn't pass through) is unsafe.
        base_exposed_upper = own_aliases | group_by_names_upper

        dependent: list[PlanComputedColumn] = []
        independent: list[PlanComputedColumn] = []
        for cc in cte.computed_columns:
            sibling_aliases = own_aliases - {cc.alias.upper()}
            referenced_idents = {ident.upper() for ident in _extract_bare_identifiers(cc.expression)}
            genuine_sibling_refs = (referenced_idents & sibling_aliases) - set(real_source_map)
            refs_sibling = bool(genuine_sibling_refs)
            if not refs_sibling:
                independent.append(cc)
            elif cc.agg_func or _expression_is_aggregating(cc.expression):
                # Needs its own (unspecified) grouping level — leave as-is;
                # fails loudly downstream, same as before this function existed.
                independent.append(cc)
            elif not _references_only_safe_columns(cc.expression, base_exposed_upper):
                # References something the base CTE's output won't expose
                # (e.g. a raw joined-table column) — moving it to the
                # wrapper would just trade one dangling reference for another.
                independent.append(cc)
            else:
                dependent.append(cc)

        if not dependent:
            new_ctes.append(cte)
            continue

        dependent_alias_names = {cc.alias.upper() for cc in dependent}
        touches_dependent_elsewhere = (
            any(f.column.upper() in dependent_alias_names for f in cte.filters)
            or any(gb.upper() in dependent_alias_names for gb in cte.group_by if "." not in gb)
            or any(
                any(ident.upper() in dependent_alias_names for ident in _extract_bare_identifiers(gf.expression))
                for gf in cte.geo_filters
            )
            or any(
                _IDENTIFIER_RE.match(ob.expr.strip()) and ob.expr.strip().upper() in dependent_alias_names
                for ob in cte.order_by
            )
        )
        if touches_dependent_elsewhere:
            new_ctes.append(cte)
            continue

        base_name = f"{cte.name}__base"
        base_cte = cte.model_copy(update={"name": base_name, "computed_columns": independent})

        passthrough_aliases: list[str] = []
        for gb in cte.group_by:
            passthrough_aliases.append(gb.rsplit(".", 1)[-1])
        for a in cte.aggregations:
            passthrough_aliases.append(a.alias)
        for cc in independent:
            passthrough_aliases.append(cc.alias)

        wrapper_cte = PlanCTE(
            name=cte.name,
            description=cte.description + " (auto-split: resolves same-step computed-column dependency)",
            selected_tables=[base_name],
            computed_columns=[cc.model_copy(update={"table": base_name}) for cc in dependent],
            group_by=passthrough_aliases,
        )

        log.info(
            "Split CTE '%s' into '%s' (base) + '%s' (wrapper) — %d computed_column(s) "
            "reference a sibling alias from the same step.",
            cte.name, base_name, cte.name, len(dependent),
        )
        new_ctes.append(base_cte)
        new_ctes.append(wrapper_cte)

    plan.ctes = new_ctes
    return plan


def autofix_cte_scope(plan: QueryPlan) -> QueryPlan:
    """Thread missing upstream-CTE columns through as passthrough SELECT items.

    A multi-step plan can reference a column of an earlier CTE — in a later
    CTE's join, filter, aggregation, computed_column, group_by, or order_by
    (either "cte.column" or a bare same-block reference) — that the earlier
    CTE never actually exposed. This is a genuine plan-authoring gap (the
    LLM's multi-step plan forgot to carry a column its own later steps
    depend on), not something the compiler can correctly guess in general.
    It can also happen transitively through a wildcard passthrough (a CTE
    with no explicit SELECT list falls back to "SELECT primary_table.*", so
    whatever it's missing is really missing from wherever THAT chain
    bottoms out).

    Where the missing column's source table can be confidently inferred
    (because it's already referenced elsewhere within the CTE that should
    expose it, e.g. inside another computed_column's expression, or because
    that CTE reads from exactly one source), it's added as a passthrough
    entry to that CTE's group_by. Cases that can't be confidently attributed
    to a table are left as-is — they'll surface as an execution error, same
    as today, rather than risk a silent wrong fix.

    Operates on and returns a deep copy; the original plan object (e.g. for
    logging the LLM's raw output) is left untouched.
    """
    if not plan.ctes:
        return plan

    plan = plan.model_copy(deep=True)
    cte_by_name = {cte.name: cte for cte in plan.ctes}
    consumers = list(plan.ctes) + [plan]

    def _pass(warn: bool) -> bool:
        changed = False

        def _check(start, column: str) -> None:
            nonlocal changed
            if start is None:
                return
            owner, exposed = _self_exposure(start, plan, cte_by_name)
            if exposed is None or column.upper() in exposed:
                return
            source_table = _find_source_table_for_column(owner, column.upper(), cte_by_name)
            if source_table is None:
                if warn:
                    log.warning(
                        "CTE scope: '%s' is referenced from a later step "
                        "but its source table could not be inferred — "
                        "left as-is.",
                        column,
                    )
                return
            passthrough = f"{source_table}.{column}"
            if passthrough not in owner.group_by:
                owner.group_by.append(passthrough)
                changed = True

        for consumer in consumers:
            for table, column in _consumer_references(consumer):
                _check(cte_by_name.get(table), column)

            for ob in consumer.order_by:
                expr = ob.expr.strip()
                qualified = _QUALIFIED_IDENTIFIER_RE.match(expr)
                if qualified:
                    _check(cte_by_name.get(qualified.group(1)), qualified.group(2))
                elif _IDENTIFIER_RE.match(expr):
                    _check(consumer, expr)
                # else: a genuine expression, not a plain column reference — skip

        return changed

    # Fixing one CTE's exposure can itself surface a further-upstream gap
    # (a chain of CTEs), so repeat until a pass makes no further progress.
    # Bounded by the number of CTEs — each pass can only add columns, never
    # remove them, so this always terminates.
    for _ in range(len(plan.ctes)):
        if not _pass(warn=False):
            break
    _pass(warn=True)  # final pass: report anything still unresolved

    return plan


def compile_plan(plan: QueryPlan, schema_slice: SchemaSlice | None = None) -> str:
    """Compile a QueryPlan into a Snowflake SQL string.

    Supports LATERAL FLATTEN for VARIANT ARRAYs and multi-step CTEs.
    After compilation, applies date-shard rewriting for partitioned tables.
    """
    if not plan.selected_tables and not plan.ctes:
        return "SELECT 1"

    plan = split_self_referencing_computed_columns(plan)
    plan = autofix_cte_scope(plan)
    cte_by_name: dict[str, PlanCTE] = {cte.name: cte for cte in plan.ctes}

    # Build alias map: qualified_name -> t1, t2, ...
    alias_map: dict[str, str] = {}
    for i, tname in enumerate(plan.selected_tables):
        alias_map[tname] = _alias(i)

    # Build column case map from SchemaSlice (for original casing)
    case_map = _build_column_case_map(schema_slice)

    def _finalize(sql: str) -> str:
        return rewrite_date_sharded_tables(sql, schema_slice)

    # ── CTE-based compilation ───────────────────────────────────────────
    if plan.ctes:
        cte_parts: list[str] = []
        defined_cte_names: set[str] = set()
        for cte in plan.ctes:
            # Build local alias map for tables referenced inside this CTE.
            # CTE source can be an upstream CTE name (no alias needed for those)
            # or a real table from selected_tables (use existing alias).
            cte_alias_map = dict(alias_map)
            for tbl in cte.selected_tables:
                if tbl in defined_cte_names:
                    # Upstream CTE name — reference directly
                    cte_alias_map[tbl] = tbl
                elif tbl not in cte_alias_map:
                    # Real table not yet aliased — assign a fresh alias
                    cte_alias_map[tbl] = _alias(len(cte_alias_map))

            # Also add geo_join right tables to the alias map
            for gj in cte.geo_joins:
                if gj.right_table not in cte_alias_map:
                    idx = len(cte_alias_map)
                    cte_alias_map[gj.right_table] = _alias(idx)

            block = _compile_single_block(
                selected_tables=cte.selected_tables,
                joins=cte.joins,
                geo_joins=cte.geo_joins,
                flatten_ops=cte.flatten_ops,
                filters=cte.filters,
                geo_filters=cte.geo_filters,
                group_by=cte.group_by,
                aggregations=cte.aggregations,
                order_by=cte.order_by,
                limit=cte.limit,
                alias_map=cte_alias_map,
                case_map=case_map,
                computed_columns=cte.computed_columns,
                plan=plan,
                cte_by_name=cte_by_name,
            )
            cte_parts.append(f"{cte.name} AS (\n{block}\n)")
            defined_cte_names.add(cte.name)

        # Final SELECT: use the last CTE as source by default
        last_cte = plan.ctes[-1].name

        # Top-level filters/aggregations/group_by only apply if they reference
        # the last CTE's output — the final SELECT's FROM only exposes that
        # CTE, so a reference to any other (pre-CTE) table is out of scope.
        # This is almost always a duplicate of a filter already applied inside
        # the CTE, so dropping it here doesn't drop a constraint, just a stale
        # out-of-scope repeat of one already enforced.
        top_level_filters = [f for f in plan.filters if f.table == last_cte]
        top_level_aggregations = [a for a in plan.aggregations if a.table == last_cte]
        top_level_computed_columns = [c for c in plan.computed_columns if c.table == last_cte]
        top_level_group_by = [
            gb for gb in plan.group_by
            if "." not in gb or gb.rsplit(".", 1)[0] == last_cte
        ]

        # If there are top-level aggregations/group_by/filters, build a final
        # SELECT from the last CTE. Otherwise just SELECT * FROM last_cte.
        if (top_level_aggregations or top_level_group_by or top_level_filters
                or top_level_computed_columns or plan.geo_filters):
            final_alias_map = {last_cte: last_cte}
            final_block = _compile_single_block(
                selected_tables=[last_cte],
                joins=[],
                geo_joins=[],
                flatten_ops=[],
                filters=top_level_filters,
                geo_filters=plan.geo_filters,
                group_by=top_level_group_by,
                aggregations=top_level_aggregations,
                order_by=plan.order_by,
                limit=plan.limit,
                alias_map=final_alias_map,
                case_map=None,
                computed_columns=top_level_computed_columns,
                plan=plan,
                cte_by_name=cte_by_name,
            )
        else:
            final_parts = [f"SELECT *\nFROM {last_cte}"]
            if plan.order_by:
                final_alias_map = {last_cte: last_cte}
                final_parts.append(f"ORDER BY {_format_order_by(plan.order_by, final_alias_map, None)}")
            if plan.limit is not None:
                final_parts.append(f"LIMIT {plan.limit}")
            final_block = "\n".join(final_parts)

        return _finalize("WITH " + ",\n".join(cte_parts) + "\n" + final_block)

    # Add geo_join right tables to alias_map
    for gj in plan.geo_joins:
        if gj.right_table not in alias_map:
            alias_map[gj.right_table] = _alias(len(alias_map))

    # ── Single-block compilation (no CTEs) ──────────────────────────────
    return _finalize(_compile_single_block(
        selected_tables=plan.selected_tables,
        joins=plan.joins,
        geo_joins=plan.geo_joins,
        flatten_ops=plan.flatten_ops,
        filters=plan.filters,
        geo_filters=plan.geo_filters,
        group_by=plan.group_by,
        aggregations=plan.aggregations,
        order_by=plan.order_by,
        limit=plan.limit,
        alias_map=alias_map,
        case_map=case_map,
        computed_columns=plan.computed_columns,
        plan=plan,
        cte_by_name=cte_by_name,
    ))


def _try_resolve(table_part: str, col_part: str, alias_map: dict[str, str]) -> str:
    """Resolve table_part.col_part using alias_map, trying exact then suffix match."""
    if table_part in alias_map:
        return f'{alias_map[table_part]}."{col_part}"'
    # Try suffix match (e.g. "ORDERS" matching "DB.SCHEMA.ORDERS")
    for full_name, alias in alias_map.items():
        if full_name.endswith(f".{table_part}") or full_name == table_part:
            return f'{alias}."{col_part}"'
    return f'{table_part}."{col_part}"'
