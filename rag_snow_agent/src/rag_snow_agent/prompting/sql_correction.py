"""Deterministic correction layer for raw (non-plan) LLM-generated SQL.

Unlike constraints.validate_sql (regex-based, assumes the deterministic
compiler's exact double-quoting convention), this parses SQL with sqlglot so
it works regardless of the LLM's quoting/casing style. Scope is deliberately
the same as the plan-based validators: schema-driven, unambiguous checks only
(does this column/table exist, does this type look wrong) — never query
logic/intent, which isn't safe to auto-correct.
"""

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as sqlglot_exp
from sqlglot.optimizer.scope import traverse_scope

from ..retrieval.schema_slice import SchemaSlice
from .constraints import ValidationResult

_NUMERIC_TYPE_PREFIXES = (
    "NUMBER", "INT", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL",
)
_DATE_SHAPED_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$")


def _build_lookup(schema_slice: SchemaSlice) -> tuple[dict[str, set[str]], dict[str, dict[str, str]]]:
    """Return ({TABLE_UPPER: {COLUMN_UPPER, ...}}, {TABLE_UPPER: {COLUMN_UPPER: data_type}})."""
    cols: dict[str, set[str]] = {}
    types: dict[str, dict[str, str]] = {}
    for ts in schema_slice.tables:
        qn = ts.qualified_name.upper()
        cols[qn] = {c.name.upper() for c in ts.columns}
        types[qn] = {c.name.upper(): c.data_type for c in ts.columns}
    return cols, types


def _real_table_qualified_name(table_node: sqlglot_exp.Table) -> str:
    parts = [p for p in (table_node.catalog, table_node.db, table_node.name) if p]
    return ".".join(parts).upper()


def _scope_used_aliases(scope) -> set[str]:
    """Aliases actually referenced in *this* SELECT's own FROM/JOIN clause.

    scope.sources (from sqlglot's Scope) lists every name reachable from this
    point in the query — including sibling CTEs this particular SELECT never
    actually references in its FROM — so it can't be used alone to tell
    "used exactly one real table" from "one real table plus two other CTEs
    that happen to exist somewhere in the WITH clause." Only args["from_"]/
    ["joins"] reflect what this SELECT's own FROM actually names.
    """
    from_node = scope.expression.args.get("from_")
    joins = scope.expression.args.get("joins") or []
    tables = list(from_node.find_all(sqlglot_exp.Table)) if from_node else []
    for j in joins:
        tables.extend(j.find_all(sqlglot_exp.Table))
    return {(t.alias_or_name or t.name).upper() for t in tables}


def _scope_real_sources(scope, table_cols: dict[str, set[str]]) -> dict[str, str]:
    """Map alias (UPPER) -> qualified real table name (UPPER), for sources
    actually named in *scope*'s own FROM/JOIN that resolve to a real schema
    table rather than a CTE/subquery/derived table. sqlglot's Scope already
    distinguishes real-table vs. CTE/subquery sources (a CTE/subquery source
    is another Scope object; a real table is an exp.Table) — this needs no
    name-based guessing, just scoping scope.sources down to what's used here.
    """
    used = _scope_used_aliases(scope)
    real: dict[str, str] = {}
    for alias, source in scope.sources.items():
        if alias.upper() not in used:
            continue
        if isinstance(source, sqlglot_exp.Table):
            qualified = _real_table_qualified_name(source)
            if qualified in table_cols:
                real[alias.upper()] = qualified
    return real


def validate_raw_sql(sql: str, schema_slice: SchemaSlice) -> ValidationResult:
    """Check that *sql* (arbitrary LLM-authored SQL, any quoting/casing style)
    references only real tables/columns in *schema_slice*.

    Scope-aware via sqlglot's optimizer.scope.traverse_scope: each SELECT
    (including inside CTEs/subqueries/UNION branches) is checked against only
    its OWN FROM/JOIN sources, not the whole query — a bare column in an outer
    SELECT that's actually a CTE-defined alias (e.g. an output column of an
    upstream CTE) is correctly left unchecked rather than mistaken for a real
    table's column just because that real table happens to be the only one
    used anywhere else in the query.

    Conservative by construction: a source that isn't a real schema table
    (CTE, subquery, LATERAL FLATTEN alias) is never flagged — only a
    qualified reference to a known real table, or a bare column when that
    scope's FROM/JOIN resolves to exactly one real table with nothing else
    mixed in, is checked.
    """
    result = ValidationResult()
    try:
        tree = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception as exc:
        result.warnings.append(f"SQL did not parse: {exc}")
        return result

    table_cols, _ = _build_lookup(schema_slice)

    try:
        scopes = traverse_scope(tree)
    except Exception as exc:
        result.warnings.append(f"Scope resolution failed: {exc}")
        return result

    for scope in scopes:
        real_sources = _scope_real_sources(scope, table_cols)
        # Only a scope whose FROM/JOIN actually names a single source, and
        # that source is a real table, gives an unambiguous home for a bare
        # column — _scope_used_aliases (not scope.sources, which also lists
        # every sibling CTE reachable from here whether or not this SELECT
        # uses it) is what tells us "actually named", not just "reachable".
        used = _scope_used_aliases(scope)
        single_real_table = (
            next(iter(real_sources.values())) if len(used) == 1 and len(real_sources) == 1 else None
        )
        for col in scope.columns:
            if col.table:
                qualified = real_sources.get(col.table.upper())
                if qualified is None:
                    continue  # CTE/subquery/unknown alias — not ours to validate
                if col.name.upper() not in table_cols[qualified]:
                    result.valid = False
                    result.errors.append(
                        f"Column '{col.table}.{col.name}' not found on {qualified} "
                        f"(known columns: {sorted(table_cols[qualified])[:20]})"
                    )
            elif single_real_table is not None:
                if col.name.upper() not in table_cols[single_real_table]:
                    result.valid = False
                    result.errors.append(
                        f"Column '{col.name}' not found on {single_real_table} "
                        f"(known columns: {sorted(table_cols[single_real_table])[:20]})"
                    )

    return result


def check_type_mismatches_raw(sql: str, schema_slice: SchemaSlice) -> list[str]:
    """Flag filter comparisons between a NUMBER-family column and a
    date-shaped string literal — see constraints._check_filter_value_type for
    the plan-based equivalent and why this stays narrow (NUMBER-vs-date-string
    is the concrete, high-confidence failure mode; DATE/TIMESTAMP/VARCHAR
    columns have too many legitimate literal shapes to check safely).
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception:
        return []

    cols_lookup, table_types = _build_lookup(schema_slice)
    errors: list[str] = []

    try:
        scopes = traverse_scope(tree)
    except Exception:
        return []

    # scope.expression.find_all(...) also walks into nested subquery scopes
    # (sqlglot's Scope doesn't expose a "this scope's WHERE/ON only" view the
    # way it does for .columns), so the same comparison can be visited under
    # more than one scope's real_sources — harmless here since it can only
    # ever produce a duplicate of a correct finding, deduped below, never a
    # wrong one: a comparison only matches at all when its qualifier resolves
    # to a real table in that scope's OWN sources.
    comparison_classes = (sqlglot_exp.EQ, sqlglot_exp.NEQ, sqlglot_exp.LT, sqlglot_exp.GT, sqlglot_exp.LTE, sqlglot_exp.GTE)
    for scope in scopes:
        real_sources = _scope_real_sources(scope, cols_lookup)
        for cmp in scope.expression.find_all(*comparison_classes):
            left, right = cmp.this, cmp.expression
            col, lit = None, None
            if isinstance(left, sqlglot_exp.Column) and isinstance(right, sqlglot_exp.Literal) and right.is_string:
                col, lit = left, right
            elif isinstance(right, sqlglot_exp.Column) and isinstance(left, sqlglot_exp.Literal) and left.is_string:
                col, lit = right, left
            if col is None or not col.table:
                continue
            qualified = real_sources.get(col.table.upper())
            if not qualified or qualified not in table_types:
                continue
            col_type = table_types[qualified].get(col.name.upper())
            if not col_type:
                continue
            type_upper = col_type.upper()
            if not any(type_upper.startswith(p) for p in _NUMERIC_TYPE_PREFIXES):
                continue
            value = lit.this.strip()
            if _DATE_SHAPED_RE.match(value):
                errors.append(
                    f"{qualified}.{col.name}: type {col_type} but compared to "
                    f"date-shaped value '{value}' — check if this column is "
                    f"epoch/YYYYMMDD-encoded and needs the literal converted to "
                    f"match, rather than compared directly."
                )
    return list(dict.fromkeys(errors))  # dedupe, preserve order


_FIX_SQL_SYSTEM = """\
You are a Snowflake SQL expert.
The previous SQL query has one or more problems: invalid column/table \
references, or a filter comparing a NUMBER column to a date-shaped string \
literal (check whether that column is epoch/YYYYMMDD-encoded and needs the \
literal converted to match, rather than compared directly).
Fix the SQL to resolve every error listed below.
Return ONLY the corrected SQL statement. No markdown, no explanation.\
"""

_FIX_SQL_USER = """\
Schema:
{schema_text}

Previous SQL:
{sql}

Validation errors:
{errors}

Return the corrected SQL query.\
"""


def build_fix_sql_prompt(sql: str, schema_text: str, errors: list[str]) -> list[dict[str, str]]:
    """Return messages for fixing raw SQL that failed validate_raw_sql /
    check_type_mismatches_raw."""
    return [
        {"role": "system", "content": _FIX_SQL_SYSTEM},
        {
            "role": "user",
            "content": _FIX_SQL_USER.format(
                schema_text=schema_text, sql=sql, errors="\n".join(f"- {e}" for e in errors),
            ),
        },
    ]
