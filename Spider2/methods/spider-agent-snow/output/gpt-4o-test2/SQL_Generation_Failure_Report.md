# Spider2 Test2 SQL Generation Failure Analysis

## Scope

- Run analyzed: `gpt-4o-test2`
- Source artifacts: per-case traces in `sf_bq*/spider/result.json` and run summary in `token_usage_summary.json`
- Cases attempted: 25
- Cases finished: 23
- Cases failed: 2
- Report date: 2026-03-24

## Executive Summary

The dominant quality issue is SQL compilation failure, primarily caused by identifier/schema mismatches and dialect misuse. Overall run accuracy is low (`0.12`), two cases failed after exhausting the 20-step budget, and multiple finished cases required recovery from repeated SQL errors. The most frequent failure mode is invalid identifiers, indicating brittle schema grounding and inconsistent quoting/field access in generated SQL.

## Run-Level Metrics

From `token_usage_summary.json`:

| Metric | Value |
|---|---:|
| attempted_cases | 25 |
| finished_cases | 23 |
| failed_cases | 2 |
| accuracy | 0.12 |
| llm_calls | 271 |
| total_tokens | 3,901,315 |

## Failure Taxonomy (Primary Buckets)

| Failure bucket | Cases | Rate | Notes |
|---|---:|---:|---|
| SQL compilation error (broad) | 15 | 60% | Umbrella bucket covering identifier, function, typing, and qualification issues |
| Invalid identifier / wrong alias / wrong field path | 9 | 36% | Most common root cause |
| Wrong DB/schema/table qualification or authorization | 2 | 8% | Object resolution errors |
| Missing current database context | 2 | 8% | Session context not set before query |
| SQL syntax error | 2 | 8% | Malformed expressions/casts/path access |
| Group-by/window misuse | 1 | 4% | Invalid aggregation/window combination |
| Step-limit failure (unfinished at 20 steps) | 2 | 8% | Hard failures: `sf_bq008`, `sf_bq099` |

## SQL Compilation Subtypes (Deeper Breakdown)

| Subtype | Cases | Case IDs |
|---|---:|---|
| invalid_identifier | 9 | sf_bq004, sf_bq008, sf_bq099, sf_bq209, sf_bq211, sf_bq213, sf_bq214, sf_bq270, sf_bq275 |
| unknown_function | 4 | sf_bq026, sf_bq033, sf_bq091, sf_bq209 |
| unsupported_subquery | 2 | sf_bq011, sf_bq209 |
| type_signature_mismatch | 2 | sf_bq026, sf_bq268 |
| db_schema_table_not_found | 2 | sf_bq001, sf_bq008 |
| missing_current_database | 2 | sf_bq001, sf_bq212 |
| sql_syntax_error | 2 | sf_bq008, sf_bq099 |
| group_by_window_misuse | 1 | sf_bq008 |

## Representative Evidence

### 1) Identifier and field-path mismatch

- `sf_bq004`: `invalid identifier '"productName"'`
- `sf_bq270`: `invalid identifier '"hits"."eCommerceAction"."action_type"'`
- `sf_bq213`: `invalid identifier 'IPC_U.CODE'`

### 2) Qualification/context errors

- `sf_bq001`: `Database 'GOOGLE_ANALYTICS_SAMPLE' does not exist or not authorized.`
- `sf_bq001`, `sf_bq212`: `This session does not have a current database.`

### 3) Syntax and query-structure errors

- `sf_bq008`: `syntax error ... unexpected ''campaign''`
- `sf_bq099`: `syntax error ... unexpected '0.'` and `unexpected '::'`
- `sf_bq008`: `not a valid group by expression`

### 4) Dialect/function incompatibility

- `sf_bq033`: `Unknown function ARRAY_EXISTS`
- `sf_bq091`: `Unknown function FLATTEN`
- `sf_bq011`: `Unsupported subquery type cannot be evaluated`
- `sf_bq026`, `sf_bq268`: `Invalid argument types for function ...`

## Cases With Hard Failure

| Case ID | Finished | Steps | Dominant issues |
|---|---|---:|---|
| sf_bq008 | No | 20 | table/db qualification, invalid identifiers, syntax errors, group-by misuse |
| sf_bq099 | No | 20 | invalid identifiers, syntax errors, repeated compile failures |

## Accuracy Impact Interpretation

1. Hard failures (2 unfinished cases) are only part of the total misses.
2. The `0.12` accuracy indicates most misses occur even among finished cases, consistent with repeated compile/recovery loops ending in incorrect final SQL.
3. The concentration of errors in identifier/dialect classes suggests model output is often semantically close but not execution-safe under Snowflake constraints.

## Per-Case Issue Map (Only Cases With Detected Issues)

- `sf_bq001`: missing_current_database, schema_discovery_query_attempt, sql_compilation_error, wrong_db_schema_or_qualification
- `sf_bq004`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq008`: group_by_window_misuse, invalid_identifier_column_or_alias, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq011`: sql_compilation_error
- `sf_bq026`: sql_compilation_error
- `sf_bq033`: sql_compilation_error
- `sf_bq091`: sql_compilation_error
- `sf_bq099`: invalid_identifier_column_or_alias, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit
- `sf_bq209`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq211`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq212`: missing_current_database
- `sf_bq213`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq214`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq268`: sql_compilation_error
- `sf_bq270`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq275`: invalid_identifier_column_or_alias, sql_compilation_error

## Notes on Method

- Analysis performed by parsing all `sf_bq*/spider/result.json` files.
- Pattern classes were assigned from observed error strings in trajectory observations.
- Counts are occurrence-by-case (a case contributes once per bucket).
