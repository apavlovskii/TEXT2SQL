# Spider2 Test1 SQL Generation Failure Analysis

## Scope

- Run analyzed: `gpt-4o-mini-test1`
- Source artifacts: per-case traces in `sf_bq*/spider/result.json`, run summary in `token_usage_summary.json`, and evaluator correct-ID output `spider2-snow/evaluation_suite/gpt-4o-mini-test1.csv`
- Cases attempted: 25
- Cases finished: 7
- Cases failed: 18
- Report date: 2026-03-24

## Executive Summary

This run shows severe execution instability: only 7/25 cases finished, and evaluator-correct accuracy is `0.00` (0 correct out of 25). The run-completion accuracy from token summary is `0.28` (7/25), but no case appears in the evaluator correct-ID list. The dominant failure mode is SQL compilation error, with strong concentration in database/schema qualification failures and missing current database context. A large fraction of cases exhausted the 20-step budget, indicating repeated recovery loops without converging to executable SQL.

## Run-Level Metrics

From `token_usage_summary.json` and evaluator output CSV:

| Metric | Value |
|---|---:|
| attempted_cases | 25 |
| finished_cases | 7 |
| failed_cases | 18 |
| run_completion_accuracy (`finished/attempted`) | 0.28 |
| evaluator_correct_examples | 0 |
| evaluator_total_examples | 25 |
| evaluator_correct_accuracy (`correct/total`) | 0.00 |
| llm_calls | 450 |
| total_tokens | 7,221,373 |

## Failure Taxonomy (Primary Buckets)

| Failure bucket | Cases | Rate | Notes |
|---|---:|---:|---|
| SQL compilation error (broad) | 20 | 80% | Umbrella bucket covering identifier, syntax, and qualification issues |
| Missing current database context | 14 | 56% | Session context not set before query execution |
| Wrong DB/schema/table qualification or authorization | 13 | 52% | Object resolution and qualification failures |
| SQL syntax error | 7 | 28% | Malformed SQL and invalid tokenization |
| Invalid identifier / wrong alias / wrong field path | 6 | 24% | Incorrect column/field references and aliasing |
| Group-by/window misuse | 1 | 4% | Invalid aggregation expression |
| Step-limit failure (unfinished at 20 steps) | 18 | 72% | Hard failures with no converged executable result |

## SQL Compilation Subtypes (Deeper Breakdown)

| Subtype | Cases | Case IDs |
|---|---:|---|
| invalid_identifier | 6 | sf_bq003, sf_bq004, sf_bq008, sf_bq009, sf_bq010, sf_bq268 |
| unknown_function | 0 | — |
| unsupported_subquery | 0 | — |
| type_signature_mismatch | 1 | sf_bq268 |
| db_schema_table_not_found | 13 | sf_bq001, sf_bq004, sf_bq026, sf_bq027, sf_bq029, sf_bq099, sf_bq209, sf_bq210, sf_bq211, sf_bq213, sf_bq269, sf_bq270, sf_bq275 |
| missing_current_database | 14 | sf_bq001, sf_bq002, sf_bq004, sf_bq008, sf_bq009, sf_bq029, sf_bq091, sf_bq099, sf_bq210, sf_bq213, sf_bq214, sf_bq270, sf_bq275, sf_bq374 |
| sql_syntax_error | 7 | sf_bq010, sf_bq099, sf_bq210, sf_bq212, sf_bq213, sf_bq214, sf_bq268 |
| group_by_window_misuse | 1 | sf_bq269 |

## Representative Evidence

### 1) Qualification/context errors

- `sf_bq001`: `Object 'GA_SESSIONS_20170201' does not exist or not authorized.`
- `sf_bq029`: `Object 'PUBLICATIONS' does not exist or not authorized.`
- `sf_bq210`: `Object 'PATENTS' does not exist or not authorized.`
- `sf_bq001`, `sf_bq002`: `This session does not have a current database. Call 'USE DATABASE', or use a qualified name.`

### 2) Identifier mismatch

- `sf_bq003`: `invalid identifier 'DATE'`
- `sf_bq008`: `invalid identifier 'HITS.PAGE.PAGEPATH'`
- `sf_bq009`: `invalid identifier '"trafficSource"."source"'`

### 3) Syntax and query-structure errors

- `sf_bq212`: `syntax error line 6 at position 11 unexpected '-'.`
- `sf_bq213`: `syntax error line 5 at position 11 unexpected '-'.`
- `sf_bq268`: `syntax error line 1 at position 169 unexpected ''mobile''.`
- `sf_bq269`: `[SESSION_TYPE] is not a valid group by expression`

## Cases With Hard Failure

| Case ID | Finished | Steps | Dominant issues |
|---|---|---:|---|
| sf_bq001 | No | 20 | missing database context, wrong object qualification, repeated SQL compilation errors |
| sf_bq002 | No | 20 | missing database context |
| sf_bq004 | No | 20 | invalid identifiers, missing database context, wrong object qualification |
| sf_bq008 | No | 20 | invalid identifiers, missing database context, repeated SQL compilation errors |
| sf_bq010 | No | 20 | invalid identifiers, SQL syntax errors |
| sf_bq011 | No | 20 | step-limit exhaustion (no converged executable SQL) |
| sf_bq029 | No | 20 | missing database context, wrong object qualification |
| sf_bq033 | No | 20 | step-limit exhaustion (no converged executable SQL) |
| sf_bq091 | No | 20 | missing database context |
| sf_bq099 | No | 20 | missing database context, wrong object qualification, SQL syntax errors |
| sf_bq210 | No | 20 | missing database context, wrong object qualification, SQL syntax errors |
| sf_bq212 | No | 20 | SQL syntax errors, repeated SQL compilation errors |
| sf_bq213 | No | 20 | missing database context, wrong object qualification, SQL syntax errors |
| sf_bq214 | No | 20 | missing database context, SQL syntax errors |
| sf_bq269 | No | 20 | wrong object qualification, group-by expression error |
| sf_bq270 | No | 20 | missing database context, wrong object qualification |
| sf_bq275 | No | 20 | missing database context, wrong object qualification |
| sf_bq374 | No | 20 | missing database context |

## Accuracy Impact Interpretation

1. Run-completion accuracy (`0.28`) overstates end quality because it counts task completion, not evaluator correctness.
2. Evaluator-correct accuracy is `0.00` (0/25), indicating no final prediction matched gold under exec-result evaluation.
3. The 18 unfinished cases (72%) plus repeated qualification/context and syntax failures in the remaining cases explain why completed traces did not convert into correct outcomes.

## Per-Case Issue Map (Only Cases With Detected Issues)

- `sf_bq001`: missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq002`: missing_current_database, unfinished_timeout_or_step_limit
- `sf_bq003`: invalid_identifier_column_or_alias, sql_compilation_error
- `sf_bq004`: invalid_identifier_column_or_alias, missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq008`: invalid_identifier_column_or_alias, missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit
- `sf_bq009`: invalid_identifier_column_or_alias, missing_current_database, sql_compilation_error
- `sf_bq010`: invalid_identifier_column_or_alias, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit
- `sf_bq011`: unfinished_timeout_or_step_limit
- `sf_bq026`: sql_compilation_error, wrong_db_schema_or_qualification
- `sf_bq027`: sql_compilation_error, wrong_db_schema_or_qualification
- `sf_bq029`: missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq033`: unfinished_timeout_or_step_limit
- `sf_bq091`: missing_current_database, unfinished_timeout_or_step_limit
- `sf_bq099`: missing_current_database, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq209`: sql_compilation_error, wrong_db_schema_or_qualification
- `sf_bq210`: missing_current_database, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq211`: sql_compilation_error, wrong_db_schema_or_qualification
- `sf_bq212`: sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit
- `sf_bq213`: missing_current_database, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq214`: missing_current_database, sql_compilation_error, sql_syntax_error, unfinished_timeout_or_step_limit
- `sf_bq268`: invalid_identifier_column_or_alias, sql_compilation_error, sql_syntax_error
- `sf_bq269`: group_by_window_misuse, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq270`: missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq275`: missing_current_database, sql_compilation_error, unfinished_timeout_or_step_limit, wrong_db_schema_or_qualification
- `sf_bq374`: missing_current_database, unfinished_timeout_or_step_limit

## Notes on Method

- Analysis performed by parsing all `sf_bq*/spider/result.json` files under this run.
- Correct-accuracy values are taken from `spider2-snow/evaluation_suite/gpt-4o-mini-test1.csv` (header-only file implies 0 correct IDs).
- Pattern classes were assigned from observed error strings in trajectory observations.
- Counts are occurrence-by-case (a case contributes once per bucket).