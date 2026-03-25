# DSR Lite Accuracy, Token Usage, and Error Analysis Report

## Scope

- Run directory: `DSR-SQL/DSR_Lite/logs/run_snow_2026-03-21_164927`
- Run mode: Snowflake (`evaluation_mode = sql`)
- Primary sources:
  - `final_output_score.json`
  - `outcome/token_usage_summary.json`
  - `sql-ids.csv`
  - `outcome/*_result.json`
  - `log/*/main_*.log`
  - `log/*/status_*.jsonl`
- Report date: 2026-03-25

## Accuracy Summary

From `final_output_score.json` and `sql-ids.csv`:

| Metric | Value |
|---|---:|
| attempted_examples (evaluated subset) | 23 |
| correct_examples | 0 |
| final_score | 0.00 |
| real_correct_examples | 0 |
| real_total_examples | 547 |
| real_score | 0.00 |

`sql-ids.csv` contains only the header (`instance_id`) and no correct IDs.

## Token Usage Summary

From `outcome/token_usage_summary.json`:

| Metric | Value |
|---|---:|
| total_input_tokens | 3,936,321 |
| total_output_tokens | 274,534 |
| total_tokens | 4,210,855 |

Derived efficiency (23 attempted examples):

- tokens per attempted example: `183,080.65`
- tokens per correct example: not defined (0 correct)

## Coverage and Workload Profile

Per-case outputs available: `23` (`outcome/*_result.json`)

- Step counter min/max/avg: `1 / 20 / 13.30`
- Median step count: `20`
- Cases at step cap (`20`): `12 / 23`

Database distribution of attempted cases:

| Database | Cases |
|---|---:|
| `GITHUB_REPOS` | 15 |
| `ETHEREUM_BLOCKCHAIN` | 3 |
| `GEO_OPENSTREETMAP` | 3 |
| `BRAZE_USER_EVENT_DEMO_DATASET` | 2 |

## Highest-Token Testcases

From `outcome/*_result.json` token usage:

| Instance ID | Total Tokens | Step Counter | DB |
|---|---:|---:|---|
| `sf_bq193` | 881,744 | 20 | `ETHEREUM_BLOCKCHAIN` |
| `sf_bq194` | 617,359 | 20 | `ETHEREUM_BLOCKCHAIN` |
| `sf_bq377` | 586,075 | 20 | `GITHUB_REPOS` |
| `sf_bq225` | 314,259 | 13 | `GITHUB_REPOS` |
| `sf_bq233` | 213,106 | 20 | `GEO_OPENSTREETMAP` |
| `sf_bq187` | 212,120 | 20 | `ETHEREUM_BLOCKCHAIN` |
| `sf_bq100` | 188,907 | 19 | `GITHUB_REPOS` |
| `sf_bq180` | 148,779 | 20 | `GITHUB_REPOS` |

## Deep Log Analysis (Typical Errors)

Log coverage:

- testcase log folders scanned: `23`
- `status_*.jsonl` files scanned: `23`
- `main_*.log` files scanned: `23`
- cases with explicit triggering errors (`status.triggering_error`): `9`

Cases with explicit triggering errors:

- `sf_bq017_1`, `sf_bq036_1`, `sf_bq100_1`, `sf_bq131_1`, `sf_bq187_1`, `sf_bq194_1`, `sf_bq233_1`, `sf_bq248_1`, `sf_bq450_1`

### Typical Error Categories

Error events below are counted from `triggering_error` entries across status logs (multiple events per case possible).

| Error category | Events | Cases | Typical signature |
|---|---:|---:|---|
| object_not_found_or_not_authorized | 23 | 5 | `Object '... ... ...' does not exist or not authorized` |
| invalid_identifier | 19 | 6 | `invalid identifier '...` |
| sql_syntax_error | 7 | 5 | `syntax error line ... unexpected ...` |
| other_execution_error | 19 | 6 | aggregation/shape failures (e.g., nested aggregate, non-scalar expectations) |

Representative frequent signatures:

- `Object 'GITHUB_REPOS.GITHUB_REPOS.SAMPLE_CONTENTS_20260101' does not exist or not authorized`
- `Object 'ETHEREUM_BLOCKCHAIN.ETHEREUM_BLOCKCHAIN.TOKEN_TRANSFERS_20260101' does not exist or not authorized`
- `invalid identifier '"repo_name"'`
- `invalid identifier 'F.PATH'`
- `syntax error ... unexpected 'SELECT'`
- `Invalid argument types for function 'ST_DWITHIN'`
- `Numeric value '...' is not recognized`

### Repair/Recovery Behavior

From `main_*.log` markers (`Repair Failed`, `[Error occurred]`, and later `SQL Execution Successful`):

- cases with observed in-log recovery after an error: `8`
  - `sf_bq017_1`, `sf_bq036_1`, `sf_bq100_1`, `sf_bq187_1`, `sf_bq194_1`, `sf_bq233_1`, `sf_bq248_1`, `sf_bq450_1`
- error case without observed in-log recovery: `1`
  - `sf_bq131_1`
- cases hitting `Maximum Repair Attempts Exceeded`: `7`
  - `sf_bq017_1`, `sf_bq100_1`, `sf_bq131_1`, `sf_bq187_1`, `sf_bq194_1`, `sf_bq233_1`, `sf_bq248_1`

## What Steps Helped Improve Queries (Observed Patterns)

Pattern presence is derived from status/main logs (SQL revisions + reasoning traces), and “recovered” indicates the pattern appeared in cases with in-log post-error success.

| Improvement pattern | Cases using pattern | Recovered cases with pattern |
|---|---:|---:|
| CTE/JOIN decomposition | 23 | 8 |
| Small probe queries (`LIMIT`, `DISTINCT`, quick checks) | 20 | 7 |
| Text normalization (`TRIM`/`REPLACE`/`ILIKE`/regex-like cleanup) | 15 | 5 |
| Type normalization (`CAST`, `::`, conversion fixes) | 10 | 5 |
| String/variant flattening (`SPLIT`/`FLATTEN`) | 3 | 1 |

Interpretation:

1. **Schema/object resolution is the dominant blocker** (many repairs still reference non-existent monthly tables or wrong object qualifiers).
2. **Iterative probing + decomposition often restored executability locally**, but did not translate to evaluator-correct final SQL in this run.
3. **High token consumption aligns with repeated repair loops** in hard cases (`sf_bq193`, `sf_bq194`, `sf_bq377`, `sf_bq233`).

## Final Summary

- This run attempted `23` examples and achieved `0` correct (`final_score = 0.00`).
- Total token usage was `4,210,855`, with heavy concentration in a small set of high-iteration cases.
- Typical failures were dominated by object-qualification/authorization errors, invalid identifiers, and syntax/aggregation shape issues.
- Although many cases showed partial in-log recovery after repair, end-to-end correctness remained zero in evaluator output for this run.