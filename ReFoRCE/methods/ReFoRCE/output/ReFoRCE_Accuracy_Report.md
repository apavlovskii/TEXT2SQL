# ReFoRCE Accuracy and Token Usage Report

## Scope

- Source directory: `ReFoRCE/methods/ReFoRCE/output`
- Runs included: all runs with available `*-log-<timestamp>/token_usage_summary.json`
- Accuracy source: evaluator correct-ID files `*-csv-<timestamp>-ids.csv`
- Token source: `*-log-<timestamp>/token_usage_summary.json` (`total` and aggregated `per_test`)
- Report date: 2026-03-25

## Detected Runs

1 run was detected with complete token summary + evaluator ID artifact.

| Run ID | Attempted Tests | Correct Tests | Evaluator Accuracy |
|---|---:|---:|---:|
| `gpt-5-mini-lite-20260320-005805` | 25 | 9 | 0.36 |

## Correct IDs (Evaluator)

For `gpt-5-mini-lite-20260320-005805`, the correct IDs recorded in `gpt-5-mini-lite-csv-20260320-005805-ids.csv` are:

- `sf_bq227`
- `sf_bq300`
- `sf_bq399`
- `sf_local284`
- `sf_local310`
- `sf_bq091`
- `sf_bq092`
- `sf_bq135`
- `sf_bq171`

## Run-Level Token Summary

| Run ID | Requests | Prompt Tokens | Completion Tokens | Total Tokens |
|---|---:|---:|---:|---:|
| `gpt-5-mini-lite-20260320-005805` | 487 | 6,668,848 | 832,514 | 7,501,362 |

## Efficiency Snapshot

For `gpt-5-mini-lite-20260320-005805`:

- Attempted tests: `25`
- Correct tests: `9`
- Tokens per attempted test: `300,054.48`
- Tokens per correct test: `833,484.67`

## Domain-Level Token Distribution

(Per-test token usage aggregated by test prefix)

| Domain | Tests | Requests | Total Tokens | Share |
|---|---:|---:|---:|---:|
| BigQuery (`bq*`, `ga*`) | 8 | 167 | 4,219,450 | 56.25% |
| Snowflake (`sf_bq*`) | 14 | 307 | 3,201,920 | 42.68% |
| Local (`local*`) | 3 | 13 | 79,992 | 1.07% |

## Highest-Cost Tests (By Total Tokens)

| Test ID | Total Tokens | Requests | Prompt Tokens | Completion Tokens | Candidates |
|---|---:|---:|---:|---:|---:|
| `bq169` | 1,198,594 | 53 | 1,113,338 | 85,256 | 4 |
| `bq399` | 926,648 | 35 | 886,290 | 40,358 | 4 |
| `bq019` | 748,151 | 12 | 725,554 | 22,597 | 4 |
| `ga018` | 643,752 | 45 | 529,415 | 114,337 | 4 |
| `sf_bq093` | 623,951 | 70 | 525,343 | 98,608 | 4 |
| `sf_bq163` | 509,866 | 47 | 451,118 | 58,748 | 4 |
| `sf_bq249` | 471,108 | 35 | 424,587 | 46,521 | 4 |
| `bq288` | 448,222 | 5 | 443,250 | 4,972 | 4 |

## Deep Log Analysis (Per-Testcase)

Analyzed directory:
- `ReFoRCE/methods/ReFoRCE/output/gpt-5-mini-lite-log-20260320-005805`

Coverage:
- Testcases scanned: `25`
- Log files scanned: `125` (`5` per testcase: `0log.log..3log.log` and `log.log`)
- Cases with explicit execution-error payloads (`{'status': 'error', ...}`): `13`
- Cases showing recovery in-log (error followed by later `[Successfully executed]`): `4`

Recovered cases:
- `bq169`, `ga018`, `sf_bq083`, `sf_bq249`

Error cases without observed in-log recovery:
- `bq123`, `bq227`, `bq288`, `local169`, `sf_bq091`, `sf_bq092`, `sf_bq093`, `sf_bq135`, `sf_bq195`

### Typical Error Categories

Counts below are execution-error events extracted from per-case logs (one case can contribute multiple events):

| Error category | Events | Cases | Typical signature |
|---|---:|---:|---|
| SQL compilation errors | 17 | 7 | `SQL compilation error`, unknown function, unsupported type/construct |
| SQL syntax errors | 11 | 3 | BigQuery `invalidQuery` syntax failures (`OFFSET`, `END`, etc.) |
| Other execution errors | 6 | 3 | Aggregation-shape errors (e.g., nested aggregate restrictions) |
| Type/signature mismatch | 5 | 1 | `No matching signature`, incompatible function argument types |
| Statement timeout | 5 | 2 | Snowflake timeout/cancel events (`57014`) |
| Grouping/aggregation violations | 4 | 2 | Non-grouped selected fields, invalid aggregate usage |
| Correlated subquery unsupported | 3 | 1 | BigQuery cannot de-correlate subquery |
| Permission/access errors | 3 | 1 | Access denied / missing privilege on table |
| Invalid subquery output shape | 1 | 1 | `IN` subquery returns more than one column |

### What Steps Helped Improve Queries

From `Thinking:` blocks and correction sequences in logs, the most useful improvements were:

1. **Decompose into CTE/JOIN plans instead of monolithic SQL**
	- Present in recovered-error cases: `3/4`
	- Typical effect: resolves correlated-subquery and grouping issues by making joins explicit.

2. **Probe schema/data with small exploratory queries before final SQL**
	- Present in recovered-error cases: `2/4`
	- Typical pattern: `DISTINCT`, `LIMIT`, sample previews, and column checks to reduce wrong assumptions.

3. **Normalize data types before aggregation/filtering**
	- Present in recovered-error cases: `2/4`
	- Typical effect: addresses signature/type failures from variant/JSON or mixed numeric fields.

4. **Flatten semi-structured fields when needed**
	- Present in recovered-error cases: `1/4`
	- Typical pattern: `LATERAL FLATTEN` for Snowflake variant arrays before filtering/aggregation.

5. **Rewrite correlated subqueries into joinable intermediate relations**
	- Present in recovered-error cases: `1/4`
	- Example observed in `bq169`: de-correlated region predicates into per-clone aggregates then joined.

### Log-Analysis Summary

- The dominant blockers were **compile/syntax and aggregation-shape errors**, not just model output emptiness.
- Cases that recovered generally used a **structured correction loop**: inspect/probe → rewrite with CTE/JOIN → re-run.
- High-token cases (`bq169`, `ga018`, `sf_bq093`, `sf_bq249`) align with repeated correction iterations, indicating that recovery is possible but expensive when initial query shape is off.

## Notes

- Attempted test count was derived from available run outputs (`*.sql`/`*.csv`) for the run.
- Evaluator accuracy in this report is computed as `correct_tests / attempted_tests`.
- This accuracy is evaluator-correctness based and is distinct from pipeline completion metrics.
- Candidate-level entries (`@cand0..@cand3`) were aggregated to test-level totals for token analysis.
- Error taxonomy was derived from explicit execution payloads in per-test logs (`{'status': 'error', 'error_msg': ...}`).
- Recovery in this report means an error marker appears earlier in the same testcase logs and a later `[Successfully executed]` marker is observed.