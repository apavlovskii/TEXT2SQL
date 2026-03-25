# Cross-Project Comparison: DSR Lite vs ReFoRCE vs Spider+GPT-4o

## Scope

This report compares three approaches using the previously generated run reports:

- DSR Lite (Snow run): `DSR-SQL/DSR_Lite/logs/run_snow_2026-03-21_164927/DSR_Lite_Accuracy_Token_Error_Report.md`
- ReFoRCE: `ReFoRCE/methods/ReFoRCE/output/ReFoRCE_Accuracy_Report.md`
- Spider agent + GPT-4o: `Spider2/methods/spider-agent-snow/output/gpt-4o-test2/SQL_Generation_Failure_Report.md`

Reference artifact for Spider token accounting:
- `Spider2/methods/spider-agent-snow/output/gpt-4o-test2/token_usage_summary.json`

Report date: 2026-03-26

## Headline Metrics

| Approach | Evaluated/Attempted | Correct | Accuracy (comparison basis) | Total Tokens |
|---|---:|---:|---:|---:|
| DSR Lite | 23 | 0 | 0.00 | 4,210,855 |
| ReFoRCE | 25 | 9 | 0.36 | 7,501,362 |
| Spider + GPT-4o | 25 | ~3 | 0.12 | 3,901,315 |

Notes:
- Spider accuracy is taken from the previously corrected failure report (`0.12`).
- Spider `token_usage_summary.json` contains run-completion metrics and reports `accuracy=0.92`; this is a different metric source than corrected evaluator-style score and should not be mixed directly.

## Efficiency Snapshot

| Approach | Tokens / attempted case | Observation |
|---|---:|---|
| DSR Lite | 183,080.65 | Mid token spend but no evaluator-correct outputs |
| ReFoRCE | 300,054.48 | Highest token spend, best accuracy among the three |
| Spider + GPT-4o | 156,052.60 | Lowest token spend, moderate corrected accuracy |

Interpretation:
- ReFoRCE appears to trade higher token budget for stronger correctness.
- DSR consumed substantial budget but failed to convert recoverable execution into correct final outputs.
- Spider run is comparatively token-efficient, but metric-source inconsistency requires caution.

## Typical Issues by Project

### DSR Lite
- Dominant failures: object qualification/authorization, invalid identifiers, syntax/shape errors.
- Frequent repair loops and many step-cap trajectories (`12/23` at step 20).
- In-log recoveries occurred, but did not produce evaluator-correct final answers.

### ReFoRCE
- Dominant failures: SQL compilation/syntax, aggregation-shape errors, occasional timeouts/type issues.
- Recovery pattern exists but remains expensive on high-cost cases.
- Better end performance than DSR despite similar classes of SQL fragility.

### Spider + GPT-4o
- Dominant failures: invalid identifiers, dialect/function incompatibility, schema/db qualification mistakes.
- A subset of cases reached step-limit hard failure.
- Finishes many cases but still has meaningful execution-safety gaps reflected in corrected score.

## Cross-Project Common Failure Themes

1. **Schema/object grounding failures**
   - Wrong table/schema/database qualification and non-existent objects are recurring blockers.
2. **Identifier/path mismatch**
   - Invalid aliases/column names/field-path extraction appears in all pipelines.
3. **Dialect-specific SQL fragility**
   - Unsupported functions, subquery forms, and syntax mismatches repeatedly break execution.
4. **Repair-loop inefficiency**
   - Systems often revise SQL repeatedly without enough diversification or hard constraints.
5. **Execution success ≠ evaluator correctness**
   - Partial recoveries and successful intermediate execution do not reliably yield correct final answers.

## Potential Improvement Areas

1. **Pre-execution schema guardrails (high priority)**
   - Add mandatory object-existence and qualifier validation before executing generated SQL.
   - Enforce canonical fully-qualified naming policy for each backend.

2. **Dialect-aware SQL planning layer**
   - Introduce backend-specific templates/operators (Snowflake/BigQuery/SQLite) and function whitelist checks.
   - Reject or auto-rewrite unsupported constructs before execution.

3. **Repair strategy diversification + deduping**
   - Prevent near-duplicate retries by hashing normalized SQL and forcing strategy changes per retry.
   - Escalate in fixed order: identifier check → type normalization → join/CTE decomposition → fallback template.

4. **Structured intermediate validation**
   - Add lightweight checks for result-shape correctness (columns, row semantics) prior to finalizing SQL.
   - Require explicit alignment between question intent and computed metrics.

5. **Token-budget governance**
   - Allocate per-case budgets with stop conditions and confidence-triggered fallback plans.
   - Route very hard cases early to deterministic fallback prompts to avoid expensive loops.

6. **Metric harmonization across pipelines**
   - Standardize reporting to always include both:
     - run-completion metrics, and
     - evaluator-correct metrics.
   - Publish both in one summary artifact to remove ambiguity.

## Overall Conclusion

- **Best observed correctness** in this snapshot is ReFoRCE (`0.36`), but at the highest token cost.
- **Spider+GPT-4o** is more token-efficient and likely recoverable with stronger schema/dialect guardrails.
- **DSR Lite** currently suffers the largest execution-to-correctness gap; improving object grounding and repair-loop policy is likely the highest ROI.
- Across all three, the fastest path to improvement is: **schema validation + dialect constraints + non-redundant repair control + unified evaluator-centric reporting**.