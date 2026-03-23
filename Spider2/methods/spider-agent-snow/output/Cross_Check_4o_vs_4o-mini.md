# Cross-Check Report: `gpt-4o-test2` vs `gpt-4o-mini-test1`

## Scope

This report compares two Spider2-Snow runs on the same 25-case slice:

- `gpt-4o-test2`
- `gpt-4o-mini-test1`

Primary sources:
- `Spider2/methods/spider-agent-snow/output/gpt-4o-test2/SQL_Generation_Failure_Report.md`
- `Spider2/methods/spider-agent-snow/output/gpt-4o-mini-test1/SQL_Generation_Failure_Report.md`
- `Spider2/methods/spider-agent-snow/output/gpt-4o-test2/token_usage_summary.json`
- `Spider2/methods/spider-agent-snow/output/gpt-4o-mini-test1/token_usage_summary.json`
- `Spider2/spider2-snow/evaluation_suite/gpt-4o-test2.csv`
- `Spider2/spider2-snow/evaluation_suite/gpt-4o-mini-test1.csv`
- Runner config in `Spider2/methods/spider-agent-snow/run.py`

## Metric Cross-Check

### Accuracy source alignment

There is a known source mismatch for `gpt-4o-test2`:

- `token_usage_summary.json` shows `0.92`.
- The run-level final score used in the failure report was corrected to `0.12` (user-confirmed).

There is also a mismatch for `gpt-4o-mini-test1`:

- `token_usage_summary.json` shows run-completion accuracy `0.28` (`finished/attempted`).
- Evaluator correct-ID output (`gpt-4o-mini-test1.csv`) is header-only, implying `0/25 = 0.00` correct accuracy.

For this cross-check, conclusions about “which model is better” use:
- `gpt-4o-test2`: **0.12** (final score basis)
- `gpt-4o-mini-test1`: **0.00** (evaluator-correct basis)

### Side-by-side summary

| Metric | `gpt-4o-test2` | `gpt-4o-mini-test1` | Delta (mini - 4o) |
|---|---:|---:|---:|
| attempted_cases | 25 | 25 | 0 |
| finished_cases | 23 | 7 | -16 |
| failed_cases | 2 | 18 | +16 |
| accuracy (comparison basis) | 0.12 | 0.00 | -0.12 |
| run_completion_accuracy (`finished/attempted`) | 0.92* | 0.28 | -0.64 |
| llm_calls | 271 | 450 | +179 |
| total_tokens | 3,901,315 | 7,221,373 | +3,320,058 |
| avg steps per case | 10.84 | 18.00 | +7.16 |
| unfinished cases at step cap | 2/25 | 18/25 | +16/25 |

\* `gpt-4o-test2` run-completion value from token summary; separate from corrected final score basis (`0.12`).

## Retry / Step-Limit Cross-Check

No evidence suggests a stricter retry/step policy for `gpt-4o`:

- Runner default: `--max_steps 20` in `run.py`.
- `retry_failed` default is `False` (same default behavior unless explicitly enabled).
- In both runs, unfinished cases terminate at 20 steps.

Therefore, observed performance differences are not explained by a lower step budget for `gpt-4o`.

## Failure-Profile Differences

Rates are case-level occurrence rates over 25 attempted cases.

| Failure bucket | `gpt-4o-test2` | `gpt-4o-mini-test1` | Delta (mini - 4o) |
|---|---:|---:|---:|
| SQL compilation error | 60% | 80% | +20 pts |
| invalid identifier | 36% | 24% | -12 pts |
| wrong DB/schema qualification | 8% | 52% | +44 pts |
| missing current database | 8% | 56% | +48 pts |
| SQL syntax error | 8% | 28% | +20 pts |
| group-by/window misuse | 4% | 4% | 0 pts |
| unknown function | 16% | 0% | -16 pts |
| unsupported subquery | 8% | 0% | -8 pts |
| type-signature mismatch | 8% | 4% | -4 pts |

Interpretation:
- `gpt-4o-mini-test1` is dominated by environment/qualification failures (database context and object resolution).
- `gpt-4o-test2` shows relatively more dialect/operator-level mistakes (unknown function/subquery/type mismatch), but far fewer context failures.

## Case-Level Outcome Differences

- Both runs finished only 7 common cases: `sf_bq003`, `sf_bq009`, `sf_bq026`, `sf_bq027`, `sf_bq209`, `sf_bq211`, `sf_bq268`.
- `gpt-4o-test2` finished 16 additional cases that `gpt-4o-mini-test1` did not.
- `gpt-4o-mini-test1` had no “mini-only finished” cases.

This indicates `gpt-4o-mini-test1` is weaker on both completion coverage and evaluator-correct outcomes on the currently available artifacts (`0.00` correct accuracy with no mini-only finished cases).

## Conclusions

1. **No lower retry/step limit for `gpt-4o`** is observed; both are constrained by 20-step cap and same retry-default behavior.
2. **The main behavioral gap is failure mode mix**:
   - Mini: heavy schema/context qualification collapse.
   - 4o: lower context-failure rate but some dialect-function mistakes.
3. **With the new evaluator-correct finding (`mini = 0.00`)**, `gpt-4o` is better on both corrected final-score basis (`0.12` vs `0.00`) and completion coverage (23 finished vs 7).

## Recommended Next Validation

To eliminate ambiguity, run a strict apples-to-apples re-evaluation with:

- identical model-independent runner args (`--max_steps`, `--temperature`, `--top_p`, `--max_tokens`, `--N`, selection order)
- explicit capture of final evaluator score (`evaluate.py --mode exec_result`) for both runs
- one consolidated score file committed alongside per-run token summaries

That will separate true model differences from reporting-source mismatch.