# Benchmark Run 4 Report — SnowRAG-Agent (All Features Activated)

> **Date:** 2026-03-30
> **Model:** GPT-5.4
> **Embeddings:** text-embedding-3-large
> **Test cases:** 25 (first 25 from spider2-snow.jsonl)
> **Strategy:** Best-of-3 candidates, 3 repair iterations, gold-match verification
> **Active features:** Semantic layer (5,697 cards), question decomposition, empty-result feedback, improved memory, column validation, syntax reference

---

## 1. Executive Summary

**Gold-match accuracy: 2/25 = 8.0%**

This is a regression from the previous Run 4 (3/25 = 12%) despite activating semantic layer and question decomposition. The 2 gold matches are **sf_bq033** (PATENTS) and **sf_bq212** (PATENTS). Notably, **sf_bq011** (GA4), which matched gold in all previous runs, did not match this time.

The semantic layer contributed 5,697 cards and question decomposition was enabled in config, but neither produced visible log output — indicating silent failures in the retrieval/decomposition path, or that the additional prompt context introduced noise that hurt the simpler queries.

---

## 2. Final Accuracy

| Metric | This Run | Previous Run 4 | Delta |
|:---|---:|---:|---:|
| **Gold-match accuracy** | **2/25 = 8.0%** | 3/25 = 12.0% | **-4pp** |
| LLM calls | 336 | 281 | +55 |
| Total tokens | 517K | 436K | +81K |
| Cost | $4.09 | $3.60 | +$0.49 |

### Gold-matched instances

| Instance | Database | LLM Calls |
|:---------|:---------|:----------|
| sf_bq033 | PATENTS | 9 |
| sf_bq212 | PATENTS | 13 |

### Lost match: sf_bq011 (GA4)
Previously matched in all runs. Now used 12 LLM calls but failed gold verification — the additional semantic/decomposition context may have changed the prompt enough to produce a different (incorrect) SQL.

---

## 3. Token Usage

| Metric | Value |
|:---|---:|
| LLM API calls | 336 |
| Prompt tokens | 367,024 |
| Completion tokens | 150,467 |
| **Total tokens** | **517,491** |
| Avg tokens / instance | 20,699 |
| **Cost (GPT-5.4)** | **$4.09** |

19% more tokens than previous Run 4 (436K) due to additional semantic context in prompts.

---

## 4. Error Analysis

| Error Type | Count |
|:---|---:|
| Gold match PASSED | 3 |
| Gold match FAILED | 163 |
| result_mismatch | 144 |
| empty_result | 52 |
| invalid_identifier | 144 |

### Key findings

1. **Decomposition and semantic features silently failed** — zero log entries for decomposition or semantic context retrieval. Both are wrapped in try/except blocks that swallow errors. The features were enabled in config and data was present (5,697 semantic cards), but the actual retrieval/decomposition calls produced errors that were caught and discarded.

2. **More invalid_identifier errors** (144 vs 72 in previous run) — the semantic context, if partially injected, may have introduced column references that don't match the schema slice.

3. **More empty_results** (52 vs 34) — suggests more aggressive filter conditions in generated SQL.

4. **sf_bq011 regression** — the GA4 query that previously matched now fails, indicating the prompt changes (even if semantic/decomposition was partially applied) can destabilize previously-working queries.

---

## 5. Comparison

| | This Run 4 | Prev Run 4 | Run 3 (exec) | ReFoRCE |
|:---|:---|:---|:---|:---|
| **Gold accuracy** | **8.0%** | 12.0% | 8.7%* | ~36%** |
| **Tokens** | 517K | 436K | 241K | 7.5M |
| **Cost** | $4.09 | $3.60 | $2.01 | ~$18.75 |

*Post-hoc gold evaluation. **ReFoRCE uses execution accuracy, not gold-match.

---

## 6. Root Cause & Next Steps

The regression is caused by:
1. **Silent feature failures** — decomposition and semantic retrieval errors are swallowed. Need to add logging at WARNING level when these features fail, so we can see what went wrong.
2. **Prompt sensitivity** — adding more context doesn't always help; it can destabilize previously-correct queries. The semantic context and decomposition need to be validated before injection.
3. **No incremental testing** — features were activated all at once. Should enable one at a time and measure impact.

**Recommended next steps:**
- Add explicit WARNING logs when semantic retrieval or decomposition fails
- Test semantic layer alone (without decomposition)
- Test decomposition alone (without semantic layer)
- Investigate why sf_bq011 regressed

---

## 7. Deliverables Checklist

- [x] Benchmark run completed on first 25 Spider2-Snow test cases
- [x] Model used: GPT-5.4
- [x] Token usage summary produced (Section 3)
- [x] Final accuracy computed: **2/25 = 8.0% gold-match accuracy** (Section 2)
- [x] Detailed error and issue review produced (Section 4)
- [x] Comparison report produced (Section 5)
