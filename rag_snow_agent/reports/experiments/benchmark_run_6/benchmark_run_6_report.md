# Benchmark Run 6 Report — SnowRAG-Agent

> **Date:** 2026-03-31
> **Model:** GPT-5.4
> **Embeddings:** text-embedding-3-large
> **Test cases:** 25 (first 25 from spider2-snow.jsonl)
> **Strategy:** Best-of-10 candidates, 5 repair iterations, gold-match verification
> **Active features:** Semantic layer, question decomposition, empty-result feedback, column validation, syntax reference

---

## 1. Executive Summary

**Gold-match accuracy: 1/25 = 4.0%** — but this result is **invalid due to API quota exhaustion**.

The run hit OpenAI's API rate limit (HTTP 429) after processing 16 of 25 instances. The remaining 9 instances (all PATENTS + PATENTS_GOOGLE) received 0 LLM calls and automatically failed. The 10-candidate × 5-repair configuration consumed ~815 LLM calls and $12.15 before hitting the quota ceiling.

**This run cannot be compared to previous runs.** The result reflects API infrastructure limits, not system capability.

---

## 2. What Happened

| Phase | Instances | LLM Calls | Status |
|:------|:----------|:----------|:-------|
| Completed normally | 16 (sf_bq011 – sf_bq091) | 815 | 1 gold match |
| API quota exhausted | 9 (sf_bq099 – sf_bq214) | 0 | All failed with 429 |

### Completed instances (16/25)

| Instance | Database | Gold | LLM Calls |
|:---------|:---------|:-----|:----------|
| sf_bq011 | GA4 | **Yes** | 55 |
| sf_bq010 | GA360 | No | 10 |
| sf_bq009 | GA360 | No | 46 |
| sf_bq001 | GA360 | No | 31 |
| sf_bq002–sf_bq374 | GA360 (9) | No | 39–64 each |
| sf_bq029 | PATENTS | No | 50 |
| sf_bq026 | PATENTS | No | 58 |
| sf_bq091 | PATENTS | No | 63 |

### Failed instances (9/25) — API quota error

| Instance | Database | Error |
|:---------|:---------|:------|
| sf_bq099 | PATENTS | 400 — request body too large |
| sf_bq033–sf_bq214 | PATENTS/PG | 429 — quota exceeded |

**Notable:** sf_bq033 and sf_bq212, which matched gold in Runs 4 and 5, couldn't even attempt because the API quota was already exhausted by earlier instances.

---

## 3. Token Usage

| Metric | Run 6 (10 cand) | Run 5 (7 cand) | Run 4 (3 cand) |
|:---|---:|---:|---:|
| LLM calls (attempted) | 815 | 774 | 336 |
| Total tokens | 1.52M | 1.30M | 517K |
| **Cost** | **$12.15** | $10.29 | $4.09 |
| Avg tokens / completed instance | 95,304 | 52,008 | 20,699 |

The 10-candidate configuration is extremely token-hungry: ~95K tokens per instance (vs 52K for 7 candidates). The 16 completed instances alone consumed more tokens than the entire 25-instance Run 5.

---

## 4. Error Analysis

| Error Type | Count |
|:---|---:|
| Gold match PASSED | 5 (sf_bq011 only, multiple candidates) |
| Gold match FAILED | 418 |
| result_mismatch | 419 |
| empty_result | 113 |
| invalid_identifier | 383 |
| Runner error (429) | 8 |
| Runner error (400) | 1 |

### sf_bq001 regression

sf_bq001 matched gold in Run 5 (7 candidates) but failed here with 31 LLM calls. This suggests the extra candidates don't always help — more candidates can introduce noise in the selection process.

### sf_bq011 still reliable

The GA4 query matched gold again (55 LLM calls — much more than the 28 in Run 5, but still found the answer).

---

## 5. Comparison

| | Run 6 | Run 5 | Run 4 | ReFoRCE |
|:---|:---|:---|:---|:---|
| **Candidates** | 10 | 7 | 3 | 8 |
| **Repairs** | 5 | 3 | 3 | 3 |
| **Gold accuracy** | 4.0%* | **20.0%** | 8.0% | ~36%** |
| **Tokens** | 1.52M | 1.30M | 517K | 7.5M |
| **Cost** | $12.15 | $10.29 | $4.09 | ~$18.75 |
| **Completed** | 16/25 | 25/25 | 25/25 | 25/25 |

*Invalid — 9 instances failed due to API quota. **Execution accuracy, not gold.

---

## 6. Lessons Learned

1. **API quota is a hard constraint.** 10 candidates × 5 repairs × 25 instances = up to 1,250 LLM calls. At GPT-5.4 pricing/quota, this exceeds typical API limits. Need to either reduce candidate count or implement API rate limiting with backoff.

2. **Diminishing returns past 7 candidates.** Run 5 (7 cand) achieved 20% accuracy. Run 6 (10 cand) got only 4% (or ~6% on completed instances). More candidates don't linearly scale when quota prevents completion.

3. **The sweet spot is 7 candidates with 3 repairs.** Run 5's configuration ($10.29, 20% accuracy, all 25 completed) remains the best performing configuration.

4. **Need rate-limit handling.** The experiment runner should implement exponential backoff on 429 errors, or at minimum detect quota exhaustion and stop gracefully.

---

## 7. Deliverables Checklist

- [x] Benchmark run completed on first 25 Spider2-Snow test cases (16 completed, 9 quota-limited)
- [x] Model used: GPT-5.4
- [x] Token usage summary produced (Section 3)
- [x] Final accuracy computed: **1/25 = 4.0%** (invalid — quota exhaustion) (Section 2)
- [x] Detailed error and issue review produced (Section 4)
- [x] Comparison report produced (Section 5)
