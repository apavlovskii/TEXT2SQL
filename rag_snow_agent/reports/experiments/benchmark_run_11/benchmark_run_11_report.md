# Benchmark Run 11 Report — SnowRAG-Agent

> **Date:** 2026-04-15
> **Model:** GPT-5.4-mini
> **Embeddings:** text-embedding-3-large
> **Test cases:** 100 (first 100 from spider2-snow.jsonl)
> **Strategy:** Best-of-8 candidates, 4 repair iterations, gold-output verification
> **New features:** Geospatial compiler support (PlanGeoJoin, PlanGeoFilter), geo_first strategy, per-instance external knowledge injection, geospatial syntax reference

---

## 1. Executive Summary

**Gold-match accuracy: 81/100 = 81.0%** — a **6pp regression** from Run 10 (87.0%).

- **0 geospatial fixes** out of 7 targeted failures — the geospatial changes did not resolve any of the spatial query failures
- **8 regressions** from Run 10 (2 API errors, 6 execution failures)
- **2 new matches** (sf_bq127 PATENTS_GOOGLE, sf_bq222 PATENTS)
- The geo_first strategy was selected as winner in 2 instances, but neither was a geospatial query

---

## 2. Final Accuracy

| Metric | Run 11 (100q) | Run 10 (100q) | Delta |
|:---|---:|---:|---:|
| **Gold-match accuracy** | **81/100 = 81.0%** | 87/100 = 87.0% | **-6pp** |
| LLM calls | 3,514 | 3,601 | -87 |
| Avg LLM calls / instance | 35.1 | 36.0 | -0.9 |

### By database

| Database | Queries | Gold | Accuracy | Run 10 | Delta |
|:---------|--------:|-----:|---------:|-------:|------:|
| GITHUB_REPOS | 15 | 15 | **100%** | 100% | = |
| CMS_DATA | 7 | 7 | **100%** | 100% | = |
| GITHUB_REPOS_DATE | 6 | 6 | **100%** | 100% | = |
| PATENTS | 15 | 15 | **100%** | 93% | **+7pp** |
| PATENTS_GOOGLE | 4 | 4 | **100%** | 75% | **+25pp** |
| GA4 | 1 | 1 | 100% | 100% | = |
| GEO_OPENSTREETMAP_BOUNDARIES | 1 | 1 | 100% | 100% | = |
| NEW_YORK_GEO | 1 | 1 | 100% | 100% | = |
| NOAA_DATA_PLUS | 2 | 2 | 100% | 100% | = |
| NOAA_GSOD | 1 | 1 | 100% | 100% | = |
| PATENTSVIEW | 3 | 3 | 100% | 100% | = |
| PYPI | 1 | 1 | 100% | 100% | = |
| NOAA_DATA | 12 | 11 | 92% | 100% | **-8pp** |
| GA360 | 12 | 8 | 67% | 83% | **-17pp** |
| GEO_OPENSTREETMAP | 6 | 2 | 33% | 67% | **-33pp** |
| NEW_YORK_CITIBIKE_1 | 3 | 1 | 33% | 33% | = |
| PATENTS_USPTO | 2 | 1 | 50% | 100% | **-50pp** |
| CENSUS_BUREAU_ACS_2 | 4 | 1 | 25% | 50% | **-25pp** |
| NEW_YORK_NOAA | 3 | 0 | 0% | 33% | **-33pp** |
| NOAA_GLOBAL_FORECAST_SYSTEM | 1 | 0 | 0% | 0% | = |

---

## 3. Regressions from Run 10 (8 instances)

### API Errors (2)
| Instance | Database | Error |
|:---------|:---------|:------|
| sf_bq420 | PATENTS_USPTO | 400: Could not parse JSON body |
| sf_bq042 | NOAA_DATA | 400: Could not parse JSON body |

These are transient API errors unrelated to the geospatial changes. The enlarged prompt (with injected external knowledge content) may have caused request body size issues.

### Execution Failures (6)
| Instance | Database | Notes |
|:---------|:---------|:------|
| sf_bq004 | GA360 | All 8 candidates failed execution (scored -40) |
| sf_bq275 | GA360 | All 8 candidates failed execution (scored -50) |
| sf_bq047 | NEW_YORK_NOAA | All 8 candidates failed execution (scored -50) |
| sf_bq131 | GEO_OPENSTREETMAP | All 8 candidates failed execution (scored -30) |
| sf_bq429 | CENSUS_BUREAU_ACS_2 | All 8 candidates failed execution (scored -50) |
| sf_bq253 | GEO_OPENSTREETMAP | All 8 candidates failed execution (scored -30) |

All 6 regressions show "best score among failed candidates" — no candidate executed successfully. These queries succeeded in Run 10 under identical model/config, indicating non-deterministic LLM behavior or prompt changes (larger system prompt with geospatial guidance) affecting plan generation quality.

---

## 4. New Matches (2 instances)

| Instance | Database | Strategy | Score | Notes |
|:---------|:---------|:---------|------:|:------|
| sf_bq127 | PATENTS_GOOGLE | metric_first | 75.0 | Was API error in Run 10, now succeeded |
| sf_bq222 | PATENTS | time_first | 70.0 | Complex CPC exponential moving average |

---

## 5. Geospatial Results

### 0/7 geospatial failures fixed

| Instance | Database | Run 11 | Issue |
|:---------|:---------|:-------|:------|
| sf_bq050 | NEW_YORK_CITIBIKE_1 | FAILED | SQL generated but no ST_WITHIN used |
| sf_bq426 | NEW_YORK_CITIBIKE_1 | FAILED | SQL generated but no spatial functions |
| sf_bq291 | NOAA_GLOBAL_FORECAST_SYSTEM | FAILED | Plan parse failed → SELECT 1 |
| sf_bq208 | NEW_YORK_NOAA | FAILED | Manual lat/lon math instead of ST_DWITHIN |
| sf_bq048 | NEW_YORK_NOAA | FAILED | All candidates failed execution |
| sf_bq348 | GEO_OPENSTREETMAP | FAILED | All candidates failed execution |
| sf_bq254 | GEO_OPENSTREETMAP | FAILED | All candidates failed execution |

### Root Cause Analysis

Despite the compiler now supporting `PlanGeoJoin` and `PlanGeoFilter`, and the prompt including geospatial guidance, the LLM (gpt-5.4-mini) did not generate plans using these new elements. The geo_first strategy won 2 instances, but both were non-geospatial queries. For actual geospatial queries, the LLM either:
1. Fell back to manual lat/lon arithmetic (sf_bq208)
2. Ignored spatial functions entirely (sf_bq050, sf_bq426)
3. Failed to parse plans (sf_bq291)

The external knowledge docs were injected but did not lead to geo function usage in plans.

---

## 6. Strategy Wins

| Strategy | Run 11 Wins | Run 10 Wins | Delta |
|:---------|------------:|------------:|------:|
| default | 40 | 41 | -1 |
| flatten_first | 15 | 20 | -5 |
| join_first | 9 | 8 | +1 |
| metric_first | 7 | 7 | = |
| time_first | 5 | 3 | +2 |
| cte_first | 3 | 8 | -5 |
| geo_first | 2 | — | new |

---

## 7. Comparison

| | Run 11 | Run 10 | Run 9 | 
|:---|:---|:---|:---|
| **Queries** | 100 | 100 | 25 |
| **Gold accuracy** | **81.0%** | 87.0% | 92.0% |
| **Model** | gpt-5.4-mini | gpt-5.4-mini | gpt-5.4 |
| **New features** | Geospatial support | — | Data profiling |
| **Regressions** | 8 from Run 10 | 3 from Run 9 | — |
| **New matches** | 2 | — | 17 |

---

## 8. Diagnosis and Next Steps

### Why geospatial changes didn't help
1. **gpt-5.4-mini doesn't reliably use new plan elements** — despite guidance in prompts, the model defaulted to familiar plan structures (PlanJoin, PlanFilter) and ignored PlanGeoJoin/PlanGeoFilter
2. **External knowledge injection may have bloated prompts** — adding full markdown docs to semantic_context increases prompt size, potentially degrading plan quality on unrelated queries
3. **The geo_first strategy was not selected for geo queries** — the strategy rotation is round-robin (by candidate index), so geo_first only runs for candidate #7. By that point, 6 other strategies already failed

### Why regressions occurred
1. **Prompt length increase** — the expanded _SNOWFLAKE_GUIDANCE (geospatial rules) and external knowledge injection increased system prompt size for all queries, potentially hurting plan quality on non-geo queries
2. **API 400 errors** — 2 instances hit request body parse errors, possibly from oversized prompts with injected docs
3. **LLM non-determinism** — 6 regressions show all-candidate failures where Run 10 succeeded, suggesting the model's plan generation was destabilized

### Recommended actions
1. **Revert prompt bloat** — move geospatial guidance out of _SNOWFLAKE_GUIDANCE (which is injected into ALL queries) into a conditional section only for geo queries
2. **Cap external knowledge injection** — truncate injected docs to a token budget (e.g., 500 tokens) to prevent prompt bloat
3. **Prioritize geo_first strategy** — for queries with geospatial keywords, place geo_first earlier in the rotation (candidate #1 or #2)
4. **Consider gpt-5.4 for geo queries** — the mini model may lack capacity to use the new plan elements
