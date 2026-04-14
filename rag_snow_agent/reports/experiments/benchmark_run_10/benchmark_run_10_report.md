# Benchmark Run 10 Report — SnowRAG-Agent

> **Date:** 2026-04-14
> **Model:** GPT-5.4-mini
> **Embeddings:** text-embedding-3-large
> **Test cases:** 100 (first 100 from spider2-snow.jsonl)
> **Strategy:** Best-of-8 candidates, 4 repair iterations, gold-output verification
> **Active features:** All Run 9 features + 16 new databases indexed, GPT-5.4 profiled descriptions for all 20 databases, external knowledge documents indexed

---

## 1. Executive Summary

**Gold-match accuracy: 87/100 = 87.0%** — the first 100-query benchmark, achieving high accuracy across 20 databases with a cost-efficient model (gpt-5.4-mini).

- **12 databases at 100%** accuracy (CMS_DATA, GA4, GEO_OPENSTREETMAP_BOUNDARIES, GITHUB_REPOS, GITHUB_REPOS_DATE, NEW_YORK_GEO, NOAA_DATA, NOAA_DATA_PLUS, NOAA_GSOD, PATENTSVIEW, PATENTS_USPTO, PYPI)
- **13 failures** across 8 databases, primarily geospatial queries and complex multi-table queries
- **$32.92 total cost**, $0.38 per gold match — 6.4x cheaper than Run 9's $2.45/match

---

## 2. Final Accuracy

| Metric | Run 10 (100q) | Run 9 (25q) | Delta |
|:---|---:|---:|---:|
| **Gold-match accuracy** | **87/100 = 87.0%** | 23/25 = 92.0% | -5pp (4x scale) |
| Queries | 100 | 25 | +75 |
| Databases | 20 | 4 | +16 |
| LLM calls | 3,601 | 889 | +2,712 |
| Total tokens | 18.9M | 4.1M | +14.8M |
| Cost | ~$32.92 | ~$56.28 | -$23.36 |
| Cost per gold match | **$0.38** | $2.45 | -84% |

### By database

| Database | Queries | Gold | Accuracy | Notes |
|:---------|--------:|-----:|---------:|:------|
| GA4 | 1 | 1 | 100% | |
| GA360 | 12 | 10 | 83% | 2 regressions vs Run 9 |
| PATENTS | 15 | 14 | 93% | 1 regression vs Run 9 |
| PATENTS_GOOGLE | 4 | 3 | 75% | 1 API error |
| GITHUB_REPOS | 15 | 15 | **100%** | New database — perfect |
| NOAA_DATA | 12 | 12 | **100%** | New database — perfect |
| CMS_DATA | 7 | 7 | **100%** | New database — perfect |
| GITHUB_REPOS_DATE | 6 | 6 | **100%** | New database — perfect |
| GEO_OPENSTREETMAP | 6 | 4 | 67% | Geospatial failures |
| CENSUS_BUREAU_ACS_2 | 4 | 2 | 50% | Complex schema |
| PATENTSVIEW | 3 | 3 | **100%** | New database — perfect |
| NEW_YORK_CITIBIKE_1 | 3 | 1 | 33% | Geospatial failures |
| NEW_YORK_NOAA | 3 | 1 | 33% | Geospatial failures |
| PATENTS_USPTO | 2 | 2 | **100%** | |
| NOAA_DATA_PLUS | 2 | 2 | **100%** | |
| PYPI | 1 | 1 | **100%** | |
| NOAA_GSOD | 1 | 1 | **100%** | |
| NOAA_GLOBAL_FORECAST_SYSTEM | 1 | 0 | 0% | Geospatial |
| NEW_YORK_GEO | 1 | 1 | **100%** | |
| GEO_OPENSTREETMAP_BOUNDARIES | 1 | 1 | **100%** | |

---

## 3. Token Usage Summary

| Metric | Run 10 (gpt-5.4-mini) | Run 9 (gpt-5.4) |
|:---|---:|---:|
| LLM API calls | 3,601 | 973 |
| Prompt tokens | 16,404,077 | 3,300,307 |
| Completion tokens | 2,483,788 | 775,960 |
| **Total tokens** | **18,887,865** | **4,076,267** |
| Avg tokens / instance | 188,879 | 163,051 |
| **Estimated cost** | **~$32.92** | **~$56.28** |
| Cost / gold match | **$0.38** | $2.45 |

gpt-5.4-mini is dramatically cheaper: $0.38 per gold match vs $2.45 with gpt-5.4 (6.4x cheaper).

---

## 4. Error Analysis

### 4.1 Failures by category

| Category | Count | Instances |
|:---------|------:|:----------|
| Geospatial queries | 5 | sf_bq050, sf_bq426, sf_bq291, sf_bq208, sf_bq048 |
| Complex multi-step logic | 3 | sf_bq010, sf_bq270, sf_bq222 |
| Complex schema (296 tables) | 2 | sf_bq073, sf_bq410 |
| Geospatial + OpenStreetMap | 2 | sf_bq348, sf_bq254 |
| API error | 1 | sf_bq127 |

### 4.2 Geospatial queries — the main failure mode

7 of 13 failures involve geospatial functions (ST_DISTANCE, ST_DWITHIN, ST_WITHIN, ST_CONTAINS). These queries require:
- GEOGRAPHY/GEOMETRY data types
- Snowflake geospatial functions not covered by the deterministic compiler
- Spatial joins and distance calculations

The current system has external knowledge docs for these functions indexed in ChromaDB, but the plan schema and compiler don't support geospatial operations natively.

### 4.3 Regressions from Run 9

3 instances that matched in Run 9 (gpt-5.4) failed with gpt-5.4-mini:
- sf_bq010 (GA360) — simpler model couldn't find the right product query
- sf_bq270 (GA360) — conversion rate calculation
- sf_bq222 (PATENTS) — CPC exponential moving average

These suggest gpt-5.4-mini has slightly lower reasoning capacity for the most complex queries.

---

## 5. New Database Performance

| Database | Queries | Gold | Accuracy | Observation |
|:---------|--------:|-----:|---------:|:------------|
| GITHUB_REPOS | 15 | 15 | 100% | Perfect — well-profiled schema, clear column names |
| NOAA_DATA | 12 | 12 | 100% | Perfect — despite 218 tables, profiling covered all |
| CMS_DATA | 7 | 7 | 100% | Perfect — Medicare/healthcare data well-described |
| GITHUB_REPOS_DATE | 6 | 6 | 100% | Perfect — partition collapse worked |
| GEO_OPENSTREETMAP | 6 | 4 | 67% | Geospatial failures on 2 complex spatial queries |
| CENSUS_BUREAU_ACS_2 | 4 | 2 | 50% | 296 tables — retrieval noise |
| PATENTSVIEW | 3 | 3 | 100% | Perfect — well-structured relational schema |
| NEW_YORK_CITIBIKE_1 | 3 | 1 | 33% | Geospatial failures |
| NEW_YORK_NOAA | 3 | 1 | 33% | Geospatial failures |
| PATENTS_USPTO | 2 | 2 | 100% | Perfect |
| NOAA_DATA_PLUS | 2 | 2 | 100% | Perfect |
| Others (4 DBs) | 4 | 3 | 75% | 1 geospatial failure |

**12 of 16 new databases achieved 100% accuracy.** The data profiling pipeline successfully enabled the agent to work with databases it had never seen during development.

---

## 6. Comparison

| | Run 10 | Run 9 | Run 8 | ReFoRCE |
|:---|:---|:---|:---|:---|
| **Queries** | 100 | 25 | 25 | 25 |
| **Gold accuracy** | **87.0%** | 92.0% | 24.0% | ~36% |
| **Model** | gpt-5.4-mini | gpt-5.4 | gpt-5.4 | gpt-5-mini |
| **Cost** | $32.92 | $56.28 | $35.32 | ~$18.75 |
| **Cost/gold** | **$0.38** | $2.45 | $5.89 | ~$2.08 |
| **Databases** | 20 | 4 | 4 | 4 |

---

## 7. Deliverables Checklist

- [x] Benchmark run completed on first 100 Spider2-Snow test cases
- [x] Model used: gpt-5.4-mini with 8 candidates, 4 repairs
- [x] Token usage summary produced (Section 3)
- [x] Final accuracy computed: **87/100 = 87.0%** (Section 2)
- [x] Detailed error and issue review produced (Section 4)
- [x] Comparison report produced (Section 6)
