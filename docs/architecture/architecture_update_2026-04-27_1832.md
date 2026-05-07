# Architecture Update — Benchmark Run 12 Fixes

> **Date:** 2026-04-27
> **Baseline:** Run 11 (81/100 = 81.0%), Run 10 (87/100 = 87.0%)
> **Target:** Recover Run 10 regressions + fix persistent failures

---

## Fix 1: Retry on API 400 Errors

**File:** `rag_snow_agent/src/rag_snow_agent/agent/llm_client.py`

**Problem:** Two instances (sf_bq420, sf_bq042) failed with `400: Could not parse JSON body` — transient API errors, not logic failures. Both succeeded in Run 10. The enlarged prompt (with injected external knowledge) may produce request bodies that intermittently trigger API parse errors.

**Change:** Add retry with exponential backoff in `_call_with_fallback()` for HTTP 400 errors whose message contains "could not parse" or "invalid json". Retry up to 2 times with 2s/4s delays before raising. Non-parse 400 errors (e.g., invalid model, bad parameter) are not retried.

**Expected impact:** +2 instances (sf_bq420, sf_bq042)

---

## Fix 2: Join-Graph Guided Retrieval

**File:** `rag_snow_agent/src/rag_snow_agent/retrieval/connectivity.py`

**Problem:** The schema retriever selects tables by semantic similarity to the question, but some queries require auxiliary tables that are semantically distant. Key example: NEW_YORK_NOAA queries (sf_bq208, sf_bq047, sf_bq048) need the `STATIONS` table (with `lat`/`lon` columns) to do geospatial filtering — but the GSOD weather tables are what match the question. Without `STATIONS` in the schema slice, the LLM fabricates non-existent `lat`/`lon` columns on GSOD tables.

Similarly, CENSUS_BUREAU_ACS_2 queries (sf_bq429, sf_bq073) need `GEO_US_BOUNDARIES.ZIP_CODES` to map ZCTA geo_ids to state names, but this table isn't semantically similar to employment/income questions.

**Change:** Extend `expand_connectivity()` to also add 1-hop join-graph neighbors of every already-selected table when those neighbors contribute columns that are referenced in the question (spatial keywords like "within", "radius", "distance", "near", coordinate-like patterns, or state/zip references). This is a targeted expansion: it only fires when the question signals a need for columns not present in the current schema slice.

The existing `expand_connectivity_with_join_graph()` already adds bridge tables between disconnected selected tables. The new logic adds a second pass: for each selected table, check if any of its 1-hop join-graph neighbors have columns matching question keywords (lat, lon, latitude, longitude, geometry, geom, state_name, state_code, zip_code) that aren't already in the slice. If so, add them.

**Expected impact:** +3-4 instances (sf_bq208, sf_bq047, sf_bq429, sf_bq073)

---

## Fix 3: Model Routing — GPT-5.4 for Geospatial, GPT-5.4-mini for Rest

**Files:** `rag_snow_agent/src/rag_snow_agent/eval/experiment_runner.py`, `rag_snow_agent/config/defaults.yaml`

**Problem:** Run 11 showed that gpt-5.4-mini cannot reliably generate plans using geospatial plan elements (PlanGeoJoin, PlanGeoFilter). Despite prompt guidance and the geo_first strategy, the model defaults to familiar patterns (manual lat/lon arithmetic, ignoring spatial functions). All 7 geospatial failures remain unfixed. Run 9 used gpt-5.4 (full) and achieved 92% on 25 queries.

**Change:** Add a `geo_model` config field (default: `gpt-5.4`). In the experiment runner, detect geospatial queries by checking `external_knowledge` for spatial function references (`functions_st_within`, `functions_st_dwithin`, `functions_st_intersects`, `functions_st_contains`) and override the model to `geo_model` for those instances. All other instances continue using the default `gpt-5.4-mini`.

Detection heuristic: if `external_knowledge` field contains `"functions_st_"` or `"st_within"` or `"st_dwithin"` or `"st_intersects"`, use the geo model. This is conservative and only targets instances the benchmark explicitly marks as needing spatial functions.

**Expected impact:** +2-4 geospatial instances (sf_bq050, sf_bq426, sf_bq208, sf_bq131)

---

## Fix 4: GA360 Date-Shard Table Rewriting

**Files:** `rag_snow_agent/src/rag_snow_agent/prompting/sql_compiler.py`, `rag_snow_agent/src/rag_snow_agent/retrieval/schema_slice.py`

**Problem:** GA360 has 366 daily tables (`GA_SESSIONS_20160801` through `GA_SESSIONS_20170801`), each containing data for exactly one date. The index collapses these into one representative (`GA_SESSIONS_20170801`). When a question asks about July 2017, the LLM generates SQL querying only `GA_SESSIONS_20170801` (August 1st), which has zero July rows.

Successful instances (sf_bq001) work only when the LLM independently generates UNION ALL of 28+ daily tables — a fragile, non-deterministic approach producing 6000+ char SQL.

**Change:** Post-compilation SQL rewriting. After `compile_plan()` produces SQL, a new `rewrite_date_sharded_tables()` function:

1. Scans the compiled SQL for table references matching the date-shard pattern (`GA_SESSIONS_YYYYMMDD`)
2. Looks up the table's comment in the SchemaSlice (which contains the partition metadata: count, date range)
3. Extracts the date range from the SQL's WHERE clause (looks for `"date"` column comparisons like `>= '20170701'` AND `<= '20170731'`)
4. If a date range is found, replaces the single-table reference with a CTE that UNION ALLs all daily tables for that date range
5. If no explicit date range is found in the WHERE clause, falls back to the original single-table reference (no change)

The rewrite is applied as a post-processing step in `compile_plan()`, preserving the existing plan-based compilation logic. The rewrite only triggers for tables whose SchemaSlice comment contains `"Partitioned:"` and `"daily tables"`.

**Expected impact:** +3-4 instances (sf_bq010, sf_bq004, sf_bq270, sf_bq275)

---

## Combined Expected Impact

| Fix | Targeted Instances | Expected Gain |
|:----|---:|---:|
| API 400 retry | 2 | +2 |
| Join-graph guided retrieval | 6 | +3-4 |
| Geo model routing (gpt-5.4) | 7 | +2-4 |
| GA360 date-shard rewriting | 4 | +3-4 |
| **Total** | **19** | **+10-14** |

**Projected accuracy: 91-95/100**
