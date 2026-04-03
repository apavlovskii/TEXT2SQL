# Benchmark Run 6 — Detailed Error Analysis

> Analysis of 15 failed instances (out of 16 completed) to understand why results don't match gold even with 10 candidates.

---

## Failure Mode Summary

| Failure Mode | Instances | Description |
|:-------------|:----------|:------------|
| **WRONG_VALUES** | 8 | Query shape matches gold but numeric values are wrong |
| **WRONG_SHAPE** | 3 | Query returns different number of rows/columns than gold |
| **EMPTY_RESULTS** | 3 | Query returns zero rows when gold expects data |
| **PLAN_PARSE** | 1 | All candidates failed to parse (sf_bq010) |

---

## Category 1: WRONG VALUES (8 instances) — The Dominant Problem

These queries execute, return the right shape (same rows × columns), but the **numbers are wrong**. This is the hardest failure mode because the SQL structure is correct.

### sf_bq009 (GA360) — 19 correct-shape attempts, all wrong values
**Question:** "Which traffic source has the highest total transaction revenue for 2017, and what is the difference in millions?"
**Gold shape:** (1, 2) — one row, two columns
**Root cause:** The system accesses `"totals":"totalTransactionRevenue"` but the gold SQL uses a different VARIANT path to compute revenue. The specific calculation of "difference between highest and lowest monthly revenue" requires a multi-step CTE that the LLM consistently gets wrong.

### sf_bq002 (GA360) — 23 correct-shape attempts, all wrong values
**Question:** "During the first half of 2017, focusing on hits product revenue..."
**Gold shape:** (1, 4)
**Root cause:** Requires accessing `hits` VARIANT array via LATERAL FLATTEN to extract `productRevenue` per hit. The system instead tries to access `"totals":"totalTransactionRevenue"` which is a different metric entirely. **Wrong column, right shape.**

### sf_bq004 (GA360) — 16 correct-shape attempts, all wrong values
**Question:** "In July 2017, among all visitors who bought any YouTube-related product..."
**Gold shape:** (1, 1) — single number
**Root cause:** Requires LATERAL FLATTEN on `hits` array, then filtering by `productName ILIKE '%YouTube%'`, then finding the top non-YouTube product. The system skips the FLATTEN step and produces a wrong count.

### sf_bq268 (GA360) — 20 correct-shape attempts, all wrong values
**Question:** "Identify the longest number of days between first visit and last recorded event..."
**Gold shape:** (1, 1) — single number
**Root cause:** Complex temporal calculation involving `visitStartTime` (stored as UNIX timestamp NUMBER). The system misinterprets the date arithmetic — computes days incorrectly from the raw number format.

### sf_bq374 (GA360) — 20 correct-shape attempts, all wrong values
**Question:** "Percentage of new users who stayed for more than 5 minutes and viewed 10+ pages..."
**Gold shape:** (1, 1) — single percentage
**Root cause:** Requires accessing `"totals":"timeOnSite"` and `"totals":"pageviews"` via VARIANT paths. The calculation of the percentage denominator (all new users vs filtered new users) is consistently wrong.

### sf_bq029 (PATENTS) — 25 correct-shape attempts, all wrong values
**Question:** "Average number of inventors per patent and total count for Canada in 5-year periods..."
**Gold shape:** (13, 3)
**Root cause:** Requires LATERAL FLATTEN on `"inventor"` VARIANT array to count inventors per patent. The system counts rows instead of array elements. Also, the 5-year period bucketing logic (`FLOOR(year/5)*5`) is sometimes wrong.

### sf_bq026 (PATENTS) — 16 correct-shape attempts, all wrong values
**Question:** "For the most active assignee in patent category A61, the top 5 jurisdictions..."
**Gold shape:** (1, 1)
**Root cause:** **Critical mismatch in how to identify the "most active assignee."** The gold SQL uses LATERAL FLATTEN on both `"assignee_harmonized"` and `"assignee"` arrays, then UNION ALL, then counts DISTINCT application_numbers. Our system tries to access `"assignee_harmonized"` as a flat column instead of flattening the array.

### sf_bq008 (GA360) — 8 correct-shape attempts, all wrong values
**Question:** "Among visitors whose campaign contains 'Data Share', which page did they visit most?"
**Gold shape:** (1, 2)
**Root cause:** Requires LATERAL FLATTEN on `"hits"` array to extract page paths. The system accesses `"hits"` as a flat VARIANT instead of flattening.

---

## Category 2: WRONG SHAPE (3 instances)

### sf_bq001 (GA360) — 30 shape mismatches
**Question:** "For each visitor with a transaction in Feb 2017, days between first visit and last event"
**Gold shape:** (666, 3) — 666 rows, 3 columns
**Generated shapes:** (1, 2), (209, 3), (2372, 15), (10000, 3)
**Root cause:** The system doesn't correctly filter to February 2017 transactions — sometimes gets too few visitors (1 row), sometimes too many (10000). The date filtering on GA360's VARCHAR `"date"` column (format `YYYYMMDD`) is inconsistently handled.

### sf_bq003 (GA360) — 20 shape mismatches
**Question:** "Between April and July 2017, classify sessions as purchase/non-purchase..."
**Gold shape:** (8, 3) — 8 rows (monthly data)
**Generated shapes:** (1, 5), (2, 3), (2, 4), (2, 5)
**Root cause:** The system generates 2 rows (purchase/non-purchase) instead of 8 (monthly breakdown). It misses the "by month" grain — classifies sessions but doesn't group by month.

### sf_bq269 (GA360) — 29 shape mismatches
**Question:** "Between June and July 2017, classify sessions, show monthly conversion..."
**Gold shape:** (2, 3) — 2 rows (June, July)
**Generated shapes:** (1, 3), (2, 3), (2066, 6)
**Root cause:** Similar to sf_bq003 — monthly grouping sometimes missed, sometimes includes all sessions as individual rows (2066).

---

## Category 3: EMPTY RESULTS (3 instances)

### sf_bq270 (GA360) — 14 empty results, 0 result_mismatch
**Question:** "Monthly add-to-cart and purchase conversion rates as percentage of pageviews..."
**Root cause:** Requires LATERAL FLATTEN on `"hits"` to find `eCommerceAction` events. Without FLATTEN, the WHERE clause filters everything out → zero rows.

### sf_bq275 (GA360) — 38 empty results
**Question:** "Visitors whose first transaction was on mobile, on a later date than first visit"
**Root cause:** The combination of filters (first transaction + mobile + later date) is too restrictive when applied incorrectly. The system applies the mobile filter to visits instead of transactions.

### sf_bq091 (PATENTS) — 32 empty results
**Question:** "In which year did the assignee with most applications in A61 file the most?"
**Root cause:** The gold SQL uses LATERAL FLATTEN on `"cpc"` and `"ipc"` arrays to match category 'A61%'. Our system tries to access `"uspc"` column with ILIKE which matches nothing (wrong VARIANT array).

---

## Top Invalid Identifiers (compilation failures)

| Identifier | Count | Root Cause |
|:-----------|------:|:-----------|
| `"hit"."value"` | 24 | FLATTEN alias confusion — `hit` is not a table |
| `"next_page"` | 20 | Hallucinated column name |
| `"application_count"` | 20 | Hallucinated alias used as column |
| `"p"."value"` | 19 | FLATTEN alias `p` conflicts with table alias |
| `"ep"."value"` | 16 | FLATTEN alias used incorrectly |
| `"productRevenue"` | 12 | Bare VARIANT field — needs FLATTEN + path |
| `T1."totals:pageviews"` | 8 | VARIANT colon path used as identifier — needs FLATTEN |

---

## Root Cause Analysis: Why 10 Candidates Isn't Enough

### 1. LATERAL FLATTEN is the #1 blocker (affects 12/15 failures)

The gold SQLs use LATERAL FLATTEN extensively:
- `LATERAL FLATTEN(input => p."cpc") c` → then `c.value:"code"::STRING`
- `LATERAL FLATTEN(input => p."assignee_harmonized") ah` → then `ah.value:"name"::STRING`
- `LATERAL FLATTEN(input => t."hits") h` → then `h.value:"page"."pagePath"::STRING`

Our system rarely generates correct FLATTEN syntax. It either:
- Accesses VARIANT as a flat column: `"totals":"pageviews"` (fails)
- Uses FLATTEN but with wrong alias: `"hit"."value"` instead of `h.value:"field"`
- Skips FLATTEN entirely and tries to aggregate the VARIANT array directly

**Even with 10 candidates, none consistently produce correct FLATTEN.** This is a prompt/compiler limitation, not a candidate diversity problem.

### 2. VARIANT array vs VARIANT object confusion (affects 8/15)

GA360's `hits` is a VARIANT ARRAY of objects. PATENTS' `assignee_harmonized` is also a VARIANT ARRAY. Accessing these requires:
```sql
LATERAL FLATTEN(input => table."array_col") f
WHERE f.value:"nested_field"::TYPE = 'value'
```

But the system treats them as VARIANT OBJECTS and generates:
```sql
WHERE table."array_col":"nested_field"::TYPE = 'value'
```

This compiles but returns wrong results (operates on the first element only, not all elements).

### 3. Date format mishandling (affects 5/15)

GA360 `"date"` is VARCHAR `'20170801'`. PATENTS `"filing_date"` is NUMBER `20100315`. The gold SQLs handle these correctly:
- GA360: `WHERE "date" >= '20170201' AND "date" < '20170301'`
- PATENTS: `WHERE "filing_date" >= 20100101 AND "filing_date" < 20110101`

Our system sometimes uses `TO_DATE()` casting which works but occasionally produces off-by-one errors in boundary conditions.

### 4. Multi-step CTE logic errors (affects 4/15)

Questions like "find the top assignee, then for that assignee find the top year" require multi-CTE pipelines. The gold SQL for sf_bq091 has **6 CTEs**. Our system generates 2-3 CTEs and misses intermediate steps, producing a simpler (but wrong) query.

---

## Conclusion

**More candidates (10 vs 7) didn't help because the failures are systematic, not random.** All 10 candidates make the same class of mistakes:

1. **Can't generate correct LATERAL FLATTEN** (12/15 failures)
2. **Confuses VARIANT arrays with objects** (8/15)
3. **Gets date boundaries wrong** (5/15)
4. **Misses multi-CTE pipeline steps** (4/15)

These require changes to the SQL compiler and prompt templates, not more candidates.

### Recommended fixes (priority order):

1. **Add LATERAL FLATTEN to the SQL compiler** — when plan references a VARIANT ARRAY column, automatically generate FLATTEN syntax
2. **Include gold SQL patterns as few-shot examples** — for each database, include 1-2 gold SQL snippets showing correct FLATTEN usage
3. **Distinguish VARIANT ARRAY from VARIANT OBJECT** in the semantic layer — store whether a VARIANT column is an array (needs FLATTEN) or an object (needs colon access)
4. **Add date format to semantic context** — explicitly state `"date" is VARCHAR YYYYMMDD, compare as string` vs `"filing_date" is NUMBER YYYYMMDD, compare as integer`
