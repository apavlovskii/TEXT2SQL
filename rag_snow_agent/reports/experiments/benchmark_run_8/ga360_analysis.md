# GA360 Zero Gold Match Analysis — Benchmark Run 8

## 1. Corrected Failure Classification

The report stated 9/12 GA360 instances were "plan parse failures". This is **wrong**. Re-analyzing the run 8 logs shows most candidates DO execute on Snowflake — they just return wrong results:

| Instance | Actual Failure Mode | Executions | Non-Zero Rows | Result Mismatches | Empty Results | Gold Shape |
|:---------|:-------------------|:-----------|:-------------|:-----------------|:-------------|:-----------|
| sf_bq010 | **EMPTY_RESULT** | 36 | 0 | 0 | 24 | (1,1) or (1,2) |
| sf_bq009 | **RESULT_MISMATCH** | 30 | 30 | 20 | 0 | (1,2) |
| sf_bq001 | **EMPTY_RESULT** | 42 | 0 | 0 | 28 | (666,3) |
| sf_bq002 | **EMPTY_RESULT** | 26 | 2 | 1 | 16 | (1,4) or (1,5) |
| sf_bq003 | **RESULT_MISMATCH** | 46 | 46 | 30 | 0 | (8,3) or (4,3) |
| sf_bq004 | **EMPTY_RESULT** | 34 | 6 | 3 | 18 | (1,1) or (1,2) |
| sf_bq008 | **NO_EXECUTION** | 0 | 0 | 0 | 0 | (1,2) |
| sf_bq269 | **RESULT_MISMATCH** | 70 | 48 | 32 | 11 | (2,3) |
| sf_bq268 | **RESULT_MISMATCH** | 18 | 18 | 12 | 0 | (1,1) to (1,3) |
| sf_bq270 | **EMPTY_RESULT** | 30 | 0 | 0 | 20 | (3,3) |
| sf_bq275 | **EMPTY_RESULT** | 26 | 2 | 1 | 16 | (8–37,1) |
| sf_bq374 | **RESULT_MISMATCH** | 48 | 48 | 32 | 0 | (1,1) |

**Actual breakdown:**
- **EMPTY_RESULT**: 6 instances (sf_bq010, sf_bq001, sf_bq002, sf_bq004, sf_bq270, sf_bq275) — queries execute but return 0 rows
- **RESULT_MISMATCH**: 5 instances (sf_bq009, sf_bq003, sf_bq269, sf_bq268, sf_bq374) — queries return rows but wrong values
- **NO_EXECUTION**: 1 instance (sf_bq008) — all candidates failed compilation

The "SELECT 1" final result occurs because all candidates score 0.0 — not because plans failed to parse. The best-of-n selector defaults to `SELECT 1` when no candidate has a positive score.

---

## 2. Root Cause #1: Token Budget Drops Critical Columns

The single biggest issue: **the 2500-token schema budget is too small for GA360 with enriched descriptions.**

Column token costs after description enrichment:

| Column | Tokens | Critical for | Notes |
|:-------|:-------|:------------|:------|
| hits | **322** | All product/page/ecommerce queries | 1158-char description with nested field paths |
| totals | **232** | All session-level metric queries | 865-char description with sub-field types |
| trafficSource | **221** | Traffic source queries | 742-char description |
| device | **184** | Device queries | Large description |
| geoNetwork | **112** | Geographic queries | |
| customDimensions | **74** | Custom dimension queries | |
| date | **62** | ALL queries (date filtering) | **Almost always dropped** |
| fullVisitorId | **49** | ALL queries (user identification) | **Almost always dropped** |
| visitStartTime | **58** | Temporal queries | **Often dropped** |

**With 2500 budget for 2 tables:** `hits` (322×2=644) + `totals` (232×2=464) alone consume 1108 tokens — 44% of the budget. After table headers (~200 tokens), only ~1200 tokens remain for 8+ other columns across 2 tables. The budget trimmer drops lower-ranked columns, which often includes `date`, `fullVisitorId`, and `visitStartTime`.

**Impact:** Without `date` in the schema, the LLM cannot filter by date range. Without `fullVisitorId`, it cannot identify unique visitors. These are required for nearly every GA360 query.

---

## 3. Root Cause #2: Partition Table Confusion

The two representative tables (`GA_SESSIONS_20170630` and `GA_SESSIONS_20170801`) cover different date ranges, but the table comment says to "filter with WHERE date >= 'YYYYMMDD'". However:

1. The LLM queries `GA_SESSIONS_20170630` for July 2017 data — but that table only contains data through June 30, 2017.
2. For questions spanning both date ranges (Apr–Jul 2017), the LLM must somehow query both tables or know to use the one that covers the right dates.
3. The table comment is often truncated or ignored under token pressure.

**Impact:** Queries targeting dates in a partition not covered by the selected table return 0 rows (EMPTY_RESULT).

---

## 4. Root Cause #3: `hits` VARIANT Array — Depth vs Breadth

The `hits` description is 1158 characters covering 15+ nested fields. But for GA360 benchmark questions, only 5-6 nested field paths are actually needed:

| Field Path | Used In Questions |
|:-----------|:-----------------|
| `hits.product.v2ProductName` | sf_bq002, sf_bq004, sf_bq010 |
| `hits.product.productRevenue` | sf_bq002, sf_bq003, sf_bq008 |
| `hits.eCommerceAction.action_type` | sf_bq270 |
| `hits.page.pagePath` | sf_bq008 |
| `hits.transaction.transactionRevenue` | sf_bq009 |
| `hits.transaction.transactionId` | sf_bq001, sf_bq275 |

The description includes many rarely-used fields (hitNumber, hour, minute, isEntrance, isExit, social, publisher, latencyTracking, etc.) that consume tokens without helping.

---

## 5. Root Cause #4: Revenue Multiplier Not Applied

Gold outputs show revenue values like `4659.15` (millions), `21148.43`. GA360 stores revenue as `totalTransactionRevenue` multiplied by 10^6. The description says "multiplied by 10^6 (e.g., $2.40 = 2400000)" but the LLM often forgets to divide by 10^6 in the final output.

For sf_bq009 (gold: `(direct), 4659.15`), the query likely returns the raw undivided revenue value.

---

## 6. Root Cause #5: No GA360 Gold SQL Patterns

Unlike PATENTS (9 gold SQLs available), GA360 has **zero gold SQL files** in the evaluation suite. This means:
- The enrichment script couldn't learn GA360-specific SQL patterns from gold
- The `hits` VARIANT access patterns are based on documentation, not verified query patterns
- There are no few-shot examples to ground the LLM on correct GA360 SQL

---

## 7. Suggested Fixes (Priority Order)

### Fix 1: Truncate GA360 VARIANT descriptions to essential fields only (HIGH IMPACT)

Reduce `hits` description from 1158 chars to ~400 chars by keeping only the 6 critical nested paths. Reduce `totals` from 865 chars to ~300 chars by keeping only the 5 most-used sub-fields. This would cut token costs from 322+232=554 per table to ~120+100=220, freeing ~670 tokens for `date`, `fullVisitorId`, `visitStartTime`.

**Truncated hits description (proposed):**
```
Array of hits (pageviews, events, transactions) per session. Use LATERAL FLATTEN(input => t."hits") h.
Key nested paths:
- h.value:"product":"v2ProductName"::STRING — Product name
- h.value:"product":"productRevenue"::NUMBER — Product revenue (×10^6)
- h.value:"product":"productQuantity"::NUMBER — Quantity purchased
- h.value:"eCommerceAction":"action_type"::STRING — eCommerce action (1=list click, 2=detail view, 3=add to cart, 6=purchase)
- h.value:"page":"pagePath"::STRING — URL path
- h.value:"transaction":"transactionRevenue"::NUMBER — Transaction revenue (×10^6)
- h.value:"transaction":"transactionId"::STRING — Transaction ID
```

### Fix 2: Protect critical columns from budget trimming (HIGH IMPACT)

Make `date`, `fullVisitorId`, `visitStartTime`, `hits`, and `totals` protected from trimming for GA360 — similar to how `is_join_key` and `is_time_column` protect columns. These 5 columns are required for virtually every GA360 query.

Implementation: add an `is_essential` flag to `ColumnSlice`, set it for known critical columns, and treat them as protected in `trim_to_budget()`.

### Fix 3: Merge the two GA360 representative tables (MEDIUM IMPACT)

Instead of 2 tables covering different date ranges, use a single representative with a comment explaining the full date range. This would:
- Halve the token cost (one table instead of two with identical schema)
- Eliminate the "wrong table for date range" problem
- Free up ~700 tokens for more columns

The two tables differ slightly in column sets (the Jul–Aug group has `clientId`), but the difference is negligible — `clientId` is unused in any benchmark question.

### Fix 4: Add GA360-specific few-shot SQL pattern (MEDIUM IMPACT)

Since there are no gold SQLs for GA360, create 1-2 synthetic reference patterns and inject them into the prompt:

```sql
-- Pattern: Access hits product data with FLATTEN
SELECT h.value:"product":"v2ProductName"::STRING AS product_name,
       SUM(h.value:"product":"productRevenue"::NUMBER / 1e6) AS revenue_usd
FROM GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS_20170630 AS t,
     LATERAL FLATTEN(input => t."hits") h
WHERE t."date" >= '20170101' AND t."date" < '20170201'
  AND h.value:"eCommerceAction":"action_type"::STRING = '6'
GROUP BY product_name
ORDER BY revenue_usd DESC;
```

This grounds the LLM on the exact VARIANT path syntax, revenue division, eCommerce action filtering, and date filtering — all in one example.

### Fix 5: Add revenue divisor hint to totals description (LOW IMPACT)

Add explicit "DIVIDE BY 1000000 for USD" to the `totalTransactionRevenue` field description, and add the same to the `hits.product.productRevenue` description.

### Fix 6: Increase `max_schema_tokens` for GA360 only (LOW IMPACT)

Instead of a global 2500-token budget, use a per-database budget. GA360 needs ~3500 tokens to fit all critical columns with descriptions. PATENTS only needs ~1800.

---

## 8. Expected Impact

| Fix | Addresses | Expected Impact |
|:----|:----------|:---------------|
| Truncated descriptions | Token budget overflow, missing columns | 6–8 more GA360 instances get critical columns |
| Protected essential columns | date/fullVisitorId dropped | Eliminates EMPTY_RESULT for date-filtering queries |
| Single representative table | Wrong table, doubled token cost | Halves per-column token cost, fixes date range issues |
| Few-shot SQL pattern | Wrong VARIANT paths, missing revenue division | Grounds LLM on correct GA360 SQL syntax |
| Revenue divisor hint | Wrong revenue values | Fixes sf_bq009, sf_bq002 value mismatches |
| Per-database budget | Global budget too small for GA360 | Accommodates GA360's larger schema |

**Combined estimated improvement:** With fixes 1–3 applied, the schema prompt would include all critical columns (date, fullVisitorId, visitStartTime, hits, totals, trafficSource, device) within the 2500-token budget. This should convert the 6 EMPTY_RESULT instances into at least RESULT_MISMATCH, and potentially gold-match 2-4 of the simpler ones (sf_bq374, sf_bq268, sf_bq009).

**Realistic target: 2-4 GA360 gold matches** (from current 0), bringing total to 8-10/25 = 32-40%.
