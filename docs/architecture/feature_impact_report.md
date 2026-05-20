# SnowRAG-Agent: Feature Impact Report

> **Last updated:** 2026-05-21
> **Scope:** Benchmark Runs 8 → 9 → 10 (incremental development)
> **Accuracy trajectory:** 24 % → **92 % (Run 9, 25q, `gpt-5.4`)** → **87 % (Run 10, 100q over 20 databases, `gpt-5.4-mini`)**
> **Canonical ablation data:** [`docs/THESIS_EN.md` — Chapter 7](../THESIS_EN.md). The numbers in the Summary Matrix below are derived from the same 25-query controlled ablation.

---

## Impact Tier 1 — Game-Changers (each individually responsible for large accuracy gains)

### 1. Data Profiling + LLM-Generated Descriptions
**Impact: CRITICAL — enabled +68pp jump (Run 8→9), GA360 from 0% to 92%**

Offline pipeline (`scripts/profile_data.py`) extracts 100 sample rows per table from Snowflake, profiles each column (null rates, unique counts, value ranges, VARIANT structure), then uses GPT-5.4 to generate natural-language descriptions for every table and column. Descriptions are stored in `data/table_column_descriptions.json` and embedded into ChromaDB `schema_cards`.

**Why it matters:** The LLM no longer hallucinates VARIANT field paths, date formats, or column semantics — it reads exact names and access patterns from descriptions. Example: `assignee_harmonized: VARIANT ARRAY → LATERAL FLATTEN(assignee_harmonized => a) a.value:"name"::STRING`. Without this, every VARIANT query was a coin flip.

**Evidence:** GA360 went from 0% accuracy across all prior runs to 92% in Run 9. PATENTS went from 55% to 100%. On Run 10, 12 of 16 newly-profiled databases achieved 100% accuracy on first exposure.

---

### 2. Partition Table Collapsing
**Impact: CRITICAL — prerequisite for GA360/GA4 retrieval to work at all**

Merged 366 GA360 daily partition tables into 1 representative entry and 92 GA4 tables into 1. Groups by schema + base_name, takes union of all columns, keeps the latest partition name (which exists in Snowflake).

**Why it matters:** 366 near-identical tables completely polluted semantic search — the retrieval layer returned partition tables instead of columns. Collapsing to 1 entry eliminated 99% of retrieval noise and ensured all critical columns (hits, totals, date, fullVisitorId) appeared in prompts.

**Evidence:** GA360 was unreachable (0%) in every run before partition collapsing was introduced in Run 9.

---

### 3. Deterministic SQL Compiler (Plan→SQL)
**Impact: HIGH — foundational architecture choice, prevents entire categories of errors**

The LLM generates a structured `QueryPlan` (Pydantic model), which is then deterministically compiled to Snowflake SQL. The compiler handles consistent aliasing (t1, t2...), CTE formatting, LATERAL FLATTEN syntax, safe quoting, and identifier validation against the SchemaSlice.

**Why it matters:** Separates reasoning (LLM) from SQL formatting (deterministic). Eliminates syntax errors, inconsistent aliasing, and quote-style bugs that plague end-to-end LLM SQL generation. Every SQL output is reproducible and debuggable.

**Evidence:** The architecture enables the entire repair loop and Best-of-N pipeline. Without it, each candidate would have independent formatting bugs that compound with model variance.

---

### 4. VARIANT Sub-field Enrichment + ARRAY/OBJECT Classification
**Impact: HIGH — +45pp on PATENTS (55%→100%), enabled all VARIANT queries**

`_enrich_variant_fields()` queries ChromaDB for VARIANT_FIELD entries, classifies them as ARRAY (needs FLATTEN) vs OBJECT (colon access), and attaches field names to `ColumnSlice.variant_fields`. Uses `LATERAL FLATTEN + OBJECT_KEYS` fallback when `OBJECT_KEYS` alone fails.

**Why it matters:** Discovered 93 sub-fields for PATENTS (previously 0). Without classification, the LLM applies FLATTEN to OBJECTs or colon access to ARRAYs, both producing runtime errors.

**Evidence:** PATENTS: 55% → 100%. The combination with data profiling descriptions means the LLM has exact field paths and knows the correct access pattern for each VARIANT column.

---

### 5. Candidate Scoring Fix
**Impact: HIGH — prevented selector from choosing SELECT 1 over real SQL**

Changed `execution_success` logic: candidates that execute and return rows now score 100+ (execution success bonus + shape bonuses) instead of 0.0. Previously, gold-mismatch candidates scored identically to errors, so the selector defaulted to `SELECT 1`.

**Why it matters:** A critical bug — the system was generating correct SQL in candidates but then selecting the wrong one. Fixing this alone unlocked multiple gold matches that were already being generated but discarded.

**Evidence:** Multiple instances in Run 9 where correct SQL was among the 8 candidates but would have been discarded under old scoring.

---

## Impact Tier 2 — Significant Contributors (meaningful accuracy gains or error prevention)

### 6. Increased Retrieval Budget
**Impact: MEDIUM-HIGH — ensured all critical columns present in prompts**

`max_schema_tokens: 10000` (was 2500), `top_k_columns: 100` (was 25), `top_k_tables: 25` (was 8).

**Why it matters:** With the old budget, essential columns like `hits`, `totals`, `date`, `fullVisitorId` were trimmed from GA360 prompts. Counterintuitively, more tokens led to higher accuracy — the increased cost per prompt was offset by fewer repair iterations.

---

### 7. LATERAL FLATTEN Compiler Support
**Impact: MEDIUM-HIGH — enabled VARIANT array queries without LLM writing raw SQL**

`PlanFlatten` model in plan_schema.py with deterministic compilation to `LATERAL FLATTEN(input => alias) alias.value:"field"::TYPE`. Stable alias generation, integrated into CTE pipelines.

**Why it matters:** FLATTEN syntax is the most common source of LLM SQL errors on Snowflake. Deterministic compilation eliminates this entire error class.

---

### 8. CTE Pipeline Compiler
**Impact: MEDIUM-HIGH — enabled multi-step queries**

`PlanCTE` model with multiple CTE stages, compiled to `WITH cte1 AS (...), cte2 AS (...) SELECT ...` with ordered dependency resolution.

**Why it matters:** Complex analytical queries (rolling averages, filtered aggregations, multi-step joins) require CTEs. Without compiler support, these fell back to LLM SQL generation with high error rates.

---

### 9. Plan Retry + LLM SQL Fallback
**Impact: MEDIUM — recovery mechanism for compiler failures**

When `compile_plan()` produces `SELECT 1` (empty `selected_tables`): (a) retries with feedback telling the LLM to include tables, then (b) falls back to direct LLM SQL generation bypassing the deterministic compiler.

**Why it matters:** Safety net that catches plan-parse failures and prevents entire instances from producing only `SELECT 1` candidates.

---

### 10. Revenue ÷10^6 Domain Hint
**Impact: MEDIUM — fixed 3 specific GA360 instances**

Added to `_SNOWFLAKE_GUIDANCE`: "GA360 revenue fields are stored multiplied by 10^6. ALWAYS divide by 1000000 to get USD values."

**Why it matters:** A one-line hint that fixed sf_bq009, sf_bq002, sf_bq003 — values were off by exactly 10^6 without it. Demonstrates high ROI of targeted domain knowledge.

---

### 11. Best-of-N with Multi-Signal Selection (N=8)
**Impact: MEDIUM — safety net, especially for ambiguous queries**

Generates 8 candidates using diverse prompt strategies (default, join_first, metric_first, time_first, flatten_first, cte_first), executes and repairs each, then scores using execution success, repair penalties, shape alignment, and verifier probability.

**Why it matters:** Multiple independent attempts increase the probability that at least one candidate is correct. Strategy diversity ensures different reasoning paths are explored.

---

### 12. Universal Pydantic Type Coercion
**Impact: MEDIUM — eliminated plan parse failures**

`_CoercingBase` base class using `model_validator(mode="before")` automatically coerces LLM-generated integers/booleans to strings in all plan models.

**Why it matters:** LLMs inconsistently output `1` vs `"1"` or `true` vs `"true"`. Without coercion, ~15% of plans failed to parse in Run 7, causing a 0% accuracy regression.

---

## Impact Tier 3 — Incremental Improvements (measurable but smaller contributions)

### 13. Hybrid Retrieval (Dense + Lexical + RRF)
**Impact: MODERATE — better recall than pure embedding search**

Dense embedding similarity + lexical keyword matching + Reciprocal Rank Fusion (k=60). Protected columns (join keys, time columns) resist trimming.

---

### 14. Error Taxonomy + Targeted Repair Strategies
**Impact: MODERATE — efficient repair iterations**

8-category error classifier (object_not_found, invalid_identifier, aggregation_error, etc.) with error-specific repair templates. Max 2-4 repairs with delta-only prompts (not full conversation history).

---

### 15. Trace Memory (Few-Shot from Prior Successes)
**Impact: LOW-MODERATE — helpful for repeated patterns, limited first-pass impact**

After each successful solve, persists a compact TraceRecord. Before plan generation, retrieves similar instructions on same db_id and injects as few-shot context (max 800 tokens).

---

### 16. Learned Verifier / Reranker
**Impact: LOW-MODERATE — marginal improvement in candidate selection**

LogisticRegression trained from prior run logs. 20+ tabular features per candidate. Weighted integration into selector scoring (verifier_weight=20.0). Contributed ~3pp improvement in uncertain cases.

---

### 17. Sample Records in Prompts
**Impact: LOW-MODERATE — helps with format understanding**

2-5 sample rows per table as compact JSON. Shows actual data values, column formats, VARIANT structure.

---

### 18. Semantic Layer Annotations
**Impact: LOW — useful but largely redundant with data profiling descriptions**

Heuristic annotations: primary_time_column, metric_candidate, dimension_candidate, nested_container_column. Redundant when LLM-generated descriptions already contain richer semantics.

---

### 19. Gold Verification Path Fix
**Impact: MEASUREMENT ONLY — didn't improve accuracy, but revealed it**

Corrected `--gold_dir` path. Runs 4-7 compared against non-existent files, producing false negatives. The agent may have been performing better than reported in those runs.

---

## Canonical Ablation Results (25-query subset, Run 9 baseline = 92 %)

Each row disables a single component and re-runs the full pipeline. Source: thesis Chapter 7.1, three runs with fixed seeds, median values (σ ≤ 2 pp).

| Configuration | Accuracy | Δ vs Run 9 | Token change | Tier |
|:--------------|--------:|-----------:|-------------:|:----:|
| **Run 9 — full configuration** | **92 %** | — | baseline | — |
| Without LLM-profiled column descriptions | 24 % | **−68 pp** | −5 % | **A** |
| Without partition collapsing (GA360) | 8 % (GA360: 0 %) | **−84 pp** for GA360 | +120 % | **A** |
| Base LLM (no RAG, full DDL in the prompt) | 36 % | −56 pp | +400 % | **A** |
| Without VARIANT field enrichment | 47 % | −45 pp | −10 % | B |
| Without `LATERAL FLATTEN` in the compiler | 53 % | −39 pp | +20 % | B |
| Without the deterministic Plan → SQL compiler | 56 % | −36 pp | +35 % | B |
| Without Best-of-N (N=1) | 64 % | −28 pp | −78 % | C |
| Without Best-of-N repairs (`max_repairs=0`) | 71 % | −21 pp | −60 % | C |
| Without 1-hop neighbour expansion (join graph) | 80 % | −12 pp | −3 % | C |
| Without `sample_records` | 82 % | −10 pp | −12 % | D |
| Without `semantic_cards` | 84 % | −8 pp | −5 % | D |
| Without `trace_memory` (few-shot from history) | 88 % | −4 pp | −2 % | D |

**Tier legend** — A: >50 pp drop (must-have); B: 30–50 pp (high impact); C: 10–30 pp (significant); D: <10 pp (refinement).

## Summary Matrix (qualitative impact across the development trajectory)

| # | Feature | Impact | Quantitative contribution | Introduced |
|---|---------|--------|----------------------|------------|
| 1 | Data Profiling + LLM Descriptions | CRITICAL | **−68 pp** when removed; GA360: 0 → 92 % | Run 9 |
| 2 | Partition Table Collapsing | CRITICAL | **−84 pp on GA360** when removed; prerequisite for temporal-shard domains | Run 9 |
| 3 | Deterministic SQL Compiler | HIGH | **−36 pp** when removed; foundational error-class eliminator | Run 1 |
| 4 | VARIANT Enrichment + Classification | HIGH | **−45 pp** when removed; PATENTS: 55 → 100 % | Run 9 |
| 5 | LATERAL FLATTEN Compiler | HIGH | **−39 pp** when removed; eliminates the single most error-prone Snowflake construct | Run 8 |
| 6 | Best-of-N (N=8) Diversification | HIGH | **−28 pp** when reduced to N=1; safety net for ambiguous queries | Run 5 |
| 7 | Best-of-N Repair Loop (max_repairs=4) | MEDIUM-HIGH | **−21 pp** when disabled; ~80 % of fixes land in iterations 1–2 | Run 5 |
| 8 | Candidate Scoring Fix | HIGH | Unlocked discarded-but-correct SQL; unmeasured Δ but observable on Run 9 instances | Run 9 |
| 9 | Increased Retrieval Budget | MEDIUM-HIGH | Ensured column completeness (`max_schema_tokens 2500 → 10000`) | Run 9 |
| 10 | CTE Pipeline Compiler | MEDIUM-HIGH | Enabled multi-step analytical queries | Run 8 |
| 11 | 1-hop Join-graph Expansion | MEDIUM | **−12 pp** when removed; recovers tables missed by primary retrieval | Run 8 |
| 12 | Sample Records in Prompts | LOW-MODERATE | **−10 pp** when removed | Run 9 |
| 13 | Plan Retry + LLM Fallback | MEDIUM | Recovery path when `compile_plan()` produces `SELECT 1` | Run 9 |
| 14 | Revenue ÷10^6 Domain Hint | MEDIUM | Fixed 3 specific GA360 instances (sf_bq002/003/009) | Run 9 |
| 15 | Pydantic Type Coercion | MEDIUM | Eliminated parse failures (~15 % of Run 7 plans) | Run 9 |
| 16 | Hybrid Retrieval (Dense + Lexical + RRF) | MODERATE | Better recall than pure dense; k = 60 | Run 3 |
| 17 | Error Taxonomy + Targeted Repair | MODERATE | Efficient repair loops; 8 error categories | Run 5 |
| 18 | `semantic_cards` annotations | LOW | **−8 pp** when removed; largely redundant with profiling | Run 7 |
| 19 | `trace_memory` (few-shot from prior runs) | LOW | **−4 pp** when removed | Run 7 |
| 20 | Learned Verifier / Reranker | LOW-MODERATE | ~3 pp marginal improvement in candidate selection | Run 9 |
| 21 | Gold Verification Path Fix | MEASUREMENT | Revealed true accuracy in Run 8; no accuracy delta itself | Run 8 |

---

## Key Insight

No single technique achieved the 24 % → 92 % jump. It was the **synergistic stack**: data profiling gave the LLM correct column knowledge (−68 pp if removed), partition collapsing made retrieval viable on temporally-sharded sources (−84 pp on GA360), the deterministic compiler eliminated formatting bugs (−36 pp), VARIANT enrichment and LATERAL FLATTEN compilation enabled complex access patterns (−45 pp and −39 pp), and the candidate-scoring fix ensured the best candidate was actually selected. Remove any one Tier-A or Tier-B component and accuracy drops substantially.

A separate, often-overlooked finding: **the structural metadata selection matters more than the raw model capability.** Switching to the base LLM with the full DDL in the prompt (no RAG) drops accuracy to 36 % — the same level as the ReFoRCE agent with its full multi-step pipeline. This confirms the thesis hypothesis that **what you put into the context window dominates the choice of model** on industrial-scale schemas.

The 92 % → 87 % scaling from 25 to 100 queries (Run 9 → Run 10) validates that the architecture generalizes: 12 of 16 newly-profiled databases achieved 100 % accuracy on first exposure, with failures concentrated in **geospatial queries** (a known compiler gap) and **complex multi-step reasoning** (a model-capability gap when downgrading from `gpt-5.4` to `gpt-5.4-mini` for cost efficiency).

---

## Known Gaps (Remaining 13% on Run 10)

| Gap | Failures | Fix Path |
|-----|----------|----------|
| Geospatial queries (ST_DISTANCE, ST_DWITHIN, etc.) | 7/13 | Add geospatial ops to compiler |
| Complex multi-step reasoning (gpt-5.4-mini limit) | 3/13 | Use gpt-5.4 for hard queries |
| Large schema noise (296 tables) | 2/13 | Better retrieval pruning |
| API errors | 1/13 | Retry logic |
