# Architecture Update (2026-03-29 21:50)

## Objectives

1. Add empty-result repair guidance.
2. Improve memory-management with join conditions and access patterns.
3. Implement question decomposition before final SQL generation.
4. Implement Automated Semantic Layer Induction.
5. Use semantic context and sample rows during candidate SQL generation.

---

## 1) Empty-Result Simplification Feedback

When a query returns zero rows, add explicit feedback to the repair prompt:

> Since output is empty, please simplify some conditions. Consider:
> - Relaxing date range filters — check if dates are stored as NUMBER (YYYYMMDD) or VARCHAR, not DATE type.
> - Removing restrictive WHERE clauses that may filter out all rows.
> - Checking if column values match expected format (e.g., country_code='US' vs 'United States').
> - Using ILIKE instead of = for string matching.
> - Verifying VARIANT field access paths — ensure colon syntax is correct.

Implemented in `refiner.py`: a dedicated `_build_empty_result_repair_prompt` handles `empty_result` errors separately from `result_mismatch`.

---

## 2) Memory-Management Improvements

### Existing behavior (verified)

- Gold-match-aware trace persistence: `_persist_trace` fires only when `result.success=True`, which means gold-matched when `--gold_dir` is set.
- `TraceRecord` includes `tables_used` and `key_columns_used`.

### Amendments

- Added `join_conditions: list[str]` to `TraceRecord` — stores actual `table.col = table.col` pairs from successful plans.
- Added `column_access_patterns: list[str]` — stores VARIANT access patterns like `"trafficSource":"source"::STRING` extracted from successful SQL via regex.
- These are included in the memory context prompt so the LLM can reuse exact quoting and access patterns from prior successes.

---

## 3) Question Decomposition Before Final SQL

### Output model

`QuestionDecomposition` Pydantic model:

```python
class QuestionDecomposition(BaseModel):
    temporal_scope: str | None       # "year 2017", "January 7, 2021 23:59:59"
    temporal_grain: str | None       # "month", "day", "year"
    filters: list[str]               # "campaign name contains 'Data Share'"
    cohort_conditions: list[str]     # "visitors who made at least one transaction"
    target_entity: str | None        # "distinct pseudo users", "products"
    target_grain: str | None         # "per visitor", "per product"
    measures: list[str]              # "COUNT DISTINCT", "SUM(revenue)"
    set_operations: list[str]        # "MINUS", "excluding", "but not"
    ranking: str | None              # "top 5", "highest"
    grouping: list[str]              # "by month", "for each category"
    nested_fields: list[str]         # "trafficSource.source", "hits.product"
    expected_shape: str | None       # "single number", "table with 12 rows"
    notes: list[str]                 # any ambiguities or special requirements
```

### LLM-based extraction

A single LLM call with a structured prompt:
- Input: the natural language question + schema column hints from semantic layer
- Output: JSON matching `QuestionDecomposition`
- Cost: ~1-3K tokens per instance

### Semantic layer integration

The decomposition module uses the `SemanticProfile` to resolve extracted subgoals to specific columns:

- If decomposition extracts `temporal_scope: "year 2017"` and semantic layer says `primary_time_column: "date" (VARCHAR, format YYYYMMDD)`, the resolved constraint becomes `WHERE "date" BETWEEN '20170101' AND '20171231'`
- If decomposition extracts `measures: ["total transaction revenue"]` and semantic layer says `metric_candidate: "totals":"totalTransactionRevenue" (VARIANT field)`, the resolved measure becomes `SUM("totals":"totalTransactionRevenue"::NUMBER)`

### Post-SQL verification

Each subgoal is independently verifiable after SQL generation:
- "Does this SQL contain a date filter matching the temporal scope?" → check for WHERE clause with date column
- "Does this SQL GROUP BY the correct grain?" → check GROUP BY clause
- "Does this SQL use the correct metric column?" → check aggregation function targets
- If any check fails → trigger targeted repair before execution

### Pipeline placement

Inserted between schema retrieval and plan generation:

```
schema retrieval → semantic context retrieval → question decomposition → plan generation → SQL compilation
```

### Prompt integration

`decomposition_context: str | None` parameter added to `build_plan_prompt`. Rendered as a structured block:

```
Question decomposition:
  Temporal: year 2017 → column "date" (YYYYMMDD format)
  Measure: total transaction revenue → SUM("totals":"totalTransactionRevenue"::NUMBER)
  Filter: visitors who made at least one transaction → WHERE "totals":"transactions" > 0
  Grouping: by traffic source → GROUP BY "trafficSource":"source"::STRING
  Output: single row with source name + revenue difference
```

### Tests

- `test_question_decomposition.py` — model creation, rendering, mock LLM decomposition
- `test_subgoal_validator.py` — temporal scope detection, aggregation matching, GROUP BY, ranking checks

---

## 4) Automated Semantic Layer Induction

### Problem

Current accuracy is much lower than execution accuracy. The main failure mode is **semantic mismatch**: wrong metric column, wrong date field, wrong nested/VARIANT field access, wrong filter semantics, wrong grouping grain.

A **database-specific semantic layer** is **automatically induced** — no manual database knowledge required.

### Goal

Build an automated semantic layer induction pipeline that infers semantic hints for each database from:
1. Schema metadata (column names, types, comments)
2. External docs (markdown files from Spider2)
3. Lightweight data profiling probes (sample values, min/max, distinct counts)
4. Successful prior traces (commonly used columns, joins, access patterns)

The semantic layer is: compact, confidence-scored, stored in Chroma, retrievable alongside schema cards, usable by planner and verifier.

### Modules under `rag_snow_agent/src/rag_snow_agent/semantic_layer/`

#### 4.1) Semantic models (`models.py`)

Pydantic models:

- **`SemanticFact`**: `fact_type`, `subject`, `value`, `confidence: float`, `evidence: list[str]`, `source: list[str]` (metadata | docs | probes | traces)
- **`SemanticCard`**: `id`, `document`, `metadata` — for Chroma storage
- **`SemanticProfile`**: `db_id`, `time_columns`, `metric_candidates`, `dimension_candidates`, `nested_field_patterns`, `join_semantics`, `filter_value_hints`, `sample_rows`, `column_stats` — each a list of SemanticFact

#### 4.2) Infer from metadata (`infer_from_metadata.py`)

Deterministic heuristics based on column name, type, comments, nullability, join edge presence:
- `primary_time_column`: columns named `event_date`, `created_at`, `visitStartTime` with DATE/TIMESTAMP type
- `date_format_pattern`: NUMBER columns with names containing `date` → likely YYYYMMDD integer (e.g., PATENTS `filing_date` is NUMBER `20100315`). VARCHAR columns with date-like names → likely date string (e.g., GA360 `date` is VARCHAR `'20170801'`). Include detected format pattern as fact value.
- `metric_candidate`: numeric columns named `amount`, `revenue`, `count`, `duration`, `score`
- `dimension_candidate`: string/category fields with names like `source`, `status`, `country`
- `nested_container_column`: VARIANT/OBJECT/ARRAY columns
- `identifier_column` / `join_key_candidate`: columns ending in `_id`, `id`, `key`

Returns a `SemanticProfile`.

#### 4.3) Infer from docs (`infer_from_docs.py`)

Extracts semantic hints from external markdown docs (referenced by Spider2's `external_knowledge` field):
- Canonical field meanings, date semantics, metric definitions
- Nested field access patterns, common joins

Uses regex + keyword extraction (no LLM calls). Returns `SemanticFact`s with `source=docs`.

#### 4.4) Infer from probes (`infer_from_probes.py`)

Lightweight Snowflake probes to validate/refine semantics:
- Top 5 distinct values for candidate filter columns → `fact_type=sample_values`
- Min/max for time columns (confirms date range) → `fact_type=column_stats`
- Distinct count for categorical columns → `fact_type=column_stats`
- Null ratio checks → `fact_type=column_stats`
- 5 sample rows per table → `fact_type=sample_rows`
- Sampling from VARIANT/nested columns → `fact_type=nested_field_patterns`

```python
infer_semantics_from_probes(db_id, executor, metadata, max_probe_budget=10)
```

**Probe budget:** `max_probe_budget=10` means 10 probes **per unique table schema**, not per database. After de-duplicating GA360's daily tables (~1 unique schema), 10 probes covers it. For PATENTS with 3 distinct tables, 10 probes per table = 30 total, manageable.

All probe-derived facts carry confidence and evidence. Tables are de-duplicated before probing.

Data sourcing clarification:
- **INFORMATION_SCHEMA** provides: row_count, nullability (available without querying data)
- **Lightweight probes** provide: sample values, min/max, distinct counts, sample rows (require actual queries)

#### 4.5) Infer from traces (`infer_from_traces.py`)

Extracts patterns from successful trace memory:
- Commonly used date fields, metric columns, join paths
- Recurring nested-field access expressions (e.g., `"trafficSource":"source"::STRING`)
- Recurring aggregation patterns

```python
infer_semantics_from_traces(db_id, trace_store, top_k=100)
```

Returns `SemanticFact`s with `source=traces`.

#### 4.6) Merger (`merge.py`)

```python
merge_semantic_facts(db_id, metadata_facts, doc_facts, probe_facts, trace_facts) -> SemanticProfile
```

Rules:
- Merge equivalent facts, combine evidence and sources
- Raise confidence when multiple sources agree: `max(confidences)`
- Lower confidence for conflicting signals: `min(confidences)`
- Deterministic and explainable logic

Also:
```python
render_semantic_profile_for_prompt(profile, max_tokens=800) -> str
```
Compact prompt-safe semantic context: top high-confidence facts only for time fields, metrics, grouping dimensions, nested-field hints, filter value hints, sample rows.

#### 4.7) Chroma persistence (`store.py`)

Collection: `semantic_cards`

Each card includes: `db_id`, `object_type="semantic"`, `fact_type`, `subject`, `confidence`, `source`, `token_estimate`.

Methods: `upsert_semantic_profile(profile)`, `query_semantic_cards(db_id, instruction, top_k=10)`.

**Collection count:** 4 total (`schema_cards`, `trace_memory`, `snowflake_syntax`, `semantic_cards`). Retrieval issues a unified query across schema + semantic cards.

#### 4.8) Builder CLI (`build_semantic_layer.py`)

```bash
uv run python -m rag_snow_agent.semantic_layer.build_semantic_layer \
  --db_id GA360 --credentials snowflake_credentials.json
```

Steps: load metadata → infer from metadata → infer from docs → run probes → infer from traces → merge → persist → print summary.

#### 4.9) Retrieval integration (`retrieval/semantic_retriever.py`)

```python
retrieve_semantic_context(db_id, instruction, top_k=8)
```

At query time, retrieves semantic cards alongside schema cards and passes to plan generation, SQL generation, and verifier.

#### 4.10) Planner integration

`prompting/prompt_builder.py` and `agent/plan_sql_pipeline.py` include semantic context in prompts. Bounded: only top relevant facts, not full profile.

#### 4.11) Debug CLI (`debug_semantic_profile.py`)

```bash
uv run python -m rag_snow_agent.semantic_layer.debug_semantic_profile \
  --db_id GA360 --query "Which traffic source has the highest total transaction revenue for 2017?"
```

Prints top semantic facts, retrieved facts for query, rendered prompt block.

### Config

```yaml
semantic_layer:
  enabled: true
  collection_name: semantic_cards
  max_prompt_tokens: 800
  max_probe_budget: 10   # per unique table schema
  min_confidence: 0.45
  retrieval_top_k: 8
```

### Tests

- `test_infer_from_metadata.py` — synthetic schemas → correct fact types, including date_format_pattern for NUMBER/VARCHAR date columns
- `test_merge_semantic_facts.py` — multi-source merge + confidence logic
- `test_render_semantic_profile.py` — token budget respected
- `test_semantic_store.py` — Chroma round-trip with temp dir

### Implementation notes

- **Probe budget**: GA360 has 366 tables but ~1 unique schema after de-duplication. Probe only unique schemas; 10 probes per unique schema.
- **Docs inference**: Regex/keyword-only for v1. LLM summarization is an optional future enhancement.
- **Confidence calibration**: Simple rules (`max` if agree, `min` if conflict). Tune after benchmark data available.
- **date_format_pattern**: Critical for PATENTS (NUMBER dates like `20100315`) and GA360 (VARCHAR dates like `'20170801'`). Without this, the LLM doesn't know to use `TO_DATE(col::VARCHAR, 'YYYYMMDD')`.

---

## 5) Use Semantic Context During Candidate Generation

- At query time, retrieve relevant `SemanticFact`s from `semantic_cards` collection
- Add sample rows, column stats, and semantic hints to `SchemaSlice` context for SQL generation
- Pass semantic context to plan prompt alongside schema text and decomposition context

---

## Cross-Section Interactions

### Decomposition ↔ Semantic Layer

The question decomposition (Section 3) and semantic layer (Section 4) work together:

1. **Semantic layer** answers: "Which columns exist and what do they mean?"
2. **Decomposition** answers: "What does the question ask for?"
3. **Resolution** connects the two: maps question subgoals to specific columns

Example flow:
```
Question: "Which traffic source had highest revenue in 2017?"

Decomposition:
  temporal_scope: "year 2017"
  measure: "highest revenue"
  grouping: "by traffic source"

Semantic layer:
  primary_time_column: "date" (VARCHAR, YYYYMMDD) → confidence 0.9
  metric_candidate: "totals":"totalTransactionRevenue" (VARIANT field) → confidence 0.8
  dimension_candidate: "trafficSource":"source" (VARIANT field) → confidence 0.85

Resolved plan context:
  - Filter: WHERE "date" BETWEEN '20170101' AND '20171231'
  - Measure: SUM("totals":"totalTransactionRevenue"::NUMBER)
  - Group: GROUP BY "trafficSource":"source"::STRING
  - Ranking: ORDER BY revenue DESC LIMIT 1
```

This resolved context is injected into the plan prompt, giving the LLM concrete column references instead of requiring it to figure out the schema semantics independently.
