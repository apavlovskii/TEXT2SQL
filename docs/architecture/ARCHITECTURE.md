# ARCHITECTURE.md
## SnowRAG-Agent for Spider2-Snow (Snowflake)

This document describes the architecture of `rag_snow_agent/`, an agentic Text-to-SQL system targeting the Spider2-Snow benchmark (Snowflake only). The primary goal is to **reduce prompt tokens** versus ReFoRCE-style baselines while maintaining or improving **execution accuracy**.

---

## 1. High-level overview

For each Spider2 instance (`instance_id`, `instruction`, `db_id`, optional `external_knowledge`), the system:

1. Retrieves a **minimal schema slice** (tables/columns/joins) and relevant documentation from a **local ChromaDB** index.
2. Optionally retrieves **trace memory** — compact summaries of prior successful solutions for the same database — and injects them as few-shot context.
3. Generates a **typed plan** (JSON) and compiles it to **Snowflake SQL** using a deterministic compiler.
4. Executes SQL in Snowflake with strict **session guardrails** (`USE DATABASE`, `USE SCHEMA`).
5. If execution fails, applies **targeted repair** with minimal prompt growth:
   - classify error into taxonomy (8 categories)
   - patch identifiers using retrieval
   - expand schema slice if grounding is incomplete
   - apply structural rewrite templates for aggregation/shape errors
6. Optionally generates **N diverse candidates** (Best-of-N) using different prompt strategies, executes+repairs each, and selects the best using:
   - result fingerprinting (row/column counts, null ratios, numeric stats)
   - expected output shape inference (small/grouped/aggregate/time-series)
   - metamorphic/counterfactual checks (limit expansion, shape consistency)
   - learned verifier score (LogisticRegression trained from prior run logs)
7. Persists successful traces to **trace memory** for future retrieval.
8. Writes results in **Spider2-compatible** format.
9. Official evaluation is performed using Spider2's `evaluate.py`.

---

## 2. Components and responsibilities

### 2.1 Offline Indexing Layer (ChromaDB)
**Purpose:** eliminate the need to send full schemas to the LLM by retrieving only relevant "cards".

**Artifacts:**
- Persistent ChromaDB directory: `rag_snow_agent/.chroma/`
- Collections:
  - `schema_cards` — tables, columns, and join edges
  - `external_docs` — markdown chunks (planned)
  - `trace_memory` — successful solution traces for few-shot retrieval

**Cards stored:**
- **TableCard**: `db.schema.table`, short description, key columns, time columns, canonical joins
- **ColumnCard**: `db.schema.table.column`, type, comment, sample top values (optional)
- **JoinCard**: `tableA.col → tableB.col` with confidence and source metadata (FK / heuristic)

**Sources:**
- Snowflake `INFORMATION_SCHEMA` metadata (tables, columns, types, comments)
- Foreign key constraints (when available)
- Heuristic join inference: matching column names (`*_ID`, `ID`, `*_KEY`) with type compatibility
- External markdown docs referenced by Spider2 (`external_knowledge`) — planned

**Key modules:**
- `chroma/schema_cards.py` — TableCard, ColumnCard, JoinCard Pydantic models
- `chroma/chroma_store.py` — ChromaStore class (upsert table/column/join cards)
- `chroma/build_index.py` — CLI to build index from Snowflake INFORMATION_SCHEMA
- `chroma/trace_memory.py` — TraceMemoryStore for solution traces

**Key design rule:** the index must store **canonical qualified names** and **dialect usage hints** to prevent hallucinated identifiers.

---

### 2.2 Retrieval Layer (Hybrid Retriever + Join Graph Connectivity)
**Purpose:** select schema/doc context that is both **relevant** and **joinable**.

Steps:
1. **Hybrid retrieval**:
   - dense embedding similarity over card text (ChromaDB)
   - identifier/keyword matching (split on `_`, `.`, camelCase boundaries; case-insensitive overlap)
   - fuse results with RRF (Reciprocal Rank Fusion, k=60)
2. **Schema linking**:
   - pick top-K tables/columns
   - keep join-relevant columns (keys) and time columns as protected
3. **Token-budget enforcement**:
   - configurable `max_schema_tokens`, `max_tables`, `max_columns_per_table`
   - trimming order: drop lowest-ranked unprotected columns first, then whole tables
4. **Connectivity expansion** (join-graph-aware):
   - fetch JoinCards for db_id, build JoinGraph
   - ensure selected tables form a connected subgraph
   - if disconnected, add minimal bridge tables via BFS shortest path (confidence-aware)
   - fallback to heuristic bridging if no JoinCards exist

**Output:** a compact `SchemaSlice` object:
- selected tables with pruned columns
- join hints explicitly derived from JoinGraph
- optional doc snippets

**Key modules:**
- `retrieval/hybrid_retriever.py` — HybridRetriever with dense + lexical + RRF
- `retrieval/schema_slice.py` — SchemaSlice, TableSlice, ColumnSlice dataclasses
- `retrieval/budget.py` — Token-budget enforcement
- `retrieval/join_graph.py` — JoinGraph with BFS, bridge tables, confidence filtering
- `retrieval/connectivity.py` — Join-graph-aware expansion with heuristic fallback

**Key design rule:** prefer **high recall** early, then prune by prompt budget. Low recall causes retries and token explosions.

---

### 2.3 Trace Memory Layer
**Purpose:** reuse prior successful solutions to improve planning stability and reduce token waste.

**How it works:**
- After each successful solve, a compact `TraceRecord` is persisted to the `trace_memory` ChromaDB collection
- Before plan generation, the system queries trace memory for similar instructions on the same `db_id`
- Top matching traces (above a configurable similarity threshold) are formatted into a compact few-shot context block and injected into the plan prompt
- Memory context respects a configurable token budget (`max_memory_tokens`)

**TraceRecord includes:**
- Instruction summary (truncated)
- Schema slice summary (tables + key columns)
- Plan summary (tables, joins, aggregations)
- Final SQL (truncated)
- Repair summary
- Tables and key columns used

**Key modules:**
- `chroma/trace_memory.py` — TraceMemoryStore (upsert, query, delete)
- `agent/memory.py` — TraceRecord dataclass + summarizers
- `prompting/prompt_builder.py` — `build_memory_context()` + memory_context injection

---

### 2.4 Prompting Layer (Token-budgeted prompts)
**Purpose:** generate SQL while keeping prompt size bounded and stable across iterations.

Strategy:
- Two-stage generation:
  1) **Plan generation** (JSON only) — produces a structured `QueryPlan`
  2) **SQL compilation** (deterministic) — compiles plan to Snowflake SQL
- Strategy-diverse prompts for Best-of-N:
  - `default` — standard plan generation
  - `join_first` — prioritize JOIN relationships
  - `metric_first` — prioritize target metric/aggregation
  - `time_first` — prioritize date/time columns

Prompt budget enforcement:
- `max_schema_tokens` (e.g., 2000–3000)
- `max_doc_tokens` (e.g., 500–1000)
- `max_memory_tokens` (e.g., 800)
- always include:
  - db_id and session naming policy
  - selected tables/columns + join hints
  - memory context (if available)
  - Snowflake dialect guidance

Repair prompts must NOT grow unbounded:
- include only: previous SQL, error message, delta schema, short state summary (≤200 tokens)
- Do not append full conversation history.

**Key modules:**
- `prompting/plan_schema.py` — QueryPlan, PlanJoin, PlanFilter, PlanAggregation, PlanOrderBy
- `prompting/prompt_builder.py` — Plan/SQL/fix-plan/fix-JSON prompts + strategy variants + memory context
- `prompting/sql_compiler.py` — Deterministic QueryPlan → SQL with stable aliases (t1, t2, ...)
- `prompting/constraints.py` — Identifier validation + join validation against SchemaSlice/JoinGraph

---

### 2.5 SQL Compiler / Constraint Layer
**Purpose:** reduce "runs but wrong" and "does not compile" errors by enforcing structure.

Responsibilities:
- Compile plan JSON into Snowflake SQL with:
  - consistent aliasing (t1, t2, ... in stable order)
  - deterministic CTE formatting
  - safe quoting policy (avoid accidental double-quoted identifiers)
- Enforce constraints:
  - use only columns present in SchemaSlice (regex-based identifier validation)
  - validate joins against JoinGraph (emit warnings for unknown joins)
  - avoid illegal aggregation patterns

**Key design rule:** compilation should be deterministic so that model's reasoning is separated from SQL formatting noise.

---

### 2.6 Execution Layer (Snowflake session + executor)
**Purpose:** run queries reliably and consistently.

Responsibilities:
- Load `snowflake_credentials.json` (and optional env overrides)
- Establish connection with optional role/warehouse
- Apply session guardrails:
  - `USE DATABASE ...`
  - `USE SCHEMA ...`
  - enforce consistent qualification strategy (`session` vs `fully_qualified`)
- Execution steps:
  1) `EXPLAIN` (optional, cheap preflight)
  2) execute query with configurable timeout
  3) fetch limited result sample (configurable `sample_rows`, default 20)

**Key modules:**
- `snowflake/client.py` — `connect()` from credentials JSON
- `snowflake/session.py` — `set_session()` guardrails
- `snowflake/executor.py` — `SnowflakeExecutor` with `explain()` and `execute()`
- `snowflake/metadata.py` — Schema extraction + join edge inference

**Key design rule:** eliminate baseline failures like "missing current database" by hard guardrails.

---

### 2.7 Repair / Refinement Agent
**Purpose:** converge quickly to a correct executable SQL with minimal token usage.

Error taxonomy (8 categories) drives actions:
- **Object not found / not authorized**: re-check qualification, refresh table list
- **Invalid identifier / ambiguous column**: retrieve ColumnCards, patch identifiers
- **Syntax/dialect errors**: apply deterministic Snowflake rewrite rules
- **Aggregation/shape errors**: structural CTE + GROUP BY rewrite
- **Type mismatch**: add CAST, parse dates, normalize
- **Unknown function**: apply Snowflake function substitution

Repair loop policy:
- max repairs: 2–3 (configurable)
- stop early on repeated identical failures
- keep prompts short (delta-only — instruction + previous SQL + error + schema subset)
- error-specific repair strategies dispatch different prompt templates

**Key modules:**
- `agent/error_classifier.py` — `classify_snowflake_error()` + identifier/object extraction
- `agent/refiner.py` — `refine_sql()` bounded repair loop

---

### 2.8 Candidate Selection (Best-of-N)
**Purpose:** improve accuracy without huge repair depth.

Pipeline:
1. Generate N candidates using diverse prompt strategies
2. Execute + repair each through the standard refinement loop
3. Build result fingerprints (row/column counts, null ratios, numeric stats)
4. Infer expected output shape from instruction
5. Run metamorphic/counterfactual checks on successful candidates
6. Score each candidate using multi-signal selector
7. Select the highest-scoring candidate

Scoring signals:
- Execution success (+100)
- Repair count penalty (-10 per repair)
- Empty result penalty (-20)
- Expected shape alignment (grouped, aggregate, time-series, small output bonuses)
- Metamorphic check score delta
- Learned verifier probability (weighted by configurable verifier_weight)
- Error type penalties (object_not_found, invalid_identifier, aggregation)

**Key modules:**
- `agent/candidate_generator.py` — N-candidate generation with strategy diversity
- `agent/best_of_n.py` — Orchestration: generate, execute+repair, verify, select
- `agent/selector.py` — Multi-signal scoring + `explain_candidate_score()` breakdown
- `agent/result_fingerprint.py` — Result fingerprinting
- `agent/shape_inference.py` — Expected output shape heuristics
- `agent/metamorphic.py` — Lightweight metamorphic checks

---

### 2.9 Learned Verifier / Reranker
**Purpose:** improve candidate selection beyond handwritten heuristics using run data.

Architecture:
- **Feature extraction** (`agent/verifier_features.py`): 20+ lightweight tabular features per candidate (execution success, repair count, error type one-hot, row_count bucket, shape alignment, SQL complexity metrics)
- **Training** (`agent/train_verifier.py`): LogisticRegression trained from JSONL candidate logs
- **Inference** (`agent/verifier.py`): load joblib model, extract features, return `predict_proba` score
- **Fallback**: gracefully returns 0.0 when no trained model exists

**Candidate logging** (`observability/trace_logger.py`): every candidate evaluation is persisted to JSONL for future training.

---

### 2.10 Evaluation & Ablation Framework
**Purpose:** reproducible experimentation and ablation analysis.

Components:
- **Experiment runner** (`eval/experiment_runner.py`): CLI with ablation toggles (`--disable_memory`, `--disable_verifier`, `--disable_best_of_n`, `--disable_repair`, etc.)
- **Metrics aggregation** (`eval/aggregate_metrics.py`): accuracy, token stats, failure taxonomy, repair distribution
- **Experiment comparison** (`eval/compare_experiments.py`): side-by-side markdown delta tables
- **Report generation** (`eval/render_report.py`): human-readable REPORT.md per experiment
- **Ablation presets** (`config/ablations/`): baseline_single, best_of_n_only, full_system

Experiment artifacts stored under `reports/experiments/<experiment>/`:
- `manifest.json` — config snapshot, git hash, toggles
- `instance_results.jsonl` — per-instance outcomes
- `metrics.json` — aggregated metrics
- `REPORT.md` — human-readable summary

---

## 3. Data flow (per instance)

```
1. Load Spider2 instance: (instance_id, instruction, db_id)
         │
2. Retrieve schema slice from ChromaDB → SchemaSlice
         │
3. Query trace memory for similar prior solutions → memory context
         │
4. Generate QueryPlan (JSON) from instruction + schema + memory context
         │
5. Compile plan → Snowflake SQL (deterministic compiler)
         │
6. Validate identifiers against SchemaSlice
         │
7. [If Best-of-N] Repeat steps 4-6 with diverse strategies → N candidates
         │
8. Execute with session guardrails (EXPLAIN → execute)
         │
9. If error: classify → targeted repair (max 2-3 iterations)
         │
10. [If Best-of-N] Build fingerprints, infer shape, run metamorphic checks
         │
11. [If Best-of-N] Score and select best candidate
         │
12. Persist successful trace to trace_memory
         │
13. Write Spider2 output: result.json
```

---

## 4. Integration with Spider2 evaluation

This project **does not modify** Spider2 evaluation logic.

We only:
- read Spider2 `spider2-snow.jsonl`
- write outputs to Spider2 expected output directory
- run official `evaluate.py` from Spider2 (see `README_SPIDER2.md`)

---

## 5. Observability and reporting

For every instance, log:
- token usage (prompt/output)
- number of LLM calls
- number of DB executions and probes
- repair loop count
- candidate count and selection reason
- verifier score (when model available)
- memory hit/miss
- final status + error taxonomy if failed

Generate summary reports:
- accuracy on the evaluated subset
- tokens per instance (avg/median/p95)
- top failure categories
- repair count distribution
- verifier usage stats
- memory hit rate

---

## 6. Why this architecture reduces tokens

Compared to ReFoRCE-style baselines that repeatedly include large schemas:
- schema and docs live in ChromaDB; prompt contains only a minimal schema slice
- repair loops use delta prompts rather than full conversation history
- connectivity-aware schema linking (via join graph) reduces retries due to missing join bridges
- trace memory provides few-shot context without full example replay
- deterministic SQL compiler eliminates LLM calls for SQL formatting
- Best-of-N with semantic verification finds correct answers faster than deep repair chains

---

## 7. Repository organization

```
rag_snow_agent/
  src/rag_snow_agent/
    chroma/
      schema_cards.py          # TableCard, ColumnCard, JoinCard models
      chroma_store.py          # ChromaStore (persistent client)
      build_index.py           # CLI: index schema + join edges
      trace_memory.py          # TraceMemoryStore

    snowflake/
      client.py                # connect() from credentials
      session.py               # USE DATABASE / USE SCHEMA guardrails
      metadata.py              # Schema + join edge extraction
      executor.py              # SnowflakeExecutor (EXPLAIN + execute)

    retrieval/
      hybrid_retriever.py      # Dense + lexical + RRF fusion
      schema_slice.py          # SchemaSlice, TableSlice, ColumnSlice
      budget.py                # Token-budget enforcement
      join_graph.py            # JoinGraph with BFS, bridge tables
      connectivity.py          # Join-graph-aware + heuristic fallback

    prompting/
      plan_schema.py           # QueryPlan Pydantic models
      prompt_builder.py        # Plan/SQL/fix prompts + strategies + memory
      sql_compiler.py          # Deterministic plan → SQL
      constraints.py           # Identifier + join validation

    agent/
      agent.py                 # solve_instance() orchestrator
      plan_sql_pipeline.py     # Plan → SQL pipeline with retry
      refiner.py               # Bounded repair loop
      error_classifier.py      # 8-category error taxonomy
      candidate_generator.py   # N-candidate diverse generation
      best_of_n.py             # Best-of-N orchestration + verification
      selector.py              # Multi-signal candidate scoring
      result_fingerprint.py    # Result fingerprinting
      shape_inference.py       # Expected output shape heuristics
      metamorphic.py           # Metamorphic checks
      verifier.py              # Learned verifier (joblib model)
      verifier_features.py     # Feature extraction for verifier
      train_verifier.py        # CLI: train verifier from logs
      memory.py                # TraceRecord + summarizers
      llm_client.py            # OpenAI LLM abstraction

    eval/
      write_results.py         # Spider2-compatible result writer
      experiment_runner.py     # CLI: run experiments with ablation toggles
      run_spider2_snow.py      # Standardized Spider2-Snow runner
      aggregate_metrics.py     # Compute metrics from instance logs
      compare_experiments.py   # Side-by-side experiment comparison
      render_report.py         # Generate REPORT.md

    observability/
      trace_logger.py          # Candidate log persistence (JSONL)
      training_data.py         # Build verifier training dataset

  config/
    defaults.yaml              # Default configuration
    ablations/                 # Ablation presets
      baseline_single.yaml
      best_of_n_only.yaml
      full_system.yaml

  tests/                       # 182 unit tests (all mocked, no Snowflake)
```

---

## 8. Roadmap

### Implemented (v1)
- M1: Schema extraction + Chroma indexing (TableCard, ColumnCard)
- M2: Hybrid retrieval + connectivity expansion + token budgeting
- M3: Plan→SQL pipeline + deterministic compiler
- M4: Execution-guided repair loop + error taxonomy
- M5: Best-of-N candidate generation + selection
- M6: Semantic verification (fingerprinting, shape inference, metamorphic checks)
- M7: Trace memory for few-shot retrieval
- M8: Real join graph + JoinCards + stronger connectivity
- M9: Learned verifier/reranker from run logs
- M10: Ablation harness + benchmark evaluation workflow

### Future (v2)
- External knowledge doc indexing and retrieval (`external_docs` collection)
- LLM-based semantic verifier (replace rule-based stub)
- Result fingerprinting with metamorphic SQL rewriting (full parser)
- Trace memory with learned few-shot selection
- Improved join inference using sampled key overlap
- Cross-database transfer learning for the verifier

---
