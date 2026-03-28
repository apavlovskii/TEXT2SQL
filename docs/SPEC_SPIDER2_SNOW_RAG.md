# SPEC_SPIDER2_SNOW_RAG.md
## Project
**SnowRAG-Agent for Spider2-Snow (Snowflake only)**

## 1. Goal
Build an agentic Text-to-SQL system for **Spider2-Snow** that:
- **reduces token usage** vs ReFoRCE-style approaches (by avoiding full schema in prompts),
- while maintaining or improving **execution accuracy** on Spider2-Snow.

We target **Snowflake-only** execution (no BigQuery, no SQLite, no DBT).

## 2. Inputs & Outputs

### Inputs per instance (from `spider2-snow.jsonl`)
- `instance_id`
- `instruction`
- `db_id`
- optional `external_knowledge` (a markdown filename)

### Credentials
A file will be provided:
- `snowflake_credentials.json`

Expected minimal fields:
```json
{
  "user": "...",
  "password": "...",
  "account": "..."
}
```

Optional fields if needed:
```json
{
  "warehouse": "...",
  "role": "...",
  "database": "...",
  "schema": "..."
}
```

### Output artifact (Spider2-compatible)
For evaluation, write for each instance:
```
Spider2/methods/spider-agent-snow/output/<experiment>/<instance_id>/spider/result.json
```

Minimal result format:
```json
{ "sql": "SELECT ...", "success": true }
```

## 3. Success Criteria
Primary:
- Execution accuracy (Spider2 evaluation suite)

Secondary:
- Total tokens, prompt tokens per instance
- LLM calls per instance
- Snowflake queries per instance (EXPLAIN + execution + probes)

Target:
- Comparable accuracy to ReFoRCE (or better), with significantly fewer prompt tokens.

## 4. High-Level Approach
### Key idea
Preload Snowflake schema + docs into **ChromaDB** as "cards", retrieve only the relevant subset per question, and iteratively expand only when execution feedback indicates missing grounding.

### Pipeline (per query)
1) Retrieve relevant schema slice (tables/columns/joins) from ChromaDB
2) Query trace memory for similar prior successful solutions
3) Generate a structured plan (JSON) with optional memory context
4) Compile plan into Snowflake SQL (deterministic compiler)
5) Execute:
   - run `EXPLAIN` first
   - then execute
6) If error:
   - classify error (8-category taxonomy)
   - minimal fix (either patch SQL or expand schema slice)
7) Best-of-N (configurable, default N=2):
   - generate N candidates with diverse prompt strategies
   - execute + repair each
   - build result fingerprints
   - infer expected output shape
   - run metamorphic checks
   - score with multi-signal selector (including learned verifier)
   - select best candidate
8) Persist successful trace to trace memory

## 5. Repository Organization (Snow-only)

```
rag_snow_agent/
  README.md
  pyproject.toml
  src/rag_snow_agent/
    chroma/
      chroma_store.py          # ChromaStore (persistent client)
      schema_cards.py          # TableCard, ColumnCard, JoinCard models
      build_index.py           # CLI: index schema + join edges
      trace_memory.py          # TraceMemoryStore for solution traces

    snowflake/
      client.py                # connect() from credentials JSON
      session.py               # USE DATABASE / USE SCHEMA guardrails
      metadata.py              # Schema + join edge extraction (FK + heuristic)
      executor.py              # SnowflakeExecutor (EXPLAIN + execute)

    retrieval/
      hybrid_retriever.py      # Dense + lexical + RRF fusion
      schema_slice.py          # SchemaSlice, TableSlice, ColumnSlice
      budget.py                # Token-budget enforcement
      join_graph.py            # JoinGraph with BFS, bridge tables, confidence
      connectivity.py          # Join-graph-aware + heuristic fallback

    prompting/
      plan_schema.py           # QueryPlan, PlanJoin, PlanFilter, etc.
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
      metamorphic.py           # Lightweight metamorphic checks
      verifier.py              # Learned verifier (joblib model)
      verifier_features.py     # Feature extraction for verifier
      train_verifier.py        # CLI: train verifier from logs
      memory.py                # TraceRecord + summarizers
      llm_client.py            # OpenAI LLM abstraction

    eval/
      write_results.py         # Spider2-compatible result writer
      experiment_runner.py     # CLI: experiments with ablation toggles
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
```

Keep `Spider2/` vendor directory unchanged and output to its expected folder layout.

## 6. ChromaDB Usage (Local)
### Persistent path
- `rag_snow_agent/.chroma/`

### Collections
- `schema_cards` — tables, columns, join edges
- `external_docs` — markdown chunks (planned)
- `trace_memory` — successful solution traces for few-shot retrieval

### Stored docs ("cards")
**TableCard**
- qualified name (db.schema.table)
- short description (if any)
- top columns (pruned)
- time columns
- common join keys

**ColumnCard**
- qualified name (db.schema.table.column)
- type
- comment
- sample top values (optional small)

**JoinCard**
- id: `join:<LEFT_TABLE>.<LEFT_COL>-><RIGHT_TABLE>.<RIGHT_COL>`
- document: join description with confidence
- metadata: left_table, right_table, left_column, right_column, confidence, source (fk / heuristic_name)

**TraceRecord** (in `trace_memory` collection)
- instruction summary
- plan summary (tables, joins, aggregations)
- tables and key columns used
- final SQL (truncated)
- repair summary

### Metadata (mandatory)
Each Chroma item must store:
- `db_id`
- `object_type` ∈ {table, column, join, doc}
- `qualified_name`
- `source` (information_schema / docs / heuristic / fk)
- `token_estimate`

## 7. Index Build (Offline / Pre-run)
Command:
```
python -m rag_snow_agent.chroma.build_index --db_id <DB_ID> --credentials snowflake_credentials.json
```

Steps:
1) connect to Snowflake
2) extract schema via `INFORMATION_SCHEMA`:
   - tables, columns, types, comments
3) extract join edges:
   - from FK constraints (INFORMATION_SCHEMA.TABLE_CONSTRAINTS + REFERENTIAL_CONSTRAINTS)
   - heuristic fallback: match columns with same name (`*_ID`, `ID`, `*_KEY`) and compatible types
   - FK edges get confidence=1.0, heuristic edges get 0.7
4) build TableCards, ColumnCards, JoinCards and insert into `schema_cards` collection
5) index external docs (planned):
   - locate docs referenced in Spider2 `external_knowledge`
   - chunk into ≤500–800 tokens and store in `external_docs`

## 8. Retrieval & Schema Linking (Online)
### Hybrid retrieval
- Dense embeddings over card text (ChromaDB cosine similarity)
- Lexical matching: identifier token overlap (split on `_`, `.`, camelCase; case-insensitive)
- RRF fusion of dense and lexical rankings (k=60)

### Token-budget enforcement
- Configurable `max_schema_tokens`, `max_tables`, `max_columns_per_table`
- Protected columns: join keys (`*_ID`, `ID`, `*_KEY`) and time columns (`DATE`, `TIMESTAMP`, `TIME`) resist trimming
- Trimming order: lowest-ranked unprotected columns first, then whole tables

### Connectivity expansion (join-graph-aware)
After retrieval:
- fetch JoinCards for db_id, build JoinGraph
- ensure chosen tables form connected join subgraph
- if disconnected, add bridge tables using BFS shortest paths (confidence-aware, configurable max_depth)
- fallback to heuristic bridging (column name overlap) if no JoinCards exist

### Trace memory retrieval
- query `trace_memory` collection with current instruction for same `db_id`
- retrieve top 1–3 traces above similarity threshold
- format as compact few-shot context (bounded by `max_memory_tokens`)

## 9. Prompt Strategy (Token-aware)
### Two-stage generation
A) Plan generation (JSON only) — structured `QueryPlan`
B) SQL compilation (deterministic compiler, no LLM call needed)

### Strategy-diverse prompts (for Best-of-N)
- `default` — standard plan generation
- `join_first` — prioritize JOIN relationships
- `metric_first` — prioritize target metric/aggregation
- `time_first` — prioritize date/time columns

### Prompt budget
- schema slice budget: 1–3k tokens max
- doc budget: 0–1k tokens max
- memory budget: 0–800 tokens max
- always include:
  - db_id, session policy
  - selected tables/columns
  - join hints (from JoinGraph)
  - memory context (if available)
  - external knowledge snippet (only if referenced)

### Stateful refinement without prompt growth
For repairs:
- pass only:
  - previous SQL
  - Snowflake error text
  - delta schema additions
  - short "state summary" (≤200 tokens)
Do not append full conversation history.

## 10. Execution Guardrails (Must-have)
Before every query:
- set session state:
  - `USE DATABASE <db>;`
  - `USE SCHEMA <schema>;` (if applicable)
- configurable statement timeout
- fetch at most `sample_rows` (default 20) to avoid huge result sets
Ensure one naming convention:
- either fully qualify, or rely on session defaults (pick one and enforce).

## 11. Error Taxonomy & Repair Actions
Classify errors into 8 categories:
- `object_not_found` — table/schema/database does not exist
- `not_authorized` — insufficient privileges
- `invalid_identifier` — unknown column reference
- `ambiguous_column` — column name ambiguous across tables
- `sql_syntax_error` — compilation or parse error
- `aggregation_error` — GROUP BY / aggregation violation
- `type_mismatch` — incompatible types or cast failure
- `unknown_function` — unsupported function call

Repair playbook (error-specific strategies):
- `invalid_identifier` → extract offending identifier, query SchemaSlice for closest match, patch
- `object_not_found` → verify qualification, attempt with allowed tables only
- `aggregation_error` → structural CTE + GROUP BY rewrite
- `type_mismatch` / `unknown_function` / `sql_syntax_error` → minimal repair with Snowflake guidance
- `not_authorized` → re-verify session database/schema

Retry max: 2–3 repair iterations. Stop early on repeated identical failures.

## 12. Best-of-N with Semantic Verification
If enabled (configurable, default N=2):
1. Generate N candidates using diverse prompt strategies
2. Execute + repair each through standard refinement loop
3. Build result fingerprints for successful candidates:
   - row_count, column_count, column_names
   - null ratios, numeric stats (min/max/mean)
4. Infer expected output shape from instruction:
   - small result (top/highest/lowest)
   - time series (monthly/daily/weekly/yearly)
   - grouped output (for each/grouped by)
   - aggregate output (how many/count/total)
5. Run metamorphic checks:
   - limit expansion: try larger LIMIT, verify still executes
   - shape consistency: verify row_count matches expected shape
6. Score candidates using multi-signal selector:
   - execution success (+100)
   - repair penalty (-10 per repair)
   - shape alignment bonuses/penalties
   - metamorphic score delta
   - learned verifier score (weighted)
7. Select highest-scoring candidate

## 13. Learned Verifier / Reranker
- **Feature extraction**: 20+ tabular features per candidate (execution success, repair count, error type one-hot, row_count bucket, shape alignment, SQL complexity)
- **Training**: LogisticRegression from JSONL candidate logs
- **Inference**: joblib model loading with graceful 0.0 fallback when no model exists
- **Integration**: verifier score multiplied by configurable `verifier_weight` (default 20.0) in selector scoring
- **Candidate logging**: every evaluation persisted to JSONL for future training

## 14. Spider2-Snow Runner
Command:
```
python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl Spider2/spider2-snow/spider2-snow.jsonl \
  --experiment snow_rag_v1 \
  --credentials snowflake_credentials.json \
  --model gpt-4o-mini \
  --limit 25 \
  --best_of_n 2
```

Runner responsibilities:
- load config + CLI overrides
- apply ablation toggles (`--disable_memory`, `--disable_verifier`, etc.)
- for each instance:
  - retrieve schema slice
  - run agent (single or best-of-n)
  - write Spider2 output `result.json`
  - log candidate records to JSONL
- write experiment manifest (config snapshot, git hash, toggles)
- write instance_results.jsonl + metrics.json

## 15. Evaluation
Reuse Spider2 evaluation suite:
```
cd Spider2/spider2-snow/evaluation_suite
python evaluate.py --result_dir <experiment> --mode exec_result
```

Post-evaluation analysis:
```
python -m rag_snow_agent.eval.aggregate_metrics --experiment_dir reports/experiments/<exp>
python -m rag_snow_agent.eval.compare_experiments --experiments dir1 dir2
python -m rag_snow_agent.eval.render_report --experiment_dir reports/experiments/<exp>
```

## 16. Ablation Framework
Supported ablation toggles:
- `--disable_memory` — no trace memory retrieval
- `--disable_verifier` — no learned verifier scoring
- `--disable_best_of_n` — single candidate only
- `--disable_repair` — no repair loop
- `--disable_verification` — no fingerprinting/metamorphic checks
- `--disable_join_graph` — heuristic connectivity only

Preset configurations in `config/ablations/`:
- `baseline_single.yaml` — no memory, no best_of_n, no verifier
- `best_of_n_only.yaml` — best_of_n=2, no memory, no verifier
- `full_system.yaml` — everything enabled

## 17. Milestones

### M0 — Scaffold (complete)
- module structure, CLI, logging, credentials

### M1 — Schema Extraction + Chroma Indexing (complete)
- Snowflake connectivity, INFORMATION_SCHEMA extraction
- TableCard/ColumnCard ingestion into ChromaDB
- CLI: `python -m rag_snow_agent.chroma.build_index`

### M2 — Retrieval + Connectivity + Token Budgeting (complete)
- Hybrid retrieval (dense + lexical + RRF)
- Token-budget enforcement with protected columns
- Heuristic connectivity expansion
- Debug CLI: `python -m rag_snow_agent.retrieval.debug_retrieve`

### M3 — Plan→SQL Pipeline (complete)
- QueryPlan Pydantic models
- Plan-first LLM prompts with Snowflake guidance
- Deterministic SQL compiler with stable aliases
- Identifier validation against SchemaSlice
- Debug CLI: `python -m rag_snow_agent.prompting.debug_plan_sql`

### M4 — Execution + Error Taxonomy + Repair Loop (complete)
- SnowflakeExecutor with EXPLAIN + execute + session guardrails
- 8-category error classifier with identifier extraction
- Bounded repair loop with error-specific strategies
- Spider2-compatible result writer
- Debug CLI: `python -m rag_snow_agent.agent.debug_execute_refine`

### M5 — Best-of-N Candidate Generation + Selection (complete)
- N-candidate generation with diverse prompt strategies
- Candidate scoring with execution + shape heuristics
- Best-of-N orchestration reusing M4 repair loop
- Debug CLI: `python -m rag_snow_agent.agent.debug_best_of_n`

### M6 — Semantic Verification (complete)
- Result fingerprinting (row/column counts, null ratios, numeric stats)
- Expected output shape inference (small/grouped/aggregate/time-series)
- Metamorphic checks (limit expansion, shape consistency)
- Upgraded multi-signal selector with score breakdown
- Debug CLI: `python -m rag_snow_agent.agent.debug_verify_candidate`

### M7 — Trace Memory + Example Retrieval (complete)
- TraceMemoryStore (ChromaDB `trace_memory` collection)
- TraceRecord with compact summarizers
- Memory context injection into plan prompts (token-budgeted)
- Automatic persistence on successful solves
- Debug CLI: `python -m rag_snow_agent.agent.debug_memory_retrieval`

### M8 — Real Join Graph + JoinCards (complete)
- JoinEdge extraction (FK constraints + heuristic name matching)
- JoinCard storage in `schema_cards` collection
- JoinGraph with BFS shortest path, confidence-aware bridge tables
- Join-graph connectivity replaces heuristic (with fallback)
- Join hints in plan prompts + join validation in constraints
- Debug CLI: `python -m rag_snow_agent.retrieval.debug_join_graph`

### M9 — Learned Verifier / Reranker (complete)
- Tabular feature extraction (20+ features per candidate)
- LogisticRegression training from JSONL candidate logs
- Joblib model loading with graceful fallback
- Verifier score integrated into selector (weighted)
- Candidate logging for future training

### M10 — Ablation Harness + Evaluation Workflow (complete)
- Experiment runner with ablation toggles
- Metrics aggregation (accuracy, tokens, failures, repairs)
- Experiment comparison (markdown delta tables)
- Report generation (REPORT.md)
- Ablation presets (baseline_single, best_of_n_only, full_system)

### Future
- External knowledge doc indexing and retrieval
- LLM-based semantic verifier
- Full SQL parser for metamorphic rewriting
- Cross-database transfer learning for verifier
- Trace memory with learned few-shot selection
