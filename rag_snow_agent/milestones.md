# SnowRAG-Agent Milestones

## Milestone 1: Schema Extractor + Chroma Ingestion

**Status:** Complete

**Goal:** Connect to Snowflake, extract schema metadata, and ingest into local ChromaDB.

**Requirements:**

1. Implement Snowflake connectivity + schema extraction (INFORMATION_SCHEMA) for a given db_id.
2. Implement ChromaDB store + ingestion of TableCard/ColumnCard.
3. Provide a CLI: `python -m rag_snow_agent.chroma.build_index --db_id <DB_ID> --credentials rag_snow_agent/snowflake_credentials.json`
4. Add a smoke test that indexes one small db_id and prints counts of inserted TableCards/ColumnCards.

**Deliverables:**

| File | Purpose |
|------|---------|
| `snowflake/client.py` | `connect()` — reads credentials JSON, returns Snowflake connection |
| `snowflake/metadata.py` | `extract_tables()` — queries INFORMATION_SCHEMA, returns TableInfo with ColumnInfo |
| `chroma/schema_cards.py` | `TableCard` / `ColumnCard` Pydantic models |
| `chroma/chroma_store.py` | `ChromaStore` — persistent ChromaDB wrapper |
| `chroma/build_index.py` | CLI + `run()` orchestrator |
| `chroma/__main__.py` | Enables `python -m rag_snow_agent.chroma.build_index` |
| `tests/test_smoke_index.py` | Smoke test with mocked Snowflake |

**Card format:**

- TableCard id: `table:<DB>.<SCHEMA>.<TABLE>`, metadata: `db_id`, `object_type="table"`, `qualified_name`, `source`, `token_estimate`
- ColumnCard id: `column:<DB>.<SCHEMA>.<TABLE>.<COLUMN>`, metadata: `db_id`, `object_type="column"`, `qualified_name`, `table_qualified_name`, `data_type`, `token_estimate`, `source`

---

## Milestone 2: Retrieval + Schema Linking + Token Budgeting

**Status:** Complete

**Goal:** Given `(db_id, natural language query)`, retrieve a compact, connected SchemaSlice (tables + columns) under a token budget, and provide a CLI to inspect it.

**Requirements:**

1. Create a retrieval module under `rag_snow_agent/src/rag_snow_agent/retrieval/`:

   - `schema_slice.py`: defines `SchemaSlice`, `TableSlice`, and formatting to a compact prompt string.
   - `hybrid_retriever.py`: implements retrieval:
     - Dense retrieval using ChromaDB `schema_cards` collection filtered by `db_id`.
     - A lexical booster score using identifier token overlap between the NL query and the `qualified_name` (split on `_` `.` and camel-case boundaries if present; case-insensitive).
     - Fuse dense rank and lexical rank with Reciprocal Rank Fusion (RRF).
     - Output ranked tables and columns; group columns by `table_qualified_name`.
   - `budget.py`: enforce prompt budget using metadata `token_estimate`:
     - config params: `max_schema_tokens`, `max_tables`, `max_columns_per_table`
     - trimming order:
       1. drop lowest-ranked columns first
       2. if still over budget, drop lowest-ranked tables
       3. always keep join-ish columns (`*_ID`, `ID`, `KEY`) and time-ish columns (`DATE`, `TIMESTAMP`, `TIME`) when present

2. Add minimal "connectivity expansion" placeholder:
   - Since we don't have JoinCards yet, implement heuristic bridging:
     - represent each table by its kept column name tokens
     - if multiple selected tables have no shared join-ish columns, attempt to add 1 bridge table:
       - query Chroma for table cards in this db_id whose columns contain join-ish keys that overlap with 2+ selected tables.
     - cap 1 expansion round.

3. Add a debug CLI under `rag_snow_agent/src/rag_snow_agent/retrieval/debug_retrieve.py` runnable as:
   - `uv run python -m rag_snow_agent.retrieval.debug_retrieve --db_id TESTDB --query "total orders by month" --top_k 50 --max_schema_tokens 800`
   - The CLI must:
     - print top tables with scores (dense score + lexical score + fused rank)
     - print selected columns per table
     - print final SchemaSlice token estimate and the formatted schema text

4. Make it configurable:
   - Add to `rag_snow_agent/config/defaults.yaml`:
     - `retrieval.top_k_tables`, `retrieval.top_k_columns`, `retrieval.max_schema_tokens`, `retrieval.max_columns_per_table`, `retrieval.connectivity_expansion_rounds`
   - CLI flags override config.

5. Add tests:
   - `test_budget.py`: verifies trimming drops low-ranked columns and respects `max_schema_tokens` using fake cards/metadata.
   - `test_rrf.py`: verifies RRF fusion ranking deterministically.

**Deliverables:**

| File | Purpose |
|------|---------|
| `retrieval/schema_slice.py` | `SchemaSlice`, `TableSlice`, `ColumnSlice` dataclasses + `format_for_prompt()` |
| `retrieval/hybrid_retriever.py` | Dense + lexical retrieval with RRF fusion |
| `retrieval/budget.py` | Token-budget enforcement with protected columns |
| `retrieval/connectivity.py` | Heuristic bridge table finder |
| `retrieval/debug_retrieve.py` | Debug CLI |
| `tests/test_budget.py` | Budget trimming tests |
| `tests/test_rrf.py` | RRF fusion + tokenizer tests |

---

## Milestone 3: Plan-First Prompting + SQL Generation

**Status:** Complete

**Goal:** Add plan-first prompting and SQL generation (Snowflake dialect) with strict constraints and minimal prompt growth.

**Requirements:**

1. Create new modules under `rag_snow_agent/src/rag_snow_agent/prompting/`:

   - `plan_schema.py`:
     - Define Pydantic models: `QueryPlan`, `PlanJoin`, `PlanFilter`, `PlanAggregation`, `PlanOrderBy`
     - QueryPlan must include:
       - `selected_tables: list[str]` (qualified table names from SchemaSlice)
       - `joins: list[PlanJoin]` (each join has left_table, left_column, right_table, right_column, join_type)
       - `filters: list[PlanFilter]` (table, column, op, value)
       - `group_by: list[str]` (qualified or `table.column`)
       - `aggregations: list[PlanAggregation]` (func, table, column, alias)
       - `order_by: list[PlanOrderBy]` (expr, direction)
       - `limit: int | None`
       - `notes: str | None` (optional)

   - `prompt_builder.py`:
     - Build two prompts:
       A) **Plan prompt** (JSON only)
       B) **SQL prompt** (SQL only)
     - Prompts must include:
       - The instruction
       - The SchemaSlice formatted schema text
       - Strict rules:
         - "Use only tables/columns present in schema slice"
         - "Return ONLY valid JSON matching the schema" (for plan)
         - "Return ONLY SQL" (for SQL)
         - "Snowflake dialect"
         - "No markdown, no explanation"
     - Include a small Snowflake guidance section:
       - date/time filtering patterns
       - avoid unsupported constructs
       - prefer CTEs

   - `sql_compiler.py`:
     - Deterministically compile `QueryPlan` -> SQL string:
       - Always use CTE style (`WITH ...`)
       - Assign aliases `t1`, `t2`, ... in stable order of selected tables
       - Expand joins explicitly
       - Apply filters in WHERE
       - Apply group_by + aggregations correctly
       - Apply order_by and limit
     - Ensure Snowflake-safe quoting rules:
       - Do NOT quote identifiers with double-quotes unless absolutely necessary.
       - Use `ILIKE` optionally for text contains if plan indicates.

   - `constraints.py`:
     - Implement identifier validation:
       - Parse candidate SQL for tokens that look like identifiers (`table.column`, `schema.table`, etc.) using a conservative regex approach (no full SQL parser required in v1).
       - Check that all referenced tables exist in SchemaSlice tables.
       - Check that referenced columns belong to those tables (use SchemaSlice table->columns mapping).
       - If violations found, return a structured error list.

2. Add an LLM interface abstraction under `rag_snow_agent/src/rag_snow_agent/agent/llm_client.py`:
   - Single function: `call_llm(messages, model, temperature, max_tokens) -> str`
   - Compatible with OpenAI client.
   - Must log token usage if available, but do NOT print API keys.

3. Implement a "plan->sql" pipeline under `rag_snow_agent/src/rag_snow_agent/agent/plan_sql_pipeline.py`:
   - Inputs: `db_id`, `instruction`, `schema_slice`, `model`
   - Steps:
     1. Build plan prompt and call LLM -> plan_json_text
     2. Parse plan JSON into `QueryPlan` (Pydantic); if invalid, retry once with "fix JSON only"
     3. Compile plan into SQL using `sql_compiler.py`
     4. Validate identifiers using `constraints.py`
     5. If validation fails:
        - Attempt one correction pass: call LLM with a "fix plan" prompt that lists invalid identifiers and asks to adjust plan to use valid columns only.
        - Recompile and revalidate.
   - Output: final SQL string + metadata (plan, validation warnings)

4. Config:
   - Update `rag_snow_agent/config/defaults.yaml` with:
     - `llm.model`, `llm.temperature`, `llm.max_output_tokens`
     - `agent.plan_retry_limit` (default 1)
     - `agent.validation_fix_limit` (default 1)

5. Add tests:
   - `test_sql_compiler.py`: Construct a small QueryPlan with 2 tables, join, filter, group_by, aggregation. Verify compiled SQL contains expected clauses and stable aliases.
   - `test_constraints.py`: Provide a fake SchemaSlice and SQL string; ensure invalid identifiers are detected.

6. Add a dev CLI:
   - `rag_snow_agent/src/rag_snow_agent/prompting/debug_plan_sql.py`
   - Runnable as: `uv run python -m rag_snow_agent.prompting.debug_plan_sql --db_id TESTDB --query "average amount by month" --top_k 50`
   - Prints: selected tables/columns, plan JSON, final SQL, validation warnings.

**Deliverables:**

| File | Purpose |
|------|---------|
| `prompting/plan_schema.py` | QueryPlan + related Pydantic models |
| `prompting/prompt_builder.py` | Plan, SQL, fix-plan, fix-JSON prompt builders |
| `prompting/sql_compiler.py` | Deterministic QueryPlan -> SQL compiler |
| `prompting/constraints.py` | Identifier validation against SchemaSlice |
| `agent/llm_client.py` | OpenAI LLM abstraction |
| `agent/plan_sql_pipeline.py` | Plan -> SQL pipeline with retry logic |
| `prompting/debug_plan_sql.py` | Debug CLI |
| `tests/test_sql_compiler.py` | SQL compiler tests |
| `tests/test_constraints.py` | Constraint validation tests |

---

## Milestone 4: Execution + Error Taxonomy + Repair Loop

**Status:** Complete

**Goal:** Execute generated Snowflake SQL safely, classify errors, apply bounded repair strategies with minimal prompt growth, and write Spider2-compatible outputs.

**Requirements:**

1. **Execution layer** — Create modules under `rag_snow_agent/src/rag_snow_agent/snowflake/`:

   - `executor.py`:
     - `ExecutionResult` data structure with: `success`, `sql`, `error_message`, `error_type`, `row_count`, `rows_sample`, `elapsed_ms`, `explain_only`
     - `SnowflakeExecutor` class with methods: `explain(sql)` and `execute(sql, sample_rows=20)`
     - Always apply session guardrails before execution: `USE DATABASE <db_id>;`, `USE SCHEMA <schema>;`
     - Add timeout / defensive fetch behavior; fetch at most `sample_rows` rows.

   - `session.py`:
     - Centralize session state setup: `set_session(db_id, schema)`
     - Keep qualification strategy consistent (session mode preferred in v1).

2. **Error taxonomy + classifier** — Create `agent/error_classifier.py`:
   - `classify_snowflake_error(error_message) -> str`
   - Categories: `object_not_found`, `not_authorized`, `invalid_identifier`, `ambiguous_column`, `sql_syntax_error`, `aggregation_error`, `type_mismatch`, `unknown_function`, `other_execution_error`
   - Use regex/string matching on real Snowflake error text patterns.
   - Also: `extract_offending_identifier(error_message) -> str | None` and `extract_offending_object(error_message) -> str | None`

3. **Repair agent** — Create `agent/refiner.py`:
   - Bounded repair loop with max 2-3 retries.
   - Main function: `refine_sql(db_id, instruction, schema_slice, sql, executor, model, max_repairs=2) -> tuple[str, list[dict]]`
   - Behavior:
     - Run EXPLAIN first.
     - If EXPLAIN fails: classify error, attempt targeted repair.
     - If EXPLAIN passes: run execute; if execution fails, classify and repair.
     - Return final SQL and a structured repair trace list.
   - Repair trace item: `{attempt, input_sql, error_type, error_message, repair_action, output_sql}`

4. **Repair strategies** (v1):

   - A) **Invalid identifier**: extract offending identifier, use SchemaSlice to find closest matching columns/tables, build minimal repair prompt with previous SQL + error + relevant tables/columns only.
   - B) **Object not found / authorization**: verify session database/schema, attempt repair with allowed tables from SchemaSlice. Trigger schema re-retrieval/expansion hook (placeholder stub `expand_schema_slice_for_error(...)` with TODO).
   - C) **Aggregation / grouping errors**: repair prompt asking for structural rewrite with CTEs + explicit GROUP BY.
   - D) **Type mismatch / unknown function / syntax**: minimal repair prompt with Snowflake-specific guidance.

5. **Minimal prompt growth policy** (critical):
   - Do NOT append full conversation history.
   - Repair prompts include only: original instruction, previous SQL, error message, short schema subset, one-line rules.
   - Implement helper: `build_repair_prompt(...)`

6. **Agent integration** — Create `agent/agent.py`:
   - `solve_instance(instance_id, instruction, db_id, schema_slice, model, executor) -> dict`
   - Calls M3 pipeline for initial SQL, then runs `refine_sql(...)`.
   - Returns: final SQL, success/failure, repair trace, token usage metadata.

7. **Spider2 output writer** — Create `eval/write_results.py`:
   - `write_spider2_result(experiment, instance_id, sql, success, base_dir)`
   - Writes `{"sql": "...", "success": true}` to `Spider2/methods/spider-agent-snow/output/<experiment>/<instance_id>/spider/result.json`

8. **Debug CLI** — Create `agent/debug_execute_refine.py`:
   - `uv run python -m rag_snow_agent.agent.debug_execute_refine --db_id TESTDB --query "average amount by month in 2014" --top_k 50 --model gpt-4o-mini`
   - Prints: initial SQL, each repair attempt, final SQL, final success/failure.

9. **Config updates**:
   ```yaml
   agent:
     max_repairs: 2
     explain_first: true
     sample_rows: 20
     stop_on_repeated_error: true
   ```

10. **Tests**:
    - `test_error_classifier.py`: verify representative Snowflake error messages map to correct categories.
    - `test_write_results.py`: verify Spider2 result path is created and JSON is valid.
    - `test_refiner_minimal.py`: mock executor + mock LLM; verify invalid identifier triggers repair, trace is recorded, loop stops on success.

**Deliverables:**

| File | Purpose |
|------|---------|
| `snowflake/session.py` | Session guardrails |
| `snowflake/executor.py` | `SnowflakeExecutor` with explain/execute |
| `agent/error_classifier.py` | Error taxonomy classifier + identifier extraction |
| `agent/refiner.py` | Bounded repair loop with error-specific strategies |
| `agent/agent.py` | `solve_instance()` orchestrator |
| `eval/write_results.py` | Spider2-compatible result writer |
| `agent/debug_execute_refine.py` | Debug execution + repair CLI |
| `tests/test_error_classifier.py` | Error classifier tests (15) |
| `tests/test_write_results.py` | Result writer tests (4) |
| `tests/test_refiner_minimal.py` | Refiner tests with mocks (4) |

---

## Milestone 5: Best-of-N Candidate Generation + Selection

**Status:** Complete

**Goal:** Generate multiple diverse SQL candidates for the same instruction, execute and lightly repair each, and select the best final candidate using execution outcome + simple semantic heuristics.

**Requirements:**

1. **Candidate generation module** — `agent/candidate_generator.py`:
   - `generate_candidate_sqls(db_id, instruction, schema_slice, model, n=2) -> list[CandidateItem]`
   - Each candidate uses a different prompt strategy for diversity:
     - candidate 1: default plan generation prompt
     - candidate 2: "join-first" prompt variant
     - if n >= 3: "metric-first"
     - if n >= 4: "time-filter-first"
   - Prompt variants remain compact; reuse M3 prompt builder and SchemaSlice.
   - Strategy-specific plan prompts added to `prompting/prompt_builder.py`.

2. **Candidate execution and repair orchestration** — `agent/best_of_n.py`:
   - `run_best_of_n(instance_id, db_id, instruction, schema_slice, model, executor, n=2) -> dict`
   - Generates N candidates, runs each through M4 refinement/execution loop, collects structured results, selects the best.
   - Return structure includes: `best_candidate_id`, `best_sql`, `best_success`, `selection_reason`, and full `candidates` list with per-candidate metadata (strategy, initial/final SQL, success, repairs_count, error_type, row_count, rows_sample, repair_trace, score).
   - Reuses M4 code rather than duplicating repair logic.

3. **Candidate selection logic** — `agent/selector.py`:
   - `score_candidate(instruction, candidate_result) -> float`
   - Scoring rules (v1):
     - +100 if execution succeeds
     - -10 per repair
     - -20 if result is empty and instruction likely implies non-empty output
     - +10 if instruction implies small output and row_count is small (<=5)
     - +5 if instruction implies grouped output and row_count > 1
     - -30 if final error type is object_not_found / invalid_identifier
     - -15 if final error type is aggregation_error
   - `infer_expected_shape(instruction) -> dict`: simple heuristics:
     - "top", "highest", "lowest", "most" → expect small output
     - "monthly", "per month", "by month" → expect ~12 rows
     - "for each", "grouped by", "by <dimension>" → expect grouped multi-row
     - "how many", "count", "total number" → likely aggregate (1 row)

4. **Agent integration** — updated `agent/agent.py`:
   - `solve_instance(...)` now accepts `best_of_n` parameter
   - If `best_of_n > 1`, calls `run_best_of_n(...)`; otherwise uses single-candidate M4 flow
   - `InstanceResult` extended with: `best_of_n_used`, `candidate_count`, `selection_reason`, `candidate_summaries`

5. **Debug CLI** — `agent/debug_best_of_n.py`:
   - `uv run python -m rag_snow_agent.agent.debug_best_of_n --db_id TESTDB --query "average amount by month" --model gpt-4o-mini --n 2 --top_k 50`
   - Prints: SchemaSlice, each candidate (strategy, initial/final SQL, score, repair trace), selected best and reason.

6. **Config updates**:
   ```yaml
   agent:
     best_of_n: 2
     candidate_strategies:
       - default
       - join_first
     selector:
       success_bonus: 100
       repair_penalty: 10
       empty_result_penalty: 20
       small_output_bonus: 10
       grouped_output_bonus: 5
       object_not_found_penalty: 30
       invalid_identifier_penalty: 30
       aggregation_error_penalty: 15
   ```

7. **Tests**:
   - `test_selector.py`: shape inference for small/monthly/grouped/aggregate instructions; success dominates score; repair penalty; empty result penalty; small output bonus; error type penalties.
   - `test_candidate_generator.py`: mock LLM; n=2 returns 2 candidates with different strategies; n=4 cycles all 4 strategies; candidates have valid SQL.
   - `test_best_of_n_minimal.py`: mock executor + LLM; selects highest-scoring successful candidate; repaired candidate can win; output includes all metadata; scores are positive for successful candidates.

**Deliverables:**

| File | Purpose |
|------|---------|
| `prompting/prompt_builder.py` (updated) | Added `build_plan_prompt_with_strategy()` + strategy hints |
| `agent/candidate_generator.py` | N-candidate generation with diverse prompt strategies |
| `agent/selector.py` | Candidate scoring + instruction shape inference |
| `agent/best_of_n.py` | Best-of-N orchestration: generate, execute+repair, select |
| `agent/agent.py` (updated) | `solve_instance()` supports both single and best-of-N modes |
| `agent/debug_best_of_n.py` | Debug CLI for best-of-N |
| `tests/test_selector.py` | Selector scoring tests (8) |
| `tests/test_candidate_generator.py` | Candidate generator tests (4) |
| `tests/test_best_of_n_minimal.py` | Best-of-N orchestration tests (4) |

---

## Milestone 6: Semantic Verification + Stronger Candidate Validation

**Status:** Complete

**Goal:** Distinguish between merely executable SQL and semantically plausible/correct SQL. Improve candidate ranking using result fingerprinting, expected output shape inference, lightweight metamorphic checks, and upgraded selector scoring.

**Requirements:**

1. **Result fingerprinting** — `agent/result_fingerprint.py`:
   - `ResultFingerprint` dataclass: `row_count`, `column_count`, `column_names`, `null_ratios`, `numeric_stats` (min/max/mean per numeric column), `sample_rows`
   - `build_result_fingerprint(execution_result)` — lightweight, handles tuples and dicts, partial on failure

2. **Expected output shape inference** — `agent/shape_inference.py`:
   - `ExpectedShape` dataclass: `expect_small_result`, `expect_grouped_output`, `expect_aggregate_output`, `expect_time_series`, `expected_time_grain`, `notes`
   - `infer_expected_shape(instruction)` — regex-based heuristics for small/grouped/aggregate/time-series with grain (month/day/week/year)

3. **Metamorphic / counterfactual checks** — `agent/metamorphic.py`:
   - `run_metamorphic_checks(instruction, sql, executor, ...)` → `{checks_run, score_delta}`
   - v1 checks: `limit_expansion` (try larger LIMIT), `shape_consistency` (verify row_count matches expected shape)
   - Conservative: skips checks that can't be safely derived

4. **Verifier interface (stub)** — `agent/verifier.py`:
   - `score_candidate_semantics(instruction, sql, schema_slice, fingerprint)` → 0.0
   - Designed for future LLM-based verifier

5. **Upgraded selector** — `agent/selector.py` (rewritten):
   - New scoring factors: `grouped_single_row_penalty` (-15), `aggregate_single_row_bonus` (+10), `time_series_bonus` (+10), metamorphic `score_delta`, verifier score
   - `explain_candidate_score(instruction, candidate_result)` → score breakdown dict

6. **Verification integrated into Best-of-N** — `agent/best_of_n.py` (updated):
   - After execution+repair: builds `ResultFingerprint`, infers `ExpectedShape`, runs metamorphic checks, calls verifier
   - Each candidate record includes: `result_fingerprint`, `expected_shape`, `metamorphic`, `score_breakdown`
   - Selection reason mentions shape signals

7. **Debug CLI** — `agent/debug_verify_candidate.py`:
   - `uv run python -m rag_snow_agent.agent.debug_verify_candidate --db_id TESTDB --query "top selling product by month in 2017" --model gpt-4o-mini --n 2 --top_k 50`
   - Prints: expected shape, each candidate's fingerprint/metamorphic/score breakdown, selected best

8. **Config**:
   ```yaml
   agent:
     verification:
       enable_fingerprinting: true
       enable_metamorphic: true
       max_metamorphic_checks: 2
     selector:
       grouped_single_row_penalty: 15
       aggregate_single_row_bonus: 10
       time_series_bonus: 10
   ```

9. **Tests**:
   - `test_shape_inference.py` (8 tests): small/monthly/daily/yearly/grouped/aggregate/no-special/notes
   - `test_result_fingerprint.py` (6 tests): failed partial, tuples, null ratios, numeric stats, empty, mixed types
   - `test_selector_semantic.py` (8 tests): grouped penalty, aggregate bonus, time-series bonus, metamorphic delta, explain breakdown
   - `test_metamorphic_minimal.py` (6 tests): limit expansion, no-limit skip, shape consistency, max checks

**Deliverables:**

| File | Purpose |
|------|---------|
| `agent/result_fingerprint.py` | Result fingerprinting from execution results |
| `agent/shape_inference.py` | Expected output shape inference (replaces old inline logic) |
| `agent/metamorphic.py` | Lightweight metamorphic/counterfactual checks |
| `agent/verifier.py` | Semantic verifier stub (future LLM-based) |
| `agent/selector.py` (rewritten) | Upgraded scoring with shape/metamorphic/verifier signals |
| `agent/best_of_n.py` (updated) | Verification pass integrated into candidate pipeline |
| `agent/debug_verify_candidate.py` | Debug CLI for semantic verification |
| `tests/test_shape_inference.py` | Shape inference tests (8) |
| `tests/test_result_fingerprint.py` | Fingerprint tests (6) |
| `tests/test_selector_semantic.py` | Semantic selector tests (8) |
| `tests/test_metamorphic_minimal.py` | Metamorphic check tests (6) |

---

## Milestone 7: Trace Memory + Example Retrieval

**Status:** Complete

**Goal:** Persist compact successful traces into ChromaDB, retrieve similar traces at inference, inject compact few-shot context into planning prompts.

**Deliverables:**

| File | Purpose |
|------|---------|
| `chroma/trace_memory.py` | `TraceMemoryStore` — new `trace_memory` Chroma collection for successful solutions |
| `agent/memory.py` | `TraceRecord` dataclass + compact summarizers for schema/plan/repair/verification |
| `agent/debug_memory_retrieval.py` | CLI to query and inspect trace memory |
| `eval/backfill_trace_memory.py` | Stub utility for backfilling from prior runs |
| `prompting/prompt_builder.py` (updated) | `build_memory_context()` + memory_context param on plan prompts |
| `agent/plan_sql_pipeline.py` (updated) | Passes memory_context through to prompts |
| `agent/agent.py` (updated) | `_persist_trace()` on successful solves, `memory_enabled` param |
| `tests/test_memory_record.py` | Trace record and summary tests (11) |
| `tests/test_trace_memory_store.py` | Chroma round-trip tests (6) |
| `tests/test_memory_context_builder.py` | Token-budgeted context builder tests (4) |

---

## Milestone 8: Real Join Graph + JoinCards + Stronger Connectivity

**Status:** Complete

**Goal:** Build a proper join graph per Snowflake database, store JoinCards in ChromaDB, use join graph for deterministic connectivity expansion.

**Deliverables:**

| File | Purpose |
|------|---------|
| `snowflake/metadata.py` (updated) | `JoinEdge` dataclass, `extract_join_edges()` with FK + heuristic fallback |
| `chroma/schema_cards.py` (updated) | `JoinCard` Pydantic model |
| `chroma/chroma_store.py` (updated) | `upsert_join_cards()` method |
| `chroma/build_index.py` (updated) | Join edge extraction + JoinCard ingestion |
| `retrieval/join_graph.py` | `JoinGraph` — BFS shortest path, bridge table finder, confidence-aware |
| `retrieval/connectivity.py` (updated) | `expand_connectivity_with_join_graph()` with heuristic fallback |
| `prompting/prompt_builder.py` (updated) | Optional `join_hints` param on plan prompts |
| `prompting/constraints.py` (updated) | `validate_joins()` for join graph warnings |
| `retrieval/debug_join_graph.py` | CLI to inspect join graph and paths |
| `tests/test_join_graph.py` | Join graph BFS/bridge tests (13) |
| `tests/test_join_cards.py` | JoinCard creation tests (6) |
| `tests/test_connectivity_join_graph.py` | Join-graph connectivity expansion tests (5) |

---

## Milestone 9: Learned Verifier / Reranker from Run Logs

**Status:** Complete

**Goal:** Train a lightweight reranker from prior run artifacts, integrate learned score into candidate selection.

**Deliverables:**

| File | Purpose |
|------|---------|
| `agent/verifier_features.py` | `extract_candidate_features()` — 20+ tabular features |
| `agent/verifier.py` (updated) | `load_verifier()`, `score_candidate_semantics()` — joblib model loading with 0.0 fallback |
| `agent/train_verifier.py` | CLI to train LogisticRegression from candidate logs |
| `agent/debug_verifier_score.py` | CLI to inspect features and verifier score |
| `observability/training_data.py` | `build_verifier_dataset()` from JSONL candidate logs |
| `observability/trace_logger.py` | `log_candidate_records()` to JSONL for later training |
| `agent/selector.py` (updated) | `verifier_weight: 20.0` scoring integration |
| `tests/test_verifier_features.py` | Feature extraction tests (8) |
| `tests/test_verifier_inference.py` | Model loading/fallback tests (3) |
| `tests/test_training_data_builder.py` | Dataset extraction tests (5) |

---

## Milestone 10: Ablation Harness + Benchmark Evaluation Workflow

**Status:** Complete

**Goal:** Reproducible evaluation and ablation framework for Spider2-Snow with experiment management, metrics aggregation, comparison, and reporting.

**Deliverables:**

| File | Purpose |
|------|---------|
| `eval/experiment_runner.py` | CLI experiment runner with ablation toggles (--disable_memory, --disable_verifier, etc.) |
| `eval/aggregate_metrics.py` | Compute accuracy, token stats, failure taxonomy from instance logs |
| `eval/compare_experiments.py` | Side-by-side markdown comparison of multiple experiments |
| `eval/run_spider2_snow.py` | Standardized Spider2-Snow runner entry point |
| `eval/render_report.py` | Generate REPORT.md with config, metrics, failure categories |
| `config/ablations/baseline_single.yaml` | Preset: no memory, no best_of_n, no verifier |
| `config/ablations/best_of_n_only.yaml` | Preset: best_of_n=2, no memory, no verifier |
| `config/ablations/full_system.yaml` | Preset: everything enabled |
| `config/defaults.yaml` (updated) | `features:` toggle section |
| `tests/test_experiment_manifest.py` | Manifest creation tests (8) |
| `tests/test_metrics_aggregation.py` | Metrics computation tests (8) |
| `tests/test_compare_experiments.py` | Experiment comparison tests (5) |
