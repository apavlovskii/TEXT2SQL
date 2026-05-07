# Architecture Diagram Prompt

Draw a detailed, professional architecture diagram for the following Text-to-SQL system called "Analytics Insite". Use clean boxes, arrows with labels, and color-coded sections. The diagram should flow left-to-right or top-to-bottom.

## System Overview

A full-stack web application where users ask natural language questions about databases and receive SQL queries, execution results, and natural language answers. The system uses a custom RAG (Retrieval-Augmented Generation) pipeline — no agent frameworks.

## Components to draw

### 1. Frontend (React)
A single-page app with three panels:
- **Left sidebar**: database selector, datasource toggle (SQLite/Snowflake), model selector (GPT-5.4, GPT-4o, etc.), parameter controls (max retries, max candidates), schema browser (expandable tree of tables→columns), vector DB collection viewer, session list with new/delete
- **Center**: chat interface with message bubbles. User messages on the right (blue). Assistant messages on the left (gray) with collapsible cards: Answer text, Results table (open by default), SQL query (collapsed), Metadata (collapsed). Loading indicator with "thinking" status messages and cancel button
- **Right panel**: sliding execution log panel (20% width) showing step-by-step agent progress

The frontend connects to the backend via REST API and Server-Sent Events (SSE) for streaming.

### 2. Backend (FastAPI)
Show these API routes:
- POST /api/chat and GET /api/chat/stream (SSE) — main chat endpoints
- GET/POST/DELETE /api/sessions — session management
- GET /api/schema/{db_id} — schema browser data
- GET /api/collections — vector DB info
- GET /api/health — health check
- GET /api/databases — available databases per datasource

### 3. Agent Adapter (core orchestration)
This is the central component. Show it as a pipeline with numbered steps:

**Step 1: Question Classification** — Heuristic check: if clearly non-data question (greeting, general knowledge), route directly to LLM for a plain answer, bypassing the entire pipeline.

**Step 2: Schema Retrieval** — Query ChromaDB vector database using hybrid retrieval (dense embeddings + lexical matching + RRF fusion). Returns a SchemaSlice: relevant tables, columns with types, descriptions, VARIANT field paths, date formats. Budget trimming enforces a 10,000-token limit.

**Step 3: Context Enrichment** — Retrieve semantic facts (date formats, metric candidates, VARIANT structure) and sample records from ChromaDB. Enrich VARIANT columns with ARRAY/OBJECT classification and known sub-field names.

**Step 4: Question Decomposition** — LLM call to break the question into structured subgoals: temporal scope, filters, measures, groupings, nested fields, expected output shape.

**Step 5: Plan Generation (LLM)** — LLM generates a structured JSON QueryPlan containing: selected_tables, joins, flatten_ops (LATERAL FLATTEN specifications), filters, aggregations, group_by, order_by, CTEs. Multiple candidates generated using diverse prompt strategies (default, flatten_first, cte_first, join_first, metric_first, time_first).

**Step 6: Deterministic SQL Compilation** — Pure Python compiler converts QueryPlan → Snowflake SQL. No LLM call. Handles LATERAL FLATTEN syntax, CTE pipelines, VARIANT field path resolution, column quoting, stable aliases. If compilation fails (empty plan), retries with feedback then falls back to LLM-direct SQL generation.

**Step 7: SQL Validation** — Guardrails check: only SELECT/WITH allowed. Reject DDL/DML keywords. Reject multiple statements. Identifier validation against schema.

**Step 8: Execution + Repair Loop** — Execute SQL against database (Snowflake or SQLite). If EXPLAIN fails: classify error (8 categories: object_not_found, invalid_identifier, sql_syntax_error, aggregation_error, type_mismatch, etc.), apply error-specific repair prompt, retry. If execution succeeds but results don't match gold: repair with "wrong results" feedback. Up to 4 repair iterations per candidate.

**Step 9: Best-of-N Selection** — Score all candidates using multi-signal selector: +100 for execution success, shape alignment bonuses, repair penalties, metamorphic check deltas. Select highest-scoring candidate.

**Step 10: Answer Generation** — LLM call with question + SQL + result rows to produce a natural language answer.

**Step 11: Persistence** — Save user message and assistant response (with SQL, results, metadata, execution log) to SQLite session database.

### 4. ChromaDB Vector Database
Show as a cylinder/database with 5 collections:
- **schema_cards** (622 items): TableCards, ColumnCards (with LLM-generated descriptions from data profiling), JoinCards
- **semantic_cards** (5,762 items): date format patterns, metric candidates, VARIANT structure facts
- **sample_records** (10 items): sample rows per table for context injection
- **trace_memory** (5 items): successful solution traces for few-shot learning
- **snowflake_syntax** (55 items): SQL reference chunks for repair guidance

### 5. Data Profiling Pipeline (offline)
Show as a separate offline process:
- Connect to Snowflake → Extract 100 rows per table (partition-deduplicated)
- Profile columns (null rates, value ranges, unique counts, VARIANT structure)
- GPT-5.4 generates descriptions for each table and column
- Save to table_column_descriptions.json
- Enrich ChromaDB schema_cards with descriptions

### 6. Datasources
Two boxes:
- **SQLite Mirror** (data/mirror.db): local copy with 10 tables, 50 rows, JSON VARIANT columns. SQL rewriting translates Snowflake FQN→SQLite names and Snowflake dialect→SQLite (json_extract, json_each). Default for development.
- **Snowflake**: live cloud database. Connected via snowflake-connector-python. Credentials stay server-side.

### 7. External Services
- **OpenAI API**: LLM calls (plan generation, repair, decomposition, answer generation). Models: GPT-5.4, GPT-5-mini, GPT-4o, GPT-4o-mini. Also used for text-embedding-3-large embeddings in ChromaDB.

## Visual style notes
- Use distinct colors for: Frontend (blue), Backend/API (green), Agent Pipeline (orange/yellow), ChromaDB (purple), Datasources (gray), OpenAI (red/coral)
- Show the SSE streaming connection as a dashed arrow from backend to frontend
- Show the agent pipeline steps as a numbered vertical flow inside the Agent Adapter box
- Show ChromaDB being read by steps 2 and 3, and written by the Data Profiling pipeline
- Show the datasource being read by step 8 (execution)
- Show OpenAI being called by steps 4, 5, 8 (repair), and 10
