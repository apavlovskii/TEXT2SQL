# Plan: Web UI for Text-to-SQL Agent

## Context

The rag_snow_agent is currently CLI-only. We need a full-stack chatbot web app (React + FastAPI) that lets users ask natural language questions and get SQL + results back. The Snowflake account is currently suspended, so the default datasource must be the local SQLite mirror (`data/mirror.db`). All agent code exists and works — the web UI wraps it.

## Directory Structure

All new code goes under `rag_snow_agent/`:

```
rag_snow_agent/
├── backend/
│   ├── main.py              # FastAPI app, CORS, lifespan
│   ├── config.py            # Pydantic Settings from env
│   ├── dependencies.py      # FastAPI Depends providers
│   ├── routes/
│   │   ├── chat.py          # POST /api/chat, GET /api/chat/stream
│   │   ├── sessions.py      # CRUD /api/sessions
│   │   └── health.py        # GET /api/health
│   ├── models/
│   │   ├── requests.py      # ChatRequest, SessionCreate
│   │   └── responses.py     # ChatResponse, QueryResult, etc.
│   ├── services/
│   │   ├── agent_adapter.py # Async wrapper around solve_instance
│   │   ├── sqlite_executor.py # Executor using mirror.db
│   │   ├── sql_guardrails.py  # Read-only SQL validation
│   │   ├── answer_generator.py # NL answer from SQL+results via LLM
│   │   └── session_store.py   # SQLite session/message persistence
│   └── db/
│       └── migrations.py    # Session DB schema
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── index.html
│   └── src/
│       ├── main.tsx
│       ├── App.tsx
│       ├── api/client.ts    # Fetch + SSE wrapper
│       ├── types/index.ts   # TypeScript types
│       ├── hooks/
│       │   ├── useChat.ts
│       │   ├── useSessions.ts
│       │   └── useSSE.ts
│       ├── components/
│       │   ├── ChatView.tsx
│       │   ├── MessageBubble.tsx
│       │   ├── ChatInput.tsx
│       │   ├── SessionSidebar.tsx
│       │   ├── SqlCard.tsx
│       │   ├── ResultTable.tsx
│       │   ├── MetadataCard.tsx
│       │   ├── ErrorCard.tsx
│       │   └── LoadingIndicator.tsx
│       └── styles/globals.css
├── docker-compose.yml
└── README.md
```

## Backend (12 files)

### 1. `backend/config.py` — Pydantic Settings
- `DATASOURCE`: "sqlite" (default) or "snowflake"
- `SQLITE_MIRROR_PATH`: "data/mirror.db"
- `SNOWFLAKE_CREDENTIALS_JSON`, `OPENAI_API_KEY`, `CHROMA_DIR`
- `LLM_MODEL`, `MAX_RESULT_ROWS` (100), `QUERY_TIMEOUT_SEC` (30), `DEBUG_MODE` (false)
- `SESSION_DB_PATH`: "data/sessions.db"
- `CORS_ORIGINS`: ["http://localhost:5173"]
- `AVAILABLE_DB_IDS`: ["GA360", "GA4", "PATENTS", "PATENTS_GOOGLE"]

### 2. `backend/main.py` — FastAPI app
- Lifespan handler initializes: ChromaStore, HybridRetriever, executor factory, SessionStore
- Stores singletons on `app.state`
- CORS middleware from config
- Include routers: chat, sessions, health

### 3. `backend/dependencies.py`
- `get_agent()` → AgentAdapter from app.state
- `get_session_store()` → SessionStore from app.state
- `get_config()` → AppSettings from app.state

### 4. `backend/routes/chat.py`
- `POST /api/chat` — sync flow: run agent → validate SQL → execute → generate NL answer → persist message → return ChatResponse
- `GET /api/chat/stream` — SSE flow: same but yields thinking events via `StreamingResponse`
- Agent runs in ThreadPoolExecutor (it's synchronous)
- Progress callback uses stdlib `queue.Queue` for thread→async bridging

### 5. `backend/routes/sessions.py`
- `GET /api/sessions` — list sessions
- `POST /api/sessions` — create session
- `GET /api/sessions/{id}` — get session with messages
- `DELETE /api/sessions/{id}` — delete
- `PATCH /api/sessions/{id}` — rename

### 6. `backend/routes/health.py`
- Returns datasource type, available databases, agent readiness, version

### 7. `backend/models/requests.py` & `responses.py`
- Pydantic models: ChatRequest, ChatResponse, QueryResult, ExecutionMetadata, SessionResponse, MessageResponse
- ChatResponse includes: answer, sql, results (columns + rows), metadata (gated by DEBUG_MODE), error

### 8. `backend/services/agent_adapter.py` — Core integration
- Wraps `solve_instance` from `src/rag_snow_agent/agent/agent.py`
- Replicates the init sequence from `experiment_runner.py` lines 356-463:
  1. `build_schema_slice(retriever, question, db_id, ...)`
  2. `retrieve_semantic_context(...)` if semantic layer enabled
  3. `build_sample_context(...)` if sample records enabled
  4. `solve_instance(...)` with `best_of_n=1` for interactive speed
- Re-executes `final_sql` to get actual row data (solve_instance doesn't expose rows)
- Uses `ThreadPoolExecutor(max_workers=4)` for sync→async
- Progress callback sends status strings for SSE thinking messages

### 9. `backend/services/sqlite_executor.py`
- Implements same interface as `SnowflakeExecutor` (execute, explain, close)
- Loads `_metadata` table to map Snowflake FQN → SQLite table names
- Rewrites SQL: replaces `DB.SCHEMA.TABLE` with SQLite table names
- Best-effort Snowflake→SQLite dialect translation (known limitations documented with TODOs)
- Returns `ExecutionResult` matching the existing dataclass

### 10. `backend/services/sql_guardrails.py`
- Validates SQL is read-only before execution
- Whitelist: SELECT, WITH
- Blacklist: INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, MERGE, EXEC, COPY, PUT, GET
- Rejects multiple statements (semicolon-separated)

### 11. `backend/services/answer_generator.py`
- After SQL execution, calls LLM to generate NL answer from question + SQL + results
- Reuses existing `call_llm` from `src/rag_snow_agent/agent/llm_client.py`
- Runs in thread pool (call_llm is synchronous)

### 12. `backend/services/session_store.py` + `backend/db/migrations.py`
- SQLite DB at `data/sessions.db` with tables: sessions, messages
- Session auto-naming from first message (truncated to 50 chars)
- Messages store: role, content, sql, results_json, metadata_json, error

## Frontend (15 files)

### Component hierarchy
```
App → SessionSidebar + ChatView
SessionSidebar → NewSessionButton + SessionList
ChatView → MessageList + LoadingIndicator + ChatInput
MessageBubble → AnswerText + SqlCard + ResultTable + MetadataCard + ErrorCard
```

### Key behavior
- `useChat` hook: manages messages array, isLoading, streamStatus
- `useSessions` hook: manages session list, active session, CRUD
- `useSSE` hook: EventSource wrapper for /api/chat/stream
- Enter sends message, Shift+Enter for newline
- Session switch fetches full history from API
- Collapsible sections on assistant responses (SQL, Results, Metadata default collapsed)
- ResultTable renders up to MAX_RESULT_ROWS with "truncated" indicator
- MetadataCard hidden when DEBUG_MODE=false (backend strips it from response)

### Styling
- Tailwind CSS (installed via Vite plugin)
- Clean, modern chat UI with left sidebar

## Docker

```yaml
services:
  backend:
    build: { context: ., dockerfile: Dockerfile.backend }
    ports: ["8000:8000"]
    volumes: [./data:/app/data, ./.chroma:/app/.chroma]
    env_file: [.env]
  frontend:
    build: { context: ./frontend }
    ports: ["5173:5173"]
    depends_on: [backend]
```

## Key Files to Reuse

| What | File | Function/Class |
|------|------|---------------|
| Agent orchestration | `src/rag_snow_agent/agent/agent.py` | `solve_instance()` |
| Schema retrieval | `src/rag_snow_agent/retrieval/debug_retrieve.py` | `build_schema_slice()` |
| Semantic context | `src/rag_snow_agent/retrieval/semantic_retriever.py` | `retrieve_semantic_context()` |
| Sample records | `src/rag_snow_agent/chroma/sample_records.py` | `SampleRecordStore`, `build_sample_context()` |
| ChromaDB | `src/rag_snow_agent/chroma/chroma_store.py` | `ChromaStore` |
| Hybrid retriever | `src/rag_snow_agent/retrieval/hybrid_retriever.py` | `HybridRetriever` |
| LLM client | `src/rag_snow_agent/agent/llm_client.py` | `call_llm()` |
| Executor interface | `src/rag_snow_agent/snowflake/executor.py` | `SnowflakeExecutor`, `ExecutionResult` |
| Config | `config/defaults.yaml` | Retrieval/LLM/agent settings |
| Init sequence | `src/rag_snow_agent/eval/experiment_runner.py` | Lines 356-463 (canonical startup) |

## Implementation Order

1. Backend skeleton: config, main.py, health route
2. Session store + routes
3. SQL guardrails
4. SQLite executor adapter
5. Agent adapter service
6. Answer generator
7. Chat routes (sync first, then streaming)
8. Frontend: Vite scaffold, types, API client
9. Frontend: SessionSidebar + ChatView + ChatInput
10. Frontend: MessageBubble + collapsible cards
11. Frontend: SSE integration + LoadingIndicator
12. Docker compose + README

## Verification

1. `uvicorn backend.main:app` starts without errors
2. `GET /api/health` returns datasource + available DBs
3. `POST /api/sessions` creates a session
4. `POST /api/chat` with a question returns answer + SQL + results
5. `GET /api/chat/stream` sends thinking events then result
6. Frontend renders chat, sessions sidebar, collapsible cards
7. SQL guardrails reject `DROP TABLE` but accept `SELECT`
8. Session persistence survives server restart

## Known Limitations (v1)

- SQLite executor does best-effort SQL rewriting — complex VARIANT/FLATTEN queries will fail
- Only 50 sample rows per table in mirror.db
- `best_of_n=1` for interactive speed (vs 8 in benchmarks)
- No auth/multi-user support
- No websocket — SSE only (unidirectional)
