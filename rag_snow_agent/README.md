# Analytics Insite — Text-to-SQL Agent

A full-stack chatbot that converts natural language analytics questions into executable SQL queries using a custom RAG-based agent. No agent frameworks (LangChain, LangGraph, etc.) — the pipeline is hand-built for precise control over schema retrieval, SQL compilation, and repair loops.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────┐     ┌─────────────────┐
│   Frontend   │────▶│         Backend (FastAPI)     │────▶│  Datasource     │
│  React/Vite  │◀────│                              │◀────│  SQLite or      │
│  Tailwind    │ SSE │  Agent Adapter                │     │  Snowflake      │
│  TypeScript  │     │  ├─ ChromaDB retrieval        │     └─────────────────┘
└─────────────┘     │  ├─ LLM plan generation        │
   port 5173        │  ├─ SQL compiler (FLATTEN/CTE) │     ┌─────────────────┐
                    │  ├─ Repair loop                │────▶│  OpenAI API     │
                    │  └─ NL answer generation       │◀────│  (GPT-4o/5.4)   │
                    └──────────────────────────────┘     └─────────────────┘
                       port 8000
```

- **Frontend**: React 18 + Vite + TypeScript + Tailwind CSS
- **Backend**: FastAPI + Python 3.11 + Pydantic v2
- **Agent**: Custom RAG pipeline — ChromaDB vector retrieval → LLM plan generation → deterministic SQL compilation → Snowflake execution → error-specific repair loop
- **Datasource**: SQLite mirror (default, for local development) or Snowflake

---

## Prerequisites

| Tool | Version | Required for | Install guide |
|------|---------|-------------|---------------|
| **Python** | 3.11+ | Backend + agent | https://python.org or `pyenv install 3.11` |
| **uv** | latest | Python package management | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **Node.js** | 20+ | Frontend | https://nodejs.org or `nvm install 20` |
| **npm** | 10+ | Frontend dependencies | Comes with Node.js |
| **Docker** | 24+ | Container deployment (optional) | https://docs.docker.com/get-docker/ |
| **Docker Compose** | v2+ | Multi-container orchestration (optional) | Included with Docker Desktop |
| **OpenAI API key** | — | LLM calls (required) | https://platform.openai.com/api-keys |

### Optional (for Snowflake datasource)

| Tool | Required for |
|------|-------------|
| **Snowflake account** | Live database queries |
| **snowflake_credentials.json** | Snowflake authentication |

---

## Installation

### Option A: Local Development (recommended)

#### Step 1: Clone and navigate

```bash
git clone https://github.com/apavlovskii/TEXT2SQL.git
cd TEXT2SQL/rag_snow_agent
```

#### Step 2: Install Python dependencies

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all Python dependencies (creates .venv automatically)
uv sync
```

This installs: FastAPI, uvicorn, ChromaDB, OpenAI SDK, Snowflake connector, Pydantic, tiktoken, and all other dependencies specified in `pyproject.toml`.

#### Step 3: Configure environment

```bash
# Copy the example env file
cp .env.example .env

# Edit .env and set your OpenAI API key
# OPENAI_API_KEY=sk-proj-your-key-here
```

The `.env` file supports these variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | — | **Required.** Your OpenAI API key |
| `DATASOURCE` | `sqlite` | `sqlite` for local mirror, `snowflake` for live DB |
| `SQLITE_MIRROR_PATH` | `data/mirror.db` | Path to the SQLite mirror database |
| `SNOWFLAKE_CREDENTIALS_JSON` | `./snowflake_credentials.json` | Path to Snowflake credentials |
| `CHROMA_DIR` | `.chroma` | ChromaDB persistence directory |
| `LLM_MODEL` | `gpt-4o-mini` | Default LLM model (overridable per-query in UI) |
| `MAX_RESULT_ROWS` | `100` | Maximum rows returned per query |
| `QUERY_TIMEOUT_SEC` | `30` | SQL execution timeout |
| `DEBUG_MODE` | `false` | Show execution metadata in responses |
| `SESSION_DB_PATH` | `data/sessions.db` | Chat session persistence database |
| `CORS_ORIGINS` | `["http://localhost:5173"]` | Allowed frontend origins |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

#### Step 4: Verify data files exist

The following files should already be present from the repository:

```bash
ls data/mirror.db        # SQLite mirror database (900 KB, 10 tables, 50 rows)
ls data/sample_records.json  # Sample records for context injection
ls data/table_column_descriptions.json  # Column descriptions
ls .chroma/chroma.sqlite3   # ChromaDB vector index
```

If `data/mirror.db` is missing, rebuild it:

```bash
uv run python scripts/build_sqlite_mirror.py
```

#### Step 5: Start the backend

```bash
uv run uvicorn backend.main:app --reload --port 8000
```

You should see:

```
INFO backend.main: Starting backend (datasource=sqlite)
INFO backend.main: ChromaDB initialized from ./.chroma
INFO backend.main: Backend ready (agent_ready=True, databases=['GA360', 'GA4', 'PATENTS', 'PATENTS_GOOGLE'])
INFO:     Uvicorn running on http://127.0.0.1:8000
```

Verify the API:

```bash
curl http://localhost:8000/api/health
```

#### Step 6: Install and start the frontend

In a separate terminal:

```bash
cd rag_snow_agent/frontend

# Install Node.js dependencies
npm install

# Start the dev server
npm run dev
```

#### Step 7: Open the application

Navigate to **http://localhost:5173** in your browser.

---

### Option B: Docker Compose

```bash
cd TEXT2SQL/rag_snow_agent

# Configure
cp .env.example .env
# Edit .env and set OPENAI_API_KEY

# Build and start both services
docker compose up --build
```

This starts:
- **backend** on port 8000 — FastAPI server with the agent pipeline
- **frontend** on port 5173 — React development server

Data directories (`data/` and `.chroma/`) are mounted as volumes so session data and vector indexes persist across container restarts.

Open **http://localhost:5173** in your browser.

Stop with `docker compose down`.

---

### Option C: Docker (backend only)

For API-only usage without the frontend:

```bash
cd TEXT2SQL/rag_snow_agent

docker build -f Dockerfile.backend -t text2sql-backend .

docker run -p 8000:8000 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/.chroma:/app/.chroma \
  --env-file .env \
  text2sql-backend
```

---

## Snowflake Configuration (optional)

To use Snowflake instead of the SQLite mirror:

1. Create `snowflake_credentials.json`:

```json
{
    "account": "your-account.us-east-1",
    "user": "your_username",
    "password": "your_password",
    "warehouse": "COMPUTE_WH",
    "database": "GA360",
    "schema": "PUBLIC"
}
```

2. Set `DATASOURCE=snowflake` in `.env` (or select "Snowflake" in the UI sidebar).

3. Build the ChromaDB index if not already done:

```bash
uv run python -m rag_snow_agent.chroma.build_index \
  --db_id GA360 --credentials ./snowflake_credentials.json --chroma_dir .chroma/
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check, datasource, available databases |
| POST | `/api/chat` | Send question, get answer + SQL + results |
| GET | `/api/chat/stream` | SSE stream with thinking status + final result |
| GET | `/api/sessions` | List all chat sessions |
| POST | `/api/sessions` | Create new session |
| GET | `/api/sessions/{id}` | Get session with full message history |
| DELETE | `/api/sessions/{id}` | Delete session |
| PATCH | `/api/sessions/{id}` | Rename session |
| GET | `/api/schema/{db_id}` | Browse tables and columns for a database |
| GET | `/api/collections` | List ChromaDB vector index collections |

---

## UI Features

- **Chat interface** with streaming "thinking" status messages
- **Session management** — create, switch, rename, delete sessions
- **Database selector** with drill-down schema browser (tables → columns)
- **Model selector** — GPT-5.4, GPT-5-mini, GPT-5-nano, GPT-4o, GPT-4o-mini
- **Parameter tuning** — max retries, max candidates per query
- **Datasource toggle** — switch between SQLite and Snowflake
- **Response cards** — Results (open by default), SQL, Metadata (collapsible)
- **Execution log** — sliding right panel with detailed agent progress
- **Vector DB inspector** — collapsible view of ChromaDB collections
- **Cancel button** — stop agent execution mid-query
- **Smart routing** — non-data questions answered directly by LLM without agent

## Guardrails

- Only `SELECT` / `WITH ... SELECT` queries are allowed
- DDL/DML keywords are rejected (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, etc.)
- Multiple statements (semicolon-separated) are rejected
- Snowflake credentials stay server-side, never exposed to frontend
- Configurable result row limit and query timeout
- Agent retries until a compilable query is produced (up to max_retries)

---

## Benchmark Execution

The benchmark runner is independent of the web UI and remains fully functional:

```bash
cd rag_snow_agent

uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials ./snowflake_credentials.json \
  --experiment benchmark_run_N \
  --limit 25 --best_of_n 8 --max_repairs 4 \
  --model gpt-5.4 --chroma_dir .chroma/ \
  --gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/
```

All benchmark results are preserved in `reports/experiments/` (runs 1–8 plus smoke tests).

---

## Project Structure

```
rag_snow_agent/
├── backend/                  # FastAPI web server
│   ├── main.py              # App factory, lifespan, CORS
│   ├── config.py            # Pydantic Settings from env
│   ├── routes/              # API route handlers
│   ├── models/              # Pydantic request/response models
│   └── services/            # Agent adapter, executor, guardrails, sessions
├── frontend/                 # React web client
│   ├── src/components/      # UI components
│   ├── src/hooks/           # React state hooks
│   ├── src/api/             # API client + SSE
│   └── src/types/           # TypeScript types
├── src/rag_snow_agent/       # Core agent package
│   ├── agent/               # Orchestration, LLM client, candidates
│   ├── prompting/           # Plan schema, SQL compiler, prompts
│   ├── retrieval/           # Schema retrieval, budget trimming
│   ├── chroma/              # ChromaDB store, schema/sample cards
│   ├── snowflake/           # Snowflake client and executor
│   └── eval/                # Experiment runner, gold verifier
├── config/defaults.yaml      # Agent configuration
├── data/                     # SQLite mirror, samples, sessions
├── .chroma/                  # ChromaDB vector index
├── reports/experiments/      # Benchmark results
├── docker-compose.yml
└── pyproject.toml
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `OPENAI_API_KEY` not set | Copy `.env.example` to `.env` and add your key |
| Backend fails to start | Run `uv sync` to install dependencies |
| ChromaDB init failed | Verify `.chroma/chroma.sqlite3` exists |
| Frontend shows 404 on `:8000` | Backend is API-only — open `:5173` for the UI |
| Port already in use | `lsof -ti:8000 \| xargs kill` |
| `mirror.db` missing | `uv run python scripts/build_sqlite_mirror.py` |
| Node.js not found | `nvm install 20` or install from nodejs.org |
| Docker build fails | Ensure Docker daemon is running: `docker info` |
| Snowflake suspended | Use SQLite datasource (default) |
