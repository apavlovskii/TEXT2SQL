Build a full-stack chatbot application with a React + Vite + TypeScript frontend and a FastAPI backend.

The app should integrate with an existing Python RAG agent that converts natural language analytics questions into executable Snowflake SQL. 
The backend must call the agent, validate the SQL as read-only, execute it against Snowflake or local sqlite database, and return a structured response.
Implement configurable selector to chose snowflake or sqlite as data source, by default set to sqlite.
Query response must contain:
- natural language answer
- generated SQL
- tabular results
- execution metadata
- errors if any

Frontend requirements:
- modern chat UI
- session sidebar
- message history
- multiline input
- loading state
- assistant response cards with collapsible sections for Answer, SQL, Results, Metadata, Error
- table rendering for results
- ability to create a new session and reopen previous sessions
- display short "thinking messages" from the agent reasoning log to create an impression of a thinking agent.

Backend requirements:
- FastAPI app with routes for /api/chat, /api/chat/stream, /api/sessions, /api/sessions/{id}, /api/health
- service layer for agent orchestration, Snowflake execution, session storage
- SQLite persistence for v1
- Pydantic request/response models
- CORS config for frontend origin
- environment-based config
- structured logging
- robust error handling

Guardrails:
- Snowflake credentials must stay server-side
- only allow read-only SQL (SELECT / WITH ... SELECT)
- reject multiple statements and mutating DDL/DML
- configurable result row limit and query timeout
- debug metadata hidden when DEBUG_MODE=false

Use this repo shape:
- frontend/
- backend/
- shared/
- docker-compose.yml
- README.md

Implement clean, typed, modular code with clear integration points for the existing agent.
Where the existing agent API is unknown, create an adapter wrapper with TODO markers.
Also add a README with local setup and run instructions.