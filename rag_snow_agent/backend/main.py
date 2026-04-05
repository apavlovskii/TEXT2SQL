"""FastAPI application entry point."""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import AppSettings
from .services.agent_adapter import AgentAdapter
from .services.session_store import SessionStore
from .services.sqlite_executor import SQLiteExecutor

# Ensure rag_snow_agent package is importable
_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

log = logging.getLogger(__name__)


def _make_executor_factory(settings: AppSettings):
    """Return a callable(db_id) that creates the right executor."""
    if settings.DATASOURCE == "sqlite":
        def factory(db_id: str):
            return SQLiteExecutor(
                db_path=settings.SQLITE_MIRROR_PATH,
                db_id=db_id,
                sample_rows=settings.MAX_RESULT_ROWS,
                statement_timeout_sec=settings.QUERY_TIMEOUT_SEC,
            )
        return factory
    else:
        def factory(db_id: str):
            from rag_snow_agent.snowflake.executor import SnowflakeExecutor
            return SnowflakeExecutor(
                credentials_path=settings.SNOWFLAKE_CREDENTIALS_JSON,
                db_id=db_id,
                statement_timeout_sec=settings.QUERY_TIMEOUT_SEC,
                sample_rows=settings.MAX_RESULT_ROWS,
            )
        return factory


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize singletons at startup, clean up on shutdown."""
    settings = AppSettings()
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    log.info("Starting backend (datasource=%s)", settings.DATASOURCE)

    # ChromaStore + retriever
    chroma_store = None
    retriever = None
    try:
        from rag_snow_agent.chroma.chroma_store import ChromaStore
        from rag_snow_agent.retrieval.hybrid_retriever import HybridRetriever
        chroma_store = ChromaStore(persist_dir=settings.CHROMA_DIR)
        retriever = HybridRetriever(chroma_store.schema_collection())
        log.info("ChromaDB initialized from %s", settings.CHROMA_DIR)
    except Exception as exc:
        log.warning("ChromaDB init failed (agent will be unavailable): %s", exc)

    # Executor factory
    executor_factory = _make_executor_factory(settings)

    # Agent adapter
    agent = AgentAdapter(
        chroma_store=chroma_store,
        retriever=retriever,
        config=settings,
        executor_factory=executor_factory,
    )

    # Session store
    session_store = SessionStore(settings.SESSION_DB_PATH)

    # Store on app.state
    app.state.settings = settings
    app.state.agent = agent
    app.state.session_store = session_store

    log.info("Backend ready (agent_ready=%s, databases=%s)", agent.is_ready, settings.AVAILABLE_DB_IDS)
    yield
    log.info("Shutting down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Text2SQL Agent",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS
    settings = AppSettings()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routes
    from .routes import chat, collections, health, schema, sessions
    app.include_router(health.router)
    app.include_router(sessions.router)
    app.include_router(chat.router)
    app.include_router(schema.router)
    app.include_router(collections.router)

    return app


app = create_app()
