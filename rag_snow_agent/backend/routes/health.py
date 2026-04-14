"""Health check and database listing endpoints."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from fastapi import APIRouter, Depends, Query

from ..config import AppSettings
from ..dependencies import get_agent, get_settings
from ..models.responses import HealthResponse
from ..services.agent_adapter import AgentAdapter

router = APIRouter(tags=["health"])
log = logging.getLogger(__name__)


@router.get("/api/health", response_model=HealthResponse)
def health(
    settings: AppSettings = Depends(get_settings),
    agent: AgentAdapter = Depends(get_agent),
):
    return HealthResponse(
        datasource=settings.DATASOURCE,
        available_databases=settings.AVAILABLE_DB_IDS,
        agent_ready=agent.is_ready,
        debug_mode=settings.DEBUG_MODE,
    )


@router.get("/api/databases")
def list_databases(
    datasource: str = Query("sqlite"),
    settings: AppSettings = Depends(get_settings),
    agent: AgentAdapter = Depends(get_agent),
):
    """Return available databases for a given datasource."""
    if datasource == "sqlite":
        # Read from mirror.db _metadata table
        db_path = Path(settings.SQLITE_MIRROR_PATH)
        if db_path.exists():
            try:
                conn = sqlite3.connect(str(db_path))
                cur = conn.execute("SELECT DISTINCT db_id FROM _metadata ORDER BY db_id")
                dbs = [row[0] for row in cur.fetchall()]
                conn.close()
                return {"datasource": "sqlite", "databases": dbs}
            except Exception as exc:
                log.warning("Failed to read SQLite databases: %s", exc)
        return {"datasource": "sqlite", "databases": settings.AVAILABLE_DB_IDS}

    elif datasource == "snowflake":
        # Try to list databases from Snowflake via the agent's chroma store
        # (which indexes databases it knows about)
        if agent._chroma_store:
            try:
                col = agent._chroma_store.schema_collection()
                results = col.get(
                    where={"object_type": "table"},
                    include=["metadatas"],
                    limit=5000,
                )
                dbs = sorted(set(
                    m.get("db_id", "") for m in (results.get("metadatas") or []) if m.get("db_id")
                ))
                if dbs:
                    return {"datasource": "snowflake", "databases": dbs}
            except Exception as exc:
                log.warning("Failed to read Snowflake databases from ChromaDB: %s", exc)
        return {"datasource": "snowflake", "databases": settings.AVAILABLE_DB_IDS}

    return {"datasource": datasource, "databases": settings.AVAILABLE_DB_IDS}
