"""Health check endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..config import AppSettings
from ..dependencies import get_agent, get_settings
from ..models.responses import HealthResponse
from ..services.agent_adapter import AgentAdapter

router = APIRouter(tags=["health"])


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
