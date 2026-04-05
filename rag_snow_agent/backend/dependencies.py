"""FastAPI dependency providers."""

from __future__ import annotations

from fastapi import Request

from .config import AppSettings
from .services.agent_adapter import AgentAdapter
from .services.session_store import SessionStore


def get_settings(request: Request) -> AppSettings:
    return request.app.state.settings


def get_agent(request: Request) -> AgentAdapter:
    return request.app.state.agent


def get_session_store(request: Request) -> SessionStore:
    return request.app.state.session_store
