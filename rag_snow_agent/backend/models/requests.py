"""Pydantic request models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class ChatRequest(BaseModel):
    session_id: str | None = None
    message: str
    db_id: str = "GA360"
    model: str = "gpt-4o-mini"
    max_retries: int = 10
    max_candidates: int = 2
    datasource: Literal["sqlite", "snowflake"] = "sqlite"


class SessionCreate(BaseModel):
    name: str | None = None
    db_id: str = "GA360"


class SessionRename(BaseModel):
    name: str
