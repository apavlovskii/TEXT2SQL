"""Pydantic response models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool = False


class ExecutionMetadata(BaseModel):
    elapsed_ms: int | None = None
    llm_calls: int = 0
    repair_count: int = 0
    candidate_count: int = 1
    model: str = ""
    datasource: str = ""


class ChatResponse(BaseModel):
    session_id: str
    message_id: str
    answer: str
    sql: str | None = None
    results: QueryResult | None = None
    metadata: ExecutionMetadata | None = None
    error: str | None = None
    execution_log: list[str] = []
    timestamp: datetime


class SchemaTableInfo(BaseModel):
    qualified_name: str
    comment: str | None = None
    columns: list[dict] = []


class SchemaResponse(BaseModel):
    db_id: str
    tables: list[SchemaTableInfo]


class MessageResponse(BaseModel):
    id: str
    role: Literal["user", "assistant"]
    content: str
    sql: str | None = None
    results: QueryResult | None = None
    metadata: ExecutionMetadata | None = None
    error: str | None = None
    execution_log: list[str] = []
    timestamp: datetime


class SessionResponse(BaseModel):
    id: str
    name: str
    db_id: str
    created_at: datetime
    updated_at: datetime
    message_count: int


class SessionDetailResponse(SessionResponse):
    messages: list[MessageResponse]


class HealthResponse(BaseModel):
    status: str = "ok"
    datasource: str
    available_databases: list[str]
    agent_ready: bool
    debug_mode: bool
    version: str = "0.1.0"
