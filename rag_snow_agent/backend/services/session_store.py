"""SQLite-backed session and message persistence."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from uuid import uuid4

from ..db.migrations import ensure_schema
from ..models.responses import (
    ExecutionMetadata,
    MessageResponse,
    QueryResult,
    SessionDetailResponse,
    SessionResponse,
)


class SessionStore:
    def __init__(self, db_path: str):
        self._db_path = db_path
        ensure_schema(db_path)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    # ── Sessions ────────────────────────────────────────────────────────

    def create_session(self, name: str | None, db_id: str) -> SessionResponse:
        sid = str(uuid4())
        display_name = name or "New chat"
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()
        conn.execute(
            "INSERT INTO sessions (id, name, db_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (sid, display_name, db_id, now, now),
        )
        conn.commit()
        conn.close()
        return SessionResponse(
            id=sid, name=display_name, db_id=db_id,
            created_at=now, updated_at=now, message_count=0,
        )

    def list_sessions(self) -> list[SessionResponse]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT s.*, COUNT(m.id) as message_count "
            "FROM sessions s LEFT JOIN messages m ON m.session_id = s.id "
            "GROUP BY s.id ORDER BY s.updated_at DESC"
        ).fetchall()
        conn.close()
        return [
            SessionResponse(
                id=r["id"], name=r["name"], db_id=r["db_id"],
                created_at=r["created_at"], updated_at=r["updated_at"],
                message_count=r["message_count"],
            )
            for r in rows
        ]

    def get_session(self, session_id: str) -> SessionDetailResponse | None:
        conn = self._conn()
        row = conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        if not row:
            conn.close()
            return None
        msgs = conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY sort_order",
            (session_id,),
        ).fetchall()
        count = len(msgs)
        conn.close()
        return SessionDetailResponse(
            id=row["id"], name=row["name"], db_id=row["db_id"],
            created_at=row["created_at"], updated_at=row["updated_at"],
            message_count=count,
            messages=[_row_to_message(m) for m in msgs],
        )

    def delete_session(self, session_id: str) -> bool:
        conn = self._conn()
        conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
        cur = conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()
        deleted = cur.rowcount > 0
        conn.close()
        return deleted

    def rename_session(self, session_id: str, name: str) -> bool:
        conn = self._conn()
        now = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "UPDATE sessions SET name = ?, updated_at = ? WHERE id = ?",
            (name, now, session_id),
        )
        conn.commit()
        updated = cur.rowcount > 0
        conn.close()
        return updated

    # ── Messages ────────────────────────────────────────────────────────

    def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        sql: str | None = None,
        results: QueryResult | None = None,
        metadata: ExecutionMetadata | None = None,
        error: str | None = None,
    ) -> MessageResponse:
        mid = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        conn = self._conn()

        # Get next sort_order
        row = conn.execute(
            "SELECT COALESCE(MAX(sort_order), 0) + 1 FROM messages WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        sort_order = row[0]

        results_json = results.model_dump_json() if results else None
        metadata_json = metadata.model_dump_json() if metadata else None

        conn.execute(
            "INSERT INTO messages (id, session_id, role, content, sql_text, results_json, metadata_json, error, created_at, sort_order) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mid, session_id, role, content, sql, results_json, metadata_json, error, now, sort_order),
        )

        # Auto-name session from first user message
        if role == "user" and sort_order == 1:
            short_name = content[:50].strip()
            if len(content) > 50:
                short_name += "..."
            conn.execute(
                "UPDATE sessions SET name = ?, updated_at = ? WHERE id = ? AND name = 'New chat'",
                (short_name, now, session_id),
            )

        conn.execute("UPDATE sessions SET updated_at = ? WHERE id = ?", (now, session_id))
        conn.commit()
        conn.close()

        return MessageResponse(
            id=mid, role=role, content=content, sql=sql,
            results=results, metadata=metadata, error=error, timestamp=now,
        )


def _row_to_message(row: sqlite3.Row) -> MessageResponse:
    results = None
    if row["results_json"]:
        results = QueryResult.model_validate_json(row["results_json"])
    metadata = None
    if row["metadata_json"]:
        metadata = ExecutionMetadata.model_validate_json(row["metadata_json"])
    return MessageResponse(
        id=row["id"],
        role=row["role"],
        content=row["content"],
        sql=row["sql_text"],
        results=results,
        metadata=metadata,
        error=row["error"],
        timestamp=row["created_at"],
    )
