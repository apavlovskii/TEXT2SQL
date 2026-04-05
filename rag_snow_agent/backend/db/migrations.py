"""Session database schema creation."""

from __future__ import annotations

import sqlite3


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    db_id TEXT NOT NULL DEFAULT 'GA360',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    sql_text TEXT,
    results_json TEXT,
    metadata_json TEXT,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    sort_order INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_session
    ON messages(session_id, sort_order);
"""


def ensure_schema(db_path: str) -> None:
    """Create session tables if they don't exist."""
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA_SQL)
    conn.close()
