"""Snowflake session state management."""

from __future__ import annotations

import logging

import snowflake.connector

log = logging.getLogger(__name__)


def set_session(
    conn: snowflake.connector.SnowflakeConnection,
    db_id: str,
    schema: str | None = None,
) -> None:
    """Apply session guardrails: USE DATABASE and optionally USE SCHEMA."""
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {db_id}")
        log.info("Session: USE DATABASE %s", db_id)
        if schema:
            cur.execute(f"USE SCHEMA {schema}")
            log.info("Session: USE SCHEMA %s", schema)
    finally:
        cur.close()
