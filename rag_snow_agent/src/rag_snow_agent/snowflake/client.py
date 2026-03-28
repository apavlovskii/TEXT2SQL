"""Snowflake connection helper."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import snowflake.connector

log = logging.getLogger(__name__)


def connect(credentials_path: str | Path) -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection from a JSON credentials file."""
    creds: dict[str, Any] = json.loads(Path(credentials_path).read_text())
    log.info("Connecting to Snowflake account=%s user=%s", creds.get("account"), creds.get("user"))
    return snowflake.connector.connect(**creds)
