"""Application configuration from environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class AppSettings(BaseSettings):
    """All settings are read from environment variables (or .env file)."""

    # Datasource
    DATASOURCE: Literal["sqlite", "snowflake"] = "sqlite"
    SQLITE_MIRROR_PATH: str = str(_PROJECT_ROOT / "data" / "mirror.db")
    SNOWFLAKE_CREDENTIALS_JSON: str = str(_PROJECT_ROOT / "snowflake_credentials.json")

    # Agent / LLM
    OPENAI_API_KEY: str = ""
    CHROMA_DIR: str = str(_PROJECT_ROOT / ".chroma")
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_TEMPERATURE: float = 0.2
    DEFAULT_DB_ID: str = "GA360"
    AVAILABLE_DB_IDS: list[str] = ["GA360", "GA4", "PATENTS", "PATENTS_GOOGLE"]

    # Retrieval (mirrors config/defaults.yaml)
    TOP_K_TABLES: int = 25
    TOP_K_COLUMNS: int = 100
    MAX_SCHEMA_TOKENS: int = 10000

    # Guardrails
    MAX_RESULT_ROWS: int = 100
    QUERY_TIMEOUT_SEC: int = 30
    DEBUG_MODE: bool = False

    # Session persistence
    SESSION_DB_PATH: str = str(_PROJECT_ROOT / "data" / "sessions.db")

    # Server
    CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://localhost:3000"]
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": str(_PROJECT_ROOT / ".env"), "extra": "ignore"}
