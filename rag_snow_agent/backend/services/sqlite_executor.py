"""SQLite executor that mirrors the SnowflakeExecutor interface.

Executes SQL against the local mirror.db, rewriting Snowflake FQN table
references to SQLite table names.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Matches the interface of rag_snow_agent.snowflake.executor.ExecutionResult."""

    success: bool
    sql: str
    error_message: str | None = None
    error_type: str | None = None
    row_count: int | None = None
    rows_sample: list[tuple] | None = None
    column_names: list[str] | None = None
    elapsed_ms: int | None = None
    explain_only: bool = False


class SQLiteExecutor:
    """Execute SQL against the SQLite mirror database."""

    def __init__(
        self,
        db_path: str | Path,
        db_id: str,
        sample_rows: int = 100,
        statement_timeout_sec: int = 30,
    ):
        self.db_path = Path(db_path)
        self.db_id = db_id
        self.sample_rows = sample_rows
        self.statement_timeout_sec = statement_timeout_sec
        self._fqn_map: dict[str, str] = {}
        self._load_metadata()

    def _load_metadata(self) -> None:
        """Load FQN→SQLite table name mapping from _metadata table."""
        if not self.db_path.exists():
            log.warning("Mirror DB not found: %s", self.db_path)
            return
        conn = sqlite3.connect(str(self.db_path))
        try:
            cur = conn.execute("SELECT snowflake_fqn, sqlite_table FROM _metadata")
            for fqn, sqlite_name in cur.fetchall():
                self._fqn_map[fqn] = sqlite_name
                # Also map quoted versions
                self._fqn_map[f'"{fqn}"'] = sqlite_name
        except sqlite3.OperationalError:
            log.warning("No _metadata table in mirror DB")
        finally:
            conn.close()

    def _rewrite_sql(self, sql: str) -> str:
        """Rewrite Snowflake SQL to SQLite-compatible SQL."""
        rewritten = sql

        # Replace FQN references (longest first to avoid partial matches)
        for fqn in sorted(self._fqn_map, key=len, reverse=True):
            sqlite_name = self._fqn_map[fqn]
            # Handle both "DB"."SCHEMA"."TABLE" and DB.SCHEMA.TABLE patterns
            quoted_fqn = ".".join(f'"{p}"' for p in fqn.strip('"').split("."))
            rewritten = rewritten.replace(quoted_fqn, f'"{sqlite_name}"')
            rewritten = rewritten.replace(fqn, f'"{sqlite_name}"')

        # Snowflake → SQLite dialect translations (best-effort)
        # VARIANT colon access: t."col":"field"::TYPE → json_extract(t."col", '$.field')
        rewritten = re.sub(
            r'(\w+)\."(\w+)":"(\w+)"::(\w+)',
            r"CAST(json_extract(\1.\"\2\", '$.\3') AS \4)",
            rewritten,
        )
        # Simpler variant: "col":"field"::TYPE
        rewritten = re.sub(
            r'"(\w+)":"(\w+)"::(\w+)',
            r"CAST(json_extract(\"\1\", '$.\2') AS \3)",
            rewritten,
        )
        # VARIANT access without cast: "col":"field"
        rewritten = re.sub(
            r'"(\w+)":"(\w+)"(?!:)',
            r"json_extract(\"\1\", '$.\2')",
            rewritten,
        )

        # LATERAL FLATTEN → json_each (basic rewrite)
        rewritten = re.sub(
            r',?\s*LATERAL\s+FLATTEN\s*\(\s*input\s*=>\s*(\S+?)\."(\w+)"\s*\)\s+(?:AS\s+)?(\w+)',
            r', json_each(\1."\2") AS \3',
            rewritten,
            flags=re.IGNORECASE,
        )

        # FLATTEN value access: alias.value:"field" → json_extract(alias.value, '$.field')
        rewritten = re.sub(
            r'(\w+)\.value:"(\w+)"(?:::"?(\w+)"?)?',
            r"json_extract(\1.value, '$.\2')",
            rewritten,
        )

        # Remove ::TYPE casts (SQLite doesn't support them)
        rewritten = re.sub(r"::\w+", "", rewritten)

        # QUALIFY → wrap in subquery (simplified: just remove QUALIFY for now)
        # TODO: Proper QUALIFY rewriting requires subquery wrapping
        rewritten = re.sub(r"\bQUALIFY\b.*?(?=\bORDER\b|\bLIMIT\b|$)", "", rewritten, flags=re.IGNORECASE | re.DOTALL)

        # ILIKE → LIKE (SQLite is case-insensitive for ASCII by default)
        rewritten = re.sub(r"\bILIKE\b", "LIKE", rewritten, flags=re.IGNORECASE)

        # TRY_TO_DATE → date (best effort)
        rewritten = re.sub(r"\bTRY_TO_DATE\b", "date", rewritten, flags=re.IGNORECASE)
        rewritten = re.sub(r"\bTO_DATE\b", "date", rewritten, flags=re.IGNORECASE)

        # DATE_TRUNC('MONTH', x) → strftime('%Y-%m-01', x)
        rewritten = re.sub(
            r"DATE_TRUNC\s*\(\s*'MONTH'\s*,\s*(.+?)\)",
            r"strftime('%Y-%m-01', \1)",
            rewritten,
            flags=re.IGNORECASE,
        )
        rewritten = re.sub(
            r"DATE_TRUNC\s*\(\s*'YEAR'\s*,\s*(.+?)\)",
            r"strftime('%Y-01-01', \1)",
            rewritten,
            flags=re.IGNORECASE,
        )

        # TO_TIMESTAMP → datetime
        rewritten = re.sub(r"\bTO_TIMESTAMP\b", "datetime", rewritten, flags=re.IGNORECASE)

        return rewritten

    def execute(self, sql: str, sample_rows: int | None = None) -> ExecutionResult:
        """Execute SQL against SQLite mirror and return results."""
        limit = sample_rows or self.sample_rows
        rewritten = self._rewrite_sql(sql)

        start = time.monotonic()
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute(f"PRAGMA busy_timeout = {self.statement_timeout_sec * 1000}")
            cur = conn.execute(rewritten)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchmany(limit + 1)
            elapsed = int((time.monotonic() - start) * 1000)

            truncated = len(rows) > limit
            if truncated:
                rows = rows[:limit]

            conn.close()
            return ExecutionResult(
                success=True,
                sql=rewritten,
                row_count=len(rows),
                rows_sample=[tuple(r) for r in rows],
                column_names=columns,
                elapsed_ms=elapsed,
            )
        except Exception as exc:
            elapsed = int((time.monotonic() - start) * 1000)
            log.warning("SQLite execution failed: %s", exc)
            return ExecutionResult(
                success=False,
                sql=rewritten,
                error_message=str(exc),
                elapsed_ms=elapsed,
            )

    def explain(self, sql: str) -> ExecutionResult:
        """Run EXPLAIN on the SQL to check syntax."""
        rewritten = self._rewrite_sql(sql)
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.execute(f"EXPLAIN {rewritten}")
            rows = cur.fetchall()
            conn.close()
            return ExecutionResult(
                success=True, sql=rewritten, explain_only=True,
                row_count=len(rows),
            )
        except Exception as exc:
            return ExecutionResult(
                success=False, sql=rewritten, error_message=str(exc),
                explain_only=True,
            )

    def close(self) -> None:
        """No persistent connection to close."""
        pass
