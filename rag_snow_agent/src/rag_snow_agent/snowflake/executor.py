"""Snowflake SQL execution with guardrails."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import snowflake.connector

from .client import connect
from .session import set_session

log = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Outcome of a single SQL execution."""

    success: bool
    sql: str
    error_message: str | None = None
    error_type: str | None = None
    row_count: int | None = None
    rows_sample: list[tuple] | None = None
    column_names: list[str] | None = None
    elapsed_ms: int | None = None
    explain_only: bool = False


class SnowflakeExecutor:
    """Execute SQL against Snowflake with session guardrails."""

    def __init__(
        self,
        credentials_path: str | Path,
        db_id: str,
        schema: str | None = None,
        statement_timeout_sec: int = 120,
        sample_rows: int = 20,
    ) -> None:
        self.credentials_path = credentials_path
        self.db_id = db_id
        self.schema = schema
        self.statement_timeout_sec = statement_timeout_sec
        self.sample_rows = sample_rows
        self._conn: snowflake.connector.SnowflakeConnection | None = None

    def _get_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            self._conn = connect(self.credentials_path)
            set_session(self._conn, self.db_id, self.schema)
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.is_closed():
            self._conn.close()
            self._conn = None

    def explain(self, sql: str) -> ExecutionResult:
        """Run EXPLAIN on the SQL. Returns success/failure without executing."""
        explain_sql = f"EXPLAIN {sql.rstrip(';')}"
        return self._run(explain_sql, original_sql=sql, explain_only=True)

    def execute(self, sql: str, sample_rows: int | None = None) -> ExecutionResult:
        """Execute the SQL and fetch a sample of rows."""
        limit = sample_rows if sample_rows is not None else self.sample_rows
        return self._run(sql, original_sql=sql, explain_only=False, fetch_limit=limit)

    def _run(
        self,
        sql_to_run: str,
        original_sql: str,
        explain_only: bool,
        fetch_limit: int = 0,
    ) -> ExecutionResult:
        conn = self._get_conn()
        cur = conn.cursor()
        t0 = time.monotonic()
        try:
            cur.execute(
                sql_to_run,
                timeout=self.statement_timeout_sec,
            )
            elapsed = int((time.monotonic() - t0) * 1000)

            rows_sample = None
            column_names = None
            row_count = cur.rowcount

            if not explain_only and fetch_limit > 0 and cur.description:
                column_names = [desc[0] for desc in cur.description]
                rows_sample = cur.fetchmany(fetch_limit)
                if row_count is None:
                    row_count = len(rows_sample)

            log.info(
                "SQL %s: rows=%s elapsed=%dms",
                "EXPLAIN" if explain_only else "EXECUTE",
                row_count,
                elapsed,
            )
            return ExecutionResult(
                success=True,
                sql=original_sql,
                row_count=row_count,
                rows_sample=rows_sample,
                column_names=column_names,
                elapsed_ms=elapsed,
                explain_only=explain_only,
            )
        except snowflake.connector.errors.ProgrammingError as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            error_msg = str(exc)
            log.warning(
                "SQL %s failed (%dms): %s",
                "EXPLAIN" if explain_only else "EXECUTE",
                elapsed,
                error_msg[:200],
            )
            return ExecutionResult(
                success=False,
                sql=original_sql,
                error_message=error_msg,
                elapsed_ms=elapsed,
                explain_only=explain_only,
            )
        finally:
            cur.close()
