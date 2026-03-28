"""Tests for Snowflake micro-probes."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from rag_snow_agent.snowflake.probes import (
    probe_column_exists,
    probe_top_values,
    probe_variant_field_exists,
)


@dataclass
class FakeExecutionResult:
    success: bool
    error_message: str | None = None
    rows_sample: list[tuple] | None = None
    row_count: int | None = None


def _make_executor(success: bool = True, rows: list[tuple] | None = None):
    """Create a mock executor that returns a configurable result."""
    executor = MagicMock()
    result = FakeExecutionResult(
        success=success,
        error_message=None if success else "invalid identifier",
        rows_sample=rows,
        row_count=len(rows) if rows else 0,
    )
    executor.execute.return_value = result
    return executor


class TestProbeColumnExists:
    def test_returns_true_for_valid_column(self):
        executor = _make_executor(success=True)
        assert probe_column_exists(executor, "DB.SCH.TABLE", "COL1") is True
        executor.execute.assert_called_once()

    def test_returns_false_for_invalid_column(self):
        executor = _make_executor(success=False)
        assert probe_column_exists(executor, "DB.SCH.TABLE", "BAD_COL") is False

    def test_returns_false_on_exception(self):
        executor = MagicMock()
        executor.execute.side_effect = RuntimeError("connection lost")
        assert probe_column_exists(executor, "DB.SCH.TABLE", "COL1") is False

    def test_sql_uses_double_quotes(self):
        executor = _make_executor(success=True)
        probe_column_exists(executor, "DB.SCH.TABLE", "MY_COL")
        call_args = executor.execute.call_args
        sql = call_args[0][0]
        assert '"MY_COL"' in sql


class TestProbeVariantFieldExists:
    def test_returns_true_for_valid_field(self):
        executor = _make_executor(success=True)
        assert probe_variant_field_exists(
            executor, "DB.SCH.TABLE", "DATA", "field.name"
        ) is True

    def test_returns_false_for_invalid_field(self):
        executor = _make_executor(success=False)
        assert probe_variant_field_exists(
            executor, "DB.SCH.TABLE", "DATA", "bad.field"
        ) is False

    def test_returns_false_on_exception(self):
        executor = MagicMock()
        executor.execute.side_effect = RuntimeError("timeout")
        assert probe_variant_field_exists(
            executor, "DB.SCH.TABLE", "DATA", "field"
        ) is False


class TestProbeTopValues:
    def test_returns_values_on_success(self):
        rows = [("val1",), ("val2",), ("val3",)]
        executor = _make_executor(success=True, rows=rows)
        result = probe_top_values(executor, "DB.SCH.TABLE", "STATUS")
        assert result == ["val1", "val2", "val3"]

    def test_returns_empty_on_failure(self):
        executor = _make_executor(success=False)
        result = probe_top_values(executor, "DB.SCH.TABLE", "BAD_COL")
        assert result == []

    def test_returns_empty_on_exception(self):
        executor = MagicMock()
        executor.execute.side_effect = RuntimeError("boom")
        result = probe_top_values(executor, "DB.SCH.TABLE", "COL")
        assert result == []

    def test_returns_empty_when_no_rows(self):
        executor = _make_executor(success=True, rows=[])
        result = probe_top_values(executor, "DB.SCH.TABLE", "COL")
        assert result == []

    def test_custom_limit(self):
        rows = [("a",), ("b",)]
        executor = _make_executor(success=True, rows=rows)
        result = probe_top_values(executor, "DB.SCH.TABLE", "COL", limit=2)
        assert len(result) == 2
        # Verify the SQL uses the custom limit
        call_args = executor.execute.call_args
        sql = call_args[0][0]
        assert "LIMIT 2" in sql
