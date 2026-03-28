"""Minimal tests for the repair loop using mocks."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from rag_snow_agent.agent.refiner import refine_sql
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice
from rag_snow_agent.snowflake.executor import ExecutionResult


def _make_slice() -> SchemaSlice:
    return SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name="TESTDB.PUBLIC.ORDERS",
                columns=[
                    ColumnSlice(name="ORDER_ID", data_type="NUMBER", token_estimate=5),
                    ColumnSlice(name="AMOUNT", data_type="FLOAT", token_estimate=5),
                ],
            ),
        ],
    )


def _mock_executor(
    explain_results: list[ExecutionResult],
    execute_results: list[ExecutionResult],
) -> MagicMock:
    executor = MagicMock()
    executor.explain = MagicMock(side_effect=explain_results)
    executor.execute = MagicMock(side_effect=execute_results)
    return executor


def test_success_on_first_try():
    """No repairs needed when EXPLAIN and EXECUTE both succeed."""
    executor = _mock_executor(
        explain_results=[ExecutionResult(success=True, sql="SELECT 1", explain_only=True)],
        execute_results=[ExecutionResult(success=True, sql="SELECT 1", row_count=10)],
    )

    final_sql, trace, result = refine_sql(
        db_id="TESTDB",
        instruction="test",
        schema_slice=_make_slice(),
        sql="SELECT 1",
        executor=executor,
        max_repairs=2,
    )

    assert result is not None
    assert result.success
    assert len(trace) == 0
    assert final_sql == "SELECT 1"


@patch("rag_snow_agent.agent.refiner.call_llm")
def test_repair_on_invalid_identifier(mock_llm):
    """Invalid identifier triggers one repair attempt."""
    mock_llm.return_value = "SELECT t1.ORDER_ID FROM TESTDB.PUBLIC.ORDERS AS t1"

    executor = _mock_executor(
        explain_results=[
            # First EXPLAIN fails
            ExecutionResult(
                success=False,
                sql="SELECT BOGUS FROM ORDERS",
                error_message="SQL compilation error: invalid identifier 'BOGUS'",
                explain_only=True,
            ),
            # Second EXPLAIN succeeds after repair
            ExecutionResult(success=True, sql="repaired", explain_only=True),
        ],
        execute_results=[
            ExecutionResult(success=True, sql="repaired", row_count=5),
        ],
    )

    final_sql, trace, result = refine_sql(
        db_id="TESTDB",
        instruction="list orders",
        schema_slice=_make_slice(),
        sql="SELECT BOGUS FROM ORDERS",
        executor=executor,
        max_repairs=2,
    )

    assert len(trace) == 1
    assert trace[0].error_type == "invalid_identifier"
    assert trace[0].repair_action == "patch_identifier"
    assert result is not None
    assert result.success
    mock_llm.assert_called_once()


@patch("rag_snow_agent.agent.refiner.call_llm")
def test_repair_trace_recorded(mock_llm):
    """Repair trace items have all required fields."""
    mock_llm.return_value = "SELECT 1"

    executor = _mock_executor(
        explain_results=[
            ExecutionResult(
                success=False,
                sql="bad",
                error_message="SQL compilation error: invalid identifier 'X'",
                explain_only=True,
            ),
            ExecutionResult(success=True, sql="ok", explain_only=True),
        ],
        execute_results=[
            ExecutionResult(success=True, sql="ok", row_count=1),
        ],
    )

    _, trace, _ = refine_sql(
        db_id="TESTDB",
        instruction="test",
        schema_slice=_make_slice(),
        sql="bad sql",
        executor=executor,
        max_repairs=2,
    )

    assert len(trace) == 1
    item = trace[0]
    assert item.attempt == 1
    assert item.input_sql == "bad sql"
    assert item.error_type == "invalid_identifier"
    assert item.error_message  # not empty
    assert item.repair_action == "patch_identifier"
    assert item.output_sql  # not empty


@patch("rag_snow_agent.agent.refiner.call_llm")
def test_stops_on_repeated_error(mock_llm):
    """Loop stops when the same error repeats."""
    mock_llm.return_value = "SELECT BOGUS FROM ORDERS"  # repair doesn't fix it

    same_error = ExecutionResult(
        success=False,
        sql="bad",
        error_message="SQL compilation error: invalid identifier 'BOGUS'",
        explain_only=True,
    )
    executor = _mock_executor(
        explain_results=[same_error, same_error, same_error],
        execute_results=[],
    )

    final_sql, trace, result = refine_sql(
        db_id="TESTDB",
        instruction="test",
        schema_slice=_make_slice(),
        sql="SELECT BOGUS FROM ORDERS",
        executor=executor,
        max_repairs=3,
        stop_on_repeated_error=True,
    )

    # Should stop after seeing the same error twice (1 repair attempt, then stop)
    assert len(trace) == 1
    assert result is not None
    assert not result.success
