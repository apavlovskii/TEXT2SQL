"""Minimal tests for Best-of-N orchestration with mocks."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from rag_snow_agent.agent.best_of_n import run_best_of_n
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


_FAKE_PLAN = json.dumps({
    "selected_tables": ["TESTDB.PUBLIC.ORDERS"],
    "joins": [],
    "filters": [],
    "group_by": [],
    "aggregations": [
        {"func": "SUM", "table": "TESTDB.PUBLIC.ORDERS", "column": "AMOUNT", "alias": "total"}
    ],
    "order_by": [],
    "limit": None,
    "notes": None,
})


def _mock_executor_always_succeeds():
    executor = MagicMock()
    executor.explain.return_value = ExecutionResult(
        success=True, sql="ok", explain_only=True
    )
    executor.execute.return_value = ExecutionResult(
        success=True, sql="ok", row_count=10, rows_sample=[(100,)]
    )
    return executor


def _mock_executor_first_fails():
    """First explain fails, second succeeds. Both executions succeed."""
    executor = MagicMock()
    executor.explain.side_effect = [
        # Candidate 1: explain fails
        ExecutionResult(
            success=False, sql="bad",
            error_message="SQL compilation error: invalid identifier 'X'",
            explain_only=True,
        ),
        # Candidate 1 repair: explain succeeds
        ExecutionResult(success=True, sql="ok", explain_only=True),
        # Candidate 2: explain succeeds
        ExecutionResult(success=True, sql="ok", explain_only=True),
    ]
    executor.execute.side_effect = [
        ExecutionResult(success=True, sql="ok", row_count=5, rows_sample=[(50,)]),
        ExecutionResult(success=True, sql="ok", row_count=10, rows_sample=[(100,)]),
    ]
    return executor


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_selects_best_candidate(mock_llm):
    """Both candidates succeed; best score should win."""
    mock_llm.return_value = _FAKE_PLAN
    executor = _mock_executor_always_succeeds()

    result = run_best_of_n(
        instance_id="test1",
        db_id="TESTDB",
        instruction="total amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        executor=executor,
        n=2,
        strategies=["default", "join_first"],
    )

    assert "best_candidate_id" in result
    assert "best_sql" in result
    assert result["best_success"]
    assert len(result["candidates"]) == 2


@patch("rag_snow_agent.agent.refiner.call_llm")
@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_repaired_candidate_can_win(mock_gen_llm, mock_repair_llm):
    """A candidate that needed repair can still be selected if successful."""
    mock_gen_llm.return_value = _FAKE_PLAN
    mock_repair_llm.return_value = "SELECT SUM(t1.AMOUNT) AS total FROM TESTDB.PUBLIC.ORDERS AS t1"

    executor = _mock_executor_first_fails()

    result = run_best_of_n(
        instance_id="test2",
        db_id="TESTDB",
        instruction="total amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        executor=executor,
        n=2,
        strategies=["default", "join_first"],
    )

    assert result["best_success"]
    assert len(result["candidates"]) == 2
    # Both should have succeeded eventually
    successes = [c for c in result["candidates"] if c["execution_success"]]
    assert len(successes) == 2


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_output_includes_all_metadata(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    executor = _mock_executor_always_succeeds()

    result = run_best_of_n(
        instance_id="test3",
        db_id="TESTDB",
        instruction="total amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        executor=executor,
        n=2,
    )

    assert "selection_reason" in result
    for c in result["candidates"]:
        assert "candidate_id" in c
        assert "strategy" in c
        assert "initial_sql" in c
        assert "final_sql" in c
        assert "score" in c
        assert "repairs_count" in c
        assert "repair_trace" in c


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_scores_are_assigned(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    executor = _mock_executor_always_succeeds()

    result = run_best_of_n(
        instance_id="test4",
        db_id="TESTDB",
        instruction="total amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        executor=executor,
        n=2,
    )

    for c in result["candidates"]:
        assert isinstance(c["score"], float)
        assert c["score"] > 0  # successful candidates should have positive scores
