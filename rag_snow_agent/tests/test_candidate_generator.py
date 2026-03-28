"""Tests for candidate generator with mocked LLM."""

from __future__ import annotations

import json
from unittest.mock import patch

from rag_snow_agent.agent.candidate_generator import generate_candidate_sqls
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_slice() -> SchemaSlice:
    return SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name="TESTDB.PUBLIC.ORDERS",
                columns=[
                    ColumnSlice(name="ORDER_ID", data_type="NUMBER", token_estimate=5),
                    ColumnSlice(name="AMOUNT", data_type="FLOAT", token_estimate=5),
                    ColumnSlice(name="CREATED_AT", data_type="TIMESTAMP_NTZ", token_estimate=5),
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
        {"func": "AVG", "table": "TESTDB.PUBLIC.ORDERS", "column": "AMOUNT", "alias": "avg_amount"}
    ],
    "order_by": [],
    "limit": None,
    "notes": None,
})


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_generates_n_candidates(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    candidates = generate_candidate_sqls(
        db_id="TESTDB",
        instruction="average amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        n=2,
    )
    assert len(candidates) == 2
    assert candidates[0].candidate_id == 1
    assert candidates[1].candidate_id == 2


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_different_strategies(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    candidates = generate_candidate_sqls(
        db_id="TESTDB",
        instruction="average amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        n=2,
        strategies=["default", "join_first"],
    )
    assert candidates[0].strategy == "default"
    assert candidates[1].strategy == "join_first"


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_candidate_has_sql(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    candidates = generate_candidate_sqls(
        db_id="TESTDB",
        instruction="average amount",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        n=1,
    )
    assert candidates[0].sql
    assert "SELECT" in candidates[0].sql
    assert candidates[0].plan is not None


@patch("rag_snow_agent.agent.candidate_generator.call_llm")
def test_four_candidates_cycle_strategies(mock_llm):
    mock_llm.return_value = _FAKE_PLAN
    candidates = generate_candidate_sqls(
        db_id="TESTDB",
        instruction="test",
        schema_slice=_make_slice(),
        model="gpt-4o-mini",
        n=4,
    )
    strategies = [c.strategy for c in candidates]
    assert strategies == ["default", "join_first", "metric_first", "time_first"]
