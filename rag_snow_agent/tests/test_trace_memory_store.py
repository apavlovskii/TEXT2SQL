"""Tests for TraceMemoryStore: upsert/query round trip with temp ChromaDB."""

import tempfile
from unittest.mock import patch

import pytest

from rag_snow_agent.chroma.trace_memory import TraceMemoryStore


@pytest.fixture(autouse=True)
def _no_openai_embeddings():
    """Disable OpenAI embeddings in all tests in this module."""
    with patch(
        "rag_snow_agent.chroma.trace_memory._get_embedding_function",
        return_value=None,
    ):
        yield


def _make_store(tmp_path):
    return TraceMemoryStore(persist_dir=str(tmp_path))


def _sample_trace(trace_id="t1", db_id="TESTDB", instance_id="inst_001"):
    return {
        "trace_id": trace_id,
        "db_id": db_id,
        "instance_id": instance_id,
        "instruction_summary": "Show total orders by month",
        "plan_summary": "Tables: ORDERS; Aggs: SUM(AMOUNT); Group: ORDER_DATE",
        "tables_used": ["TESTDB.PUBLIC.ORDERS"],
        "token_estimate": 150,
    }


def test_upsert_and_query_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        trace = _sample_trace()
        store.upsert_trace(trace)

        results = store.query_traces(
            db_id="TESTDB",
            instruction="total orders by month",
            top_k=3,
        )
        assert len(results) >= 1
        assert results[0]["trace_id"] == "t1"
        assert results[0]["metadata"]["db_id"] == "TESTDB"
        assert "distance" in results[0]


def test_upsert_multiple_and_query():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.upsert_trace(_sample_trace("t1", "TESTDB", "inst_001"))
        store.upsert_trace(
            {
                "trace_id": "t2",
                "db_id": "TESTDB",
                "instance_id": "inst_002",
                "instruction_summary": "Count customers by region",
                "plan_summary": "Tables: CUSTOMERS; Aggs: COUNT(*); Group: REGION",
                "tables_used": ["TESTDB.PUBLIC.CUSTOMERS"],
                "token_estimate": 120,
            }
        )

        results = store.query_traces(
            db_id="TESTDB",
            instruction="how many customers per region",
            top_k=2,
        )
        assert len(results) == 2


def test_query_empty_collection():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        results = store.query_traces(
            db_id="TESTDB",
            instruction="anything",
            top_k=3,
        )
        assert results == []


def test_query_wrong_db_returns_empty():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.upsert_trace(_sample_trace("t1", "DB_A", "inst_001"))

        results = store.query_traces(
            db_id="DB_B",
            instruction="total orders",
            top_k=3,
        )
        assert results == []


def test_delete_all_for_db():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        store.upsert_trace(_sample_trace("t1", "DB_A", "inst_001"))
        store.upsert_trace(_sample_trace("t2", "DB_B", "inst_002"))

        store.delete_all_for_db("DB_A")

        results_a = store.query_traces(db_id="DB_A", instruction="anything", top_k=5)
        results_b = store.query_traces(db_id="DB_B", instruction="anything", top_k=5)
        assert results_a == []
        assert len(results_b) == 1


def test_upsert_idempotent():
    with tempfile.TemporaryDirectory() as tmp:
        store = _make_store(tmp)
        trace = _sample_trace()
        store.upsert_trace(trace)
        store.upsert_trace(trace)  # same id, should not duplicate

        results = store.query_traces(
            db_id="TESTDB",
            instruction="total orders",
            top_k=10,
        )
        assert len(results) == 1
