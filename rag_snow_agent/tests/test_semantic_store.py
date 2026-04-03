"""Tests for SemanticLayerStore: Chroma round-trip with patched embedding function."""

from __future__ import annotations

import tempfile
from unittest.mock import patch

import pytest

from rag_snow_agent.chroma.chroma_store import ChromaStore
from rag_snow_agent.semantic_layer.models import SemanticFact, SemanticProfile
from rag_snow_agent.semantic_layer.store import SemanticLayerStore


@pytest.fixture(autouse=True)
def _no_openai_embeddings():
    """Disable OpenAI embeddings in all tests in this module."""
    with patch(
        "rag_snow_agent.chroma.chroma_store._get_embedding_function",
        return_value=None,
    ):
        yield


def _make_profile(db_id: str = "TESTDB") -> SemanticProfile:
    return SemanticProfile(
        db_id=db_id,
        time_columns=[
            SemanticFact(
                fact_type="primary_time_column",
                subject=f"{db_id}.PUBLIC.ORDERS.CREATED_AT",
                value="TIMESTAMP_NTZ",
                confidence=0.8,
                source=["metadata"],
                evidence=["Column type is TIMESTAMP_NTZ"],
            ),
        ],
        metric_candidates=[
            SemanticFact(
                fact_type="metric_candidate",
                subject=f"{db_id}.PUBLIC.ORDERS.AMOUNT",
                value="AMOUNT",
                confidence=0.7,
                source=["metadata"],
                evidence=["Numeric column with metric-like name"],
            ),
        ],
        dimension_candidates=[
            SemanticFact(
                fact_type="dimension_candidate",
                subject=f"{db_id}.PUBLIC.ORDERS.STATUS",
                value="STATUS",
                confidence=0.6,
                source=["metadata"],
                evidence=["String column with dimension-like name"],
            ),
        ],
    )


class TestUpsertRoundTrip:
    def test_upsert_returns_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = _make_profile()
            count = store.upsert_semantic_profile(profile)
            assert count == 3  # 1 time + 1 metric + 1 dimension

    def test_upsert_empty_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = SemanticProfile(db_id="TESTDB")
            count = store.upsert_semantic_profile(profile)
            assert count == 0

    def test_upsert_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = _make_profile()
            store.upsert_semantic_profile(profile)
            store.upsert_semantic_profile(profile)
            col = store.collection()
            total = col.count()
            assert total == 3  # same IDs, no duplicates


class TestQueryRoundTrip:
    def test_query_returns_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = _make_profile()
            store.upsert_semantic_profile(profile)

            results = store.query_semantic_cards(
                "TESTDB", "order amount total", top_k=5
            )
            assert len(results) >= 1
            assert "metadata" in results[0]
            assert results[0]["metadata"]["db_id"] == "TESTDB"

    def test_query_wrong_db_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = _make_profile("DB_A")
            store.upsert_semantic_profile(profile)

            results = store.query_semantic_cards(
                "DB_B", "anything", top_k=5
            )
            assert results == []

    def test_query_empty_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            results = store.query_semantic_cards(
                "TESTDB", "anything", top_k=5
            )
            assert results == []


class TestSemanticCardMetadata:
    def test_card_metadata_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            chroma = ChromaStore(persist_dir=tmp)
            store = SemanticLayerStore(chroma)
            profile = _make_profile()
            store.upsert_semantic_profile(profile)

            col = store.collection()
            results = col.get(
                where={"db_id": "TESTDB"},
                include=["metadatas"],
            )
            assert len(results["ids"]) == 3
            for meta in results["metadatas"]:
                assert "db_id" in meta
                assert "object_type" in meta
                assert meta["object_type"] == "semantic"
                assert "fact_type" in meta
                assert "subject" in meta
                assert "confidence" in meta
