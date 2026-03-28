"""Tests for Snowflake syntax chunking and ChromaDB storage."""

import tempfile
from unittest.mock import patch

from rag_snow_agent.chroma.chroma_store import ChromaStore
from rag_snow_agent.chroma.snowflake_syntax import (
    SnowflakeSyntaxStore,
    build_all_syntax_chunks,
    chunk_markdown_by_sections,
)


def test_chunk_splits_by_headings():
    md = "# Title\nIntro text\n## Section A\nContent A\n## Section B\nContent B"
    chunks = chunk_markdown_by_sections(md, "TEST")
    assert len(chunks) >= 2
    topics = {c.topic for c in chunks}
    assert topics == {"TEST"}


def test_chunk_respects_token_budget():
    # Create a section larger than budget
    big_section = "## Big\n" + "\n\n".join(f"Paragraph {i} " * 50 for i in range(20))
    chunks = chunk_markdown_by_sections(big_section, "TEST", max_chunk_tokens=200)
    for c in chunks:
        assert c.token_estimate <= 250  # allow small overshoot at paragraph boundary


def test_chunk_ids_are_unique():
    md = "## A\nContent A\n## B\nContent B\n## C\nContent C"
    chunks = chunk_markdown_by_sections(md, "TEST")
    ids = [c.chroma_id() for c in chunks]
    assert len(ids) == len(set(ids))


def test_build_all_syntax_chunks():
    chunks = build_all_syntax_chunks()
    assert len(chunks) > 20  # We have 11 topics, each should produce multiple chunks
    topics = {c.topic for c in chunks}
    assert "JOIN" in topics
    assert "LATERAL_FLATTEN" in topics
    assert "QUALIFY" in topics
    assert "SNOWFLAKE_IDENTIFIERS" in topics


def test_chunk_metadata():
    chunks = build_all_syntax_chunks()
    for c in chunks:
        meta = c.chroma_metadata()
        assert meta["object_type"] == "syntax"
        assert meta["topic"]
        assert meta["section"]


@patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None)
def test_syntax_store_upsert_and_query(mock_embed):
    with tempfile.TemporaryDirectory() as tmp:
        store = ChromaStore(persist_dir=tmp)
        syntax_store = SnowflakeSyntaxStore(store)

        chunks = build_all_syntax_chunks()
        count = syntax_store.upsert_chunks(chunks)
        assert count == len(chunks)
        assert syntax_store.count() == len(chunks)


@patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None)
def test_syntax_store_query_returns_results(mock_embed):
    with tempfile.TemporaryDirectory() as tmp:
        store = ChromaStore(persist_dir=tmp)
        syntax_store = SnowflakeSyntaxStore(store)
        syntax_store.upsert_chunks(build_all_syntax_chunks())

        results = syntax_store.query("how to join two tables", top_k=3)
        assert len(results) == 3
        assert all("content" in r for r in results)
        assert all("topic" in r for r in results)


@patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None)
def test_syntax_query_lateral_flatten(mock_embed):
    with tempfile.TemporaryDirectory() as tmp:
        store = ChromaStore(persist_dir=tmp)
        syntax_store = SnowflakeSyntaxStore(store)
        syntax_store.upsert_chunks(build_all_syntax_chunks())

        results = syntax_store.query("flatten variant array nested json", top_k=3)
        topics = {r["topic"] for r in results}
        # LATERAL_FLATTEN should be among top results
        assert "LATERAL_FLATTEN" in topics or any("FLATTEN" in r["content"] for r in results)


@patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None)
def test_syntax_query_case_sensitivity(mock_embed):
    with tempfile.TemporaryDirectory() as tmp:
        store = ChromaStore(persist_dir=tmp)
        syntax_store = SnowflakeSyntaxStore(store)
        syntax_store.upsert_chunks(build_all_syntax_chunks())

        results = syntax_store.query("double quote identifier case sensitive", top_k=3)
        # SNOWFLAKE_IDENTIFIERS topic should appear
        assert any("IDENTIFIER" in r.get("topic", "").upper() for r in results) or \
               any("double-quote" in r.get("content", "").lower() or "double quote" in r.get("content", "").lower() for r in results)
