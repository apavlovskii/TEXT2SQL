"""Tests for column_validator: validate SQL column references against ChromaDB."""

from __future__ import annotations

import tempfile
from unittest.mock import patch

from rag_snow_agent.agent.column_validator import (
    _extract_column_refs,
    _find_similar,
    validate_columns_against_index,
)
from rag_snow_agent.chroma.chroma_store import ChromaStore
from rag_snow_agent.chroma.schema_cards import ColumnCard, TableCard


def _make_store_with_columns(
    columns: list[tuple[str, str, str]],
    table_names: list[str] | None = None,
) -> ChromaStore:
    """Create a temp ChromaStore and insert ColumnCards (and TableCards).

    Each column tuple is (table_qualified_name, column_name, data_type).
    Uses default (local) embeddings to avoid needing OpenAI API key.
    """
    tmpdir = tempfile.mkdtemp()
    with patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None):
        store = ChromaStore(persist_dir=tmpdir)
    cards = []
    for tqn, col_name, dtype in columns:
        cards.append(
            ColumnCard(
                db_id="TESTDB",
                qualified_name=f"{tqn}.{col_name}",
                table_qualified_name=tqn,
                data_type=dtype,
                is_nullable="YES",
            )
        )
    store.upsert_column_cards(cards)

    # Derive unique table names from columns and insert TableCards
    seen_tables: set[str] = set()
    if table_names:
        seen_tables = set(table_names)
    else:
        seen_tables = {tqn for tqn, _, _ in columns}
    tcards = []
    for tqn in seen_tables:
        col_names = [c for t, c, _ in columns if t == tqn]
        tcards.append(
            TableCard(
                db_id="TESTDB",
                qualified_name=tqn,
                table_type="BASE TABLE",
                column_names=col_names,
            )
        )
    if tcards:
        store.upsert_table_cards(tcards)

    return store


class TestExtractColumnRefs:
    def test_dotted_ref(self):
        refs = _extract_column_refs("SELECT t1.ORDER_ID FROM orders t1")
        assert "ORDER_ID" in refs

    def test_bare_ref(self):
        refs = _extract_column_refs("SELECT AMOUNT FROM orders")
        assert "AMOUNT" in refs

    def test_skips_keywords(self):
        refs = _extract_column_refs("SELECT COUNT(*) FROM orders WHERE x > 1")
        assert "COUNT" not in refs
        assert "FROM" not in refs

    def test_qualified_ref(self):
        refs = _extract_column_refs(
            "SELECT TESTDB.PUBLIC.ORDERS.ORDER_ID FROM TESTDB.PUBLIC.ORDERS"
        )
        assert "ORDER_ID" in refs


class TestFindSimilar:
    def test_close_match(self):
        known = {"ORDER_ID", "ORDER_DATE", "AMOUNT", "CUSTOMER_ID"}
        result = _find_similar("ORDR_ID", known)
        assert "ORDER_ID" in result

    def test_no_match(self):
        known = {"ORDER_ID", "AMOUNT"}
        result = _find_similar("XYZZY", known)
        # May or may not find something, but shouldn't crash
        assert isinstance(result, list)


class TestValidateColumnsAgainstIndex:
    def test_known_columns_pass(self):
        store = _make_store_with_columns([
            ("TESTDB.PUBLIC.ORDERS", "ORDER_ID", "NUMBER"),
            ("TESTDB.PUBLIC.ORDERS", "AMOUNT", "FLOAT"),
        ])
        sql = "SELECT t1.ORDER_ID, t1.AMOUNT FROM TESTDB.PUBLIC.ORDERS t1"
        is_valid, errors, suggestions = validate_columns_against_index(
            sql, "TESTDB", store
        )
        assert is_valid
        assert len(errors) == 0

    def test_unknown_columns_flagged(self):
        store = _make_store_with_columns([
            ("TESTDB.PUBLIC.ORDERS", "ORDER_ID", "NUMBER"),
            ("TESTDB.PUBLIC.ORDERS", "AMOUNT", "FLOAT"),
        ])
        sql = "SELECT t1.BOGUS_COLUMN FROM TESTDB.PUBLIC.ORDERS t1"
        is_valid, errors, suggestions = validate_columns_against_index(
            sql, "TESTDB", store
        )
        assert not is_valid
        assert any("BOGUS_COLUMN" in e for e in errors)

    def test_suggestions_provided(self):
        store = _make_store_with_columns([
            ("TESTDB.PUBLIC.ORDERS", "ORDER_ID", "NUMBER"),
            ("TESTDB.PUBLIC.ORDERS", "ORDER_DATE", "DATE"),
            ("TESTDB.PUBLIC.ORDERS", "AMOUNT", "FLOAT"),
        ])
        sql = "SELECT ORDR_ID FROM TESTDB.PUBLIC.ORDERS"
        is_valid, errors, suggestions = validate_columns_against_index(
            sql, "TESTDB", store
        )
        assert not is_valid
        assert len(suggestions) > 0
        # Should suggest ORDER_ID
        assert any("ORDER_ID" in s for s in suggestions)

    def test_empty_index_passes(self):
        """When no columns are indexed, validation is skipped (returns valid)."""
        tmpdir = tempfile.mkdtemp()
        with patch("rag_snow_agent.chroma.chroma_store._get_embedding_function", return_value=None):
            store = ChromaStore(persist_dir=tmpdir)
        sql = "SELECT ANYTHING FROM NOWHERE"
        is_valid, errors, suggestions = validate_columns_against_index(
            sql, "EMPTYDB", store
        )
        assert is_valid
        assert len(errors) == 0

    def test_variant_subfield_recognized(self):
        """VARIANT sub-field column names (with colons) should be in the index."""
        store = _make_store_with_columns([
            ("TESTDB.PUBLIC.GA_SESSIONS", "trafficSource", "VARIANT"),
            ("TESTDB.PUBLIC.GA_SESSIONS", '"trafficSource":source', "VARIANT_FIELD"),
            ("TESTDB.PUBLIC.GA_SESSIONS", '"trafficSource":medium', "VARIANT_FIELD"),
        ])
        # The validator checks bare column names against the index.
        # trafficSource should be recognized as a known column.
        sql = 'SELECT trafficSource FROM TESTDB.PUBLIC.GA_SESSIONS'
        is_valid, errors, suggestions = validate_columns_against_index(
            sql, "TESTDB", store
        )
        assert is_valid
