"""Tests for JoinCard creation, metadata, and IDs."""

from __future__ import annotations

import pytest

from rag_snow_agent.chroma.schema_cards import JoinCard


class TestJoinCardModel:
    def test_basic_creation(self):
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=1.0,
            source="fk",
        )
        assert card.db_id == "TESTDB"
        assert card.left_table == "TESTDB.PUBLIC.ORDERS"
        assert card.right_column == "ID"
        assert card.confidence == 1.0
        assert card.source == "fk"

    def test_chroma_id_format(self):
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=1.0,
            source="fk",
        )
        expected = (
            "join:TESTDB.PUBLIC.ORDERS.CUSTOMER_ID"
            "->TESTDB.PUBLIC.CUSTOMERS.ID"
        )
        assert card.chroma_id() == expected

    def test_chroma_metadata(self):
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=0.7,
            source="heuristic_name",
        )
        meta = card.chroma_metadata()
        assert meta["db_id"] == "TESTDB"
        assert meta["object_type"] == "join"
        assert meta["left_table"] == "TESTDB.PUBLIC.ORDERS"
        assert meta["left_column"] == "CUSTOMER_ID"
        assert meta["right_table"] == "TESTDB.PUBLIC.CUSTOMERS"
        assert meta["right_column"] == "ID"
        assert meta["confidence"] == 0.7
        assert meta["source"] == "heuristic_name"
        assert "token_estimate" in meta

    def test_document_contains_key_info(self):
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=1.0,
            source="fk",
        )
        doc = card.document
        assert "TESTDB.PUBLIC.ORDERS.CUSTOMER_ID" in doc
        assert "TESTDB.PUBLIC.CUSTOMERS.ID" in doc
        assert "1.0" in doc
        assert "fk" in doc

    def test_token_estimate_positive(self):
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=1.0,
            source="fk",
        )
        assert card.token_estimate > 0

    def test_heuristic_card(self):
        card = JoinCard(
            db_id="DB1",
            left_table="DB1.SCH.TABLE_A",
            left_column="ORDER_ID",
            right_table="DB1.SCH.TABLE_B",
            right_column="ORDER_ID",
            confidence=0.7,
            source="heuristic_name",
        )
        assert card.confidence == 0.7
        assert card.source == "heuristic_name"
        assert "heuristic_name" in card.document
