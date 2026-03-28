"""Tests for expand_connectivity_with_join_graph: fake tables + edges -> bridge tables added."""

from __future__ import annotations

import tempfile
from pathlib import Path

import chromadb
import pytest

from rag_snow_agent.chroma.schema_cards import JoinCard
from rag_snow_agent.retrieval.connectivity import (
    expand_connectivity,
    expand_connectivity_with_join_graph,
)
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_table_slice(qname: str, columns: list[str]) -> TableSlice:
    return TableSlice(
        qualified_name=qname,
        table_token_estimate=5,
        fused_rank=1,
        columns=[
            ColumnSlice(name=c, data_type="VARCHAR", token_estimate=3, fused_rank=1)
            for c in columns
        ],
    )


def _setup_chroma_with_joins(
    db_id: str, join_cards: list[JoinCard], table_columns: dict[str, list[str]]
) -> chromadb.Collection:
    """Create an in-memory Chroma collection with join cards and column cards."""
    client = chromadb.Client()
    col = client.get_or_create_collection(
        name="schema_cards", metadata={"hnsw:space": "cosine"}
    )

    # Upsert join cards
    if join_cards:
        col.upsert(
            ids=[c.chroma_id() for c in join_cards],
            documents=[c.document for c in join_cards],
            metadatas=[c.chroma_metadata() for c in join_cards],
        )

    # Upsert table cards
    for qname in table_columns:
        col.upsert(
            ids=[f"table:{qname}"],
            documents=[f"Table: {qname}\nColumns: {', '.join(table_columns[qname])}"],
            metadatas=[{
                "db_id": db_id,
                "object_type": "table",
                "qualified_name": qname,
                "source": "information_schema",
                "token_estimate": 10,
            }],
        )

    # Upsert column cards for bridge tables
    for qname, cols in table_columns.items():
        for c in cols:
            col.upsert(
                ids=[f"column:{qname}.{c}"],
                documents=[f"Column: {qname}.{c}\nType: VARCHAR"],
                metadatas=[{
                    "db_id": db_id,
                    "object_type": "column",
                    "qualified_name": f"{qname}.{c}",
                    "table_qualified_name": qname,
                    "data_type": "VARCHAR",
                    "source": "information_schema",
                    "token_estimate": 5,
                }],
            )

    return col


class TestExpandConnectivityWithJoinGraph:
    def test_adds_bridge_table(self):
        """Two disconnected tables with a bridge available in the join graph."""
        db_id = "TESTDB"
        join_cards = [
            JoinCard(
                db_id=db_id,
                left_table="TESTDB.PUBLIC.ORDERS",
                left_column="CUSTOMER_ID",
                right_table="TESTDB.PUBLIC.ORDER_ITEMS",
                right_column="ORDER_ID",
                confidence=1.0,
                source="fk",
            ),
            JoinCard(
                db_id=db_id,
                left_table="TESTDB.PUBLIC.ORDER_ITEMS",
                left_column="PRODUCT_ID",
                right_table="TESTDB.PUBLIC.PRODUCTS",
                right_column="ID",
                confidence=1.0,
                source="fk",
            ),
        ]
        table_columns = {
            "TESTDB.PUBLIC.ORDERS": ["ID", "CUSTOMER_ID", "ORDER_DATE"],
            "TESTDB.PUBLIC.ORDER_ITEMS": ["ID", "ORDER_ID", "PRODUCT_ID", "QTY"],
            "TESTDB.PUBLIC.PRODUCTS": ["ID", "NAME", "PRICE"],
        }
        collection = _setup_chroma_with_joins(db_id, join_cards, table_columns)

        schema = SchemaSlice(
            db_id=db_id,
            tables=[
                _make_table_slice("TESTDB.PUBLIC.ORDERS", ["ID", "CUSTOMER_ID"]),
                _make_table_slice("TESTDB.PUBLIC.PRODUCTS", ["ID", "NAME"]),
            ],
        )

        result = expand_connectivity_with_join_graph(schema, collection)
        table_names = {t.qualified_name for t in result.tables}
        assert "TESTDB.PUBLIC.ORDER_ITEMS" in table_names
        assert len(result.tables) == 3

    def test_no_expansion_when_connected(self):
        """Tables already connected in the join graph need no bridge."""
        db_id = "TESTDB"
        join_cards = [
            JoinCard(
                db_id=db_id,
                left_table="TESTDB.PUBLIC.A",
                left_column="ID",
                right_table="TESTDB.PUBLIC.B",
                right_column="A_ID",
                confidence=1.0,
                source="fk",
            ),
        ]
        table_columns = {
            "TESTDB.PUBLIC.A": ["ID", "NAME"],
            "TESTDB.PUBLIC.B": ["ID", "A_ID"],
        }
        collection = _setup_chroma_with_joins(db_id, join_cards, table_columns)

        schema = SchemaSlice(
            db_id=db_id,
            tables=[
                _make_table_slice("TESTDB.PUBLIC.A", ["ID", "NAME"]),
                _make_table_slice("TESTDB.PUBLIC.B", ["ID", "A_ID"]),
            ],
        )

        result = expand_connectivity_with_join_graph(schema, collection)
        assert len(result.tables) == 2

    def test_falls_back_to_heuristic_when_no_join_cards(self):
        """When no JoinCards exist, falls back to heuristic expansion."""
        db_id = "TESTDB"
        table_columns = {
            "TESTDB.PUBLIC.A": ["ID", "SHARED_ID"],
            "TESTDB.PUBLIC.B": ["ID", "OTHER"],
            "TESTDB.PUBLIC.BRIDGE": ["ID", "SHARED_ID", "OTHER"],
        }
        collection = _setup_chroma_with_joins(db_id, [], table_columns)

        schema = SchemaSlice(
            db_id=db_id,
            tables=[
                _make_table_slice("TESTDB.PUBLIC.A", ["ID", "SHARED_ID"]),
                _make_table_slice("TESTDB.PUBLIC.B", ["ID", "OTHER"]),
            ],
        )

        # Should fall back to heuristic without error
        result = expand_connectivity_with_join_graph(
            schema, collection, allow_heuristic_fallback=True
        )
        # Heuristic may or may not find a bridge; the point is no crash
        assert len(result.tables) >= 2

    def test_single_table_no_expansion(self):
        """Single table needs no connectivity expansion."""
        db_id = "TESTDB"
        collection = _setup_chroma_with_joins(db_id, [], {})

        schema = SchemaSlice(
            db_id=db_id,
            tables=[
                _make_table_slice("TESTDB.PUBLIC.A", ["ID"]),
            ],
        )

        result = expand_connectivity_with_join_graph(schema, collection)
        assert len(result.tables) == 1

    def test_min_confidence_respected(self):
        """Low-confidence edges should be ignored when min_confidence is set."""
        db_id = "TESTDB"
        join_cards = [
            JoinCard(
                db_id=db_id,
                left_table="TESTDB.PUBLIC.A",
                left_column="ID",
                right_table="TESTDB.PUBLIC.BRIDGE",
                right_column="A_ID",
                confidence=0.3,
                source="heuristic_name",
            ),
            JoinCard(
                db_id=db_id,
                left_table="TESTDB.PUBLIC.BRIDGE",
                left_column="B_ID",
                right_table="TESTDB.PUBLIC.B",
                right_column="ID",
                confidence=0.3,
                source="heuristic_name",
            ),
        ]
        table_columns = {
            "TESTDB.PUBLIC.A": ["ID"],
            "TESTDB.PUBLIC.B": ["ID"],
            "TESTDB.PUBLIC.BRIDGE": ["A_ID", "B_ID"],
        }
        collection = _setup_chroma_with_joins(db_id, join_cards, table_columns)

        schema = SchemaSlice(
            db_id=db_id,
            tables=[
                _make_table_slice("TESTDB.PUBLIC.A", ["ID"]),
                _make_table_slice("TESTDB.PUBLIC.B", ["ID"]),
            ],
        )

        result = expand_connectivity_with_join_graph(
            schema, collection, min_confidence=0.5, allow_heuristic_fallback=False
        )
        # Bridge should NOT be added because confidence is too low
        assert len(result.tables) == 2
