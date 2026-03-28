"""Tests for plan-guided schema expansion."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rag_snow_agent.prompting.plan_schema import PlanJoin, QueryPlan
from rag_snow_agent.retrieval.hybrid_retriever import HybridRetriever, ScoredItem
from rag_snow_agent.retrieval.plan_expansion import expand_schema_for_plan
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_slice(tables: list[str]) -> SchemaSlice:
    """Create a simple SchemaSlice with the given table names."""
    return SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name=t,
                columns=[ColumnSlice(name="ID", data_type="NUMBER")],
            )
            for t in tables
        ],
    )


def _mock_retriever(known_tables: dict[str, list[str]]) -> MagicMock:
    """Create a mock HybridRetriever that knows about given tables and their columns.

    known_tables: {qualified_name: [col_name, ...]}
    """
    retriever = MagicMock(spec=HybridRetriever)

    def _retrieve_tables(query, db_id, top_k=50):
        results = []
        for qname in known_tables:
            if query.upper() in qname.upper() or qname.upper().endswith("." + query.upper()):
                results.append(
                    ScoredItem(
                        chroma_id=f"table:{qname}",
                        object_type="table",
                        qualified_name=qname,
                        metadata={"token_estimate": 10},
                        fused_rank=1,
                        rrf_score=0.5,
                    )
                )
        return results[:top_k]

    retriever.retrieve_tables = _retrieve_tables

    # Mock the collection.get() for column lookup
    collection = MagicMock()

    def _collection_get(where=None, include=None):
        # Extract the table name from the where clause
        if where and "$and" in where:
            conditions = where["$and"]
            tqn = None
            for cond in conditions:
                if "table_qualified_name" in cond:
                    tqn = cond["table_qualified_name"]
            if tqn and tqn in known_tables:
                metas = [
                    {
                        "qualified_name": f"{tqn}.{col}",
                        "data_type": "VARCHAR",
                        "token_estimate": 5,
                        "table_qualified_name": tqn,
                    }
                    for col in known_tables[tqn]
                ]
                return {"metadatas": metas}
        return {"metadatas": []}

    collection.get = _collection_get
    retriever.collection = collection

    return retriever


class TestExpandSchemaForPlan:
    def test_missing_table_triggers_expansion(self):
        """A plan referencing a table not in the slice should add that table."""
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS"])
        plan = QueryPlan(
            selected_tables=["TESTDB.PUBLIC.ORDERS", "TESTDB.PUBLIC.CUSTOMERS"],
        )
        retriever = _mock_retriever({
            "TESTDB.PUBLIC.ORDERS": ["ID", "CUSTOMER_ID"],
            "TESTDB.PUBLIC.CUSTOMERS": ["ID", "NAME"],
        })

        result = expand_schema_for_plan(schema_slice, plan, retriever, "TESTDB")

        table_names = {ts.qualified_name for ts in result.tables}
        assert "TESTDB.PUBLIC.CUSTOMERS" in table_names
        assert len(result.tables) == 2

    def test_already_present_table_not_duplicated(self):
        """Tables already in the slice should not be added again."""
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS", "TESTDB.PUBLIC.CUSTOMERS"])
        plan = QueryPlan(
            selected_tables=["TESTDB.PUBLIC.ORDERS", "TESTDB.PUBLIC.CUSTOMERS"],
        )
        retriever = _mock_retriever({
            "TESTDB.PUBLIC.ORDERS": ["ID"],
            "TESTDB.PUBLIC.CUSTOMERS": ["ID"],
        })

        result = expand_schema_for_plan(schema_slice, plan, retriever, "TESTDB")

        assert len(result.tables) == 2

    def test_max_added_tables_cap(self):
        """Expansion should respect max_added_tables."""
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS"])
        plan = QueryPlan(
            selected_tables=[
                "TESTDB.PUBLIC.ORDERS",
                "TESTDB.PUBLIC.CUSTOMERS",
                "TESTDB.PUBLIC.PRODUCTS",
                "TESTDB.PUBLIC.REGIONS",
                "TESTDB.PUBLIC.CITIES",
            ],
        )
        retriever = _mock_retriever({
            "TESTDB.PUBLIC.ORDERS": ["ID"],
            "TESTDB.PUBLIC.CUSTOMERS": ["ID"],
            "TESTDB.PUBLIC.PRODUCTS": ["ID"],
            "TESTDB.PUBLIC.REGIONS": ["ID"],
            "TESTDB.PUBLIC.CITIES": ["ID"],
        })

        result = expand_schema_for_plan(
            schema_slice, plan, retriever, "TESTDB", max_added_tables=2,
        )

        # 1 original + 2 added = 3
        assert len(result.tables) == 3

    def test_join_table_expansion(self):
        """Tables in plan.joins but not in slice should be added."""
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS"])
        plan = QueryPlan(
            selected_tables=["TESTDB.PUBLIC.ORDERS"],
            joins=[
                PlanJoin(
                    left_table="TESTDB.PUBLIC.ORDERS",
                    left_column="CUSTOMER_ID",
                    right_table="TESTDB.PUBLIC.CUSTOMERS",
                    right_column="ID",
                ),
            ],
        )
        retriever = _mock_retriever({
            "TESTDB.PUBLIC.ORDERS": ["ID", "CUSTOMER_ID"],
            "TESTDB.PUBLIC.CUSTOMERS": ["ID", "NAME"],
        })

        result = expand_schema_for_plan(schema_slice, plan, retriever, "TESTDB")

        table_names = {ts.qualified_name for ts in result.tables}
        assert "TESTDB.PUBLIC.CUSTOMERS" in table_names

    def test_no_retriever_no_op(self):
        """When retriever is None, expansion is not possible (handled by caller)."""
        # This test just verifies the function signature works; the caller
        # gates on retriever being None.
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS"])
        plan = QueryPlan(selected_tables=["TESTDB.PUBLIC.ORDERS"])
        retriever = _mock_retriever({})

        result = expand_schema_for_plan(schema_slice, plan, retriever, "TESTDB")
        assert len(result.tables) == 1

    def test_empty_plan_no_expansion(self):
        """A plan with no tables at all should not crash."""
        schema_slice = _make_slice(["TESTDB.PUBLIC.ORDERS"])
        plan = QueryPlan(selected_tables=[])
        retriever = _mock_retriever({})

        result = expand_schema_for_plan(schema_slice, plan, retriever, "TESTDB")
        assert len(result.tables) == 1
