"""Tests for deterministic SQL compilation from QueryPlan."""

from rag_snow_agent.prompting.plan_schema import (
    PlanAggregation,
    PlanFilter,
    PlanJoin,
    PlanOrderBy,
    QueryPlan,
)
from rag_snow_agent.prompting.sql_compiler import compile_plan


def _two_table_plan() -> QueryPlan:
    return QueryPlan(
        selected_tables=["DB.PUBLIC.ORDERS", "DB.PUBLIC.CUSTOMERS"],
        joins=[
            PlanJoin(
                left_table="DB.PUBLIC.ORDERS",
                left_column="CUSTOMER_ID",
                right_table="DB.PUBLIC.CUSTOMERS",
                right_column="CUSTOMER_ID",
                join_type="INNER",
            )
        ],
        filters=[
            PlanFilter(
                table="DB.PUBLIC.ORDERS",
                column="STATUS",
                op="=",
                value="'COMPLETED'",
            )
        ],
        group_by=["DB.PUBLIC.CUSTOMERS.NAME"],
        aggregations=[
            PlanAggregation(
                func="SUM",
                table="DB.PUBLIC.ORDERS",
                column="AMOUNT",
                alias="total_amount",
            ),
            PlanAggregation(
                func="COUNT",
                table="DB.PUBLIC.ORDERS",
                column="ORDER_ID",
                alias="order_count",
            ),
        ],
        order_by=[PlanOrderBy(expr="total_amount", direction="DESC")],
        limit=10,
    )


def test_compile_contains_select():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "SELECT" in sql


def test_compile_uses_stable_aliases():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "t1" in sql  # first table alias
    assert "t2" in sql  # second table alias


def test_compile_has_join():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "INNER JOIN" in sql
    assert 't1."CUSTOMER_ID" = t2."CUSTOMER_ID"' in sql


def test_compile_has_where():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "WHERE" in sql
    assert "STATUS" in sql
    assert "'COMPLETED'" in sql


def test_compile_has_group_by():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "GROUP BY" in sql


def test_compile_has_aggregations():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert 'SUM(t1."AMOUNT")' in sql
    assert 'COUNT(t1."ORDER_ID")' in sql
    assert "total_amount" in sql
    assert "order_count" in sql


def test_compile_has_order_and_limit():
    plan = _two_table_plan()
    sql = compile_plan(plan)
    assert "ORDER BY total_amount DESC" in sql
    assert "LIMIT 10" in sql


def test_compile_single_table():
    plan = QueryPlan(
        selected_tables=["DB.PUBLIC.ITEMS"],
        aggregations=[
            PlanAggregation(func="COUNT", table="DB.PUBLIC.ITEMS", column="*", alias="cnt")
        ],
    )
    sql = compile_plan(plan)
    assert "COUNT(*)" in sql
    assert "DB.PUBLIC.ITEMS AS t1" in sql


def test_compile_count_distinct():
    plan = QueryPlan(
        selected_tables=["DB.PUBLIC.EVENTS"],
        aggregations=[
            PlanAggregation(
                func="COUNT_DISTINCT",
                table="DB.PUBLIC.EVENTS",
                column="USER_ID",
                alias="unique_users",
            )
        ],
    )
    sql = compile_plan(plan)
    assert 'COUNT(DISTINCT t1."USER_ID")' in sql


def test_compile_deterministic():
    """Same plan always produces same SQL."""
    plan = _two_table_plan()
    sql1 = compile_plan(plan)
    sql2 = compile_plan(plan)
    assert sql1 == sql2
