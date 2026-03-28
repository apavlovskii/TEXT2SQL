"""Pydantic models for the structured query plan."""

from __future__ import annotations

from pydantic import BaseModel


class PlanJoin(BaseModel):
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = "INNER"  # INNER / LEFT / RIGHT / FULL


class PlanFilter(BaseModel):
    table: str
    column: str
    op: str  # =, !=, <, >, <=, >=, IN, LIKE, ILIKE, BETWEEN, IS NULL, IS NOT NULL
    value: str | None = None  # stringified; None for IS NULL / IS NOT NULL


class PlanAggregation(BaseModel):
    func: str  # COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT, etc.
    table: str
    column: str
    alias: str


class PlanOrderBy(BaseModel):
    expr: str  # column name or alias
    direction: str = "ASC"  # ASC / DESC


class QueryPlan(BaseModel):
    selected_tables: list[str]  # qualified table names from SchemaSlice
    joins: list[PlanJoin] = []
    filters: list[PlanFilter] = []
    group_by: list[str] = []  # "table.column" or just "column"
    aggregations: list[PlanAggregation] = []
    order_by: list[PlanOrderBy] = []
    limit: int | None = None
    notes: str | None = None
