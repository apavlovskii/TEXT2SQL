"""Pydantic models for the structured query plan."""

from __future__ import annotations

import types

from pydantic import BaseModel, model_validator


def _is_str_or_optional_str(annotation) -> bool:
    """Return True if *annotation* is ``str`` or ``str | None``."""
    if annotation is str:
        return True
    # str | None produces a types.UnionType in Python 3.10+
    if isinstance(annotation, types.UnionType):
        args = annotation.__args__
        non_none = [a for a in args if a is not type(None)]
        return len(non_none) == 1 and non_none[0] is str
    return False


class _CoercingBase(BaseModel):
    """Base model that coerces non-string values to strings for str fields.

    LLMs generate JSON with "natural" types — integers for numbers like
    ``20170201``, booleans for flags, etc.  Pydantic strict mode rejects
    these.  This pre-validator walks the raw input dict and converts any
    value destined for a ``str`` or ``str | None`` annotation to ``str(value)``.
    """

    @model_validator(mode="before")
    @classmethod
    def coerce_str_fields(cls, data):
        if not isinstance(data, dict):
            return data
        for field_name, field_info in cls.model_fields.items():
            if field_name in data and data[field_name] is not None:
                if _is_str_or_optional_str(field_info.annotation):
                    data[field_name] = str(data[field_name])
        return data


class PlanJoin(_CoercingBase):
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = "INNER"  # INNER / LEFT / RIGHT / FULL


class PlanFilter(_CoercingBase):
    table: str
    column: str
    op: str  # =, !=, <, >, <=, >=, IN, LIKE, ILIKE, BETWEEN, IS NULL, IS NOT NULL
    value: str | None = None  # stringified; None for IS NULL / IS NOT NULL


class PlanAggregation(_CoercingBase):
    func: str  # COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT, etc.
    table: str
    column: str
    alias: str


class PlanOrderBy(_CoercingBase):
    expr: str  # column name or alias
    direction: str = "ASC"  # ASC / DESC


class PlanFlatten(_CoercingBase):
    """Describes a LATERAL FLATTEN on a VARIANT ARRAY column."""

    table: str  # source table containing the VARIANT column
    variant_column: str  # e.g. "hits", "assignee_harmonized"
    alias: str  # short alias for flattened output, e.g. "h", "ah"
    extract_fields: list[str] = []  # nested paths to extract, e.g. ["page.pagePath", "eCommerceAction.action_type"]


class PlanGeoJoin(_CoercingBase):
    """A spatial JOIN using a geospatial predicate in the ON clause.

    Unlike PlanJoin (equality only), this allows arbitrary spatial predicates
    such as ST_WITHIN, ST_CONTAINS, ST_INTERSECTS in the ON expression.
    """

    right_table: str  # table to join
    join_type: str = "INNER"  # INNER / LEFT / CROSS
    on_expression: str  # complete ON clause SQL, e.g. "ST_WITHIN(ST_POINT(t1.\"lon\", t1.\"lat\"), TO_GEOGRAPHY(t2.\"geom\"))"


class PlanGeoFilter(_CoercingBase):
    """A geospatial predicate for the WHERE clause.

    The expression is a complete SQL boolean predicate emitted verbatim.
    Examples:
      - "ST_DWITHIN(ST_MAKEPOINT(t1.\"lon\", t1.\"lat\"), ST_MAKEPOINT(-73.764, 41.197), 32186.8)"
      - "ST_DISTANCE(TO_GEOGRAPHY(t1.\"geography\"), TO_GEOGRAPHY('POINT(51.5 26.75)')) <= 5000"
    """

    expression: str  # complete SQL boolean predicate


class PlanCTE(_CoercingBase):
    """One step (CTE) in a multi-step query pipeline."""

    name: str  # CTE alias, e.g. "base", "top_assignee"
    description: str  # what this CTE computes
    selected_tables: list[str] = []  # tables used in this CTE (or upstream CTE names)
    joins: list[PlanJoin] = []
    geo_joins: list[PlanGeoJoin] = []  # spatial joins with geospatial predicates
    flatten_ops: list[PlanFlatten] = []
    filters: list[PlanFilter] = []
    geo_filters: list[PlanGeoFilter] = []  # geospatial WHERE predicates
    group_by: list[str] = []
    aggregations: list[PlanAggregation] = []
    order_by: list[PlanOrderBy] = []
    limit: int | None = None


class QueryPlan(_CoercingBase):
    selected_tables: list[str]  # qualified table names from SchemaSlice
    joins: list[PlanJoin] = []
    geo_joins: list[PlanGeoJoin] = []  # spatial joins with geospatial predicates
    flatten_ops: list[PlanFlatten] = []  # LATERAL FLATTEN operations on VARIANT ARRAYs
    filters: list[PlanFilter] = []
    geo_filters: list[PlanGeoFilter] = []  # geospatial WHERE predicates
    group_by: list[str] = []  # "table.column" or just "column"
    aggregations: list[PlanAggregation] = []
    order_by: list[PlanOrderBy] = []
    limit: int | None = None
    ctes: list[PlanCTE] = []  # multi-step CTE pipeline (optional)
    notes: str | None = None
