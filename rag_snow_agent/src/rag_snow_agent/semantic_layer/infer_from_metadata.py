"""Deterministic heuristics to infer semantic facts from schema metadata."""

from __future__ import annotations

import re

from ..snowflake.metadata import TableInfo
from .models import SemanticFact, SemanticProfile

_DATE_TYPES = {"DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"}
_DATE_NAME_RE = re.compile(
    r"(date|time|created_at|updated_at|_dt$|_ts$|timestamp)", re.IGNORECASE
)
_METRIC_NAME_RE = re.compile(
    r"(amount|revenue|count|price|score|duration|total|cost|quantity|sum|avg|sales)",
    re.IGNORECASE,
)
_NUMERIC_TYPES = {
    "NUMBER", "INT", "INTEGER", "BIGINT", "SMALLINT", "FLOAT",
    "DECIMAL", "NUMERIC", "DOUBLE", "REAL",
}
_DIMENSION_NAME_RE = re.compile(
    r"(source|status|country|category|type|name|channel|region|segment|tier|group|level|class)",
    re.IGNORECASE,
)
_STRING_TYPES = {"VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER"}
_VARIANT_TYPES = {"VARIANT", "OBJECT", "ARRAY"}
_ID_RE = re.compile(r"(_id$|_key$|^id$)", re.IGNORECASE)


def infer_from_metadata(tables: list[TableInfo], db_id: str) -> SemanticProfile:
    """Infer semantic facts from schema metadata using deterministic heuristics."""
    profile = SemanticProfile(db_id=db_id)

    for table in tables:
        qname = table.qualified_name
        for col in table.columns:
            col_qname = f"{qname}.{col.column_name}"
            dtype = col.data_type.upper().split("(")[0].strip()

            # primary_time_column: DATE/TIMESTAMP types
            if dtype in _DATE_TYPES:
                profile.time_columns.append(
                    SemanticFact(
                        fact_type="primary_time_column",
                        subject=col_qname,
                        value=dtype,
                        confidence=0.8,
                        evidence=[f"Column type is {dtype}"],
                        source=["metadata"],
                    )
                )
            # date_format_pattern: NUMBER columns with date-like names
            elif dtype in _NUMERIC_TYPES and _DATE_NAME_RE.search(col.column_name):
                profile.time_columns.append(
                    SemanticFact(
                        fact_type="date_format_pattern",
                        subject=col_qname,
                        value="YYYYMMDD integer",
                        confidence=0.7,
                        evidence=[
                            f"Numeric column with date-like name: {col.column_name}"
                        ],
                        source=["metadata"],
                    )
                )
            # date_format_pattern: VARCHAR columns with date-like names
            elif dtype in _STRING_TYPES and _DATE_NAME_RE.search(col.column_name):
                profile.time_columns.append(
                    SemanticFact(
                        fact_type="date_format_pattern",
                        subject=col_qname,
                        value="YYYYMMDD string",
                        confidence=0.7,
                        evidence=[
                            f"String column with date-like name: {col.column_name}"
                        ],
                        source=["metadata"],
                    )
                )

            # metric_candidate: numeric columns with metric-like names
            if dtype in _NUMERIC_TYPES and _METRIC_NAME_RE.search(col.column_name):
                profile.metric_candidates.append(
                    SemanticFact(
                        fact_type="metric_candidate",
                        subject=col_qname,
                        value=col.column_name,
                        confidence=0.7,
                        evidence=[
                            f"Numeric column with metric-like name: {col.column_name}"
                        ],
                        source=["metadata"],
                    )
                )

            # dimension_candidate: VARCHAR columns with dimension-like names
            if dtype in _STRING_TYPES and _DIMENSION_NAME_RE.search(col.column_name):
                profile.dimension_candidates.append(
                    SemanticFact(
                        fact_type="dimension_candidate",
                        subject=col_qname,
                        value=col.column_name,
                        confidence=0.6,
                        evidence=[
                            f"String column with dimension-like name: {col.column_name}"
                        ],
                        source=["metadata"],
                    )
                )

            # nested_container_column: VARIANT/OBJECT/ARRAY types
            if dtype in _VARIANT_TYPES:
                # Distinguish ARRAY vs OBJECT when the declared type is
                # explicit.  Plain "VARIANT" is treated as ARRAY by default
                # because most Snowflake VARIANT columns encountered in
                # GA360 and PATENTS benchmarks hold arrays that require
                # LATERAL FLATTEN.
                if dtype == "ARRAY":
                    variant_kind = "ARRAY"
                elif dtype == "OBJECT":
                    variant_kind = "OBJECT"
                else:
                    # VARIANT — default to ARRAY (the common case that
                    # needs LATERAL FLATTEN)
                    variant_kind = "ARRAY"

                profile.nested_field_patterns.append(
                    SemanticFact(
                        fact_type="nested_container_column",
                        subject=col_qname,
                        value={"type": dtype, "variant_kind": variant_kind},
                        confidence=0.9,
                        evidence=[f"Column type is {dtype}, classified as {variant_kind}"],
                        source=["metadata"],
                    )
                )

            # identifier_column: columns ending in _id, _key, or named "id"
            if _ID_RE.search(col.column_name):
                profile.join_semantics.append(
                    SemanticFact(
                        fact_type="identifier_column",
                        subject=col_qname,
                        value=col.column_name,
                        confidence=0.8,
                        evidence=[
                            f"Column name matches identifier pattern: {col.column_name}"
                        ],
                        source=["metadata"],
                    )
                )

    return profile
