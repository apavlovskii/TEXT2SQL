"""Tests for infer_from_metadata: synthetic TableInfo -> correct fact types."""

from __future__ import annotations

from rag_snow_agent.snowflake.metadata import ColumnInfo, TableInfo
from rag_snow_agent.semantic_layer.infer_from_metadata import infer_from_metadata


def _make_table(
    columns: list[ColumnInfo],
    table_name: str = "ORDERS",
    schema: str = "PUBLIC",
    catalog: str = "TESTDB",
) -> TableInfo:
    return TableInfo(
        table_catalog=catalog,
        table_schema=schema,
        table_name=table_name,
        table_type="BASE TABLE",
        row_count=100,
        columns=columns,
    )


def _col(name: str, dtype: str, ordinal: int = 1) -> ColumnInfo:
    return ColumnInfo(
        table_catalog="TESTDB",
        table_schema="PUBLIC",
        table_name="ORDERS",
        column_name=name,
        data_type=dtype,
        ordinal_position=ordinal,
        is_nullable="YES",
    )


class TestPrimaryTimeColumn:
    def test_date_type_detected(self):
        tables = [_make_table([_col("ORDER_DATE", "DATE")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.time_columns) == 1
        assert profile.time_columns[0].fact_type == "primary_time_column"
        assert profile.time_columns[0].confidence == 0.8

    def test_timestamp_type_detected(self):
        tables = [_make_table([_col("CREATED_AT", "TIMESTAMP_NTZ")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.time_columns) == 1
        assert profile.time_columns[0].fact_type == "primary_time_column"

    def test_numeric_date_column(self):
        tables = [_make_table([_col("event_date", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        time_facts = [f for f in profile.time_columns if f.fact_type == "date_format_pattern"]
        assert len(time_facts) == 1
        assert time_facts[0].value == "YYYYMMDD integer"
        assert time_facts[0].confidence == 0.7

    def test_varchar_date_column(self):
        tables = [_make_table([_col("order_date", "VARCHAR")])]
        profile = infer_from_metadata(tables, "TESTDB")
        time_facts = [f for f in profile.time_columns if f.fact_type == "date_format_pattern"]
        assert len(time_facts) == 1
        assert time_facts[0].value == "YYYYMMDD string"


class TestMetricCandidate:
    def test_amount_detected(self):
        tables = [_make_table([_col("TOTAL_AMOUNT", "FLOAT")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.metric_candidates) == 1
        assert profile.metric_candidates[0].fact_type == "metric_candidate"
        assert profile.metric_candidates[0].confidence == 0.7

    def test_revenue_detected(self):
        tables = [_make_table([_col("REVENUE", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.metric_candidates) == 1

    def test_non_numeric_not_metric(self):
        tables = [_make_table([_col("AMOUNT_LABEL", "VARCHAR")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.metric_candidates) == 0


class TestDimensionCandidate:
    def test_status_detected(self):
        tables = [_make_table([_col("ORDER_STATUS", "VARCHAR")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.dimension_candidates) == 1
        assert profile.dimension_candidates[0].fact_type == "dimension_candidate"
        assert profile.dimension_candidates[0].confidence == 0.6

    def test_country_detected(self):
        tables = [_make_table([_col("COUNTRY", "STRING")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.dimension_candidates) == 1

    def test_non_string_not_dimension(self):
        tables = [_make_table([_col("STATUS_CODE", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.dimension_candidates) == 0


class TestNestedContainerColumn:
    def test_variant_detected(self):
        tables = [_make_table([_col("DATA", "VARIANT")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.nested_field_patterns) == 1
        assert profile.nested_field_patterns[0].fact_type == "nested_container_column"
        assert profile.nested_field_patterns[0].confidence == 0.9

    def test_array_detected(self):
        tables = [_make_table([_col("ITEMS", "ARRAY")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.nested_field_patterns) == 1

    def test_object_detected(self):
        tables = [_make_table([_col("METADATA", "OBJECT")])]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.nested_field_patterns) == 1


class TestIdentifierColumn:
    def test_id_suffix_detected(self):
        tables = [_make_table([_col("CUSTOMER_ID", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        id_facts = [f for f in profile.join_semantics if f.fact_type == "identifier_column"]
        assert len(id_facts) == 1
        assert id_facts[0].confidence == 0.8

    def test_key_suffix_detected(self):
        tables = [_make_table([_col("ORDER_KEY", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        id_facts = [f for f in profile.join_semantics if f.fact_type == "identifier_column"]
        assert len(id_facts) == 1

    def test_bare_id_detected(self):
        tables = [_make_table([_col("ID", "NUMBER")])]
        profile = infer_from_metadata(tables, "TESTDB")
        id_facts = [f for f in profile.join_semantics if f.fact_type == "identifier_column"]
        assert len(id_facts) == 1


class TestMultipleColumns:
    def test_mixed_columns(self):
        tables = [
            _make_table([
                _col("ORDER_ID", "NUMBER", 1),
                _col("CREATED_AT", "TIMESTAMP_NTZ", 2),
                _col("AMOUNT", "FLOAT", 3),
                _col("STATUS", "VARCHAR", 4),
                _col("DATA", "VARIANT", 5),
            ])
        ]
        profile = infer_from_metadata(tables, "TESTDB")
        assert len(profile.time_columns) >= 1
        assert len(profile.metric_candidates) >= 1
        assert len(profile.dimension_candidates) >= 1
        assert len(profile.nested_field_patterns) >= 1
        assert len(profile.join_semantics) >= 1

    def test_empty_table_list(self):
        profile = infer_from_metadata([], "TESTDB")
        assert profile.db_id == "TESTDB"
        assert len(profile.all_facts()) == 0
