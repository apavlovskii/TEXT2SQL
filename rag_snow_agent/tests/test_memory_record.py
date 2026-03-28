"""Tests for TraceRecord creation and summaries."""

from rag_snow_agent.agent.memory import (
    TraceRecord,
    make_trace_record,
    summarize_plan,
    summarize_repair_trace,
    summarize_schema_slice,
    summarize_verification,
)
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_schema_slice():
    return SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name="TESTDB.PUBLIC.ORDERS",
                columns=[
                    ColumnSlice(name="ORDER_ID", data_type="NUMBER", is_join_key=True),
                    ColumnSlice(name="ORDER_DATE", data_type="DATE", is_time_column=True),
                    ColumnSlice(name="AMOUNT", data_type="NUMBER"),
                ],
            ),
            TableSlice(
                qualified_name="TESTDB.PUBLIC.CUSTOMERS",
                columns=[
                    ColumnSlice(name="CUSTOMER_ID", data_type="NUMBER", is_join_key=True),
                    ColumnSlice(name="NAME", data_type="VARCHAR"),
                ],
            ),
        ],
    )


def test_make_trace_record_basic():
    schema = _make_schema_slice()
    rec = make_trace_record(
        instance_id="inst_001",
        db_id="TESTDB",
        instruction="Show total orders by month",
        schema_slice=schema,
        final_sql="SELECT DATE_TRUNC('MONTH', ORDER_DATE), SUM(AMOUNT) FROM ORDERS GROUP BY 1;",
    )
    assert isinstance(rec, TraceRecord)
    assert rec.instance_id == "inst_001"
    assert rec.db_id == "TESTDB"
    assert len(rec.trace_id) == 16
    assert "TESTDB.PUBLIC.ORDERS" in rec.tables_used
    assert "TESTDB.PUBLIC.CUSTOMERS" in rec.tables_used


def test_trace_record_to_dict():
    schema = _make_schema_slice()
    rec = make_trace_record(
        instance_id="inst_002",
        db_id="TESTDB",
        instruction="count customers",
        schema_slice=schema,
        final_sql="SELECT COUNT(*) FROM CUSTOMERS;",
    )
    d = rec.to_dict()
    assert isinstance(d, dict)
    assert d["trace_id"] == rec.trace_id
    assert d["db_id"] == "TESTDB"
    assert isinstance(d["tables_used"], list)


def test_instruction_summary_truncated():
    schema = _make_schema_slice()
    long_instruction = "A" * 500
    rec = make_trace_record(
        instance_id="inst_003",
        db_id="TESTDB",
        instruction=long_instruction,
        schema_slice=schema,
    )
    assert len(rec.instruction_summary) == 200


def test_sql_compact_truncated():
    schema = _make_schema_slice()
    long_sql = "SELECT " + "x, " * 300 + "1;"
    rec = make_trace_record(
        instance_id="inst_004",
        db_id="TESTDB",
        instruction="test",
        schema_slice=schema,
        final_sql=long_sql,
    )
    assert len(rec.final_sql) <= 500


def test_key_columns_detected():
    schema = _make_schema_slice()
    rec = make_trace_record(
        instance_id="inst_005",
        db_id="TESTDB",
        instruction="test",
        schema_slice=schema,
    )
    # ORDER_ID is join key, ORDER_DATE is time column, CUSTOMER_ID is join key
    assert "TESTDB.PUBLIC.ORDERS.ORDER_ID" in rec.key_columns_used
    assert "TESTDB.PUBLIC.ORDERS.ORDER_DATE" in rec.key_columns_used
    assert "TESTDB.PUBLIC.CUSTOMERS.CUSTOMER_ID" in rec.key_columns_used


def test_summarize_schema_slice():
    schema = _make_schema_slice()
    summary = summarize_schema_slice(schema)
    assert "TESTDB" in summary
    assert "ORDERS" in summary
    assert "CUSTOMERS" in summary


def test_summarize_plan_none():
    assert summarize_plan(None) == ""


def test_summarize_repair_trace_empty():
    assert summarize_repair_trace([]) == "No repairs"
    assert summarize_repair_trace(None) == "No repairs"


def test_summarize_repair_trace_with_dicts():
    trace = [
        {"error_type": "OBJECT_NOT_FOUND", "repair_action": "fix_table_name"},
        {"error_type": "SYNTAX_ERROR", "repair_action": "rewrite_query"},
    ]
    summary = summarize_repair_trace(trace)
    assert "OBJECT_NOT_FOUND" in summary
    assert "SYNTAX_ERROR" in summary


def test_summarize_verification_empty():
    assert summarize_verification(None) == ""
    assert summarize_verification({}) == ""


def test_summarize_verification_success():
    rec = {
        "execution_success": True,
        "row_count": 42,
        "metamorphic": {"score_delta": 5.0},
    }
    summary = summarize_verification(rec)
    assert "exec:OK" in summary
    assert "rows:42" in summary
    assert "meta_delta" in summary
