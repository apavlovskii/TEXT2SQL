"""Tests for identifier validation against SchemaSlice."""

from rag_snow_agent.prompting.constraints import validate_sql
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_slice() -> SchemaSlice:
    return SchemaSlice(
        db_id="TESTDB",
        tables=[
            TableSlice(
                qualified_name="TESTDB.PUBLIC.ORDERS",
                columns=[
                    ColumnSlice(name="ORDER_ID", data_type="NUMBER", token_estimate=5),
                    ColumnSlice(name="CUSTOMER_ID", data_type="NUMBER", token_estimate=5),
                    ColumnSlice(name="AMOUNT", data_type="FLOAT", token_estimate=5),
                    ColumnSlice(name="CREATED_AT", data_type="TIMESTAMP_NTZ", token_estimate=5),
                ],
            ),
            TableSlice(
                qualified_name="TESTDB.PUBLIC.CUSTOMERS",
                columns=[
                    ColumnSlice(name="CUSTOMER_ID", data_type="NUMBER", token_estimate=5),
                    ColumnSlice(name="NAME", data_type="VARCHAR", token_estimate=5),
                ],
            ),
        ],
    )


def test_valid_sql_passes():
    ss = _make_slice()
    sql = (
        "SELECT t1.CUSTOMER_ID, t2.NAME, SUM(t1.AMOUNT) AS total "
        "FROM TESTDB.PUBLIC.ORDERS AS t1 "
        "INNER JOIN TESTDB.PUBLIC.CUSTOMERS AS t2 ON t1.CUSTOMER_ID = t2.CUSTOMER_ID "
        "GROUP BY t1.CUSTOMER_ID, t2.NAME"
    )
    result = validate_sql(sql, ss)
    assert result.valid, f"Expected valid, got errors: {result.errors}"


def test_invalid_column_detected():
    ss = _make_slice()
    sql = "SELECT t1.ORDER_ID, t1.NONEXISTENT_COL FROM TESTDB.PUBLIC.ORDERS AS t1"
    result = validate_sql(sql, ss)
    assert not result.valid
    assert any("NONEXISTENT_COL" in e for e in result.errors)


def test_valid_with_qualified_table_column():
    ss = _make_slice()
    sql = "SELECT TESTDB.PUBLIC.ORDERS.AMOUNT FROM TESTDB.PUBLIC.ORDERS"
    result = validate_sql(sql, ss)
    assert result.valid, f"Unexpected errors: {result.errors}"


def test_invalid_qualified_column():
    ss = _make_slice()
    sql = "SELECT TESTDB.PUBLIC.ORDERS.BOGUS_FIELD FROM TESTDB.PUBLIC.ORDERS"
    result = validate_sql(sql, ss)
    assert not result.valid
    assert any("BOGUS_FIELD" in e for e in result.errors)


def test_sql_keywords_not_flagged():
    """SQL constructs like DATE_TRUNC should not be flagged."""
    ss = _make_slice()
    sql = "SELECT DATE_TRUNC('MONTH', t1.CREATED_AT) FROM TESTDB.PUBLIC.ORDERS AS t1"
    result = validate_sql(sql, ss)
    assert result.valid, f"Unexpected errors: {result.errors}"
