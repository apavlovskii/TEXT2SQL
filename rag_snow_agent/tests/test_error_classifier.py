"""Tests for Snowflake error classification."""

from rag_snow_agent.agent.error_classifier import (
    AGGREGATION_ERROR,
    AMBIGUOUS_COLUMN,
    INVALID_IDENTIFIER,
    NOT_AUTHORIZED,
    OBJECT_NOT_FOUND,
    OTHER_EXECUTION_ERROR,
    SQL_SYNTAX_ERROR,
    TYPE_MISMATCH,
    UNKNOWN_FUNCTION,
    classify_snowflake_error,
    extract_offending_identifier,
    extract_offending_object,
)


def test_object_not_found():
    msg = "Object 'TESTDB.PUBLIC.NONEXISTENT' does not exist or not authorized."
    assert classify_snowflake_error(msg) == OBJECT_NOT_FOUND


def test_table_not_found():
    msg = "SQL compilation error: Table 'ORDERS' does not exist"
    assert classify_snowflake_error(msg) == OBJECT_NOT_FOUND


def test_not_authorized():
    msg = "Insufficient privileges to operate on table 'SECRET_TABLE'"
    assert classify_snowflake_error(msg) == NOT_AUTHORIZED


def test_invalid_identifier():
    msg = "SQL compilation error: invalid identifier 'ORDERS.BOGUS_COL'"
    assert classify_snowflake_error(msg) == INVALID_IDENTIFIER


def test_ambiguous_column():
    msg = "SQL compilation error: Column 'ID' is ambiguous"
    assert classify_snowflake_error(msg) == AMBIGUOUS_COLUMN


def test_aggregation_error():
    msg = "SQL compilation error: 'ORDERS.NAME' is not in GROUP BY"
    assert classify_snowflake_error(msg) == AGGREGATION_ERROR


def test_type_mismatch():
    msg = "Numeric value 'abc' is not recognized"
    assert classify_snowflake_error(msg) == TYPE_MISMATCH


def test_unknown_function():
    msg = "Unknown function: MY_CUSTOM_FUNC"
    assert classify_snowflake_error(msg) == UNKNOWN_FUNCTION


def test_syntax_error():
    msg = "SQL compilation error: syntax error line 3 at position 10 unexpected 'FROM'"
    assert classify_snowflake_error(msg) == SQL_SYNTAX_ERROR


def test_other_error():
    msg = "Some completely unknown error condition 12345"
    assert classify_snowflake_error(msg) == OTHER_EXECUTION_ERROR


def test_extract_identifier():
    msg = "SQL compilation error: invalid identifier 'ORDERS.BOGUS_COL'"
    assert extract_offending_identifier(msg) == "ORDERS.BOGUS_COL"


def test_extract_identifier_none():
    msg = "Some random error"
    assert extract_offending_identifier(msg) is None


def test_extract_object():
    msg = "Object 'TESTDB.PUBLIC.NONEXISTENT' does not exist or not authorized."
    assert extract_offending_object(msg) == "TESTDB.PUBLIC.NONEXISTENT"


def test_extract_object_table():
    msg = "Table 'MY_TABLE' does not exist"
    assert extract_offending_object(msg) == "MY_TABLE"


def test_extract_object_none():
    msg = "Insufficient privileges to operate on table 'SECRET_TABLE'"
    assert extract_offending_object(msg) is None
