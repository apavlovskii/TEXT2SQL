"""Tests for budget trimming logic."""

from rag_snow_agent.retrieval.budget import classify_column, trim_to_budget
from rag_snow_agent.retrieval.schema_slice import ColumnSlice, SchemaSlice, TableSlice


def _make_col(name: str, data_type: str = "VARCHAR", tokens: int = 10, rank: int = 1) -> ColumnSlice:
    col = ColumnSlice(
        name=name,
        data_type=data_type,
        token_estimate=tokens,
        fused_rank=rank,
    )
    classify_column(col)
    return col


def _make_table(qname: str, cols: list[ColumnSlice], rank: int = 1, header_tokens: int = 5) -> TableSlice:
    return TableSlice(
        qualified_name=qname,
        table_token_estimate=header_tokens,
        fused_rank=rank,
        columns=cols,
    )


def test_classify_join_key():
    col = ColumnSlice(name="ORDER_ID", data_type="NUMBER", token_estimate=5)
    classify_column(col)
    assert col.is_join_key
    assert not col.is_time_column


def test_classify_time_column():
    col = ColumnSlice(name="CREATED_AT", data_type="TIMESTAMP_NTZ", token_estimate=5)
    classify_column(col)
    assert col.is_time_column


def test_classify_plain():
    col = ColumnSlice(name="AMOUNT", data_type="FLOAT", token_estimate=5)
    classify_column(col)
    assert not col.is_join_key
    assert not col.is_time_column


def test_trim_drops_low_ranked_columns():
    """With a tight budget, lowest-ranked unprotected columns are dropped first."""
    cols = [
        _make_col("ORDER_ID", "NUMBER", tokens=10, rank=1),  # join key, protected
        _make_col("CREATED_AT", "TIMESTAMP_NTZ", tokens=10, rank=2),  # time, protected
        _make_col("AMOUNT", "FLOAT", tokens=10, rank=3),  # unprotected
        _make_col("STATUS", "VARCHAR", tokens=10, rank=4),  # unprotected, worst rank
    ]
    table = _make_table("DB.S.ORDERS", cols, rank=1, header_tokens=5)
    ss = SchemaSlice(db_id="DB", tables=[table])

    # Total: 5 (header) + 4*10 = 45 tokens.  Budget = 30 => must drop 15 tokens
    trimmed = trim_to_budget(ss, max_schema_tokens=30)
    remaining_names = [c.name for c in trimmed.tables[0].columns]

    # STATUS (rank 4) should be dropped first, then AMOUNT (rank 3)
    assert "STATUS" not in remaining_names
    # ORDER_ID and CREATED_AT are protected, should survive
    assert "ORDER_ID" in remaining_names
    assert "CREATED_AT" in remaining_names


def test_trim_respects_max_columns_per_table():
    cols = [_make_col(f"COL_{i}", tokens=5, rank=i) for i in range(10)]
    table = _make_table("DB.S.T", cols, rank=1, header_tokens=5)
    ss = SchemaSlice(db_id="DB", tables=[table])

    trimmed = trim_to_budget(ss, max_schema_tokens=9999, max_columns_per_table=3)
    assert len(trimmed.tables[0].columns) == 3


def test_trim_drops_tables_when_needed():
    """If column trimming isn't enough, whole tables get dropped."""
    t1 = _make_table(
        "DB.S.T1",
        [_make_col("A", tokens=20, rank=1)],
        rank=1,
        header_tokens=10,
    )
    t2 = _make_table(
        "DB.S.T2",
        [_make_col("B", tokens=20, rank=1)],
        rank=2,
        header_tokens=10,
    )
    ss = SchemaSlice(db_id="DB", tables=[t1, t2])
    # Total = 60 tokens.  Budget = 35 => must lose one table
    trimmed = trim_to_budget(ss, max_schema_tokens=35)
    assert len(trimmed.tables) == 1
    # T1 has better rank, should survive
    assert trimmed.tables[0].qualified_name == "DB.S.T1"


def test_trim_under_budget_is_noop():
    cols = [_make_col("X", tokens=5, rank=1)]
    table = _make_table("DB.S.T", cols, rank=1, header_tokens=5)
    ss = SchemaSlice(db_id="DB", tables=[table])
    trimmed = trim_to_budget(ss, max_schema_tokens=9999)
    assert len(trimmed.tables) == 1
    assert len(trimmed.tables[0].columns) == 1
