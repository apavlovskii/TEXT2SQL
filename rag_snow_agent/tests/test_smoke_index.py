"""Smoke test: build index from fake metadata, verify ChromaDB counts."""

from __future__ import annotations

import tempfile
from unittest.mock import MagicMock, patch

from rag_snow_agent.chroma.build_index import run
from rag_snow_agent.snowflake.metadata import ColumnInfo, TableInfo


def _fake_tables() -> list[TableInfo]:
    """One small table with 3 columns — enough to exercise the full pipeline."""
    cols = [
        ColumnInfo("TESTDB", "PUBLIC", "ORDERS", "ORDER_ID", "NUMBER", 1, "NO"),
        ColumnInfo("TESTDB", "PUBLIC", "ORDERS", "CREATED_AT", "TIMESTAMP_NTZ", 2, "YES"),
        ColumnInfo("TESTDB", "PUBLIC", "ORDERS", "AMOUNT", "FLOAT", 3, "YES"),
    ]
    return [
        TableInfo(
            table_catalog="TESTDB",
            table_schema="PUBLIC",
            table_name="ORDERS",
            table_type="BASE TABLE",
            row_count=100,
            comment="Test orders table",
            columns=cols,
        )
    ]


@patch("rag_snow_agent.chroma.build_index.connect")
@patch("rag_snow_agent.chroma.build_index.extract_tables")
def test_smoke_build_index(mock_extract, mock_connect):
    """Index one db_id with mocked Snowflake data and verify card counts."""
    mock_connect.return_value = MagicMock()
    mock_extract.return_value = _fake_tables()

    with tempfile.TemporaryDirectory() as tmp:
        counts = run(db_id="TESTDB", credentials="dummy.json", chroma_dir=tmp)

    print(f"\n=== Smoke test results ===")
    print(f"  TableCards inserted: {counts.get('table', 0)}")
    print(f"  ColumnCards inserted: {counts.get('column', 0)}")

    assert counts["table"] == 1, f"Expected 1 TableCard, got {counts.get('table')}"
    assert counts["column"] == 3, f"Expected 3 ColumnCards, got {counts.get('column')}"
