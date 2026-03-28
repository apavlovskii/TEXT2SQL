"""Tests for gold SQL join extraction and JoinCard creation."""

from __future__ import annotations

import pytest

from rag_snow_agent.chroma.gold_joins import (
    _extract_joins_from_sql,
    extract_joins_from_gold_sqls,
)
from rag_snow_agent.chroma.schema_cards import JoinCard


class TestExtractJoinsFromSql:
    def test_simple_join_on(self):
        """Extract a single JOIN ON condition with quoted identifiers."""
        sql = '''
        SELECT "E"."NAME"
        FROM "FINANCE"."CYBERSYN"."ENTITIES" AS "E"
        JOIN "FINANCE"."CYBERSYN"."TIMESERIES" AS "T"
          ON "E"."ID_RSSD" = "T"."ID_RSSD"
        WHERE "E"."IS_ACTIVE" = TRUE
        '''
        joins = _extract_joins_from_sql(sql, "test.sql")
        assert len(joins) >= 1
        j = joins[0]
        assert j["left_column"] == "ID_RSSD"
        assert j["right_column"] == "ID_RSSD"
        assert j["source_file"] == "test.sql"

    def test_unquoted_identifiers(self):
        """Extract join from unquoted SQL."""
        sql = """
        SELECT a.name
        FROM db.schema.orders AS a
        JOIN db.schema.customers AS b ON a.customer_id = b.id
        WHERE a.status = 'active'
        """
        joins = _extract_joins_from_sql(sql, "test2.sql")
        assert len(joins) >= 1
        j = joins[0]
        assert j["left_column"] == "customer_id"
        assert j["right_column"] == "id"

    def test_multiple_joins(self):
        """Extract multiple JOINs from a single SQL."""
        sql = """
        SELECT a.name, c.region
        FROM db.sch.orders AS a
        JOIN db.sch.customers AS b ON a.cust_id = b.id
        JOIN db.sch.regions AS c ON b.region_id = c.id
        WHERE a.status = 'active'
        """
        joins = _extract_joins_from_sql(sql, "test3.sql")
        assert len(joins) >= 2

    def test_no_joins(self):
        """SQL without JOINs should return empty list."""
        sql = "SELECT * FROM db.sch.orders WHERE status = 'active'"
        joins = _extract_joins_from_sql(sql, "no_join.sql")
        assert len(joins) == 0

    def test_cross_join_no_on(self):
        """CROSS JOIN has no ON clause, should not extract any join conditions."""
        sql = """
        SELECT a.val, b.val
        FROM db.sch.table1 AS a
        CROSS JOIN db.sch.table2 AS b
        LIMIT 10
        """
        joins = _extract_joins_from_sql(sql, "cross.sql")
        # CROSS JOIN doesn't have ON, so no join conditions extracted
        assert len(joins) == 0


class TestJoinCardCreation:
    def test_join_card_from_extracted_join(self):
        """Create a JoinCard from an extracted join dict."""
        sql = '''
        SELECT "E"."NAME"
        FROM "FINANCE"."CYBERSYN"."ENTITIES" AS "E"
        JOIN "FINANCE"."CYBERSYN"."TIMESERIES" AS "T"
          ON "E"."ID_RSSD" = "T"."ID_RSSD"
        WHERE 1=1
        '''
        joins = _extract_joins_from_sql(sql, "test.sql")
        assert len(joins) >= 1

        j = joins[0]
        card = JoinCard(
            db_id="FINANCE",
            left_table=j["left_table"],
            left_column=j["left_column"],
            right_table=j["right_table"],
            right_column=j["right_column"],
            confidence=1.0,
            source="gold_sql",
        )
        assert card.confidence == 1.0
        assert card.source == "gold_sql"
        assert card.left_column == "ID_RSSD"
        assert card.right_column == "ID_RSSD"
        assert "gold_sql" in card.chroma_metadata()["source"]

    def test_join_card_chroma_id(self):
        """JoinCard chroma_id should follow the standard format."""
        card = JoinCard(
            db_id="TESTDB",
            left_table="TESTDB.PUBLIC.ORDERS",
            left_column="CUSTOMER_ID",
            right_table="TESTDB.PUBLIC.CUSTOMERS",
            right_column="ID",
            confidence=1.0,
            source="gold_sql",
        )
        assert card.chroma_id().startswith("join:")
        assert "CUSTOMER_ID" in card.chroma_id()


class TestExtractJoinsFromGoldDir:
    def test_nonexistent_dir_returns_empty(self, tmp_path):
        """A non-existent directory should return an empty list."""
        result = extract_joins_from_gold_sqls(tmp_path / "nonexistent")
        assert result == []

    def test_empty_dir_returns_empty(self, tmp_path):
        """An empty directory should return an empty list."""
        result = extract_joins_from_gold_sqls(tmp_path)
        assert result == []

    def test_parses_sql_files_in_dir(self, tmp_path):
        """Should parse .sql files found in the directory."""
        sql_content = '''
        SELECT a.name
        FROM db.sch.orders AS a
        JOIN db.sch.customers AS b ON a.cust_id = b.id
        WHERE 1=1
        '''
        (tmp_path / "test.sql").write_text(sql_content)

        joins = extract_joins_from_gold_sqls(tmp_path)
        assert len(joins) >= 1
        assert joins[0]["source_file"] == "test.sql"
