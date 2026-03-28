"""Extract schema metadata from Snowflake INFORMATION_SCHEMA."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import snowflake.connector

log = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    data_type: str
    ordinal_position: int
    is_nullable: str
    comment: str | None = None


@dataclass
class TableInfo:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str
    row_count: int | None = None
    comment: str | None = None
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        return f"{self.table_catalog}.{self.table_schema}.{self.table_name}"


@dataclass
class JoinEdge:
    """A join relationship between two columns in different tables."""

    left_table: str  # qualified name DB.SCHEMA.TABLE
    left_column: str
    right_table: str  # qualified name DB.SCHEMA.TABLE
    right_column: str
    confidence: float  # 1.0 for FK, 0.7 for heuristic name match
    source: str  # "fk" or "heuristic_name"


# Column types considered compatible for heuristic join matching
_NUMERIC_TYPES = {"NUMBER", "INT", "INTEGER", "BIGINT", "SMALLINT", "FLOAT", "DECIMAL", "NUMERIC"}
_STRING_TYPES = {"VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER"}


def _types_compatible(type_a: str, type_b: str) -> bool:
    """Check whether two Snowflake data types are compatible for a join."""
    a = type_a.upper().split("(")[0].strip()
    b = type_b.upper().split("(")[0].strip()
    if a == b:
        return True
    if a in _NUMERIC_TYPES and b in _NUMERIC_TYPES:
        return True
    if a in _STRING_TYPES and b in _STRING_TYPES:
        return True
    return False


_JOIN_KEY_RE = re.compile(r"(^ID$|_ID$|_KEY$)", re.IGNORECASE)


def _heuristic_join_edges(tables: list[TableInfo]) -> list[JoinEdge]:
    """Match columns with same name ending in _ID, ID, or _KEY across tables."""
    # Build index: column_name_upper -> [(table_qualified_name, column_name, data_type)]
    col_index: dict[str, list[tuple[str, str, str]]] = {}
    for t in tables:
        for c in t.columns:
            if _JOIN_KEY_RE.search(c.column_name):
                key = c.column_name.upper()
                col_index.setdefault(key, []).append(
                    (t.qualified_name, c.column_name, c.data_type)
                )

    edges: list[JoinEdge] = []
    for _col_name, locations in col_index.items():
        if len(locations) < 2:
            continue
        for i in range(len(locations)):
            for j in range(i + 1, len(locations)):
                lt, lc, ltype = locations[i]
                rt, rc, rtype = locations[j]
                if lt == rt:
                    continue
                if _types_compatible(ltype, rtype):
                    edges.append(
                        JoinEdge(
                            left_table=lt,
                            left_column=lc,
                            right_table=rt,
                            right_column=rc,
                            confidence=0.7,
                            source="heuristic_name",
                        )
                    )
    return edges


def extract_join_edges(
    conn: "snowflake.connector.SnowflakeConnection",
    db_id: str,
    tables: list[TableInfo],
) -> list[JoinEdge]:
    """Extract join edges: try FK constraints first, fall back to heuristic."""
    fk_edges: list[JoinEdge] = []
    table_qnames = {t.qualified_name for t in tables}

    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE DATABASE {db_id}")
            cur.execute(
                "SELECT tc.table_schema, tc.table_name, "
                "       kcu.column_name, "
                "       rc.unique_constraint_schema, "
                "       rc.unique_constraint_name "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.referential_constraints rc "
                "  ON tc.constraint_name = rc.constraint_name "
                "  AND tc.constraint_schema = rc.constraint_schema "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_name = kcu.constraint_name "
                "  AND tc.constraint_schema = kcu.constraint_schema "
                "WHERE tc.constraint_type = 'FOREIGN KEY' "
                "  AND tc.table_schema != 'INFORMATION_SCHEMA'"
            )
            # NOTE: this query is best-effort; many Snowflake DBs lack FK metadata.
            for row in cur.fetchall():
                fk_schema, fk_table, fk_col = row[0], row[1], row[2]
                ref_schema, ref_constraint = row[3], row[4]
                left_qname = f"{db_id}.{fk_schema}.{fk_table}"
                # We can't always resolve the referenced table directly from
                # REFERENTIAL_CONSTRAINTS alone in Snowflake, so we log what we find.
                # For now, FK edges are added only when both tables are in our list.
                log.debug(
                    "FK found: %s.%s -> constraint %s.%s",
                    left_qname, fk_col, ref_schema, ref_constraint,
                )
        finally:
            cur.close()
    except Exception:
        log.debug("FK constraint query not supported or failed; using heuristic only")

    # Always include heuristic edges (they may find things FKs miss)
    heuristic_edges = _heuristic_join_edges(tables)

    # Deduplicate: FK edges override heuristic for the same pair
    seen: set[tuple[str, str, str, str]] = set()
    combined: list[JoinEdge] = []
    for e in fk_edges:
        key = (e.left_table, e.left_column, e.right_table, e.right_column)
        if key not in seen:
            seen.add(key)
            combined.append(e)
    for e in heuristic_edges:
        key = (e.left_table, e.left_column, e.right_table, e.right_column)
        rev_key = (e.right_table, e.right_column, e.left_table, e.left_column)
        if key not in seen and rev_key not in seen:
            seen.add(key)
            combined.append(e)

    log.info("Found %d join edges for %s (%d FK, %d heuristic)",
             len(combined), db_id, len(fk_edges), len(heuristic_edges))
    return combined


def extract_tables(
    conn: snowflake.connector.SnowflakeConnection,
    db_id: str,
) -> list[TableInfo]:
    """Return TableInfo objects (with columns) for every table/view in *db_id*."""
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {db_id}")

        # --- tables -----------------------------------------------------------
        cur.execute(
            "SELECT table_catalog, table_schema, table_name, table_type, "
            "       row_count, comment "
            "FROM information_schema.tables "
            "WHERE table_schema != 'INFORMATION_SCHEMA' "
            "ORDER BY table_schema, table_name"
        )
        tables_by_key: dict[str, TableInfo] = {}
        for row in cur.fetchall():
            t = TableInfo(
                table_catalog=row[0],
                table_schema=row[1],
                table_name=row[2],
                table_type=row[3],
                row_count=row[4],
                comment=row[5] or None,
            )
            tables_by_key[t.qualified_name] = t

        log.info("Found %d tables/views in %s", len(tables_by_key), db_id)

        # --- columns ----------------------------------------------------------
        cur.execute(
            "SELECT table_catalog, table_schema, table_name, column_name, "
            "       data_type, ordinal_position, is_nullable, comment "
            "FROM information_schema.columns "
            "WHERE table_schema != 'INFORMATION_SCHEMA' "
            "ORDER BY table_schema, table_name, ordinal_position"
        )
        for row in cur.fetchall():
            col = ColumnInfo(
                table_catalog=row[0],
                table_schema=row[1],
                table_name=row[2],
                column_name=row[3],
                data_type=row[4],
                ordinal_position=row[5],
                is_nullable=row[6],
                comment=row[7] or None,
            )
            key = f"{col.table_catalog}.{col.table_schema}.{col.table_name}"
            if key in tables_by_key:
                tables_by_key[key].columns.append(col)

        total_cols = sum(len(t.columns) for t in tables_by_key.values())
        log.info("Found %d columns across all tables", total_cols)

        return list(tables_by_key.values())
    finally:
        cur.close()
