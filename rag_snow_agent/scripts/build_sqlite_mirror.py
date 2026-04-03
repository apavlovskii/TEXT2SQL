"""Build a local SQLite database mirroring the Snowflake schema for offline testing.

Reads sample_records.json and table_column_descriptions.json to create tables
with proper column types.  VARIANT columns are stored as TEXT (JSON strings),
matching Snowflake's behavior when queried via connectors.

Usage:
    uv run python scripts/build_sqlite_mirror.py
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SAMPLE_RECORDS_PATH = DATA_DIR / "sample_records.json"
DESCRIPTIONS_PATH = DATA_DIR / "table_column_descriptions.json"
DB_PATH = DATA_DIR / "mirror.db"

# Map Snowflake types to SQLite types
_TYPE_MAP = {
    "TEXT": "TEXT",
    "VARCHAR": "TEXT",
    "STRING": "TEXT",
    "CHAR": "TEXT",
    "CHARACTER": "TEXT",
    "NUMBER": "REAL",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "BIGINT": "INTEGER",
    "SMALLINT": "INTEGER",
    "FLOAT": "REAL",
    "DOUBLE": "REAL",
    "DECIMAL": "REAL",
    "NUMERIC": "REAL",
    "BOOLEAN": "INTEGER",
    "DATE": "TEXT",
    "TIMESTAMP": "TEXT",
    "TIMESTAMP_NTZ": "TEXT",
    "TIMESTAMP_LTZ": "TEXT",
    "TIMESTAMP_TZ": "TEXT",
    "VARIANT": "TEXT",
    "OBJECT": "TEXT",
    "ARRAY": "TEXT",
    "VARIANT_FIELD": "TEXT",
}


def _sqlite_type(snowflake_type: str) -> str:
    """Map a Snowflake data type to a SQLite type."""
    base = snowflake_type.upper().split("(")[0].strip()
    return _TYPE_MAP.get(base, "TEXT")


def _sanitize_table_name(fqn: str) -> str:
    """Convert DB.SCHEMA.TABLE to a SQLite-friendly name: DB__SCHEMA__TABLE."""
    return fqn.replace(".", "__")


def _quote_col(name: str) -> str:
    """Double-quote a column name for SQLite."""
    return f'"{name}"'


def build_mirror(
    sample_path: Path | None = None,
    descriptions_path: Path | None = None,
    db_path: Path | None = None,
) -> Path:
    """Create SQLite mirror database. Returns the database path."""
    sample_path = sample_path or SAMPLE_RECORDS_PATH
    descriptions_path = descriptions_path or DESCRIPTIONS_PATH
    db_path = db_path or DB_PATH

    with open(sample_path) as f:
        samples = json.load(f)

    # Load descriptions for type info
    descriptions = {}
    if descriptions_path.exists():
        with open(descriptions_path) as f:
            desc_data = json.load(f)
        for db_id, db_info in desc_data.get("databases", {}).items():
            for tbl_key, tbl_info in db_info.get("tables", {}).items():
                descriptions[tbl_key] = tbl_info

    # Remove existing DB
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    tables_created = 0
    rows_inserted = 0

    for db_id, tables in samples.items():
        for fqn, rows in tables.items():
            if not rows:
                continue

            sqlite_table = _sanitize_table_name(fqn)
            columns = list(rows[0].keys())

            # Determine column types from descriptions or sample data
            col_types: dict[str, str] = {}
            # Try to match FQN to descriptions (handle partition table names)
            desc_key = None
            for dk in descriptions:
                # Match e.g. GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS to
                # GA360.GOOGLE_ANALYTICS_SAMPLE.GA_SESSIONS_20160801
                base = re.sub(r"_\d{6,8}$", "", fqn)
                if dk == fqn or dk == base:
                    desc_key = dk
                    break

            if desc_key and "columns" in descriptions[desc_key]:
                desc_cols = descriptions[desc_key]["columns"]
                for col_name in columns:
                    col_info = desc_cols.get(col_name, {})
                    if isinstance(col_info, dict):
                        sf_type = col_info.get("type", "TEXT")
                        col_types[col_name] = _sqlite_type(sf_type)
                    else:
                        col_types[col_name] = "TEXT"

            # Fallback: infer from sample values
            for col_name in columns:
                if col_name not in col_types:
                    sample_val = rows[0].get(col_name)
                    if isinstance(sample_val, bool):
                        col_types[col_name] = "INTEGER"
                    elif isinstance(sample_val, int):
                        col_types[col_name] = "INTEGER"
                    elif isinstance(sample_val, float):
                        col_types[col_name] = "REAL"
                    else:
                        col_types[col_name] = "TEXT"

            # CREATE TABLE
            col_defs = ", ".join(
                f'{_quote_col(c)} {col_types.get(c, "TEXT")}'
                for c in columns
            )
            create_sql = f'CREATE TABLE IF NOT EXISTS "{sqlite_table}" ({col_defs})'
            cur.execute(create_sql)

            # Also create a VIEW with the original Snowflake FQN dots replaced,
            # and a view matching just the table name for easier querying
            parts = fqn.split(".")
            if len(parts) == 3:
                short_name = parts[2]  # Just TABLE_NAME
                # Strip date suffix for partition tables
                base_name = re.sub(r"_\d{6,8}$", "", short_name)
                for view_name in {short_name, base_name}:
                    try:
                        cur.execute(
                            f'CREATE VIEW IF NOT EXISTS "{view_name}" '
                            f'AS SELECT * FROM "{sqlite_table}"'
                        )
                    except sqlite3.OperationalError:
                        pass  # View already exists with different def

            # INSERT rows
            placeholders = ", ".join(["?"] * len(columns))
            insert_sql = (
                f'INSERT INTO "{sqlite_table}" '
                f'({", ".join(_quote_col(c) for c in columns)}) '
                f'VALUES ({placeholders})'
            )
            for row in rows:
                values = []
                for col_name in columns:
                    val = row.get(col_name)
                    # Convert dicts/lists to JSON strings (VARIANT columns)
                    if isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    elif isinstance(val, bool):
                        val = int(val)
                    values.append(val)
                cur.execute(insert_sql, values)
                rows_inserted += 1

            tables_created += 1
            log.info("Created %s: %d columns, %d rows", sqlite_table, len(columns), len(rows))

    # Create a metadata table listing all tables and their Snowflake FQNs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _metadata (
            sqlite_table TEXT PRIMARY KEY,
            snowflake_fqn TEXT,
            db_id TEXT,
            schema_name TEXT,
            table_name TEXT,
            row_count INTEGER
        )
    """)
    for db_id, tables in samples.items():
        for fqn, rows in tables.items():
            parts = fqn.split(".")
            cur.execute(
                "INSERT OR REPLACE INTO _metadata VALUES (?, ?, ?, ?, ?, ?)",
                (
                    _sanitize_table_name(fqn),
                    fqn,
                    parts[0] if len(parts) > 0 else "",
                    parts[1] if len(parts) > 1 else "",
                    parts[2] if len(parts) > 2 else "",
                    len(rows),
                ),
            )

    conn.commit()
    conn.close()

    log.info(
        "Mirror database created: %s (%d tables, %d rows)",
        db_path, tables_created, rows_inserted,
    )
    return db_path


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    db_path = build_mirror()

    # Verify
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / 1024:.1f} KB\n")

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != '_metadata' ORDER BY name")
    tables = cur.fetchall()
    print(f"Tables ({len(tables)}):")
    for (name,) in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{name}"')
        count = cur.fetchone()[0]
        cur.execute(f'PRAGMA table_info("{name}")')
        cols = cur.fetchall()
        col_names = [c[1] for c in cols]
        print(f"  {name}: {count} rows, {len(cols)} columns")
        print(f"    {col_names[:8]}{'...' if len(col_names) > 8 else ''}")

    cur.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name")
    views = cur.fetchall()
    print(f"\nViews ({len(views)}):")
    for (name,) in views:
        print(f"  {name}")

    # Test a query
    print("\nSample queries:")
    cur.execute("""
        SELECT "fullVisitorId", "date", "channelGrouping"
        FROM "GA_SESSIONS" LIMIT 2
    """)
    for row in cur.fetchall():
        print(f"  GA_SESSIONS: {row}")

    cur.execute("""
        SELECT "publication_number", "country_code", "kind_code", "filing_date"
        FROM "PATENTS__PATENTS__PUBLICATIONS" LIMIT 2
    """)
    for row in cur.fetchall():
        print(f"  PUBLICATIONS: {row}")

    conn.close()


if __name__ == "__main__":
    main()
