"""Extract 5 sample records from each Snowflake table used in a benchmark run.

Usage:
    python -m rag_snow_agent.scripts.extract_sample_records \
        --credentials snowflake_credentials.json \
        --instances rag_snow_agent/reports/experiments/benchmark_run_6/instance_results.jsonl \
        --spider2 Spider2/spider2-snow/spider2-snow.jsonl \
        --output rag_snow_agent/data/sample_records.json

For GA4/GA360 databases, partitioned tables sharing the same column signature
are deduplicated — only one representative partition is sampled.
For PATENTS/PATENTS_GOOGLE, every individual table is sampled.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

PARTITIONED_DBS = {"GA4", "GA360"}
SAMPLE_SIZE = 5


def _connect(credentials_path: str) -> snowflake.connector.SnowflakeConnection:
    creds = json.loads(Path(credentials_path).read_text())
    return snowflake.connector.connect(**creds)


def _get_db_ids(instances_path: str, spider2_path: str) -> set[str]:
    """Return the set of db_id values used in the benchmark run."""
    instance_ids: set[str] = set()
    with open(instances_path) as f:
        for line in f:
            instance_ids.add(json.loads(line)["instance_id"])

    db_ids: set[str] = set()
    with open(spider2_path) as f:
        for line in f:
            d = json.loads(line)
            if d["instance_id"] in instance_ids:
                db_ids.add(d["db_id"])
    return db_ids


def _list_tables(
    conn: snowflake.connector.SnowflakeConnection, db_id: str
) -> list[dict]:
    """Return list of {schema, table, columns} for all tables in a database."""
    cur = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {db_id}")

        # Get tables
        cur.execute(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema != 'INFORMATION_SCHEMA' "
            "ORDER BY table_schema, table_name"
        )
        tables = [{"schema": r[0], "table": r[1]} for r in cur.fetchall()]

        # Get columns per table for dedup signature
        cur.execute(
            "SELECT table_schema, table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema != 'INFORMATION_SCHEMA' "
            "ORDER BY table_schema, table_name, ordinal_position"
        )
        col_map: dict[tuple[str, str], list[str]] = defaultdict(list)
        for r in cur.fetchall():
            col_map[(r[0], r[1])].append(f"{r[2]}:{r[3]}")

        for t in tables:
            t["columns_sig"] = "|".join(col_map.get((t["schema"], t["table"]), []))

        return tables
    finally:
        cur.close()


def _pick_tables(db_id: str, tables: list[dict]) -> list[dict]:
    """Deduplicate partitioned tables for GA4/GA360; keep all for others."""
    if db_id not in PARTITIONED_DBS:
        return tables

    seen_sigs: dict[str, dict] = {}
    for t in tables:
        sig = f"{t['schema']}||{t['columns_sig']}"
        if sig not in seen_sigs:
            seen_sigs[sig] = t
    deduped = list(seen_sigs.values())
    log.info(
        "%s: %d tables -> %d after partition dedup", db_id, len(tables), len(deduped)
    )
    return deduped


def _sample_table(
    conn: snowflake.connector.SnowflakeConnection,
    db_id: str,
    schema: str,
    table: str,
) -> list[dict]:
    """Fetch SAMPLE_SIZE rows from a table, returning list of dicts."""
    fqn = f"{db_id}.{schema}.{table}"
    cur = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(f"SELECT * FROM {fqn} LIMIT {SAMPLE_SIZE}")
        rows = cur.fetchall()
        # Convert non-serializable types to strings
        clean_rows = []
        for row in rows:
            clean = {}
            for k, v in row.items():
                try:
                    json.dumps(v)
                    clean[k] = v
                except (TypeError, ValueError):
                    clean[k] = str(v)
            clean_rows.append(clean)
        return clean_rows
    except Exception as e:
        log.warning("Failed to sample %s: %s", fqn, e)
        return []
    finally:
        cur.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--credentials",
        default="snowflake_credentials.json",
        help="Path to Snowflake credentials JSON",
    )
    parser.add_argument(
        "--instances",
        required=True,
        help="Path to instance_results.jsonl from the benchmark run",
    )
    parser.add_argument(
        "--spider2",
        default="Spider2/spider2-snow/spider2-snow.jsonl",
        help="Path to spider2-snow.jsonl",
    )
    parser.add_argument(
        "--output",
        default="rag_snow_agent/data/sample_records.json",
        help="Output JSON file path",
    )
    args = parser.parse_args()

    db_ids = _get_db_ids(args.instances, args.spider2)
    log.info("Databases to sample: %s", sorted(db_ids))

    conn = _connect(args.credentials)
    try:
        result: dict[str, dict[str, list[dict]]] = {}

        for db_id in sorted(db_ids):
            log.info("Processing database: %s", db_id)
            all_tables = _list_tables(conn, db_id)
            tables_to_sample = _pick_tables(db_id, all_tables)

            db_result: dict[str, list[dict]] = {}
            for t in tables_to_sample:
                fqn = f"{db_id}.{t['schema']}.{t['table']}"
                log.info("  Sampling %s ...", fqn)
                rows = _sample_table(conn, db_id, t["schema"], t["table"])
                if rows:
                    db_result[fqn] = rows
                else:
                    log.info("  -> no rows or error, skipping")

            result[db_id] = db_result
            log.info(
                "%s: sampled %d tables, %d total rows",
                db_id,
                len(db_result),
                sum(len(v) for v in db_result.values()),
            )

        # Write output
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2, default=str))
        log.info("Wrote %s", out_path)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
