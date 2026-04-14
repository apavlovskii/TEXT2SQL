"""Profile tables by extracting 100 rows and generating LLM-based descriptions.

Connects to Snowflake, extracts sample data, then uses the LLM to generate
natural language descriptions for each table and column. Outputs to
table_column_descriptions.json and optionally enriches ChromaDB.

For partitioned tables (GA360/GA4), only one representative partition is sampled.

Usage:
    cd rag_snow_agent
    uv run python scripts/profile_data.py \
        --credentials ./snowflake_credentials.json \
        --db_ids GA360 GA4 PATENTS PATENTS_GOOGLE \
        --output data/table_column_descriptions.json \
        --enrich
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

SAMPLE_SIZE = 100
PARTITIONED_DBS = {"GA4", "GA360"}
_DATE_SUFFIX_RE = re.compile(r"^(.+?)_?\d{6,8}$")

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


def _connect(credentials_path: str):
    creds = json.loads(Path(credentials_path).read_text())
    return snowflake.connector.connect(**creds)


def _list_tables(conn, db_id: str) -> list[dict]:
    """Return tables with column info for a database."""
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {db_id}")

    cur.execute(
        "SELECT table_schema, table_name, table_type, row_count, comment "
        "FROM information_schema.tables "
        "WHERE table_schema != 'INFORMATION_SCHEMA' "
        "ORDER BY table_schema, table_name"
    )
    tables = [
        {"schema": r[0], "table": r[1], "type": r[2], "row_count": r[3], "comment": r[4]}
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT table_schema, table_name, column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema != 'INFORMATION_SCHEMA' "
        "ORDER BY table_schema, table_name, ordinal_position"
    )
    cols_by_table: dict[tuple, list[dict]] = defaultdict(list)
    for r in cur.fetchall():
        cols_by_table[(r[0], r[1])].append({"name": r[2], "type": r[3]})

    for t in tables:
        t["columns"] = cols_by_table.get((t["schema"], t["table"]), [])
        t["columns_sig"] = "|".join(f"{c['name']}:{c['type']}" for c in t["columns"])

    cur.close()
    return tables


def _pick_representative_tables(db_id: str, tables: list[dict]) -> list[dict]:
    """Deduplicate partitioned tables, keeping the latest partition."""
    if db_id not in PARTITIONED_DBS:
        return tables

    groups: dict[str, list[dict]] = defaultdict(list)
    non_partition = []
    for t in tables:
        m = _DATE_SUFFIX_RE.match(t["table"])
        if m:
            base = m.group(1).rstrip("_")
            sig = f"{t['schema']}||{base}"
            groups[sig].append(t)
        else:
            non_partition.append(t)

    result = list(non_partition)
    for sig, group in groups.items():
        if len(group) >= 3:
            # Pick the latest partition
            group.sort(key=lambda x: x["table"], reverse=True)
            rep = group[0]
            base = _DATE_SUFFIX_RE.match(rep["table"]).group(1).rstrip("_")
            rep["_partition_base"] = base
            rep["_partition_count"] = len(group)
            result.append(rep)
        else:
            result.extend(group)

    log.info("%s: %d tables -> %d after partition dedup", db_id, len(tables), len(result))
    return result


_LARGE_TABLE_THRESHOLD = 1_000_000  # Use SAMPLE instead of LIMIT for tables above this


def _extract_sample(conn, db_id: str, schema: str, table: str, limit: int = SAMPLE_SIZE, row_count: int | None = None) -> list[dict]:
    """Extract sample rows from a table. Uses SAMPLE for large tables to avoid full scans."""
    fqn = f'"{db_id}"."{schema}"."{table}"'
    cur = conn.cursor(snowflake.connector.DictCursor)
    try:
        if row_count and row_count > _LARGE_TABLE_THRESHOLD:
            cur.execute(f"SELECT * FROM {fqn} TABLESAMPLE BERNOULLI (0.01) LIMIT {limit}")
        else:
            cur.execute(f"SELECT * FROM {fqn} LIMIT {limit}")
        rows = cur.fetchall()
        clean = []
        for row in rows:
            r = {}
            for k, v in row.items():
                try:
                    json.dumps(v)
                    r[k] = v
                except (TypeError, ValueError):
                    r[k] = str(v)
            clean.append(r)
        return clean
    except Exception as e:
        log.warning("Failed to sample %s.%s.%s: %s", db_id, schema, table, e)
        return []
    finally:
        cur.close()


def _profile_column(col_name: str, col_type: str, values: list) -> str:
    """Generate a column profile string from sample values."""
    non_null = [v for v in values if v is not None]
    null_count = len(values) - len(non_null)
    total = len(values)

    parts = [f"type={col_type}"]
    if null_count > 0:
        parts.append(f"nulls={null_count}/{total}")

    if not non_null:
        parts.append("all null")
        return ", ".join(parts)

    # For VARIANT columns, show structure summary
    if col_type in ("VARIANT", "OBJECT", "ARRAY"):
        sample_str = str(non_null[0])[:200]
        if sample_str.startswith("["):
            parts.append("array")
        elif sample_str.startswith("{"):
            parts.append("object")
            try:
                obj = json.loads(sample_str) if isinstance(non_null[0], str) else non_null[0]
                if isinstance(obj, dict):
                    parts.append(f"keys: {list(obj.keys())[:8]}")
            except (json.JSONDecodeError, TypeError):
                pass
        return ", ".join(parts)

    # For scalar types, show stats
    unique = set(str(v) for v in non_null)
    parts.append(f"unique={len(unique)}/{len(non_null)}")

    # Show sample values
    samples = sorted(unique)[:5]
    sample_preview = [s[:40] for s in samples]
    parts.append(f"samples: {sample_preview}")

    # Numeric stats
    if col_type in ("NUMBER", "INT", "INTEGER", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "BIGINT"):
        try:
            nums = [float(v) for v in non_null if v is not None]
            if nums:
                parts.append(f"min={min(nums)}, max={max(nums)}")
        except (ValueError, TypeError):
            pass

    return ", ".join(parts)


def _generate_descriptions_with_llm(
    db_id: str,
    table_info: dict,
    sample_rows: list[dict],
    model: str,
) -> dict:
    """Use LLM to generate table and column descriptions from profiled data."""
    from rag_snow_agent.agent.llm_client import call_llm

    columns = table_info["columns"]
    table_name = table_info["table"]
    schema_name = table_info["schema"]
    fqn = f"{db_id}.{schema_name}.{table_name}"

    # Build column profiles
    col_profiles = {}
    for col in columns:
        col_name = col["name"]
        values = [row.get(col_name) for row in sample_rows]
        col_profiles[col_name] = _profile_column(col_name, col["type"], values)

    profile_text = "\n".join(f"  {name}: {profile}" for name, profile in col_profiles.items())

    # Show a few sample rows compactly
    sample_text = ""
    for i, row in enumerate(sample_rows[:3]):
        compact = {k: (str(v)[:60] if v is not None else None) for k, v in row.items()}
        sample_text += f"  Row {i+1}: {json.dumps(compact, default=str)}\n"

    partition_note = ""
    if table_info.get("_partition_base"):
        partition_note = (
            f"\nThis is a partitioned table: {table_info['_partition_count']} daily tables "
            f"named {table_info['_partition_base']}_YYYYMMDD."
        )

    prompt = f"""Analyze this database table and generate descriptions.

Table: {fqn}{partition_note}
Row count: {table_info.get('row_count', 'unknown')}
Columns ({len(columns)}):
{profile_text}

Sample rows:
{sample_text}

Generate a JSON object with:
1. "table_description": A 1-2 sentence description of what this table contains and its purpose.
2. "columns": An object where each key is a column name and the value is a concise description (1 sentence) explaining what the column contains, its format, and how to use it in SQL. For VARIANT/ARRAY columns, describe whether it needs LATERAL FLATTEN or colon access, and list key nested fields.

Return ONLY valid JSON, no markdown fences."""

    messages = [
        {"role": "system", "content": "You are a database documentation expert. Generate precise, factual descriptions based on the data profiles provided. Focus on what helps an LLM generate correct SQL."},
        {"role": "user", "content": prompt},
    ]

    try:
        raw = call_llm(messages, model=model, temperature=0.0)
        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        result = json.loads(raw)
        return result
    except Exception as exc:
        log.error("LLM description generation failed for %s: %s", fqn, exc)
        return {
            "table_description": f"Table {fqn}",
            "columns": {c["name"]: f"{c['name']} ({c['type']})" for c in columns},
        }


def profile_database(
    conn,
    db_id: str,
    model: str = "gpt-4o-mini",
) -> dict:
    """Profile all tables in a database and return descriptions dict."""
    log.info("Profiling database: %s", db_id)

    all_tables = _list_tables(conn, db_id)
    tables_to_profile = _pick_representative_tables(db_id, all_tables)

    db_descriptions = {
        "description": "",
        "tables": {},
    }

    table_summaries = []

    for t in tables_to_profile:
        fqn = f"{db_id}.{t['schema']}.{t['table']}"
        log.info("  Extracting %d rows from %s ...", SAMPLE_SIZE, fqn)

        sample_rows = _extract_sample(conn, db_id, t["schema"], t["table"], SAMPLE_SIZE, row_count=t.get("row_count"))
        if not sample_rows:
            log.warning("  No data from %s, skipping", fqn)
            continue

        log.info("  Generating descriptions with LLM (%s) ...", model)
        descriptions = _generate_descriptions_with_llm(db_id, t, sample_rows, model)

        table_desc = descriptions.get("table_description", "")
        col_descs = descriptions.get("columns", {})

        # Build the table entry matching existing table_column_descriptions.json format
        # Use the partition base name if applicable
        if t.get("_partition_base"):
            table_key = f"{db_id}.{t['schema']}.{t['_partition_base']}"
            partition_info = (
                f"Daily partitioned as {t['_partition_base']}_YYYYMMDD "
                f"({t['_partition_count']} tables). "
                f"Filter with WHERE \"date\" >= 'YYYYMMDD'."
            )
        else:
            table_key = fqn
            partition_info = None

        table_entry = {
            "description": table_desc,
            "columns": {},
        }
        if partition_info:
            table_entry["partition_info"] = partition_info

        for col in t["columns"]:
            col_name = col["name"]
            col_desc = col_descs.get(col_name, f"{col_name} ({col['type']})")
            col_entry = {
                "type": col["type"],
                "description": col_desc,
            }
            # Mark VARIANT columns
            if col["type"] in ("VARIANT", "OBJECT", "ARRAY"):
                col_entry["variant_kind"] = "ARRAY"  # Default, can be refined
            table_entry["columns"][col_name] = col_entry

        db_descriptions["tables"][table_key] = table_entry
        table_summaries.append(f"{table_key}: {table_desc}")
        log.info("  Done: %s — %d column descriptions", table_key, len(col_descs))

    # Generate database-level description
    if table_summaries:
        db_descriptions["description"] = f"Database with {len(table_summaries)} tables: " + "; ".join(table_summaries[:5])

    return db_descriptions


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--credentials", default="./snowflake_credentials.json")
    parser.add_argument("--db_ids", nargs="+", default=["GA360", "GA4", "PATENTS", "PATENTS_GOOGLE"])
    parser.add_argument("--output", default="data/table_column_descriptions.json")
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--enrich", action="store_true", help="Also enrich ChromaDB after generating descriptions")
    parser.add_argument("--chroma_dir", default=".chroma/")
    parser.add_argument("--merge", action="store_true", help="Merge with existing descriptions instead of overwriting")
    args = parser.parse_args()

    conn = _connect(args.credentials)

    # Load existing descriptions if merging
    output_path = Path(args.output)
    existing = {"databases": {}}
    if args.merge and output_path.exists():
        with open(output_path) as f:
            existing = json.load(f)
        log.info("Loaded existing descriptions from %s", output_path)

    try:
        for db_id in args.db_ids:
            db_desc = profile_database(conn, db_id, model=args.model)
            existing["databases"][db_id] = db_desc
            log.info("Profiled %s: %d tables", db_id, len(db_desc["tables"]))
    finally:
        conn.close()

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(existing, indent=2, default=str) + "\n")
    log.info("Wrote descriptions to %s", output_path)

    # Optionally enrich ChromaDB
    if args.enrich:
        log.info("Enriching ChromaDB...")
        from enrich_descriptions import enrich
        counts = enrich(chroma_dir=args.chroma_dir, descriptions_path=output_path)
        log.info("Enrichment complete: %s", counts)


if __name__ == "__main__":
    main()
