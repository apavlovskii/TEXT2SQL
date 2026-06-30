"""Compute Schema Retrieval Rate (SRR) on instances where gold SQL is available.

For each instance: parse gold SQL → extract referenced tables → check if all
are present in our hybrid retrieval (ChromaDB BM25 + embedding, top_k_tables=8).

No LLM tokens consumed. Snowflake connection NOT required.

Usage (from rag_snow_agent/ directory):
    uv run python -m rag_snow_agent.eval.compute_srr \
        --experiments benchmark_run_10 n30_A0_full A0_full rep_idx_treat \
        --gold_sql_dir ../Spider2/spider2-snow/evaluation_suite/gold/sql \
        --split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
        [--exclude_date_sharded]

With --exclude_date_sharded, tables whose name contains a 4-digit year (e.g.
STORMS_2014, _202204, ZCTA5_2017_5YR) are excluded from the missing-table check.
This reflects the deliberate design choice of collapsing date-partitioned shard
tables into a single representative schema card.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

import sqlglot
import sqlglot.expressions as exp

_DATE_SHARD_RE = re.compile(r'\d{4}')


def is_date_sharded(qualified_name: str) -> bool:
    """Return True if the table name component contains a 4-digit year/date token.

    Matches: STORMS_2014, _202204, ZCTA5_2017_5YR, GSOD2015, YEAR._2023, etc.
    Does NOT match: ZIP_CODES, CITIBIKE_TRIPS, CONSTRUCTORS, etc.
    """
    table_part = qualified_name.rsplit(".", 1)[-1]
    return bool(_DATE_SHARD_RE.search(table_part))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def extract_tables_from_sql(sql: str, db_id: str) -> set[str]:
    """Parse SQL and return fully-qualified uppercase table names (DB.SCHEMA.TABLE).

    CTE names are filtered out — they appear as Table nodes but have no schema.
    When DB is absent in the SQL, injects db_id as the catalog.
    """
    tables: set[str] = set()
    try:
        stmts = sqlglot.parse(sql, dialect="snowflake",
                               error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception as e:
        log.warning("sqlglot parse error: %s", e)
        return tables

    # Collect CTE names so we can exclude them from table references
    cte_names: set[str] = set()
    for stmt in stmts:
        if stmt is None:
            continue
        for cte in stmt.find_all(exp.CTE):
            cte_names.add(cte.alias.upper())

    for stmt in stmts:
        if stmt is None:
            continue
        for node in stmt.walk():
            if not isinstance(node, exp.Table):
                continue
            name = (node.name or "").upper().strip('"')
            schema = (node.db or "").upper().strip('"')
            db = (node.catalog or "").upper().strip('"')

            if not name or name in cte_names:
                continue
            # Must have at least a schema to be a real table ref (not a CTE)
            if not schema:
                continue

            if not db:
                db = db_id.upper()

            tables.add(f"{db}.{schema}.{name}")

    return tables


def load_instances(split_jsonl: Path) -> dict[str, dict]:
    """Return {instance_id: {instruction, db_id}} from the benchmark JSONL."""
    instances: dict[str, dict] = {}
    with open(split_jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            instances[d["instance_id"]] = {
                "instruction": d.get("instruction", ""),
                "db_id": d.get("db_id", ""),
            }
    return instances


def compute_srr(
    instance_ids: list[str],
    instances: dict[str, dict],
    gold_sql_dir: Path,
    top_k_tables: int = 8,
    exclude_date_sharded: bool = False,
) -> list[dict]:
    """Run SRR computation. Returns per-instance result dicts."""
    from rag_snow_agent.chroma.chroma_store import ChromaStore
    from rag_snow_agent.retrieval.debug_retrieve import build_schema_slice
    from rag_snow_agent.retrieval.hybrid_retriever import HybridRetriever

    store = ChromaStore()
    collection = store.schema_collection()
    retriever = HybridRetriever(collection)

    results = []
    for iid in instance_ids:
        gold_sql_path = gold_sql_dir / f"{iid}.sql"
        if not gold_sql_path.exists():
            continue

        meta = instances.get(iid)
        if not meta:
            log.warning("No metadata for %s", iid)
            continue

        db_id = meta["db_id"]
        instruction = meta["instruction"]

        # Parse gold SQL
        gold_sql = gold_sql_path.read_text()
        gold_tables = extract_tables_from_sql(gold_sql, db_id)
        if not gold_tables:
            log.warning("%s: no tables extracted from gold SQL — skipping", iid)
            continue

        # Run retriever
        try:
            schema_slice, _, _ = build_schema_slice(
                retriever=retriever,
                query=instruction,
                db_id=db_id,
                top_k_tables=top_k_tables,
                top_k_columns=25,
                max_schema_tokens=99999,  # no token cap — we want full top-k
            )
        except Exception as e:
            log.error("%s: retrieval failed: %s", iid, e)
            results.append({
                "instance_id": iid, "db_id": db_id,
                "gold_tables": sorted(gold_tables),
                "retrieved_tables": [],
                "missing_tables": sorted(gold_tables),
                "recalled": False, "error": str(e),
            })
            continue

        retrieved = {t.qualified_name.upper() for t in schema_slice.tables}
        missing_all = gold_tables - retrieved

        if exclude_date_sharded:
            date_sharded = sorted(t for t in missing_all if is_date_sharded(t))
            missing = missing_all - set(date_sharded)
        else:
            date_sharded = []
            missing = missing_all

        recalled = len(missing) == 0

        log.info(
            "%s: gold=%d retrieved=%d missing=%d %s",
            iid, len(gold_tables), len(retrieved), len(missing),
            "✓" if recalled else "✗ " + str(missing),
        )

        results.append({
            "instance_id": iid,
            "db_id": db_id,
            "gold_tables": sorted(gold_tables),
            "retrieved_tables": sorted(retrieved),
            "missing_tables": sorted(missing),
            "date_sharded_excluded": date_sharded,
            "recalled": recalled,
            "error": None,
        })

    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiments", nargs="+",
                        help="Experiment names under reports/experiments/")
    parser.add_argument("--gold_sql_dir",
                        default="../Spider2/spider2-snow/evaluation_suite/gold/sql")
    parser.add_argument("--split_jsonl",
                        default="../Spider2/spider2-snow/spider2-snow.jsonl")
    parser.add_argument("--top_k_tables", type=int, default=8)
    parser.add_argument(
        "--exclude_date_sharded", action="store_true",
        help="Exclude year/date-sharded tables (e.g. STORMS_2014, _202204) from the "
             "missing-table check. Reflects the design choice of collapsing shard tables.",
    )
    args = parser.parse_args()

    gold_sql_dir = Path(args.gold_sql_dir)
    split_jsonl = Path(args.split_jsonl)
    exp_dir = Path("reports/experiments")

    instances = load_instances(split_jsonl)
    log.info("Loaded %d instance metadata records", len(instances))

    # Collect instance IDs from specified experiments
    all_ids: list[str] = []
    seen: set[str] = set()
    for run in (args.experiments or []):
        p = exp_dir / run / "instance_results.jsonl"
        if not p.exists():
            log.warning("Run not found: %s", run)
            continue
        run_ids = [json.loads(l)["instance_id"] for l in p.open() if l.strip()]
        new = [i for i in run_ids if i not in seen]
        all_ids.extend(new)
        seen.update(new)
        log.info("%s: %d instances (%d new)", run, len(run_ids), len(new))

    if not all_ids:
        log.error("No instances found. Check --experiments.")
        sys.exit(1)

    # Filter to those with gold SQL
    with_gold = [i for i in all_ids if (gold_sql_dir / f"{i}.sql").exists()]
    log.info("Instances with gold SQL: %d / %d", len(with_gold), len(all_ids))

    results = compute_srr(
        with_gold, instances, gold_sql_dir,
        top_k_tables=args.top_k_tables,
        exclude_date_sharded=args.exclude_date_sharded,
    )

    # Summary
    valid = [r for r in results if r["error"] is None]
    recalled = [r for r in valid if r["recalled"]]
    srr = len(recalled) / len(valid) if valid else 0.0

    mode = "excl. date-shards" if args.exclude_date_sharded else "strict"
    print("\n" + "=" * 60)
    print(f"SCHEMA RETRIEVAL RATE (SRR)  top_k={args.top_k_tables}  [{mode}]")
    print("=" * 60)
    print(f"Instances evaluated:  {len(valid)}")
    print(f"Fully recalled:       {len(recalled)}/{len(valid)}  ({100*srr:.1f}%)")
    print(f"Missed at least one:  {len(valid)-len(recalled)}")

    if len(valid) > len(recalled):
        print("\nMisses:")
        for r in valid:
            if not r["recalled"]:
                print(f"  {r['instance_id']:20s}  missing: {r['missing_tables']}")

    # Write JSONL
    out_path = exp_dir / "srr_results.jsonl"
    with open(out_path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print(f"\nPer-instance results: {out_path}")


if __name__ == "__main__":
    main()
