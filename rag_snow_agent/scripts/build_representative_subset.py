"""Build a representative / conventional-schema subset of spider2-snow.

Our default first-30 is accidentally the hardest cluster (all GA360/GA4/PATENTS
nested-analytics). This selects a stratified cross-section of the *conventional*
relational schemas so the number is higher AND defensible (representative, not
cherry-picked-easy). Writes a split JSONL the runner can target directly.

  uv run python -m scripts.build_representative_subset [--per_db 2] [--target 36]
"""
import argparse
import collections
import json
import os
import re
from pathlib import Path

SPLIT = Path("../Spider2/spider2-snow/spider2-snow.jsonl")
GOLD = Path("../Spider2/spider2-snow/evaluation_suite/gold")
OUT = Path("../Spider2/spider2-snow/spider2-snow_representative.jsonl")

# Nested / recursive / wide schemas — the hard tail we explicitly exclude here.
EXCLUDE_DB = {
    "GA360", "GA4", "PATENTS", "PATENTS_GOOGLE", "FIREBASE",
    "NOAA_DATA", "NOAA_DATA_PLUS", "NOAA_GSOD", "NOAA_GLOBAL_FORECAST_SYSTEM",
    "NEW_YORK_NOAA", "CENSUS_BUREAU_ACS_2",
}
_GEO_DOC = re.compile(r"functions_st_|st_within|st_dwithin|st_intersects|st_contains", re.I)
_GEO_DB = re.compile(r"GEO|CITIBIKE|OPENSTREETMAP|NEW_YORK", re.I)


def is_geo(r):
    ek = (r.get("external_knowledge") or "")
    return bool(_GEO_DOC.search(ek)) or bool(_GEO_DB.search(r.get("db_id", "")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per_db", type=int, default=2)
    ap.add_argument("--target", type=int, default=36)
    args = ap.parse_args()

    rows = [json.loads(l) for l in open(SPLIT)]
    has_gold_sql = {f[:-4] for f in os.listdir(GOLD / "sql") if f.endswith(".sql")}

    def needs_ek(r):
        return (r.get("external_knowledge") or "") not in ("", "None", "none", "null")

    # Eligible = conventional schema, not geo
    elig = [r for r in rows if r["db_id"] not in EXCLUDE_DB and not is_geo(r)]
    # Rank within each DB: prefer no-external-knowledge, then has-gold-sql (verifiable)
    def rank(r):
        return (needs_ek(r), r["instance_id"] not in has_gold_sql, r["instance_id"])

    by_db = collections.defaultdict(list)
    for r in sorted(elig, key=rank):
        by_db[r["db_id"]].append(r)

    # Round-robin across DBs (most populous first), up to per_db each, until target.
    dbs = [d for d, _ in collections.Counter(r["db_id"] for r in elig).most_common()]
    selected = []
    for k in range(args.per_db):
        for d in dbs:
            if len(selected) >= args.target:
                break
            if k < len(by_db[d]):
                selected.append(by_db[d][k])
        if len(selected) >= args.target:
            break

    OUT.write_text("".join(json.dumps(r) + "\n" for r in selected))
    mix = collections.Counter(r["db_id"] for r in selected)
    n_ek = sum(needs_ek(r) for r in selected)
    n_gsql = sum(r["instance_id"] in has_gold_sql for r in selected)
    print(f"Eligible conventional/non-geo instances: {len(elig)} across {len(by_db)} DBs")
    print(f"Selected: {len(selected)} | needs_external_knowledge: {n_ek} | has_open_gold_sql: {n_gsql}")
    print(f"Wrote: {OUT}\n")
    print("DB mix:", dict(mix))
    print(f"\n{'instance':12}{'db':26}{'ext_know':10}{'gold_sql'}")
    for r in selected:
        print(f"{r['instance_id']:12}{r['db_id'][:25]:26}{('Y' if needs_ek(r) else 'N'):10}"
              f"{'Y' if r['instance_id'] in has_gold_sql else 'N'}")


if __name__ == "__main__":
    main()
