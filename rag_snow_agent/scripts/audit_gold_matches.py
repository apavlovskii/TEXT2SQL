"""Audit: re-score predicted SQLs against ALL gold variants, lenient-but-honest.

For each instance in a run, re-execute the predicted final_sql and compare to every
gold variant CSV under two rules:
  - OFFICIAL : the benchmark's verify_against_gold (condition_cols, ignore_order, tol)
  - LENIENT  : column/order-agnostic value-set match against ANY variant
               (catches right-answer-wrong-arrangement; for upper-bound estimate)
Prints per-instance evidence + counts. No LLM tokens (Snowflake only).

  uv run python -m scripts.audit_gold_matches <experiment_name>
"""
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

from rag_snow_agent.eval.gold_verifier import load_eval_standards, verify_against_gold
from rag_snow_agent.snowflake.executor import SnowflakeExecutor

GOLD = Path("../Spider2/spider2-snow/evaluation_suite/gold")
EXEC_DIR = GOLD / "exec_result"
SPLIT = "../Spider2/spider2-snow/spider2-snow.jsonl"


def _norm_cell(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)):
        return round(float(v), 2)
    s = str(v).strip()
    try:
        return round(float(s), 2)
    except ValueError:
        return s.lower()


def _row_multiset(rows):
    # each row -> sorted tuple of normalized cells (column-order agnostic); then a
    # sorted list of those rows (row-order agnostic)
    out = []
    for r in rows:
        cells = sorted((str(_norm_cell(c)) for c in r))
        out.append(tuple(cells))
    return sorted(out)


def _lenient_match(pred_rows, gold_df) -> bool:
    g = _row_multiset(gold_df.itertuples(index=False, name=None))
    p = _row_multiset(pred_rows)
    if g == p:
        return True
    # answer-contained: every gold value-set row appears somewhere in pred
    pset = set(p)
    return all(gr in pset for gr in g) and len(g) > 0


def main():
    exp = sys.argv[1]
    recs = {json.loads(l)["instance_id"]: json.loads(l)
            for l in open(f"reports/experiments/{exp}/instance_results.jsonl")}
    dbmap = {json.loads(l)["instance_id"]: json.loads(l)["db_id"] for l in open(SPLIT)}
    standards = load_eval_standards(GOLD / "spider2snow_eval.jsonl")

    print(f"Auditing {exp}: {len(recs)} instances\n")
    print(f"{'instance':10}{'rec':5}{'official':9}{'lenient':8}{'pred_shape':12}{'gold_variants':14} note")
    off_pos = len_pos = rec_pos = 0
    flips = []
    for iid, r in recs.items():
        rec_gold = bool(r.get("gold_matched"))
        rec_pos += rec_gold
        sql = r.get("final_sql") or ""
        db = dbmap.get(iid, "")
        variants = sorted(f for f in os.listdir(EXEC_DIR)
                          if re.match(rf'^{re.escape(iid)}(_[a-z])?\.csv$', f))
        # OFFICIAL (re-run, all variants, condition_cols/ignore_order)
        try:
            ex = SnowflakeExecutor(credentials_path="snowflake_credentials.json", db_id=db, sample_rows=10000)
            off = verify_against_gold(iid, sql, db, ex, GOLD, standards)
            er = ex.execute(sql, sample_rows=10000)
            ex.close()
        except Exception as e:
            print(f"{iid:10}{'Y' if rec_gold else 'N':5}{'ERR':9}{'-':8}{'':12}{len(variants):<14} {str(e)[:40]}")
            continue
        official = bool(off.matched)
        pred_rows = er.rows_sample or []
        pshape = f"{len(pred_rows)}x{len(er.column_names or [])}" if er.success else "EXECFAIL"
        # LENIENT vs each variant
        lenient = False
        if er.success and pred_rows:
            for v in variants:
                try:
                    gdf = pd.read_csv(EXEC_DIR / v)
                except Exception:
                    continue
                if _lenient_match(pred_rows, gdf):
                    lenient = True
                    break
        off_pos += official
        len_pos += (official or lenient)
        note = ""
        if (official or lenient) and not rec_gold:
            note = "*** FLIPS TO POSITIVE"
            flips.append(iid)
        print(f"{iid:10}{'Y' if rec_gold else 'N':5}{('Y' if official else 'N'):9}"
              f"{('Y' if lenient else 'N'):8}{pshape:12}{len(variants):<14} {note}")

    n = len(recs)
    print(f"\nrecorded gold_matched : {rec_pos}/{n}")
    print(f"official (re-audited) : {off_pos}/{n}")
    print(f"official OR lenient   : {len_pos}/{n}   (flips: {flips})")


if __name__ == "__main__":
    main()
