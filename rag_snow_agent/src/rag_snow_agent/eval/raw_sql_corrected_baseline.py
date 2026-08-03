"""Raw LLM SQL + deterministic correction layer: full schema in context, single
generation, LLM writes SQL directly (no structured plan/compiler) — then a
lightweight deterministic layer fixes evident, schema-driven errors (bad
column/table references, NUMBER-vs-date-string mismatches, date-sharded table
references) and gives the LLM one repair attempt if validation still fails.

Identical instances, schema context, and Snowflake tips as raw_baseline.py —
the only variable added is the correction layer. Direct, apples-to-apples
comparison point: raw_schema_baseline_20 (no correction layer) and
plan_compiler_baseline (structured plan + full deterministic compiler).

Usage:
    cd rag_snow_agent
    uv run python -m rag_snow_agent.eval.raw_sql_corrected_baseline \
        --split_jsonl ../Spider2/spider2-snow/spider2-snow_rep_runnable.jsonl \
        --credentials snowflake_credentials.json \
        --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
        --model gpt-5.4 \
        --limit 20 \
        --experiment raw_sql_corrected_20
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from rag_snow_agent.eval.raw_baseline import (
    MAX_SCHEMA_TOKENS,
    _SNOWFLAKE_TIPS,
    _call_openai_single,
    _format_full_schema,
    _strip_sql_fences,
)
from rag_snow_agent.prompting.sql_compiler import rewrite_date_sharded_tables
from rag_snow_agent.prompting.sql_correction import (
    build_fix_sql_prompt,
    check_type_mismatches_raw,
    validate_raw_sql,
)
from rag_snow_agent.retrieval.schema_slice import SchemaSlice

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split_jsonl", required=True)
    parser.add_argument("--credentials", required=True)
    parser.add_argument("--eval_gold_dir", required=True)
    parser.add_argument("--model", default="gpt-5.4")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--experiment", default="raw_sql_corrected_20")
    parser.add_argument("--output_dir", default=None)
    parser.add_argument("--max_schema_tokens", type=int, default=MAX_SCHEMA_TOKENS)
    args = parser.parse_args()

    out_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(__file__).parents[3] / "reports" / "experiments" / args.experiment
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "instance_results.jsonl"

    instances = []
    with open(args.split_jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            instances.append(json.loads(line))
            if len(instances) >= args.limit:
                break
    log.info("Loaded %d instances from %s", len(instances), args.split_jsonl)

    from rag_snow_agent.eval.gold_verifier import load_eval_standards, verify_against_gold

    gold_path = Path(args.eval_gold_dir)
    eval_standards = load_eval_standards(gold_path / "spider2snow_eval.jsonl")
    log.info("Loaded %d gold eval standards", len(eval_standards))

    if not Path(args.credentials).exists():
        log.error("Credentials file not found: %s", args.credentials)
        sys.exit(1)

    done_ids: set[str] = set()
    if results_path.exists():
        with open(results_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    done_ids.add(json.loads(line)["instance_id"])
        log.info("Resuming: %d already done", len(done_ids))

    total = len(instances)
    successes = 0
    gold_matches = 0
    validation_failures = 0
    repair_successes = 0
    total_prompt_tokens = 0
    total_completion_tokens = 0

    if results_path.exists():
        with open(results_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                if r.get("gold_matched"):
                    gold_matches += 1
                if r.get("sql_executed"):
                    successes += 1
                if r.get("validation_failed"):
                    validation_failures += 1
                if r.get("repair_succeeded"):
                    repair_successes += 1
                total_prompt_tokens += r.get("prompt_tokens", 0)
                total_completion_tokens += r.get("completion_tokens", 0)

    for i, inst in enumerate(instances, 1):
        instance_id = inst["instance_id"]
        db_id = inst["db_id"]
        instruction = inst["instruction"]
        external_knowledge = inst.get("external_knowledge") or None

        if instance_id in done_ids:
            log.info("[%d/%d] SKIP %s (already done)", i, total, instance_id)
            continue

        log.info("[%d/%d] Processing %s (db=%s)", i, total, instance_id, db_id)

        record: dict = {
            "instance_id": instance_id,
            "db_id": db_id,
            "instruction": instruction,
            "model": args.model,
            "sql_executed": False,
            "execution_error": None,
            "gold_matched": None,
            "final_sql": None,
            "raw_sql": None,
            "validation_failed": False,
            "validation_errors": None,
            "repair_attempted": False,
            "repair_succeeded": False,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "llm_calls": 0,
        }

        try:
            from rag_snow_agent.snowflake.client import connect as sf_connect
            from rag_snow_agent.snowflake.executor import SnowflakeExecutor
            from rag_snow_agent.snowflake.metadata import extract_tables

            conn = sf_connect(args.credentials)
            try:
                log.info("  Fetching full schema for %s ...", db_id)
                tables = extract_tables(conn, db_id)
                schema_text, _ = _format_full_schema(tables, db_id, args.max_schema_tokens)
                schema_slice = SchemaSlice.from_table_info(db_id, tables)
            finally:
                conn.close()

            user_content = f"Database schema:\n{schema_text}\n\nQuestion: {instruction}"
            if external_knowledge and external_knowledge not in ("None", "none", "null", ""):
                ek_path = (
                    Path(external_knowledge)
                    if Path(external_knowledge).is_absolute()
                    else Path(args.split_jsonl).parent / external_knowledge
                )
                if ek_path.exists():
                    ek_text = ek_path.read_text(encoding="utf-8").strip()
                    user_content = f"External knowledge:\n{ek_text}\n\n{user_content}"

            system_content = (
                "You are a Snowflake SQL expert.\n"
                "Given the database schema and a natural-language question, write a single Snowflake SQL query that answers the question.\n"
                "Return ONLY the SQL statement — no markdown, no explanation, no comments.\n\n"
                + _SNOWFLAKE_TIPS
            )
            messages = [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content},
            ]

            log.info("  Calling LLM (%s) ...", args.model)
            raw_response, pt, ct = _call_openai_single(messages, args.model, temperature=0.2)
            record["prompt_tokens"] += pt
            record["completion_tokens"] += ct
            record["llm_calls"] += 1
            total_prompt_tokens += pt
            total_completion_tokens += ct

            sql = _strip_sql_fences(raw_response)
            record["raw_sql"] = sql

            # ── Deterministic correction layer ───────────────────────────
            sql = rewrite_date_sharded_tables(sql, schema_slice)

            id_result = validate_raw_sql(sql, schema_slice)
            type_errors = check_type_mismatches_raw(sql, schema_slice)
            all_errors = list(id_result.errors) + type_errors

            if all_errors:
                record["validation_failed"] = True
                record["validation_errors"] = all_errors
                validation_failures += 1
                log.warning("  Validation failed (%d errors): %s", len(all_errors), all_errors)

                record["repair_attempted"] = True
                fix_messages = build_fix_sql_prompt(sql, schema_text, all_errors)
                fixed_raw, pt, ct = _call_openai_single(fix_messages, args.model, temperature=0.0)
                record["prompt_tokens"] += pt
                record["completion_tokens"] += ct
                record["llm_calls"] += 1
                total_prompt_tokens += pt
                total_completion_tokens += ct

                fixed_sql = rewrite_date_sharded_tables(_strip_sql_fences(fixed_raw), schema_slice)
                fixed_id_result = validate_raw_sql(fixed_sql, schema_slice)
                fixed_type_errors = check_type_mismatches_raw(fixed_sql, schema_slice)
                if not fixed_id_result.errors and not fixed_type_errors:
                    sql = fixed_sql
                    record["repair_succeeded"] = True
                    repair_successes += 1
                    log.info("  Repair: SUCCEEDED")
                else:
                    log.warning(
                        "  Repair: still invalid (%d errors) — keeping original SQL",
                        len(fixed_id_result.errors) + len(fixed_type_errors),
                    )

            record["final_sql"] = sql
            log.info("  Final SQL (%d chars)", len(sql))

            executor = SnowflakeExecutor(
                credentials_path=args.credentials, db_id=db_id, statement_timeout_sec=120,
            )
            try:
                exec_result = executor.execute(sql)
                if exec_result.success:
                    record["sql_executed"] = True
                    successes += 1
                    log.info("  Execution: SUCCESS (%d rows)", exec_result.row_count or 0)
                else:
                    record["execution_error"] = exec_result.error_message
                    log.warning("  Execution: FAILED — %s", exec_result.error_message)

                gold_result = verify_against_gold(
                    instance_id, sql, db_id, executor, args.eval_gold_dir, eval_standards,
                )
                record["gold_matched"] = bool(gold_result.matched)
                if gold_result.matched:
                    gold_matches += 1
                    log.info("  Gold: MATCHED")
                else:
                    log.info("  Gold: MISS (%s)", gold_result.error or "result_mismatch")
            finally:
                executor.close()

        except Exception as exc:
            log.error("  ERROR for %s: %s", instance_id, exc, exc_info=True)
            record["execution_error"] = str(exc)

        with open(results_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        log.info("  Written to %s", results_path)

    print("\n" + "=" * 60)
    print(f"RAW SQL + CORRECTION LAYER — {args.experiment}")
    print("=" * 60)
    print(f"Instances:          {total}")
    print(f"Validation failed:  {validation_failures}/{total}")
    print(f"Repair succeeded:   {repair_successes}/{validation_failures if validation_failures else 0}")
    print(f"SQL executed:       {successes}/{total}")
    print(f"Gold matched:       {gold_matches}/{total} ({100*gold_matches/total:.1f}%)")
    print(f"Prompt tokens:      {total_prompt_tokens:,}")
    print(f"Completion tokens:  {total_completion_tokens:,}")
    print(f"Total tokens:       {total_prompt_tokens + total_completion_tokens:,}")

    summary = {
        "experiment": args.experiment,
        "model": args.model,
        "instances": total,
        "validation_failures": validation_failures,
        "repair_successes": repair_successes,
        "sql_executed": successes,
        "gold_matched": gold_matches,
        "accuracy": round(gold_matches / total, 4) if total else 0,
        "prompt_tokens_total": total_prompt_tokens,
        "completion_tokens_total": total_completion_tokens,
    }
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary written to: {summary_path}")


if __name__ == "__main__":
    main()
