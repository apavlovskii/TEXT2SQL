"""Pure LLM baseline: full schema in context, single generation, no architectural tricks.

Reads the same 20 instances as rep_idx_treat (first 20 from spider2-snow_rep_runnable.jsonl),
fetches the complete Snowflake INFORMATION_SCHEMA for each database, and calls the LLM once
to generate SQL. No RAG retrieval, no repair, no Best-of-N, no exploration, no planning.

Usage:
    cd rag_snow_agent
    uv run python -m rag_snow_agent.eval.raw_baseline \
        --split_jsonl ../Spider2/spider2-snow/spider2-snow_rep_runnable.jsonl \
        --credentials snowflake_credentials.json \
        --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
        --model gpt-5.4 \
        --limit 20 \
        --experiment raw_schema_baseline_20
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Schema formatting ─────────────────────────────────────────────────────────

MAX_SCHEMA_TOKENS = 50_000  # hard cap — truncate if schema exceeds this

_SNOWFLAKE_TIPS = """\
Snowflake SQL rules:
- Use DATE_TRUNC('MONTH', col) for monthly aggregation.
- Use TRY_TO_DATE / TRY_TO_NUMBER for safe casting.
- String comparison is case-sensitive; use ILIKE for case-insensitive.
- Use :: for casting (e.g. col::DATE).
- Prefer CTEs (WITH ... AS) over nested subqueries.
- Do NOT use LIMIT without ORDER BY.
- ALWAYS double-quote mixed-case column names: "fullVisitorId", "trafficSource".
- For VARIANT/ARRAY columns, use LATERAL FLATTEN:
  SELECT f.value:"field"::STRING FROM table, LATERAL FLATTEN(input => table."col") f
- Snowflake treats unquoted identifiers as UPPERCASE.
- GA360 revenue fields are stored multiplied by 10^6 — divide by 1000000 for USD.
- ORDER BY with a secondary sort key to break ties deterministically.
- NULLS LAST in ORDER BY.
- If unsure of exact string values, use ILIKE '%keyword%'."""


def _format_full_schema(tables, db_id: str, max_tokens: int = MAX_SCHEMA_TOKENS) -> tuple[str, bool]:
    """Format all TableInfo objects as text. Returns (text, was_truncated)."""
    try:
        import tiktoken
        enc = tiktoken.get_encoding("cl100k_base")
        def tok(t): return len(enc.encode(t))
    except ImportError:
        def tok(t): return len(t) // 4  # rough fallback

    lines: list[str] = [f"-- Database: {db_id}"]
    total_tokens = tok(lines[0])
    truncated = False

    for table in tables:
        table_header = f"TABLE {table.qualified_name}"
        if table.comment:
            table_header += f"  -- {table.comment}"
        header_tokens = tok(table_header)
        if total_tokens + header_tokens > max_tokens:
            truncated = True
            break
        lines.append(table_header)
        total_tokens += header_tokens

        for col in table.columns:
            col_line = f'  "{col.column_name}" {col.data_type}'
            if col.comment:
                col_line += f"  -- {col.comment}"
            col_tokens = tok(col_line)
            if total_tokens + col_tokens > max_tokens:
                truncated = True
                break
            lines.append(col_line)
            total_tokens += col_tokens
        if truncated:
            break

    return "\n".join(lines), truncated


def _strip_sql_fences(text: str) -> str:
    """Remove markdown SQL fences and return the raw SQL."""
    text = text.strip()
    # ```sql ... ```
    m = re.search(r"```(?:sql)?\s*([\s\S]+?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # ` ... ` (single backtick)
    m = re.search(r"`(SELECT[\s\S]+?)`", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text


def _call_openai_single(messages: list[dict], model: str, temperature: float = 0.2) -> tuple[str, int, int]:
    """Call OpenAI once. Returns (content, prompt_tokens, completion_tokens)."""
    from openai import OpenAI
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    kwargs: dict = {"model": model, "messages": messages}
    if temperature is not None:
        kwargs["temperature"] = temperature

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content or ""
            usage = resp.usage
            pt = usage.prompt_tokens if usage else 0
            ct = usage.completion_tokens if usage else 0
            return content, pt, ct
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if attempt < 2 and status in (429, 500, 502, 503):
                delay = 5 * (2 ** attempt)
                log.warning("OpenAI %s (attempt %d/3), retrying in %ds", status, attempt + 1, delay)
                time.sleep(delay)
                continue
            raise


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split_jsonl", required=True, help="Path to spider2-snow_rep_runnable.jsonl")
    parser.add_argument("--credentials", required=True, help="Snowflake credentials JSON")
    parser.add_argument("--eval_gold_dir", required=True, help="Gold dir for post-hoc scoring")
    parser.add_argument("--model", default="gpt-5.4", help="OpenAI model name")
    parser.add_argument("--limit", type=int, default=20, help="Number of instances to run")
    parser.add_argument("--experiment", default="raw_schema_baseline_20", help="Output experiment name")
    parser.add_argument("--output_dir", default=None, help="Output directory (default: reports/experiments/<experiment>)")
    parser.add_argument("--max_schema_tokens", type=int, default=MAX_SCHEMA_TOKENS)
    args = parser.parse_args()

    # Output dir — walk up from src/rag_snow_agent/eval/ to rag_snow_agent/
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        # __file__ = rag_snow_agent/src/rag_snow_agent/eval/raw_baseline.py
        # parents[3] = rag_snow_agent/
        out_dir = Path(__file__).parents[3] / "reports" / "experiments" / args.experiment
    out_dir.mkdir(parents=True, exist_ok=True)
    results_path = out_dir / "instance_results.jsonl"

    # Load instances
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

    # Load gold eval standards
    from rag_snow_agent.eval.gold_verifier import load_eval_standards, verify_against_gold
    gold_path = Path(args.eval_gold_dir)
    eval_standards = load_eval_standards(gold_path / "spider2snow_eval.jsonl")
    log.info("Loaded %d gold eval standards", len(eval_standards))

    # Validate credentials file exists
    if not Path(args.credentials).exists():
        log.error("Credentials file not found: %s", args.credentials)
        sys.exit(1)

    # Resume: skip already-done instances
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
    total_prompt_tokens = 0
    total_completion_tokens = 0

    # Tally already-done from the results file
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
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "schema_tables": 0,
            "schema_columns": 0,
            "schema_truncated": False,
        }

        try:
            from rag_snow_agent.snowflake.client import connect as sf_connect
            from rag_snow_agent.snowflake.executor import SnowflakeExecutor
            from rag_snow_agent.snowflake.metadata import extract_tables

            # Connect to Snowflake using the same helper as the rest of the system
            conn = sf_connect(args.credentials)

            try:
                log.info("  Fetching full schema for %s ...", db_id)
                tables = extract_tables(conn, db_id)
                record["schema_tables"] = len(tables)
                record["schema_columns"] = sum(len(t.columns) for t in tables)
                log.info("  Schema: %d tables, %d columns", record["schema_tables"], record["schema_columns"])

                schema_text, was_truncated = _format_full_schema(tables, db_id, args.max_schema_tokens)
                record["schema_truncated"] = was_truncated
                if was_truncated:
                    log.warning("  Schema TRUNCATED for %s (exceeded %d tokens)", db_id, args.max_schema_tokens)
            finally:
                conn.close()

            # Build prompt
            user_content = f"Database schema:\n{schema_text}\n\nQuestion: {instruction}"
            if external_knowledge and external_knowledge not in ("None", "none", "null", ""):
                ek_path = Path(external_knowledge) if Path(external_knowledge).is_absolute() else Path(args.split_jsonl).parent / external_knowledge
                if ek_path.exists():
                    ek_text = ek_path.read_text(encoding="utf-8").strip()
                    user_content = f"External knowledge:\n{ek_text}\n\n{user_content}"
                    log.info("  Injected external knowledge (%d chars)", len(ek_text))

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
            record["prompt_tokens"] = pt
            record["completion_tokens"] = ct
            total_prompt_tokens += pt
            total_completion_tokens += ct
            log.info("  LLM usage: prompt=%d completion=%d", pt, ct)

            sql = _strip_sql_fences(raw_response)
            record["final_sql"] = sql
            log.info("  Generated SQL (%d chars)", len(sql))

            # Execute SQL
            executor = SnowflakeExecutor(
                credentials_path=args.credentials,
                db_id=db_id,
                statement_timeout_sec=120,
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

                # Post-hoc gold scoring
                gold_result = verify_against_gold(
                    instance_id, sql, db_id, executor,
                    args.eval_gold_dir, eval_standards,
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

        # Write record
        with open(results_path, "a") as f:
            f.write(json.dumps(record) + "\n")
        log.info("  Written to %s", results_path)

    # Summary
    done_count = len(done_ids) + (total - len(done_ids))  # all processed
    print("\n" + "=" * 60)
    print(f"RAW SCHEMA BASELINE — {args.experiment}")
    print("=" * 60)
    print(f"Instances:          {total}")
    print(f"SQL executed:       {successes}/{total}")
    print(f"Gold matched:       {gold_matches}/{total} ({100*gold_matches/total:.1f}%)")
    print(f"Prompt tokens:      {total_prompt_tokens:,}")
    print(f"Completion tokens:  {total_completion_tokens:,}")
    print(f"Total tokens:       {total_prompt_tokens + total_completion_tokens:,}")
    if total > 0:
        avg_prompt = total_prompt_tokens / total
        avg_completion = total_completion_tokens / total
        print(f"Avg prompt tokens:  {avg_prompt:,.0f}")
        print(f"Avg completion tok: {avg_completion:,.0f}")
    print(f"\nResults written to: {results_path}")

    # Write summary JSON
    summary = {
        "experiment": args.experiment,
        "model": args.model,
        "instances": total,
        "sql_executed": successes,
        "gold_matched": gold_matches,
        "accuracy": round(gold_matches / total, 4) if total else 0,
        "prompt_tokens_total": total_prompt_tokens,
        "completion_tokens_total": total_completion_tokens,
        "avg_prompt_tokens": round(total_prompt_tokens / total, 1) if total else 0,
        "avg_completion_tokens": round(total_completion_tokens / total, 1) if total else 0,
    }
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"Summary written to: {summary_path}")


if __name__ == "__main__":
    main()
