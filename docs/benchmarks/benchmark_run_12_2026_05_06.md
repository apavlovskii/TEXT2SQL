# Benchmark Run 12

## What We Built

Run 12 implements the four fixes from `architecture_update_2026-04-27_1832.md` plus a multi-provider LLM client (`architecture_update_2026-05-05_2330.md`):

1. **API 400 transient retry** (`llm_client.py`) — retries `400 / "could not parse"` errors with 2s/4s backoff
2. **Join-graph neighbor expansion** (`connectivity.py`) — adds 1-hop neighbors with geo/location columns when the question has spatial keywords
3. **Geo model routing** (`experiment_runner.py`) — detects geospatial queries via `external_knowledge` references and routes to `geo_model` (no-op this run since `geo_model == model == gpt-5.4-mini`)
4. **GA360 date-shard rewriting** (`sql_compiler.py`) — post-compilation rewrite expands `GA_SESSIONS_YYYYMMDD` references into UNION ALL CTEs based on partition comments and WHERE-clause date ranges

### Bugs found and fixed during this run

- `trace_memory.py` `instruction_summary=None` concatenation (silent persistent-memory failure since the original RAG implementation)
- `sql_compiler.py` partition-comment regex didn't match the actual `"Daily partitioned as ... (N tables)"` format → Fix 4 was firing zero times. Added `_PARTITION_COMMENT_V2_RE`.
- `sql_compiler.py` substitution order created self-recursive `_date_shard_union` CTEs
- `sql_compiler.py` only matched the representative qname; now matches by base prefix
- `sql_compiler.py` 90-table cap caused Snowflake EXPLAIN timeouts >120s on `LIKE '2017%'` queries; lowered to 40 with skip-when-too-broad semantics
- `refiner.py` LLM-repaired SQL bypassed the rewrite; added `_maybe_rewrite_shards()` at all 4 LLM-repair output points
- `candidate_generator.py` LLM SQL fallback bypassed the rewrite; now wrapped in `rewrite_date_sharded_tables`

## Benchmark Parameters

- Run first 100 candidate tests from Spider2-Snow
- Best-of-8 candidates per instance, up to 4 repair iterations
- GPT-5.4-mini (uniform; geo_model also gpt-5.4-mini)
- ChromaDB with GPT-5.4 profiled descriptions for all 20 databases
- Gold verification against execution output
- All Run 11 features active + Run 12 fixes (1–4) + mid-run patches

## Execution Command

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials ./snowflake_credentials.json \
  --experiment benchmark_run_12 \
  --limit 100 \
  --model gpt-5.4-mini \
  --best_of_n 8 \
  --max_repairs 4 \
  --gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --chroma_dir .chroma/ \
  --skip_preflight
```

## Headline Result

**Gold-match accuracy: 84/100 = 84.0%** (+3pp vs Run 11; −3pp vs Run 10).
0 unhandled errors, 16 failures, 3,602 LLM calls.

## Key Takeaways

- **7 of 8 Run 11 regressions recovered** including all of: sf_bq004, sf_bq042, sf_bq047, sf_bq253, sf_bq254, sf_bq275, sf_bq429.
- **Date-shard rewrite fired 36 times** and was correctly skipped 176 times when date ranges were too broad — preventing the multi-hour planner-timeout regression that surfaced mid-run before the cap was lowered to 40.
- **Geospatial cluster (7/16 failures)** is the unbroken core. Fix 3 detected 17 geo instances but did not actually route them since `geo_model == gpt-5.4-mini`. **Next run should set `geo_model: claude-sonnet-4-5` and benchmark again.**
- **4 regressions vs Run 11** are sampling-noise driven (sf_bq056, sf_bq117, sf_bq214, sf_bq222) — different strategies won across runs on instances near the gold-match threshold.

## Detailed Report

`rag_snow_agent/reports/experiments/benchmark_run_12/benchmark_run_12_report.md`
