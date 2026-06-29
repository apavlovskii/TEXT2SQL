#!/usr/bin/env bash
# Run raw schema baseline: full DB schema in context, single LLM call, no architecture.
# Compares directly to rep_idx_treat (7/20 = 35%) to measure architecture contribution.

set -euo pipefail
cd "$(dirname "$0")/.."

uv run python -m rag_snow_agent.eval.raw_baseline \
    --split_jsonl ../Spider2/spider2-snow/spider2-snow_rep_runnable.jsonl \
    --credentials snowflake_credentials.json \
    --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
    --model gpt-5.4 \
    --limit 20 \
    --experiment raw_schema_baseline_20
