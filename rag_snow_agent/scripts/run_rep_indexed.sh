#!/usr/bin/env bash
# Representative run on now-indexed conventional DBs. Single arm (full strong config).
# LIMIT controls how many: 20 first, then 35 to finish the rest (resumes, skips done).
#   LIMIT=20 bash scripts/run_rep_indexed.sh   # first batch
#   LIMIT=35 bash scripts/run_rep_indexed.sh   # resume the rest
set -uo pipefail
cd "$(dirname "$0")/.."
LIMIT="${LIMIT:-20}"

uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl ../Spider2/spider2-snow/spider2-snow_rep_runnable.jsonl \
  --credentials snowflake_credentials.json \
  --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --model gpt-5.4 --limit "$LIMIT" \
  --best_of_n 4 --max_repairs 3 --skip_preflight --resume \
  --enable_self_critic --self_critic_max 3 \
  --enable_exploration --enable_planning --exploration_max_probes 6 \
  --experiment rep_idx_treat
rc=$?
echo "rep_idx_treat (limit $LIMIT) -> exit $rc ($(date -u +%FT%TZ))"
[ "$rc" -eq 2 ] && echo "!! QUOTA PAUSE. Re-run to resume." || echo "RUN DONE"
exit $rc
