#!/usr/bin/env bash
# 2-way no-gold A/B on 30 non-geo instances (gpt-5.4):
#   control   = consensus + self-critic (+ always-on dialect/determinism)
#   treatment = control + exploration + planning   (the only delta)
#
# Resumable: both arms run with --resume. If quota is hit, the runner checkpoints
# and exits 2; this script aborts immediately. After topping up tokens, just
# re-run this SAME script — --resume skips completed instances and retries the
# quota-interrupted one. Control fully completes before treatment begins.
set -uo pipefail
cd "$(dirname "$0")/.."

COMMON="--split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials snowflake_credentials.json \
  --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --model gpt-5.4 --limit 30 --exclude_geospatial \
  --best_of_n 4 --max_repairs 3 --skip_preflight --resume \
  --enable_self_critic --self_critic_max 3"

run_arm() {
  local name="$1"; shift
  echo; echo "################ ARM: $name -- $(date -u +%FT%TZ) ################"
  uv run python -m rag_snow_agent.eval.experiment_runner $COMMON --experiment "$name" "$@"
  local rc=$?
  echo "  $name -> exit $rc ($(date -u +%FT%TZ))"
  if [ "$rc" -eq 2 ]; then
    echo "!! QUOTA PAUSE during $name. Checkpoint written. Top up tokens, then re-run this script to resume."
    exit 2
  fi
  return 0
}

run_arm ab30_ctrl_g54
run_arm ab30_treat_g54 --enable_exploration --enable_planning --exploration_max_probes 6

echo; echo "################ A/B COMPLETE -- $(date -u +%FT%TZ) ################"
