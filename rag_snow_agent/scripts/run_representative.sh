#!/usr/bin/env bash
# Run the representative/conventional-schema subset (36 instances, 36 DBs) on gpt-5.4,
# no-gold. Treatment arm runs FIRST so the headline number completes even if quota
# pauses; control second for the A/B. Resumable: re-run after topping up tokens.
set -uo pipefail
cd "$(dirname "$0")/.."

SPLIT="../Spider2/spider2-snow/spider2-snow_representative.jsonl"
COMMON="--split_jsonl $SPLIT \
  --credentials snowflake_credentials.json \
  --eval_gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --model gpt-5.4 --limit 36 \
  --best_of_n 4 --max_repairs 3 --skip_preflight --resume \
  --enable_self_critic --self_critic_max 3"

run_arm() {
  local name="$1"; shift
  echo; echo "################ ARM: $name -- $(date -u +%FT%TZ) ################"
  uv run python -m rag_snow_agent.eval.experiment_runner $COMMON --experiment "$name" "$@"
  local rc=$?
  echo "  $name -> exit $rc ($(date -u +%FT%TZ))"
  if [ "$rc" -eq 2 ]; then
    echo "!! QUOTA PAUSE during $name. Top up tokens, then re-run this script to resume."
    exit 2
  fi
  return 0
}

# Single arm: full strong config (headline capability number). Control omitted by
# design — an absolute accuracy number needs no counterfactual, and A/B at n=36
# can't reach significance, so it isn't worth 2x the tokens.
run_arm rep_treat_g54 --enable_exploration --enable_planning --exploration_max_probes 6

echo; echo "################ REPRESENTATIVE RUN COMPLETE -- $(date -u +%FT%TZ) ################"
