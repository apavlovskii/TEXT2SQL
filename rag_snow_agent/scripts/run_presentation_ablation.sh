#!/usr/bin/env bash
# Ablation sweep for the presentation report.
#
# Runs the first 25 instances of spider2-snow under 8 configurations:
#   A0 Full | A1 no Best-of-N | A2 no Verification | A3 no Repair
#   A4 no Sample-records | A5 no Join-graph | A6 no Semantic | A7 Baseline
#
# Reports land under reports/experiments/A{0..7}_*/  with manifest +
# instance_results.jsonl (now carrying the new telemetry block).

set -uo pipefail
cd "$(dirname "$0")/.."

MODEL="${MODEL:-gpt-5.4-mini}"
LIMIT="${LIMIT:-25}"
BON="${BON:-4}"
MAX_REPAIRS="${MAX_REPAIRS:-3}"

SPLIT="../Spider2/spider2-snow/spider2-snow.jsonl"
CREDS="snowflake_credentials.json"
GOLD="../Spider2/spider2-snow/evaluation_suite/gold/"

run() {
    local name="$1"; shift
    echo
    echo "================================================================"
    echo "  $name  -- $(date -u +%FT%TZ)"
    echo "================================================================"
    # Continue on failure so one bad run doesn't kill the whole sweep.
    uv run python -m rag_snow_agent.eval.experiment_runner \
        --split_jsonl "$SPLIT" \
        --credentials "$CREDS" \
        --gold_dir "$GOLD" \
        --model "$MODEL" \
        --limit "$LIMIT" \
        --experiment "$name" \
        --skip_preflight \
        "$@"
    local rc=$?
    echo "  $name -> exit $rc  ($(date -u +%FT%TZ))"
    return 0
}

# A0 — Full system (reference)
run A0_full                --best_of_n "$BON" --max_repairs "$MAX_REPAIRS"

# A1 — drop Best-of-N (single candidate)
run A1_no_best_of_n        --best_of_n 1     --max_repairs "$MAX_REPAIRS"

# A2 — drop Verification (no fingerprint/metamorphic in selector)
run A2_no_verification     --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_verification

# A3 — drop Repair loop
run A3_no_repair           --best_of_n "$BON" --max_repairs 0

# A4 — drop Sample-records prompting
run A4_no_sample_records   --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_sample_records

# A5 — drop Join-graph neighbor expansion
run A5_no_join_graph       --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_join_graph

# A6 — drop Semantic layer
run A6_no_semantic         --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_semantic

# A7 — Baseline (strip down to bare retrieval + plan->SQL)
run A7_baseline            --best_of_n 1     --max_repairs 0 \
                           --disable_sample_records --disable_join_graph \
                           --disable_semantic --disable_verification

echo
echo "================================================================"
echo "  Sweep complete -- $(date -u +%FT%TZ)"
echo "================================================================"
