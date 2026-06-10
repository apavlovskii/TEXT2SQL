#!/usr/bin/env bash
# Tiered-impact ablation sweep — first 100 NON-geospatial spider2-snow instances.
#
# Leave-one-out arms (each removes exactly one component from the full system):
#   A0 Full | A1 no Best-of-N | A2 no Verification | A3 no Repair
#   A4 no Sample-records | A5 no Join-graph | A6 no Semantic | A8 no Verifier
#   A7 Baseline (everything stripped)
#
# Designed for ranking components into impact tiers via paired per-instance
# flips (see rank_ablation.py), not for significance testing.
#
# Resumable: every arm runs with --resume, so re-running this script after a
# token-quota stop skips already-completed instances and continues cleanly.
# If an arm stops on quota (exit 2), the whole sweep aborts so you can top up
# and re-run, rather than burning every remaining arm against the same wall.

set -uo pipefail
cd "$(dirname "$0")/.."

MODEL="${MODEL:-gpt-5.4-mini}"
LIMIT="${LIMIT:-100}"
BON="${BON:-4}"
MAX_REPAIRS="${MAX_REPAIRS:-3}"
# Prefix for experiment dir names. Set this per run (e.g. EXP_PREFIX=n30_) so a
# new sweep NEVER resumes into stale dirs from a previous, differently-coded run.
EXP_PREFIX="${EXP_PREFIX:-}"

SPLIT="../Spider2/spider2-snow/spider2-snow.jsonl"
CREDS="snowflake_credentials.json"
GOLD="../Spider2/spider2-snow/evaluation_suite/gold/"

# Preflight once on the first arm (catch Snowflake/OpenAI/Chroma issues early);
# skip it on the rest to save time. Override with PREFLIGHT=0 to skip entirely.
PREFLIGHT="${PREFLIGHT:-1}"

run() {
    local name="${EXP_PREFIX}$1"; shift
    local preflight_flag="$1"; shift
    echo
    echo "================================================================"
    echo "  $name  -- $(date -u +%FT%TZ)"
    echo "================================================================"
    uv run python -m rag_snow_agent.eval.experiment_runner \
        --split_jsonl "$SPLIT" \
        --credentials "$CREDS" \
        --gold_dir "$GOLD" \
        --model "$MODEL" \
        --limit "$LIMIT" \
        --exclude_geospatial \
        --resume \
        --experiment "$name" \
        $preflight_flag \
        "$@"
    local rc=$?
    echo "  $name -> exit $rc  ($(date -u +%FT%TZ))"
    if [ "$rc" -eq 2 ]; then
        echo
        echo "!! Token/quota limit reached during $name. Sweep aborted."
        echo "   Top up your quota, then re-run this script to resume from here."
        exit 2
    fi
    return 0
}

FIRST_PREFLIGHT=""
[ "$PREFLIGHT" = "1" ] || FIRST_PREFLIGHT="--skip_preflight"

# A0 — Full system (reference). Preflight here only.
run A0_full                "$FIRST_PREFLIGHT"  --best_of_n "$BON" --max_repairs "$MAX_REPAIRS"

# A1 — drop Best-of-N (single candidate)
run A1_no_best_of_n        "--skip_preflight"  --best_of_n 1     --max_repairs "$MAX_REPAIRS"

# A2 — drop Verification (no fingerprint/metamorphic in selector)
run A2_no_verification     "--skip_preflight"  --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_verification

# A3 — drop Repair loop
run A3_no_repair           "--skip_preflight"  --best_of_n "$BON" --max_repairs 0

# A4 — drop Sample-records prompting
run A4_no_sample_records   "--skip_preflight"  --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_sample_records

# A5 — Join-graph is geospatial-only (inert on the non-geo set), so it is
#      intentionally NOT run here. Documented as geo-only in the audit.

# A6 — drop Semantic layer
run A6_no_semantic         "--skip_preflight"  --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_semantic

# A8 — drop Verifier (now that it fires; placed after A6 to keep A7=baseline last)
run A8_no_verifier         "--skip_preflight"  --best_of_n "$BON" --max_repairs "$MAX_REPAIRS" --disable_verifier

# A7 — Baseline (strip down to bare retrieval + plan->SQL)
run A7_baseline            "--skip_preflight"  --best_of_n 1     --max_repairs 0 \
                           --disable_sample_records --disable_join_graph \
                           --disable_semantic --disable_verification --disable_verifier

echo
echo "================================================================"
echo "  Sweep complete -- $(date -u +%FT%TZ)"
echo "  Rank components with:"
echo "    uv run python -m rag_snow_agent.eval.rank_ablation \\"
echo "      --experiments_dir reports/experiments \\"
echo "      --arms ${EXP_PREFIX}A0_full ${EXP_PREFIX}A1_no_best_of_n ${EXP_PREFIX}A2_no_verification \\"
echo "             ${EXP_PREFIX}A3_no_repair ${EXP_PREFIX}A4_no_sample_records ${EXP_PREFIX}A6_no_semantic \\"
echo "             ${EXP_PREFIX}A8_no_verifier ${EXP_PREFIX}A7_baseline"
echo "================================================================"
