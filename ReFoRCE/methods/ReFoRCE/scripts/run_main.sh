#!/bin/bash
set -e
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
AZURE=false
N_TESTS=""
NUM_VOTES=8
NUM_WORKERS=16
TEST_DELAY=3
PYTHON_BIN="${PYTHON_BIN:-$(command -v python)}"
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --azure)
      AZURE=true
      shift # past argument
      ;;
    --task)
      TASK="$2"
      shift
      shift
      ;;
    --model)
      API="$2"
      shift
      shift
      ;;
    --N)
      N_TESTS="$2"
      shift
      shift
      ;;
    --num_votes)
      NUM_VOTES="$2"
      shift
      shift
      ;;
    --num_workers)
      NUM_WORKERS="$2"
      shift
      shift
      ;;
    --test_delay)
      TEST_DELAY="$2"
      shift
      shift
      ;;
    *)
      shift
      ;;
  esac
done

LOG_DIR="output"
mkdir -p "$LOG_DIR"
MODEL_NAME="${API//\//_}"
LOG_FILE="${LOG_DIR}/${MODEL_NAME}_${TASK}_${TIMESTAMP}.log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Logging to: $LOG_FILE"
echo "Using Python: $PYTHON_BIN"
"$PYTHON_BIN" --version

print_stage() {
  echo ""
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

get_usage_totals() {
  local usage_file="$1/token_usage_summary.json"
  "$PYTHON_BIN" - <<PY
import json
import os

usage_file = "${usage_file}"
if not os.path.exists(usage_file):
    print("0 0 0 0")
else:
    with open(usage_file) as f:
        usage = json.load(f)
    total = usage.get("total", {})
    print(f"{int(total.get('requests', 0))} {int(total.get('prompt_tokens', 0))} {int(total.get('completion_tokens', 0))} {int(total.get('total_tokens', 0))}")
PY
}

print_stage_usage_delta() {
  local stage_name="$1"
  local before_vals="$2"
  local after_vals="$3"

  read -r b_req b_prompt b_completion b_total <<< "$before_vals"
  read -r a_req a_prompt a_completion a_total <<< "$after_vals"

  local d_req=$((a_req - b_req))
  local d_prompt=$((a_prompt - b_prompt))
  local d_completion=$((a_completion - b_completion))
  local d_total=$((a_total - b_total))

  echo "[TokenUsage][${stage_name}] requests=${d_req}, prompt=${d_prompt}, completion=${d_completion}, total=${d_total}"
}

count_step_folders() {
  local output_path="$1"
  if [ ! -d "$output_path" ]; then
    echo 0
    return
  fi
  find "$output_path" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' '
}

render_progress_bar() {
  local label="$1"
  local current="$2"
  local total="$3"
  local width=32

  if [ "$total" -le 0 ]; then
    return
  fi

  if [ "$current" -gt "$total" ]; then
    current="$total"
  fi

  local percent=$(( current * 100 / total ))
  local filled=$(( current * width / total ))
  local empty=$(( width - filled ))
  local bar_filled
  local bar_empty

  bar_filled=$(printf "%${filled}s" "" | tr ' ' '#')
  bar_empty=$(printf "%${empty}s" "" | tr ' ' '-')

  printf "\r[%s Progress] [%s%s] %3d%% (%d/%d)" "$label" "$bar_filled" "$bar_empty" "$percent" "$current" "$total"
}

monitor_step_progress() {
  local label="$1"
  local output_path="$2"
  local total="$3"
  local pid="$4"
  local last_count=-1

  while kill -0 "$pid" 2>/dev/null; do
    local current
    current=$(count_step_folders "$output_path")
    if [ "$current" -ne "$last_count" ]; then
      render_progress_bar "$label" "$current" "$total"
      last_count="$current"
    fi
    sleep 1
  done

  local final_count
  final_count=$(count_step_folders "$output_path")
  render_progress_bar "$label" "$final_count" "$total"
  echo ""
}

print_stage "START Pipeline"

# # Set up
if [ "$TASK" = "lite" ]; then
  print_stage "START Setup lite resources"
    gdown 'https://drive.google.com/uc?id=1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG' -O ../../spider2-lite/resource/
    rm -rf ../../spider2-lite/resource/databases/spider2-localdb
    mkdir -p ../../spider2-lite/resource/databases/spider2-localdb
    unzip ../../spider2-lite/resource/local_sqlite.zip -d ../../spider2-lite/resource/databases/spider2-localdb
fi

print_stage "START Data setup"
"$PYTHON_BIN" spider_agent_setup_${TASK}.py --example_folder examples_${TASK}

# Reconstruct data
print_stage "START Reconstruct data"
"$PYTHON_BIN" reconstruct_data.py \
    --example_folder examples_${TASK} \
    --add_description \
    --add_sample_rows \
    --rm_digits \
    --make_folder \
    --clear_long_eg_des

echo "Number of prompts.txt files in examples_${TASK} larger than 200KB before reducing: $(find examples_${TASK} -type f -name "prompts.txt" -exec du -b {} + | awk '$1 > 200000' | wc -l)"

# Run Schema linking and voting
print_stage "START Schema linking"
"$PYTHON_BIN" schema_linking.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --linked_json_pth ../../data/linked_${TASK}_tmp0.json \
    --reduce_col

echo "Number of prompts.txt files in examples_${TASK} larger than 200KB before reducing: $(find examples_${TASK} -type f -name "prompts.txt" -exec du -b {} + | awk '$1 > 200000' | wc -l)"

OUTPUT_PATH="output/${API}-${TASK}-log-${TIMESTAMP}"
# OUTPUT_PATH="output/${API}-${TASK}-log"
MAX_TESTS_ARG=""
if [ -n "$N_TESTS" ]; then
  MAX_TESTS_ARG="--max_tests $N_TESTS"
fi
echo "AZURE mode: $AZURE"
echo "Model: $API"
echo "Task: $TASK"
echo "N tests limit: ${N_TESTS:-all}"
echo "num_votes: ${NUM_VOTES}"
echo "num_workers: ${NUM_WORKERS}"
echo "Per-test launch delay: ${TEST_DELAY}s"
echo "Output Path: $OUTPUT_PATH"

# Step 1: Self-refinement + Majority Voting
CMD1="$PYTHON_BIN run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_self_refinement \
    --generation_model ${API} \
    --max_iter 5 \
    --temperature 1 \
    --early_stop \
    --do_vote \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS \
    $MAX_TESTS_ARG \
    --test_delay $TEST_DELAY"

# Step 2: Self-refinement + Majority Voting + Column Exploration + Rerun
CMD2="$PYTHON_BIN run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_self_refinement \
    --generation_model ${API} \
    --do_column_exploration \
    --column_exploration_model ${API} \
    --max_iter 5 \
    --temperature 1 \
    --early_stop \
    --do_vote \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS \
    $MAX_TESTS_ARG \
    --test_delay $TEST_DELAY \
    --rerun \
    --overwrite_unfinished"

if [ "$AZURE" = true ]; then
  CMD1="$CMD1 --azure"
  CMD2="$CMD2 --azure"
fi

print_stage "START Step 1/4 generation"
STAGE1_BEFORE=$(get_usage_totals "$OUTPUT_PATH")
if [[ "$N_TESTS" =~ ^[0-9]+$ ]] && [ "$N_TESTS" -gt 0 ]; then
  eval "$CMD1" &
  STEP1_PID=$!
  monitor_step_progress "Step1" "$OUTPUT_PATH" "$N_TESTS" "$STEP1_PID"
  wait "$STEP1_PID"
else
  eval "$CMD1"
fi
STAGE1_AFTER=$(get_usage_totals "$OUTPUT_PATH")
print_stage_usage_delta "Step 1/4 generation" "$STAGE1_BEFORE" "$STAGE1_AFTER"
print_stage "START Step 1/4 evaluation"
echo "Evaluation for Step 1"
"$PYTHON_BIN" eval.py --log_folder $OUTPUT_PATH --task $TASK

print_stage "START Step 2/4 generation"
STAGE2_BEFORE=$(get_usage_totals "$OUTPUT_PATH")
if [[ "$N_TESTS" =~ ^[0-9]+$ ]] && [ "$N_TESTS" -gt 0 ]; then
  eval "$CMD2" &
  STEP2_PID=$!
  monitor_step_progress "Step2" "$OUTPUT_PATH" "$N_TESTS" "$STEP2_PID"
  wait "$STEP2_PID"
else
  eval "$CMD2"
fi
STAGE2_AFTER=$(get_usage_totals "$OUTPUT_PATH")
print_stage_usage_delta "Step 2/4 generation" "$STAGE2_BEFORE" "$STAGE2_AFTER"
print_stage "START Step 2/4 evaluation"
echo "Evaluation for Step 2"
"$PYTHON_BIN" eval.py --log_folder $OUTPUT_PATH --task $TASK

# Step 3: Random vote for tie
print_stage "START Step 3/4 generation"
STAGE3_BEFORE=$(get_usage_totals "$OUTPUT_PATH")
"$PYTHON_BIN" run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_vote \
    --random_vote_for_tie \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS \
    $MAX_TESTS_ARG \
    --test_delay $TEST_DELAY
  STAGE3_AFTER=$(get_usage_totals "$OUTPUT_PATH")
  print_stage_usage_delta "Step 3/4 generation" "$STAGE3_BEFORE" "$STAGE3_AFTER"
print_stage "START Step 3/4 evaluation"
echo "Evaluation for Step 3"
"$PYTHON_BIN" eval.py --log_folder $OUTPUT_PATH --task $TASK

# Step 4: Random vote final_choose
print_stage "START Step 4/4 generation"
STAGE4_BEFORE=$(get_usage_totals "$OUTPUT_PATH")
"$PYTHON_BIN" run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_vote \
    --random_vote_for_tie \
    --final_choose \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS \
    $MAX_TESTS_ARG \
    --test_delay $TEST_DELAY
  STAGE4_AFTER=$(get_usage_totals "$OUTPUT_PATH")
  print_stage_usage_delta "Step 4/4 generation" "$STAGE4_BEFORE" "$STAGE4_AFTER"
print_stage "START Step 4/4 evaluation"
echo "Evaluation for Step 4"
"$PYTHON_BIN" eval.py --log_folder $OUTPUT_PATH --task $TASK

# Final evaluation and get files for submission
print_stage "START Final metadata export"
"$PYTHON_BIN" get_metadata.py --result_path $OUTPUT_PATH --output_path output/${API}-${TASK}-csv-${TIMESTAMP}
"$PYTHON_BIN" get_metadata.py --result_path $OUTPUT_PATH --output_path output/${API}-${TASK}-sql-${TIMESTAMP} --file_type sql
cd ../../spider2-${TASK}/evaluation_suite
print_stage "START Official evaluation_suite"
"$PYTHON_BIN" evaluate.py --mode exec_result --result_dir ../../methods/ReFoRCE/output/${API}-${TASK}-csv-${TIMESTAMP}

print_stage "FINAL cumulative token usage"
"$PYTHON_BIN" - <<PY
import json
pth = "../../methods/ReFoRCE/${OUTPUT_PATH}/token_usage_summary.json"
try:
    with open(pth) as f:
        usage = json.load(f)
    total = usage.get("total", {})
    per_test = usage.get("per_test", {})
    print(f"Token usage file: {pth}")
    for test_id in sorted(per_test.keys()):
        s = per_test[test_id]
        print(f"[TokenUsage][{test_id}] requests={s.get('requests', 0)}, prompt={s.get('prompt_tokens', 0)}, completion={s.get('completion_tokens', 0)}, total={s.get('total_tokens', 0)}")
    print(f"[TokenUsage][TOTAL] requests={total.get('requests', 0)}, prompt={total.get('prompt_tokens', 0)}, completion={total.get('completion_tokens', 0)}, total={total.get('total_tokens', 0)}")
except FileNotFoundError:
    print(f"Token usage file not found: {pth}")
PY

print_stage "Pipeline completed"