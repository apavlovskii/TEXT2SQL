# Spider2 Benchmark Setup & Run Guide

This document is a practical supplement for local runs in this workspace.

Primary source of truth:
- `Spider2/README.md`

It does not replace official benchmark policies, credentials, or submission guidance from the upstream Spider2 docs.

## Scope
This guide covers:
- Running Spider2-Snow with `methods/spider-agent-snow`
- Limiting execution to first `N` testcases
- Evaluating with denominator based on evaluated subset (`N` when used)
- Displaying total token usage after run

## Prerequisite: Docker Desktop
Before running the benchmark:
- Install Docker Desktop (or Docker Engine on Linux) from the official Docker docs.
- Start Docker and ensure it is running.
- Quick check:

```bash
docker --version
docker ps
```

If Docker is not running, Spider-Agent-Snow execution will fail because tasks are executed in Dockerized environments.

## 1) Environment Setup
From repository root:

```bash
cd Spider2/methods/spider-agent-snow
# Optional
# conda create -n spider2 python=3.11 -y
# conda activate spider2
pip install -r requirements.txt
```

## 2) Credentials and Data Setup
Follow official instructions in:
- `Spider2/README.md`
- `Spider2/assets/Snowflake_Guideline.md`

Then run setup:

```bash
cd Spider2/methods/spider-agent-snow
python spider_agent_setup_snow.py
```

## 3) Run Spider-Agent-Snow
Basic run:

```bash
cd Spider2/methods/spider-agent-snow
export OPENAI_API_KEY=your_openai_api_key
python run.py --model gpt-4o -s test1
```

Run first 25 testcases only:

```bash
python run.py --model gpt-4o -s test1 --N 25
```

Notes:
- `--N` limits to first `N` selected testcases.
- End-of-run output now includes:
  - attempted/finished/failed counts
  - run accuracy over attempted subset
  - total token usage (prompt/completion/total)
- Token summary JSON is written to:
  - `Spider2/methods/spider-agent-snow/output/<experiment_id>/token_usage_summary.json`

## 4) Build Submission Files
Convert run output to evaluation submission format:

```bash
cd Spider2/methods/spider-agent-snow
python get_spider2snow_submission_data.py \
  --experiment_suffix gpt-4o-test1 \
  --results_folder_name ../../spider2-snow/evaluation_suite/gpt-4o-test1
```

## 5) Evaluate Accuracy

```bash
cd Spider2/spider2-snow/evaluation_suite
python evaluate.py --result_dir gpt-4o-test1 --mode exec_result
```

Evaluate first 25 matched testcases only:

```bash
python evaluate.py --result_dir gpt-4o-test1 --mode exec_result --N 25
```

Behavior:
- With `--N`, evaluator uses first `N` matched IDs after sorting.
- Reported accuracy denominator is the evaluated subset size.

## 6) Non-contradiction Notes
Aligned with `Spider2/README.md`:
- Uses official Spider2-Snow method path and evaluation suite.
- Keeps official credential setup flow and benchmark structure.
- Adds local run conveniences (`--N`, token summary) without changing benchmark data format.
