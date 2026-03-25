# ReFoRCE Benchmark — Setup & Run Guide

This document lists steps to set up the environment and run the ReFoRCE benchmark (including the small modifications we've made to the original pipeline such as stage banners and token accounting).

## 1. Prerequisites
- Linux / WSL environment (tested on Ubuntu)
- Python 3.10+ (use the project's conda env if provided)
- git, curl, tar, bash
- OpenAI / Azure API key with permission to call chosen models

## 2. Recommended environment (conda)
1. Create the conda environment (example):

```bash
conda create -n reforce python=3.10 -y
conda activate reforce
```

2. Install Python requirements from the methods folder:

```bash
pip install -r ReFoRCE/methods/ReFoRCE/requirements.txt
```

Notes: some packages (pandas, sqlglot, tokenizers, etc.) may require binary wheels matched to your Python version. If `pip install` fails, prefer using the project's provided conda environment or adjust Python version to match available wheels.

## 3. Configuration
- Set your OpenAI/Azure API key in the environment before running:

```bash
export OPENAI_API_KEY="sk-..."
```

- If using BigQuery or Snowflake, copy credentials to the expected locations under `ReFoRCE/methods/ReFoRCE/` (e.g., `bigquery_credential.json`, `snowflake_credential.json`) or set the environment variables your workflow expects.

### Reference: original Spider2 setup steps (BigQuery & Snowflake)
The ReFoRCE benchmark re-uses the same data access patterns as the Spider2 codebase. For complete, authoritative setup steps for BigQuery and Snowflake access, consult the original Spider2 evaluation-suite READMEs located under:

- `spider2-lite/evaluation_suite/README.md`
- `spider2-snow/evaluation_suite/README.md`

Key steps you will typically need to perform (see the Spider2 READMEs for exact commands and examples):
- BigQuery (GCP):
  - Create or select a Google Cloud project, enable the BigQuery API.
  - Create a service account with the necessary BigQuery and Storage roles.
  - Download the service account JSON key and place it where the benchmark expects it (or set `GOOGLE_APPLICATION_CREDENTIALS` to point to the file).
  - If the Spider2 evaluation suite includes example scripts, follow them to load or point at the dataset snapshots used by the benchmark.
- Snowflake:
  - Create a Snowflake user/role with appropriate permissions to the databases used by the benchmark.
  - Provide account credentials either via the `snowflake_credential.json` under `methods/ReFoRCE/` or via environment variables according to the Spider2 README (account, user, password/secret, role, warehouse, database, schema).
  - Ensure network access (IP allowlists, private connectivity) if your Snowflake account enforces network restrictions.

If you prefer direct pointers, open the Spider2 readmes above for step-by-step commands and sample credential JSON formats.

## 4. Running the benchmark
Run the pipeline from `methods/ReFoRCE` (project root is fine). Example commands used in our runs:

- Spider-lite (example):

```bash
cd ReFoRCE/methods/ReFoRCE
bash scripts/run_main.sh --task lite --model gpt-5-mini --N 25 --num_workers 2 --num_votes 4 --test_delay 4
```

- Spider-snow (example):

```bash
cd methods/ReFoRCE
bash scripts/run_main.sh --task snow --model gpt-5-mini --N 25 --num_workers 2 --num_votes 4 --test_delay 4
```

Flags explained (common):
- `--task`: `lite` or `snow` dataset
- `--model`: model id to call (e.g. `gpt-5-mini`)
- `--N`: number of examples/candidates per test run (project-specific)
- `--num_workers`: worker threads to run in parallel
- `--num_votes`: number of voting/model-vote passes
- `--test_delay`: per-test delay (seconds) used to spread requests

## 5. Logs & outputs
- Run logs and outputs are written under `methods/ReFoRCE/output/` with run-specific folders.
- Token summary: `.../token_usage_summary.json` — contains prompt/completion/total tokens per test/candidate.
- Test summary report (generated):
  - `ReFoRCE/methods/ReFoRCE/output/ReFoRCE_Accuracy_Report.md`


## 6. What changed from the original pipeline
- `scripts/run_main.sh`: added clear stage banners and per-stage token delta printing.
- `methods/ReFoRCE/chat.py`: token accounting added. Per-LLM call usage is recorded and persisted.
- Output artifact: a `token_usage_summary.json` file is written under the run output directory (e.g., `output/gpt-5-mini-snow-log-YYYYMMDD-HHMMSS/token_usage_summary.json`). This file contains `total` and `per_test` token usage breakdowns.


## 7. Avoiding rate-limits (practical tips)
- The project observed TPM (tokens per minute) / 429 rate-limits when running heavy parallel workloads. To reduce 429s:
  - Lower `--num_workers` or `--num_votes` (product `num_workers * num_votes` is the primary driver).
  - Increase `--test_delay` to spread out requests.
  - Use the `token_usage_summary.json` per-request stats to compute safe concurrency for your org quota (safe rule-of-thumb: keep `num_workers * num_votes <= 8` for typical quotas; adjust downward if you see 429s).

## 8. Running evaluation
- The project includes `methods/ReFoRCE/eval.py`. After a run completes, you can evaluate outputs (also prints token usage summary):

```bash
cd methods/ReFoRCE
python eval.py --run_dir output/<your-run-folder>
```

## 9. Outputs — What the `output/` folder contains

- **Top-level run log:** `methods/ReFoRCE/output/<MODEL>_<TASK>_<TIMESTAMP>.log` — the combined stdout/stderr captured by `run_main.sh`.
- **Run output folder:** `methods/ReFoRCE/output/<API>-<TASK>-log-<TIMESTAMP>/` — this is the `$OUTPUT_PATH` created by `run_main.sh` and contains the run artifacts:
  - `token_usage_summary.json` — aggregated token accounting recorded by the chat layer. Structure:
    - `total`: overall requests/prompt_tokens/completion_tokens/total_tokens
    - `per_test`: mapping from `testid` or `testid@candX` → `{requests,prompt_tokens,completion_tokens,total_tokens}`
  - Per-instance subfolders (one per `instance_id`, e.g., `bq019/`) containing:
    - `result.sql` — the final chosen SQL for that instance.
    - `result.csv` — execution metadata or result rows (when available).
    - `log.log` — per-instance run log (generation traces, errors, execution notes).
    - Candidate artifacts: `0result.sql`, `0log.log`, `0result.csv`, `1result.sql`, ... — individual candidate outputs produced during generation/voting.
  - Other artifacts: intermediate JSON/JSONL metadata, linked schema files, and per-stage outputs produced by `schema_linking.py` and `reconstruct_data.py`.

- **Submission / metadata exports:** `output/${API}-${TASK}-csv-${TIMESTAMP}` and `output/${API}-${TASK}-sql-${TIMESTAMP}` — created by `get_metadata.py`, used for evaluation and submission.
- **Official evaluation outputs:** `spider2-<task>/evaluation_suite` (when `run_main.sh` calls `evaluate.py`) — the official Spider2 evaluation writes reports under its own folders.

Quick inspection commands:
```bash
# view the main run log
less methods/ReFoRCE/output/gpt-5-mini_lite_20260320-005805.log

# list a run folder and inspect a single instance result
ls -la methods/ReFoRCE/output/gpt-5-mini-lite-log-20260320-005805/
cat methods/ReFoRCE/output/gpt-5-mini-lite-log-20260320-005805/bq019/result.sql

# show token usage summary
jq . methods/ReFoRCE/output/gpt-5-mini-lite-log-20260320-005805/token_usage_summary.json
```

Latest retained experiment artifacts (current workspace):
- `methods/ReFoRCE/output/gpt-5-mini_lite_20260320-005805.log`
- `methods/ReFoRCE/output/gpt-5-mini-lite-log-20260320-005805/`
- `methods/ReFoRCE/output/gpt-5-mini-lite-csv-20260320-005805/`
- `methods/ReFoRCE/output/gpt-5-mini-lite-csv-20260320-005805-ids.csv`
- `methods/ReFoRCE/output/gpt-5-mini-lite-sql-20260320-005805/`
- `methods/ReFoRCE/output/ReFoRCE_Accuracy_Report.md`

Notes:
- `run_main.sh` populates `$OUTPUT_PATH` and calls `run.py` multiple times (different flags). Each run may append candidate files and logs into the same `$OUTPUT_PATH` so you will find artifacts from all stages there.
- `token_usage_summary.json` is written incrementally by the chat layer and aggregated at the end of the pipeline; it is useful to compute TPM and concurrency heuristics.


## 10. Troubleshooting
- Missing `pandas` or binary wheel errors: use the conda env shipped with the project or install matching Python version packages.
- Missing API key errors: ensure `OPENAI_API_KEY` (or other provider env vars) are exported in the same shell you run `run_main.sh` from.
- If `run_main.sh` exits with many 429s, reduce concurrency and/or increase `--test_delay`.

## 11. Copying outputs to Windows (WSL users)
If you run inside WSL and want to copy the output folder to Windows, use:

```bash
# open folder in Explorer
explorer.exe "$(wslpath -w /home/<you>/ReFoRCE/methods/ReFoRCE/output)"

# or copy files into your Windows Downloads
mkdir -p /mnt/c/Users/YourWindowsUser/Downloads/ReFoRCE_output
cp -a methods/ReFoRCE/output/<your-run-folder> /mnt/c/Users/YourWindowsUser/Downloads/ReFoRCE_output/
```

Replace `YourWindowsUser` and paths as needed.

