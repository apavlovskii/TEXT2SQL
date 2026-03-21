# DSR Lite Benchmark — Setup & Run Guide

## Overview
This guide covers environment setup and how to run the DSR Lite benchmark.

Project location:
- `DSR-SQL/DSR_Lite`

## 1) Environment Setup
From repository root:

```bash
cd DSR-SQL/DSR_Lite
conda create -n dsrsql python=3.11 -y
conda activate dsrsql
pip install -r requirements.txt
```

## 2) Credentials
Prepare the following before running:

- BigQuery credentials:
  - `DSR-SQL/DSR_Lite/spider2-lite/evaluation_suite/bigquery_credential.json`
- Snowflake credentials:
  - `DSR-SQL/DSR_Lite/spider2-lite/evaluation_suite/snowflake_credential.json`
- LLM config:
  - `DSR-SQL/DSR_Lite/LLM/LLM_config.json`

## 3) Run DSR Lite
Run from `DSR-SQL/DSR_Lite`.

```bash
python main_lite.py --input_path data/Spider2Lite_Snowflake.json --N 25
```

### Per-testcase retry cap (optional)
```bash
python main_lite.py --input_path data/Spider2Lite_Snowflake.json --N 25 --max_attempts_per_case 3
```

### Multi-path mode
```bash
python main_lite.py --input_path data/Spider2Lite_Snowflake.json --N 25 --multi_path
```

## 4) Check run folders
If `--data_sub_dir` is omitted, `main_lite.py` now auto-creates output folder in this format:

- `logs/run_<task>_<YYYY-MM-DD_HHMMSS>`

Examples:
- `logs/run_snow_2026-03-20_123456`
- `logs/run_bq_2026-03-20_123456`
- `logs/run_sqlite_2026-03-20_123456`

`<task>` is inferred from `--input_path` filename (e.g., `Snowflake` → `snow`, `Bigquery`/`bq` → `bq`, `Sqlite` → `sqlite`).

## 5) Outputs
Under each run folder:

- `outcome/` — result files and token summary
- `log/` — per-instance logs and status jsonl
- `temp/` — temporary artifacts
- `sql/` — generated SQL files used for official evaluation

Example:
- `DSR-SQL/DSR_Lite/logs/run_snow_2026-03-20_123456/`

## 6) Useful Notes
- `--N` limits the number of instances from the input JSON.
- `--max_attempts_per_case K` skips a testcase after `K` failed attempts and moves to the next one.
- If `--max_attempts_per_case` is omitted, behavior is unchanged (no per-testcase attempt limit).
- If a run is interrupted, rerun with the same `--data_sub_dir` to continue in the same folder.
- For Snowflake/BigQuery inputs, the script performs preflight connectivity checks before processing.
- At the end of the run, `main_lite.py` automatically launches `spider2-lite/evaluation_suite/evaluate.py` and prints `Final score` / `Real score`.
- This README is consistent with `DSR-SQL/DSR_Lite/README.md` on Python version (`3.11`) and dependency setup.
