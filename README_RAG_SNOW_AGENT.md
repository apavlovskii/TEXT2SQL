# rag_snow_agent

## A) What this module is
Snow-only RAG agent for Spider2-Snow. It uses ChromaDB for retrieval over Snowflake-side metadata/context, runs agent inference on Spider2-Snow tasks, and writes outputs in the Spider2 folder structure so official evaluation scripts can be used without custom adapters.

## B) Setup (uv)
From repository root:

```bash
cd rag_snow_agent

# initialize only if pyproject/uv project is not already initialized
uv init

uv venv --python 3.11
uv add chromadb openai pydantic python-dotenv rich snowflake-connector-python tiktoken
```

Environment variables:

- `OPENAI_API_KEY`
- `SNOWFLAKE_CREDENTIALS_JSON` (optional override path)
- `CHROMA_DIR` (optional, default `./.chroma`)
- `LOG_LEVEL` (optional)

You can start from `.env.example` and load via your shell or runtime.

## C) Credentials
Expected credential file (default path: `rag_snow_agent/snowflake_credentials.json`):

```json
{
	"user": "<snowflake_user>",
	"password": "<snowflake_password_or_pat>",
	"account": "<account_identifier>",
	"warehouse": "<optional_warehouse>",
	"role": "<optional_role>",
	"database": "<optional_database>",
	"schema": "<optional_schema>"
}
```

Notes:

- `snowflake_credentials.json` is gitignored at workspace level.
- You can override credential path with:

```bash
export SNOWFLAKE_CREDENTIALS_JSON=/absolute/path/to/snowflake_credentials.json
```

## D) Smoke test
Run:

```bash
cd rag_snow_agent
./scripts/smoke_test.sh
```

Expected success shape:

- `chromadb import: OK`
- `snowflake connector import: OK`
- `Snowflake connectivity: OK`
- followed by query columns / first rows from `SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER()`

If authentication fails (for example disabled/expired PAT), the script exits non-zero with Snowflake error details.

## E) Build index
Example:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.chroma.build_index --db_id GA360 --credentials snowflake_credentials.json
```

Chroma persistence:

- Default local store: `rag_snow_agent/.chroma/`

Wipe/rebuild:

```bash
rm -rf .chroma
uv run python -m rag_snow_agent.chroma.build_index --db_id GA360 --credentials snowflake_credentials.json
```

## F) Debug retrieval

After building the index (step E), inspect what schema slice would be retrieved for a query:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.retrieval.debug_retrieve \
  --db_id GA360 \
  --query "total sessions by month" \
  --top_k 10 \
  --max_schema_tokens 800 \
  -v
```

This prints:
- Top tables with dense/lexical/fused ranks and RRF scores
- Selected columns per table (with join-key and time-column flags)
- Final SchemaSlice token count and formatted prompt text

CLI flags override values from `config/defaults.yaml`.

## G) Debug plan → SQL pipeline

After building the index (step E), generate a query plan and compile SQL:

```bash
cd rag_snow_agent
export OPENAI_API_KEY="sk-..."
uv run python -m rag_snow_agent.prompting.debug_plan_sql \
  --db_id GA360 \
  --query "average amount by month" \
  --top_k 10 \
  --model gpt-4o-mini \
  -v
```

This prints:
- Retrieved SchemaSlice (tables + columns)
- Generated plan JSON
- Compiled SQL (deterministic from plan, or LLM-generated with `--use_llm_sql`)
- Identifier validation result (PASS/FAIL with details)
- Pipeline warnings and LLM call count

Use `--use_llm_sql` to have the LLM generate SQL directly from the plan instead of the deterministic compiler.

## H) Debug execution + repair loop

End-to-end: retrieve schema, generate SQL, execute against Snowflake, and run repair loop:

```bash
cd rag_snow_agent
export OPENAI_API_KEY="sk-..."
uv run python -m rag_snow_agent.agent.debug_execute_refine \
  --db_id GA360 \
  --query "total sessions by month" \
  --credentials snowflake_credentials.json \
  --top_k 10 \
  --model gpt-4o-mini \
  --max_repairs 2 \
  -v
```

This prints:
- SchemaSlice summary
- Initial SQL from the plan pipeline
- Each repair attempt (error type, action taken, repaired SQL)
- Final SQL and success/failure status

Add `--experiment <name>` to also write a Spider2-compatible `result.json`.

## I) Debug Best-of-N candidate selection

Generate multiple diverse SQL candidates, execute+repair each, and select the best:

```bash
cd rag_snow_agent
export OPENAI_API_KEY="sk-..."
uv run python -m rag_snow_agent.agent.debug_best_of_n \
  --db_id GA360 \
  --query "total sessions by month" \
  --credentials snowflake_credentials.json \
  --model gpt-4o-mini \
  --n 2 \
  --top_k 10 \
  -v
```

This prints:
- SchemaSlice summary
- Each candidate: strategy, initial SQL, final SQL, score, repair trace
- Selected best candidate with selection reason

Strategies cycle through: `default`, `join_first`, `metric_first`, `time_first`.
Override `--n` to increase candidate count. Add `--experiment <name>` to write Spider2 result.

## J) Debug semantic verification

Run Best-of-N with full semantic verification (fingerprinting, shape inference, metamorphic checks):

```bash
cd rag_snow_agent
export OPENAI_API_KEY="sk-..."
uv run python -m rag_snow_agent.agent.debug_verify_candidate \
  --db_id GA360 \
  --query "top selling product by month in 2017" \
  --model gpt-4o-mini \
  --n 2 \
  --top_k 10 \
  -v
```

This prints:
- Expected output shape (small/grouped/aggregate/time-series)
- Each candidate: final SQL, row count, result fingerprint, metamorphic checks, score breakdown
- Selected best candidate with shape-aware selection reason

## K) Debug trace memory retrieval

Query trace memory to see what prior successful solutions are stored for a database:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.agent.debug_memory_retrieval \
  --db_id GA360 \
  --query "total sessions by month" \
  --top_k 5 \
  -v
```

This prints:
- Matching traces with distance scores
- Instance IDs and tables used for each trace
- Document summaries (instruction + plan)

Trace memory is populated automatically when `memory.enabled: true` in `config/defaults.yaml` and an instance solves successfully. You can also backfill from prior runs (stub utility at `rag_snow_agent.eval.backfill_trace_memory`).

## L) Run on N instances
Example runner command:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.run_spider2_snow --limit 25 --experiment rag_v1
```

Typical knobs to add as needed:

- `--model ...`
- `--split ...`
- `--credentials snowflake_credentials.json`

## M-1) Train verifier model from run logs

After collecting candidate logs (via `log_candidate_records` in the agent loop), train a learned verifier:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.agent.train_verifier \
  --run_dir reports/candidate_logs \
  --output_model rag_snow_agent/models/verifier.joblib
```

This builds a LogisticRegression model from JSONL candidate records, prints train/test accuracy and feature weights, and saves the model for use in Best-of-N scoring.

## M-2) Debug verifier score for a candidate

Inspect the features and verifier score for a single candidate record:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.agent.debug_verifier_score \
  --candidate_json path/to/candidate.json
```

This prints all extracted features and the model's predicted probability. If no trained model is found, the score defaults to 0.0.

## N) Evaluate with official Spider2 scripts
For benchmark execution & evaluation commands, see `README_SPIDER2.md`.

Minimal output location expectation:

- `Spider2/methods/spider-agent-snow/output/<experiment>/...`

## O) Ablation Experiment Runner

Run a full experiment with ablation toggles:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials rag_snow_agent/snowflake_credentials.json \
  --experiment ablation_v1 \
  --limit 25 \
  --model gpt-4o-mini \
  --best_of_n 2 \
  --disable_memory \
  --disable_verifier
```

Ablation toggles: `--disable_memory`, `--disable_verifier`, `--disable_best_of_n`, `--disable_repair`, `--disable_verification`, `--disable_join_graph`.

Use `--ablation_preset config/ablations/baseline_single.yaml` to load a preset configuration.

Available presets in `config/ablations/`:
- `baseline_single.yaml` -- no memory, no best_of_n, no verifier
- `best_of_n_only.yaml` -- best_of_n=2, no memory, no verifier
- `full_system.yaml` -- everything enabled

Output: `reports/experiments/<experiment>/` with `manifest.json`, `instance_results.jsonl`.

## P) Aggregate Metrics

Compute aggregate metrics from an experiment run:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.aggregate_metrics \
  --experiment_dir reports/experiments/ablation_v1
```

Writes `metrics.json` with accuracy, avg/median/p95 LLM calls, avg repairs, failure taxonomy, and candidate count distribution.

## Q) Compare Experiments

Print a markdown comparison table across two or more experiments:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.compare_experiments \
  --experiments reports/experiments/baseline reports/experiments/full_system
```

Shows deltas for accuracy, LLM calls, and repairs.

## R) Render Report

Generate a markdown report from experiment results:

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.render_report \
  --experiment_dir reports/experiments/ablation_v1
```

Writes `REPORT.md` with config snapshot, summary metrics, top token-consuming queries, and failure categories.

