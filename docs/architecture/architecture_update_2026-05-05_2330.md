# Architecture Update — Multi-Provider LLM Client + Run 12 Re-execution

> **Date:** 2026-05-05
> **Baseline:** Run 11 (81/100 = 81.0%)
> **Context:** Run 12 (this run) re-executes the four fixes from `architecture_update_2026-04-27_1832.md` after a previous attempt was aborted by an OpenAI quota exhaustion. The fixes themselves were already documented; this file captures the **one architecture change made in the interim that was not previously documented**.

---

## Change: Provider-agnostic LLM client (OpenAI + Anthropic)

**File:** `rag_snow_agent/src/rag_snow_agent/agent/llm_client.py`

**Problem:** The previous `llm_client.py` was hard-bound to the OpenAI Chat Completions API. After Run 11's analysis showed gpt-5.4-mini struggling with geospatial plan generation and date-shard reasoning, we wanted the option to route the harder instances to Anthropic Claude. The existing client could not do this without a parallel branch in every call site.

**Change:** `llm_client.call_llm()` now auto-detects the provider from the model name:

- Models starting with `claude-` route to the Anthropic Messages API
- Everything else routes to the OpenAI Chat Completions API

Both backends return a normalized `_LLMResult(content, usage)` so callers (candidate generator, plan / SQL prompts, repair loop, verifier) need no changes.

### Key implementation details

1. **Provider detection** — `_is_anthropic_model(model)` checks the `claude-` prefix.
2. **Anthropic backend** — `_call_anthropic()` separates system messages from the conversation (the Anthropic API requires `system` as a top-level kwarg, not a role), enforces `max_tokens` (Anthropic requires a value, defaulting to 8192), and retries on 429/500/502/503/529 with 2s/4s/8s backoff.
3. **OpenAI backend** — preserved existing behavior (parameter fallback for `max_completion_tokens` / `temperature` quirks across model families) plus a new `_openai_call_with_transient_retry()` that retries `400 / "could not parse"` JSON-body errors (Run 12 Fix 1).
4. **Usage normalization** — both backends return `_LLMUsage(prompt_tokens, completion_tokens, total_tokens)` with consistent semantics. Anthropic's `input_tokens / output_tokens` are mapped accordingly.
5. **Default model** — `defaults.yaml` `llm.model` was switched to `claude-sonnet-4-5` during this period of provider experimentation. **For Run 12, both `llm.model` and `llm.geo_model` are reverted to `gpt-5.4-mini`** per the user's instruction to keep the benchmark on OpenAI.

### Environment variables

- `OPENAI_API_KEY` — required for OpenAI calls (gpt-* and gpt-5.4-mini)
- `ANTHROPIC_API_KEY` — required only when a `claude-*` model is selected; not used in Run 12

Both keys are loaded from `rag_snow_agent/.env` via `python-dotenv` at import time.

### Why this matters for Run 12

Run 12 still uses gpt-5.4-mini exclusively, so the Anthropic path is dormant. The relevant Run 12 code paths in this client are:

- `_openai_call_with_transient_retry()` — directly exercises Fix 1 from the prior architecture update
- The provider-detection branch — adds essentially no overhead (string prefix check) for OpenAI calls

No call-site changes were required in `agent.py`, `candidate_generator.py`, `plan_sql_pipeline.py`, or the verifier modules.

---

## Run 12 Configuration

```yaml
llm:
  model: gpt-5.4-mini
  geo_model: gpt-5.4-mini   # geo routing disabled by using same model for both paths
  temperature: 0.2
  max_output_tokens: null
```

CLI:

```bash
uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials ./snowflake_credentials.json \
  --experiment benchmark_run_12 \
  --limit 100 \
  --model gpt-5.4-mini \
  --best_of_n 8 \
  --max_repairs 4 \
  --gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --chroma_dir .chroma/
```

## Active Run 12 Fixes (from prior architecture update)

| Fix | Module | Wired? |
|:----|:-------|:-------|
| 1. API 400 retry on transient JSON-parse errors | `agent/llm_client.py:_openai_call_with_transient_retry` | ✅ — exercised on every OpenAI call |
| 2. Join-graph neighbor expansion (geo cols) | `retrieval/connectivity.py:expand_join_graph_neighbors` | ✅ — called from `build_schema_slice` (`debug_retrieve.py:207`), which is the schema slice path used by `experiment_runner.py:420` |
| 3. Geo model routing | `eval/experiment_runner.py:_is_geo_query` (line 438) | ⚠️ Active but no-op: `geo_model == model == gpt-5.4-mini` for this run |
| 4. GA360 date-shard rewriting | `prompting/sql_compiler.py:rewrite_date_sharded_tables` | ✅ — called for both CTE and single-block paths in `compile_plan` |
