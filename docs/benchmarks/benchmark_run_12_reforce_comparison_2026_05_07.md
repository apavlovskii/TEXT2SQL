# Benchmark Run 12 — SnowRAG-Agent vs ReFoRCE Comparison

## What We Compared

Both systems on the **same 100 Spider2-Snow questions** (first 100 from `Spider2/spider2-snow/spider2-snow.jsonl`), both using **GPT-5.4-mini** as the only LLM, both verifying against the **Snowflake gold output** in `Spider2/spider2-snow/evaluation_suite/gold/`.

## Setup

- **SnowRAG-Agent (ours):** `benchmark_run_12` — `--best_of_n 8 --max_repairs 4`, ChromaDB retrieval slice (top-K tables/columns, 10K-token schema budget), deterministic SQL compiler, execution-guided repair loop.
- **ReFoRCE (upstream):** `scripts/run_smoke.sh --task snow --N 100 --num_votes 8 --num_workers 4 --test_delay 4` — 4-stage pipeline: self-refinement + vote → +column-exploration + rerun → tie-break vote → final-choose. Schema linking emits per-instance reduced DDL bundles inlined into every prompt.

ReFoRCE input subset (`examples_snow_100/`) was built to contain exactly the same 100 instance_ids as our agent benchmark. Snowflake credentials and the spider2-snow JSONL were synced to the upstream `Spider2/` source.

## Results

| Metric | SnowRAG-Agent | ReFoRCE | Ratio |
|:---|---:|---:|---:|
| Gold-match | **84/100 = 84.0%** | 20/100 = 20.0% | **4.2× better** |
| Pass@k | n/a | 29/100 = 29.0% | — |
| Valid SQL | 100% | 98% | — |
| LLM requests | 3,708 | 5,006 | 0.74× |
| Prompt tokens | 17.6M | 133.4M | **0.13×** |
| Total tokens | 19.9M | 137.0M | **0.15×** |
| Estimated cost | ~$9.11 | ~$40.60 | **~78% cheaper** |

## Set Overlap

- Both gold-match: 20 instances (= ReFoRCE's full 20)
- **Only ours: 64**
- **Only ReFoRCE: 0**

Our agent strictly dominates — every instance ReFoRCE solves, we also solve.

Even allowing ReFoRCE its pass@k (any of 8 candidates correct), only 2 instances exist in ReFoRCE pass@k that aren't in our gold-match: sf_bq056 and sf_bq073, both cases where ReFoRCE had a working SQL but its voting picked a wrong one.

## Key Drivers

1. **Retrieval slice vs full schema linking** — our prompt averages 4.7K tokens vs ReFoRCE's 26.7K (5.6× compression).
2. **Deterministic SQL compiler** — `compile_plan` produces correct shape from a `QueryPlan`, freeing the LLM to focus on plan decisions.
3. **Date-shard rewriting** (Fix 4) — recovers GA360-style failures ReFoRCE can't.
4. **Execution-guided structural repair** — error classification, column-existence probes, and EXPLAIN-then-execute cycle.
5. **Voting noise on ReFoRCE side** — pass@k 29 vs final 20 means voting throws away ~30% of correct candidates.

## Detailed Report

`rag_snow_agent/reports/experiments/benchmark_run_12/reforce_comparison_report.md`
