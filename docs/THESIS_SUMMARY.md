# SnowRAG-Agent — Thesis Summary

> One-page executive overview of the master's thesis "Development of an AI Assistant for Big-Data Analytics Using Dynamic RAG and Advanced NLP Methods in ML4Code" (HSE FCS, 2026).
> Full document: [`docs/THESIS_EN.md`](THESIS_EN.md) · Architecture: [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) · Web UI: [`docs/WEB_UI_DESIGN.md`](WEB_UI_DESIGN.md)

---

## Headline result

**SnowRAG-Agent reaches 87 % gold-match accuracy on the Spider 2.0 — Snow benchmark (100 queries, 20 industrial Snowflake databases) at $0.11 per correct answer. Against the published ReFoRCE baseline at the same model size on the same 100 queries, this is a 4.2× gain in accuracy and an 18× reduction in cost per correct answer.**

## Key numbers

| Metric | SnowRAG-Agent | ReFoRCE (same 100q, same `gpt-5.4-mini`) | Ratio |
|:-------|--------------:|-----------------------------------------:|:-----:|
| Gold-match accuracy | **87 %** (100q) / **92 %** (25q) | 20.0 % | **×4.2** more accurate |
| Total tokens spent | 19.9 M | 137.0 M | **×0.15** (6.9× fewer) |
| Run cost | ~$9.11 | ~$40.60 | ×0.22 (4.5× cheaper) |
| Cost per correct answer | **$0.11** | $2.03 | **×0.05 (18× cheaper)** |
| Compared baselines (25q) | 92 % | DSR-Lite 0 %, Spider-Agent + GPT-4o 12 %, ReFoRCE 36 % | — |

## What the project delivers

1. **An entity-oriented dynamic RAG architecture for Text-to-SQL**, where every index entry is a discrete metadata element (table card, column card, join card, semantic fact, sample record, prior-success trace, Snowflake syntax fragment) rather than a fixed-length text chunk. Hybrid retrieval (dense `text-embedding-3-large` + lexical BM25) is fused via Reciprocal Rank Fusion (k = 60).
2. **A deterministic Plan → SQL compiler** that takes the LLM's structured `QueryPlan` and emits Snowflake-correct SQL: stable aliases, double-quoted identifiers, `LATERAL FLATTEN` synthesis, multi-stage CTE assembly, and post-processing for date-partitioned tables. The LLM no longer owns SQL formatting.
3. **Partition collapsing at index time** for date-sharded sources such as GA360 (366 daily tables → 1 representative card) — the precondition that makes RAG applicable to industrial warehouses with temporal sharding.
4. **Best-of-N with strategy diversification** (N = 8 prompt rotations: `default`, `join_first`, `metric_first`, `time_first`, `flatten_first`, `cte_first`, `geo_first`, plus a second `default`) and a multi-signal candidate selector combining execution success, shape alignment, repair-count penalty, and a learned verifier.
5. **A self-correction loop classified by error category** (`INVALID_IDENTIFIER`, `OBJECT_NOT_FOUND`, `AGGREGATION_ERROR`, `RESULT_MISMATCH`, etc.) with category-specific repair prompts and a safety cap of `max_repairs = 4` per candidate.
6. **A production-ready web application** — FastAPI backend + React frontend, deployed with Docker Compose to a public host, with a settings panel that lets analysts change LLM model, tune Best-of-N / repair parameters, and toggle individual RAG components for interactive debugging.
7. **Reproducibility infrastructure** — every benchmark run produces a `manifest.json` with the full config snapshot, an `instance_results.jsonl` per-question log, a token-usage summary, and a comparative report against the upstream ReFoRCE baseline.

## What is new

- **First demonstration on Spider 2.0 — Snow that RAG outperforms full agentic pipelines on industrial schemas while costing 6.9× fewer tokens.** Prior published methods (DSR-Lite, Spider-Agent + GPT-4o, ReFoRCE) all rely on agentic schema linking with the full DDL inlined; SnowRAG-Agent shows that structural metadata selection dominates the choice of model and pipeline depth.
- **The deterministic Plan → SQL compiler** as an architectural separation between LLM reasoning and SQL formatting. Disabling the compiler costs 36 percentage points of accuracy on the ablation.
- **Partition collapsing as a first-class indexing step** for temporal sharding. Removing this step makes GA360 (the largest database in the benchmark) physically unreachable: accuracy collapses from 92 % to 0 % on that domain alone.
- **Multi-stage SchemaSlice post-processing** — protected identifiers, 1-hop join-graph expansion, VARIANT field enrichment with ARRAY/OBJECT classification, and a 10 000-token budget enforced by relevance-aware trimming.

## What contributes most to accuracy

Source: 25-query controlled ablation, three runs with fixed seeds (σ ≤ 2 pp). Full table: [`docs/architecture/feature_impact_report.md`](architecture/feature_impact_report.md).

### Tier A — removing any one of these collapses the system (≥50 pp drop)

1. **LLM-profiled column descriptions** — **−68 pp**. Offline profiling of 100 sample rows per table feeds GPT-5.4 to generate natural-language descriptions that anchor every `VARIANT` access path and date format. Without these, the model hallucinates field paths blind.
2. **Partition collapsing for GA360-style temporally-sharded tables** — **−84 pp on GA360**. 366 daily shards otherwise drown the retriever in duplicate cards.
3. **Structural RAG itself vs. baseline LLM with full DDL** — **−56 pp**. Confirms that *what* you put in the context window matters more than the model choice.

### Tier B — high impact (30–50 pp)

4. **VARIANT field enrichment with ARRAY / OBJECT classification** — **−45 pp**. PATENTS goes from 55 % to 100 %.
5. **`LATERAL FLATTEN` compilation in the SQL compiler** — **−39 pp**. The single most error-prone Snowflake construct, handled deterministically.
6. **Deterministic Plan → SQL compiler** — **−36 pp**. Eliminates a whole class of formatting and quoting errors.

### Tier C — significant (10–30 pp)

7. **Best-of-N strategy diversification (N=8 vs N=1)** — **−28 pp**.
8. **Best-of-N repair loop (`max_repairs=4` vs `0`)** — **−21 pp**. ~80 % of fixes land in iterations 1–2.
9. **1-hop neighbour expansion in the join graph** — **−12 pp**. Recovers auxiliary tables that primary retrieval misses.

### Tier D — refinement (<10 pp)

10. **`sample_records`** (−10 pp), **`semantic_cards`** (−8 pp), **`trace_memory`** few-shot (−4 pp). Individually small, collectively responsible for the system's robustness on borderline phrasings.

## Why this matters for the business

- **Cost efficiency at industrial scale.** $0.11 per correct answer makes embedded use in BI tools (interactive dashboards, ad-hoc analyst queries) economically viable. A naive ReFoRCE-style pipeline costs $2.03 per correct answer at the same accuracy ceiling — 18× more.
- **No model lock-in.** The architecture is provider-neutral: any retrieval-capable LLM with structured-output support works. The thesis used `gpt-5.4` / `gpt-5.4-mini`; switching to a same-tier model from another vendor requires only an SDK swap.
- **Operational simplicity.** Index once per database (profiling + embedding take minutes per source); subsequent queries hit the index and the agent. No retraining, no fine-tuning, no GPU.
- **Reproducible benchmarking.** Every run is captured in `rag_snow_agent/reports/experiments/<run_id>/` with config snapshot, per-question results, and token accounting. Comparative report against the upstream ReFoRCE baseline is regenerated on every benchmark execution.

## Software deliverables

- **`rag_snow_agent/`** — Python 3.11 package: indexing pipeline, hybrid retriever, schema-slice post-processing, Plan → SQL compiler, Best-of-N orchestrator, self-correction loop, learned verifier. ChromaDB-backed vector store.
- **`rag_snow_agent/frontend/`** — React + Vite single-page application; chat-style query UI; collapsible execution log; live schema browser; settings panel for model / parameter / RAG-component toggles.
- **`rag_snow_agent/docker-compose.yml`** — backend + frontend + ChromaDB persistence, ready to deploy on any container host. Public deployment is the demo for the project.
- **Reproducible benchmark harness** — scripts under `rag_snow_agent/scripts/` with `--limit`, `--num_votes`, `--max_repairs` flags; manifests captured automatically.

## Remaining gaps (Run 10, 13 % miss budget)

| Gap | Failures | Path forward |
|:----|:--------:|:-------------|
| Geospatial queries (`ST_DISTANCE`, `ST_DWITHIN`, …) | 7/13 | Add geospatial ops to the Plan → SQL compiler |
| Complex multi-step reasoning (`gpt-5.4-mini` capability ceiling) | 3/13 | Promote hard queries to `gpt-5.4` |
| Schema noise on 296-table sources | 2/13 | Tighter retrieval pruning + verifier-driven re-ranking |
| Transient API errors | 1/13 | Retry policy already specified, not yet enforced everywhere |

## References

- Thesis full text: [`docs/THESIS_EN.md`](THESIS_EN.md)
- Architecture: [`docs/ARCHITECTURE.md`](ARCHITECTURE.md)
- Feature impact (detailed ablation): [`docs/architecture/feature_impact_report.md`](architecture/feature_impact_report.md)
- Comparative benchmark report: [`docs/benchmarks/benchmark_run_12_reforce_comparison_2026_05_07.md`](benchmarks/benchmark_run_12_reforce_comparison_2026_05_07.md)
- Web UI design: [`docs/WEB_UI_DESIGN.md`](WEB_UI_DESIGN.md)
