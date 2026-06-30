# Text-to-SQL Research — Architecture, Results, and SOTA Comparison

*Spider 2.0-Snow: 547 NL→SQL tasks over enterprise Snowflake databases.*
*Source data: `GOLD_FED_ABLATION_REPORT.md`, `GOLD_FED_100Q_REPORT.md`, `NOGOLD_EXPERIMENT_REPORT.md`, `FINAL_REPORT.md`, `PRESENTATION.md`.*

---

# PART 1 — GOLD-ASSISTED SOLUTION

*The gold-fed system uses the reference answer as a stopping signal during self-refinement.
This is the **research ceiling** — not deployable accuracy. It is the cleanest setting for
isolating component value because the oracle removes selection ambiguity.*

---

## 1. Architecture

Five layers, each addressing a concrete failure class in enterprise Text-to-SQL.

### Layer 1 — Hybrid retrieval index

**What:** BM25 + `text-embedding-3-large` with Reciprocal Rank Fusion (k = 60) over a
purpose-built entity-oriented index stored in ChromaDB.

**Why:** Full-schema prompting fails on enterprise warehouses (3 000+ columns). Retrieving the
right schema cards rather than dumping everything is the prerequisite for every other layer.

**Index cards (one discrete metadata element each):**

| Card type | What it captures |
|:----------|:----------------|
| `TableCard` | Table name, description, row count, business role |
| `ColumnCard` | Column name, type, description, sample values, cardinality hint |
| `JoinCard` | Explicit join relationships (FK, implicit, common-key) |
| `SemanticCard` | Business term → column mappings; synonym dictionaries |
| `SampleRecord` | Representative row values per table (value grounding) |
| `TraceMemory` | Successful past query traces (not yet wired into production path) |
| `SnowflakeSyntax` | Dialect tips: LATERAL FLATTEN, VARIANT access, NULLS LAST, date casts |

**Partition collapsing:** temporally-sharded sources (GA360: 366 daily tables, GA4) are collapsed
at index-build time into one representative `TableCard` per logical table. Without this, GA360
schema linking is retrieval-noise — the 366 shards overwhelm retrieval ranking.

**SchemaSlice post-processing:** after retrieval, a deterministic multi-stage filter trims the
retrieved set further:
- Protected identifiers (join columns, time columns) resist trimming
- 1-hop join-graph expansion: add tables reachable via one FK hop from any retained table
- VARIANT enrichment: inject `SnowflakeSyntax` cards for tables with nested JSON/ARRAY columns

**Retrieval recall — honest gap:** we have not computed a direct schema-linking recall metric
(no ground-truth table/column sets for the Spider 2.0-Snow queries). The only proxy we have is
the schema indexing coverage result (§3f): 5/36 queries execute when the DB is *not* indexed
vs 19/20 when it is — which measures coverage, not column-level recall quality. Two known failure
classes attributable to retrieval: (1) `CENSUS_BUREAU_ACS_2` (296 tables — retrieval noise
causes rank dilution on broad schemas); (2) `GEO_OPENSTREETMAP` spatial tables missing from
retrieval when non-spatial cards crowd out the top-K. For comparison: APEX-SQL measures schema
linking on the same benchmark at **88.33% Strict Recall Rate** (correct table+column sets in
top results, N=120 pilot). We do not have an equivalent number.

---

### Layer 2 — Plan → SQL compiler (deterministic)

**What:** the LLM does not emit SQL directly. It emits a structured `QueryPlan` (YAML/JSON
describing tables, columns, joins, filters, aggregations, output columns). A deterministic Python
compiler transforms the plan into Snowflake SQL.

**Why:** the LLM's syntactic priors are wrong for Snowflake. `LATERAL FLATTEN`, `VARIANT:field`,
`TO_TIMESTAMP(col/1e6)::DATE`, quoting rules — prompting doesn't reliably override these. A
compiler that *owns* syntax closes the entire class of dialect-formatting errors.

**Effect:** the LLM owns *intent*; the compiler owns *correctness*. Semantically correct plans
that would have produced syntax errors now execute.

**Current compiler plan types:** `PlanSelect`, `PlanAggregate`, `PlanJoin`, `PlanFlatten`,
`PlanCTE`, `PlanWindowFunction`, `PlanDateShard`. `PlanGeoJoin`/`PlanGeoFilter` are partially
implemented (geospatial is the largest remaining gap — see §5).

---

### Layer 3 — Best-of-N with structural strategy diversification

**What:** generate N SQL candidates, each from a structurally different prompting strategy, then
select the best by a multi-signal selector.

**Strategies (cycled by index, one per candidate):**

All strategies share the same two-step pipeline — the LLM produces a structured JSON plan, a
deterministic compiler emits Snowflake SQL. The strategy is a single extra instruction prepended
to the plan-generation system prompt, biasing *where the LLM starts reasoning*:

| Strategy | Opening instruction to LLM | Targets |
|:---------|:---------------------------|:--------|
| `default` | *(no extra hint)* | General queries |
| `flatten_first` | Start by identifying VARIANT/ARRAY columns needing LATERAL FLATTEN | Nested / semi-structured data |
| `cte_first` | Break the question into sequential steps; each step becomes a CTE | Multi-step aggregations, ranking, set ops |
| `join_first` | Start by identifying the correct JOIN relationships; build outward from joins | Multi-table joins |
| `metric_first` | Start by identifying the target metric/aggregation, then trace back to sources | COUNT/SUM/AVG questions |
| `time_first` | Start by identifying date/time filters or time-based grouping | Time-series, date-range filters |
| `geo_first` | Start by identifying geospatial predicates (ST_WITHIN, ST_DWITHIN, radius…) | Spatial / location queries |

Rotation order for N=4: `default` → `flatten_first` → `cte_first` → `join_first`. For N=8 all
seven strategies fire, then `default` repeats. Temperature ramps from T=0.2 (candidate 1) to
T=0.3 (candidates 2+) for additional stochastic diversity on top of the structural hint.

This creates *structural* diversity, not stochastic re-sampling of the same prompt.

**Selection (multi-signal):** the selector scores candidates on: (1) execution success,
(2) result non-emptiness, (3) result-set agreement across candidates (self-consistency voting),
(4) fingerprint verification, (5) metamorphic check, (6) learned verifier score (when trained).

**Gold-fed mode:** gold result is used as an additional selection signal — the selector picks the
candidate whose executed result matches gold. This is the oracle that creates the gold-fed ceiling.

---

### Layer 4 — Self-correction repair loop

**What:** after a candidate executes (success or error), an LLM-driven repair loop inspects
`(question, SQL, execution result or error)` and attempts targeted fixes.

**Gold-fed signal:** when `--gold_dir` is active, the loop injects a synthetic `RESULT_MISMATCH`
error if the result differs from gold — driving repair even on executing-but-wrong SQL.

**Error categories (8 Snowflake-specific classes):**
`SYNTAX_ERROR`, `COLUMN_NOT_FOUND`, `TYPE_MISMATCH`, `RESULT_MISMATCH`, `EMPTY_RESULT`,
`WRONG_AGGREGATION`, `WRONG_JOIN`, `VARIANT_ACCESS_ERROR`

Each category triggers a different repair prompt with category-specific guidance (e.g.
`VARIANT_ACCESS_ERROR` hints at `:field` vs `['field']` vs `GET_PATH`).

**Budget:** `max_repairs` attempts per candidate (typically 3–4). Repair stops on gold match
(gold-fed) or when the budget is exhausted.

---

### Layer 5 — Verification (fingerprint + metamorphic)

**What:** two lightweight post-execution checks on the selected candidate.

- **Fingerprint check:** hashes the executed result and checks it against a cached fingerprint for
  the same question. Catches exact repeats of known-wrong results.
- **Metamorphic check:** runs a semantically equivalent reformulation of the question and checks
  whether both produce consistent results. Catches silent semantic errors.

**Observation:** both checks fired 0% on the ablation's 25-query slice — these components are
targeted at failure modes that appear in larger or more diverse query sets.

---

## 2. Gold-fed results

### 2a. Headline — 100-query benchmark (Best-of-N = 8, three independent runs)

*The primary gold-fed result: the full system run across **100 representative Spider 2.0-Snow
instances** (broader than the ablation slice — includes geospatial databases), replicated three
times to estimate variance. Model: gpt-5.4-mini. Best-of-N = 8, max_repairs = 4.*

| Run | Accuracy |
|:----|---:|
| benchmark_run_10 | 87% (87/100) |
| benchmark_run_11 | 81% (81/100) |
| benchmark_run_12 | 84% (84/100) |
| **Mean** | **84%** (range 81–87%) |

**The gold-assisted full system solves ~84% of a representative 100-query Snow set.**
Run-to-run variance ±3 pp is real: the ±3 pp band is not noise — it reflects a genuine 14-query
flaky core that sometimes succeeds, sometimes doesn't (stochastic sampling at temperature 0.2).

**Per-instance stability:**

| Outcome | Count |
|:--------|------:|
| Always correct (3/3 runs) | 76 |
| Flaky (1–2 of 3 runs) | 14 |
| Never correct (0/3 runs) | 10 |

Reliable solved core: **76%**. Hard unsolvable core: **10%**. The honest "reliable capability"
is 76%; the best-case is 87%; the mean ceiling is 84%.

**Domain breakdown (majority of 3 runs):**

| Database | Solved | |
|:---------|:------:|:---|
| GITHUB_REPOS | 15/15 | ✅ |
| NOAA_DATA | 12/12 | ✅ |
| PATENTS | 14/15 | ✅ |
| GA360 | 10/12 | ✅ |
| CMS_DATA | 7/7 | ✅ |
| CENSUS_BUREAU_ACS_2 | 2/4 | ⚠️ retrieval noise on 296-table schema |
| GEO_OPENSTREETMAP | 3/6 | ❌ |
| NEW_YORK_NOAA | 1/3 | ❌ |
| NEW_YORK_CITIBIKE_1 | 1/3 | ❌ |

Geospatial (`ST_*` / distance predicates) is the dominant weak domain. Conventional analytics
(patents, GitHub, weather, GA360, healthcare) are handled well at Best-of-N = 8.

**Connection to ablation:** the 30-query component ablation showed Best-of-N is the dominant
component (+80 pp). This 100-query run is that recipe at its strong setting (N=8 + 4 repairs),
confirming the finding scales: the same architecture that scores 90% on the easier 30-query
non-geo slice scores 84% on the harder geo-inclusive 100-query set.

---

### 2b. Component ablation (n = 30, gpt-5.4-mini, Best-of-N = 4, max_repairs = 3)

*Paired leave-one-out: each arm removes one component; all others held fixed. Gold-fed throughout.
This identifies which components drive the 84–90% ceiling.*

| Rank | Component removed | Accuracy | Δ vs full | 95% CI | Tier |
|-----:|:------------------|---:|---:|:---:|:---|
| — | **Full system** | **90.0%** (27/30) | — | — | reference |
| 1 | **Best-of-N** | 10.0% (3/30) | **−80.0 pp** | [63.3, 93.3] | **Dominant** |
| — | **Bare baseline** (all off) | 3.3% (1/30) | **−86.7 pp** | [73.3, 96.7] | — |
| 2 | Repair loop | 73.3% (22/30) | −16.7 pp | [0.0, 33.3] | Signal (weak) |
| 3 | Sample-records prompting | 80.0% (24/30) | −10.0 pp | [0.0, 23.3] | Within noise |
| 4 | Verification | 83.3% (25/30) | −6.7 pp | [−6.7, 20.0] | Within noise |
| 5 | Semantic layer | 83.3% (25/30) | −6.7 pp | [0.0, 16.7] | Within noise |
| 6 | Learned verifier | 83.3% (25/30) | −6.7 pp | [−6.7, 20.0] | Within noise |

**Token cost by arm:**

| Arm | Total tokens | Avg LLM calls/q |
|:----|---:|---:|
| Full system | 2,670,814 | 17.2 |
| − Best-of-N | 673,291 | 5.3 |
| − Repair loop | 1,537,049 | 8.0 |
| Bare baseline | 327,038 | 3.0 |

**Key readings:**
- Best-of-N uses **4× tokens** for **+80 pp accuracy** — unambiguously worth it.
- Repair loop costs **~2× tokens** for **+16.7 pp** — real but the CI floor touches 0.
- Knowledge components (semantic, sample-records, verifier, verification) show **no statistically
  reliable gain at n=30** — not proven useless (true +5–10 pp effects are invisible at n=30),
  but require validation at larger N before further investment.

---

### 2c. 25-query sweep with cost tracking (gpt-5.4-mini, Best-of-N = 4)

*Full 8-cell leave-one-out with token and dollar-cost telemetry. Reference: A0_full.*

| Cell | Accuracy | Δ acc | Tokens | $ cost | Tok/correct | Wall-clock |
|:-----|---------:|------:|-------:|-------:|------------:|----------:|
| **A0_full** (reference) | **80.0%** | = | 2,058,508 | $0.437 | 102,925 | 302 s |
| A1 no Best-of-N | 16.0% | −64 pp | 553,626 | $0.121 | 138,406 | 256 s |
| A2 no Verification | 88.0% | +8 pp | 2,088,229 | $0.444 | 94,920 | 342 s |
| A3 no Repair | 68.0% | −12 pp | 1,203,450 | $0.252 | 70,791 | 75 s |
| A4 no Sample-records | 92.0% | +12 pp | 1,821,852 | $0.409 | 79,211 | 108 s |
| A5 no Join-graph | 80.0% | 0 pp | 2,068,766 | $0.441 | 103,438 | 103 s |
| A6 no Semantic | 84.0% | +4 pp | 2,046,037 | $0.436 | 97,430 | 115 s |
| A7 Baseline | 8.0% | −72 pp | 263,548 | $0.057 | 131,774 | 24 s |

**$0.022 per correct answer at Best-of-N = 4, 25-query slice.**

**Activation rates in A0_full:**

| Component | Fired | Rate |
|:----------|------:|-----:|
| Best-of-N | 25/25 | 100% |
| Semantic layer | 25/25 | 100% |
| Sample records | 25/25 | 100% |
| Date-shard rewriter | 4/25 | 16% |
| Join-graph expansion | 0/25 | 0% |
| Geo-model routing | 0/25 | 0% |
| Trace memory (read) | 0/25 | 0% |
| Learned verifier | 0/25 | 0% |

Zero-rate components are **targeted triggers** — they fire on the right query class. Join-graph
(0% on 25 non-geo) fires 5× on the 100-query geo-inclusive benchmark.

---

### 2d. Best-of-N scaling curve (same 25 instances, gpt-5.4-mini)

| N | Source | Accuracy | Tokens | Cost |
|--:|:-------|---:|---:|---:|
| 1 | A1_no_best_of_n | 16% | 554k | $0.12 |
| **4** | **A0_full (ablation reference)** | **80%** | 2.06M | $0.44 |
| 8 | benchmark_run_gpt54mini_25 | **100%** | 4.22M | $0.90 |

The scaling is **monotonic and large**: every doubling of N adds 64 pp then 20 pp. The 5 failures
at N=4 decompose as: 1 unreachable (needed strategy index ≥ 5), 4 lost to sampling variance
at the winning strategy (existed at N=4 but wasn't drawn). The architecture's measured ceiling on
this 25-query slice is **100% at N=8**.

Note: the 25-query slice is non-geo only; the 100-query run (§2a) with N=8 hits 84% on the harder
geo-inclusive set. The ceiling is query-class dependent, not a universal 100%.

---

### 2e. Interpreting the gold-fed number — what it means and doesn't

Gold is consumed in **two separate places**:
1. **Selection** — pick the best-of-N candidate whose result matches gold
2. **Repair** — keep iterating toward gold until it matches

The ablation's `no_repair` arm removed only the repair *iterations* with gold *selection still
on*. So "+16.7 pp for repair" means the value of iterative repair **given** gold-assisted
selection — not the value of gold itself.

| Config | Gold for selection? | Gold-driven repair? | Accuracy |
|:-------|:-------------------:|:-------------------:|---:|
| Full gold-fed | ✅ | ✅ | ~84–90% |
| No-repair arm (ablation) | ✅ | ❌ | ~73% |
| **No gold at all (deployable)** | ❌ | ❌ | **~33–60%** |

**Going to no-gold removes the signal from both** — and the bigger loss is selection (~40 pp
combined), not the repair loop (~17 pp). The gold-fed ceiling is the target a good
*no-gold verifier* would chase; it is not deployable accuracy.

---

# PART 2 — NO-GOLD (DEPLOYABLE) SOLUTION

*No gold oracle at any point during inference. Scored post-hoc by running predictions against
gold after the fact, with the agent fully blind.*

---

## 3. What changed: the no-gold architecture

### 3a. Self-consistency / MBR voting (replaces gold-assisted selection)

**Implementation:** `selector.py` / `best_of_n.py`.

Candidates are clustered by an order-insensitive signature of their **executed result** (not SQL
text). A candidate receives `consensus_bonus × (independent_votes − 1)`, where a "vote" counts
only from a **distinct strategy** — so running the same strategy twice cannot inflate its score.
The candidate with the highest total bonus wins.

**Why result-clustering, not SQL-text clustering:** two queries with different SQL but identical
result sets are equivalent. Result-cluster voting selects by *semantic correctness*, not syntactic
similarity.

**Why consensus works:** independent strategies produce the same result only when they agree on
table semantics, join path, and aggregation logic. Accidental agreement across structurally diverse
strategies is rare; genuine agreement is evidence of correctness.

---

### 3b. Self-critic repair loop (replaces gold-driven RESULT_MISMATCH)

**Implementation:** `refiner.py` — `SELF_CRITIQUE` repair mode.

An LLM inspects `(question, SQL, result preview)` and, if it diagnoses a concrete error, drives
a repair attempt. It never marks a successfully-executing SQL as failed — it only spends extra
repair budget when it finds a specific problem.

**Diagnostic quality vs repair quality:** the self-critic's diagnoses are model-independent (both
gpt-5.4-mini and gpt-5.4 produce sharp diagnoses like *"returns only one month but the question
asks for June and July"*, *"scans only EVENTS_20210131 so the Jan 1–7 filter can't access those
dates"*). But **repair quality is model-dependent** — only gpt-5.4 can translate a correct
diagnosis into a fixed query. This is the model × method interaction.

**Robustness guard:** on `sf_bq269` the critic's repairs pushed all 4 candidates into
`sql_syntax_error` (baseline: wrong-but-executing → improved: broken). Fix: revert to the last
successfully-executing SQL when a repair breaks execution.

---

### 3c. Exploration front-end

Runs read-only SQL probes *before* generating the answer query:
- **Entity resolution:** `SELECT DISTINCT col FROM table LIMIT 20` — discovers real values for
  filter conditions (product names, category codes, ID formats).
- **Format detection:** probes epoch-vs-YYYYMMDD date columns, VARIANT field names, nested array depth.
- **Existence checks:** confirms the candidate tables are non-empty and the relevant columns exist.

Results feed the prompt as grounding facts, replacing model guesses with real database evidence.

---

### 3d. Deterministic data-format enforcement

**Problem:** the model's priors (e.g. "date = YYYYMMDD") override soft prompt hints reliably.
A query that should use `TO_TIMESTAMP(col/1e6)::DATE` will get `CAST(col AS DATE)` from prompting.

**Solution:** code-level rewrite rules in `prompting/sql_compiler.py` that detect and fix:
- Epoch-microsecond integer columns: `col` → `TO_TIMESTAMP(col/1e6)::DATE`
- Native date columns incorrectly cast: fix to `DATE 'YYYY-MM-DD'` literals
- VARIANT access: normalize `:field` vs `['field']` vs `GET_PATH(col, 'field')` per dialect

These are applied *after* generation, deterministically, regardless of what the LLM emitted.

---

### 3e. Information-aggregation planning

A structured pre-generation step that builds a plan before emitting SQL:
- Entity → column mapping (which real column names map to the question's entities)
- Join path (which tables connect, via which keys)
- Constraint checklist (every filter condition explicitly listed)
- Format reconciliation (date encodings, ID types, VARIANT paths resolved from exploration)

**Effect:** eliminates dropped constraints (a missing filter that the model silently ignored).
Single-instance recovery: `sf_bq153` (a specific gene filter constraint that vanilla generation dropped).

---

### 3f. Schema indexing as infrastructure prerequisite

| Schema indexed in vector store? | Executed successfully |
|:-------------------------------|:---------------------:|
| No (agent starts blind) | **5/36** |
| Yes (after `build_index`) | **19/20** |

A "low accuracy" run on an unindexed database is an infrastructure gap, not a model failure.
Indexing coverage must be verified before attributing failures to the architecture.

---

## 4. No-gold results

### 4a. Oracle gap (hardest slice — GA360/GA4, first 10 non-geo instances)

| Mode | Accuracy |
|:-----|---:|
| Gold-fed (sees gold, repairs to it) | 10/10 (100%) |
| No-gold baseline | 1/10 (10%) |
| No-gold improved (gpt-5.4 + consensus + self-critic) | **5/10 (50%)** |

**The gold→no-gold gap on this hardest slice is ~90 pp.** The improved no-gold system recovers
half the ceiling.

---

### 4b. Model × method interaction

*Same 10-instance hard slice (GA360/GA4), no-gold:*

| | gpt-5.4-**mini** | gpt-5.4 |
|:---|:---:|:---:|
| Baseline (no consensus, no self-critic) | 1/10 | 1/10 |
| + self-consistency + self-critic (max 3) | **1/10** | **5/10** |

**Agentic techniques are amplifiers, not substitutes for model capability.**
- mini + methods = 1/10 (same as baseline): the critic diagnoses errors correctly but mini
  cannot translate them into fixed SQL.
- gpt-5.4 + methods = 5/10 (+400%): the same diagnoses now produce corrected candidates.
- LLM call count: 88 → 152 calls (+73%) for +4 correct answers — efficient.

---

### 4c. Attribution — what the improved no-gold system actually does

*Per-candidate gold-checking shows exactly where the +4 instances came from (gpt-5.4 arms):*

| Arm | Selected correct | Any correct in pool |
|:----|:---:|:---:|
| Baseline | 1/10 | 2/10 |
| Improved (consensus + critic) | **5/10** | **5/10** |

- **Self-critic *creates* correct candidates:** pool grew from 2 → 5 instances with ≥1
  gold-correct candidate. It rescued `sf_bq011`, `sf_bq010`, `sf_bq002` — all cases where the
  initial generation was wrong but the repair was successful.
- **Consensus *captures* the winners:** baseline left `sf_bq001` on the table (correct
  candidate existed but heuristic picked wrong). Improved selection captured it with no regressions.
- The two components are **complementary**: self-critic raises the generation ceiling;
  consensus raises the selection capture rate.

---

### 4d. Conventional schemas — fix-by-fix progression (no-gold, n=20, lenient scoring)

| Stage | Score | Δ | What was fixed |
|:------|:-----:|:-:|:---------------|
| Baseline (front-end, bon=4) | 8/20 | — | — |
| + constraint-complete plan | 9/20 | +1 | `sf_bq153`: dropped gene filter constraint |
| + 8-candidate voting | 10/20 | +1 | `sf_local056`: selection ambiguity resolved |
| + deterministic date-encoding rewrite | 11/20 | +1 | `sf_bq121`: epoch vs YYYYMMDD |
| + native-date rewrite / final bon=8 | **12/20** | +1 | `sf_local068`: date cast pattern |

**8/20 → 12/20 (40% → 60%)** over the project. Each fix recovers ~1 instance; they compound
without regressing earlier gains. Diminishing returns are real and expected — the remaining 8
failures are structural (recursive CTEs, complex multi-step logic) rather than format bugs.

---

### 4e. Overall no-gold accuracy (gpt-5.4, n=20 conventional schemas)

| Configuration | No-gold accuracy |
|:------|:----------------:|
| **Raw LLM baseline** — full schema in context, single generation, no architecture | **35% (7/20)** |
| **SnowRAG-Agent best config** — Best-of-N=4, repair, exploration, planning | **60% (12/20)** |
| Hard nested/analytics — GA360/PATENTS (n=10 hard slice) | **33%** (control) → **50%** (improved) |

The architecture adds **+25 pp** over raw LLM on the same query set.

**Failure taxonomy on remaining misses:**

| Class | Example | Status |
|:------|:--------|:-------|
| Value/date-format misread | `sf_bq121`, `sf_local064` | ✅ deterministic rewrite (done) |
| Dropped constraint | `sf_bq153` | ✅ constraint-complete plan (done) |
| Wrong literal → empty result | GA360 product names | ✅ exploration entity resolution (done) |
| Recursive-CTE dialect | `sf_local269` | ⚠️ recipe added; still hard |
| Complex multi-step / domain logic | `sf_local309`, `sf_bq193` | ❌ architectural ceiling |
| Geospatial (`ST_*`) | 7 of 16 failures in 100q run | ❌ compiler gap (`PlanGeoJoin` not wired) |

---

### 4f. Benchmark validity caveat

**~63% of Spider 2.0-Snow gold SQL is erroneous or ambiguous** (VLDB 2026, *"Pervasive
Annotation Errors"*). Concrete cases we hit:
- Gold stores IDs as integers → drops leading zeros → marks correct prediction wrong
- Column-order / column-name differences → exact-match false-negative
- Decimal vs float representation mismatches

**We use lenient any-gold scoring** (`audit_gold_matches.py`): score against *any* published
gold variant using exact value-set comparison. Strict official scoring is lower. Do not
over-optimize to exact-match gold on small samples.

---

---

# PART 3 — STATE OF THE ART

---

## 5. APEX-SQL — current academic/open SOTA

*Paper: "APEX-SQL: Talking to the Data via Agentic Exploration for Text-to-SQL"*
*Authors: Bowen Cao, Weibin Liao, Yushi Sun et al. (CUHK / Peking University / Tencent Lightspeed)*
*Venue: KDD 2026. arXiv: 2602.16720. Code: github.com/Tencent/APEX-SQL-Project*

**Core thesis:** static schema representations fail to resolve semantic ambiguity in real
enterprise databases (opaque column names, domain codes, value-format conventions). APEX-SQL
shifts from "passive schema perception" to **agentic exploration** — it forms hypotheses about the
data, verifies them by running SQL against the live database, and only then generates the answer.

---

### Stage 1 — Schema Linking (4 steps)

**Step 1: Hypothesis Generation (Logical Planning)**

- Generates N=2 schema-*agnostic* reasoning plans (temperature 0.8): solution steps in natural
  language ("filter by X, aggregate Y over Z") with **no column names at all**.
- Aggregates to a single master plan (temperature 0.2).
- Purpose: decouple intent parsing from schema vocabulary. The model doesn't latch onto
  superficially similar column names when it reasons without them.
- *Removing this step: −5 pp SRR on Spider 2.0-Snow (120-case pilot).*

**Step 2: Dual-Pathway Pruning**

- Two simultaneous LLM passes over the full column set:
  - **Negative pass:** identify columns to *delete* (confidently irrelevant)
  - **Positive pass:** identify columns to *keep* (confidently needed)
- **Union logic:** a column survives unless *both* passes reject it — high precision without
  sacrificing recall. Equivalent to a conservative AND of two independent pruning signals.
- Compresses ~3 430 columns → ~383 columns (**9× reduction**, ~7k tokens).
- *Ablation (120-case pilot, Strict Recall Rate):*

| Pruning configuration | SRR |
|:----------------------|----:|
| **Both passes (full)** | **97.5%** |
| Selection pass only | 93.3% (−4.2 pp) |
| Deletion pass only | 80.8% (−16.7 pp) |

Baseline ReFoRCE schema linking on the same protocol: **35.0% SRR**.

**Step 3: Parallel Data Profiling**

- Independent agents per table run role-specific exploratory SQL queries against the live database.
- Validates each column's *actual role*: is it a real filter key, a join key, a metric, nullable?
  Not inferred from schema text — validated against actual value distributions.
- Results >30 rows are compressed to top-10 + aggregate statistics.
- Runs in parallel across tables for speed.
- *Removing this step: −9.2 pp SRR.*

**Step 4: Global Synthesis**

- Integrates all cross-table empirical observations.
- Enforces **topological connectivity**: the output schema subgraph D* must have all join paths
  traversable. No dangling tables.
- Includes a **Semantic Linking** sub-step that hypothesises table/column roles *before* profiling,
  directing the probes toward the most relevant areas.

*Ablation on all Stage 1 verification sub-steps (120-case pilot):*

| Configuration | SRR |
|:--------------|----:|
| With logical planning → full verification | **77.5%** |
| Without logical planning | 72.5% (−5.0 pp) |
| Without Semantic Linking | 63.3% (−14.2 pp) |
| Without Data Profiling | 68.3% (−9.2 pp) |
| Without Global Synthesis | 73.3% (−4.2 pp) |
| **Without all agentic verification** | **55.8% (−21.7 pp)** |

---

### Stage 2 — SQL Generation (pre-processing + agentic loop + selection)

**Pre-processing A: Macro Plan Aggregation** (`preprocess_macro_plans.py`)

Multiple reasoning paths from Stage 1 are consolidated into a single unified master plan injected
into the agent's initial context. Prevents the agent from starting blind or from anchoring on a
single reasoning path.

**Pre-processing B: Deterministic Guidance Retrieval** (`preprocess_select_tips.py`)

A **rule engine** (not embedding retrieval) maps operational keywords extracted from the master
plan to categories in a pre-built directive library ℳ containing **14 categories**:

| Category | Examples |
|:---------|:---------|
| Evidence Enforcement | Use only the tables/columns the plan identifies |
| String Matching | ILIKE, LOWER(), TRIM(), exact vs. fuzzy |
| Output Columns | Which columns to SELECT, naming conventions |
| Table Selection | Primary table vs. lookup tables |
| Column Interpretation | NULL meaning, sentinel values, units |
| Schema Grounding | Use the pruned schema, not guesses |
| Join Strategy | Which direction, which key, LEFT vs. INNER |
| Filter Implementation | Correlated vs. independent subqueries |
| Aggregation | GROUP BY, DISTINCT, window vs. aggregate |
| Sorting and Limiting | NULLS LAST, ORDER BY determinism |
| Multi-Step Logic | CTE decomposition, subquery layering |
| SQL Syntax | Snowflake dialect specifics |
| Common Pitfalls | Off-by-one, date boundary, empty-set traps |
| Advanced Patterns | Recursive CTE, LATERAL FLATTEN, PIVOT |

The keyword-to-category mapping achieves >95% recall and is noise-free vs. fuzzy retrieval.

*Ablation — adding deterministic guidance (EX@8 metric):*

| Model | With guidance | Without guidance | Δ |
|:------|:-------------:|:----------------:|:-:|
| GPT-4o | 29.79% | 26.04% | **+3.75%** |
| GPT-5 | 47.92% | 43.65% | **+4.27%** |
| DeepSeek-R1 | 48.75% | 47.50% | +1.25% |

Adding guidance also *reduces* the number of agentic rounds (R̄) without reducing meaningful
query count — the agent is more directed.

**Agentic Exploration Loop (max 40 actions, 56 k token budget)**

SQL synthesis is force-triggered at 52 k tokens to guarantee a candidate before budget exhaustion.

| Action | What happens |
|:-------|:-------------|
| **PROFILING** | Generates exploratory SQL to probe data distributions, NULL rates, FK integrity, value ranges. Results >30 rows compressed to top-10 + statistics. |
| **CONSOLIDATION** | Periodic state compression: prunes interaction history to retain only exploratory queries + execution results + latest consolidated plan. Prevents context bloat within the 56k budget. |
| **SQL SYNTHESIS** | Maps all accumulated evidence to physical SQL. If execution fails, re-enters the loop for targeted repair. |
| **CONFIRMATION** | Secondary semantic verification: does the SQL logic align with user intent, accumulated observations, and the current logical plan? Finalises only when conditions pass. |

**Answer Selection**

| Benchmark | Selection method |
|:----------|:----------------|
| Spider 2.0-Snow | Majority voting on executed results (8 samples); reward model tie-breaker |
| BIRD-Dev | Reward model (`ContextualAI/ctx-bird-reward-250121`) run via `reward.py` |

`--revote` flag re-runs selection on cached samples without regenerating.
`--num_votes 8` default (configurable).

---

### APEX-SQL pipeline diagram

```
NL question
    │
    ▼
┌──────────────────────────── STAGE 1: Schema Linking ─────────────────────────────┐
│                                                                                    │
│  ① Hypothesis Generation (schema-agnostic plans, T=0.8) → merge (T=0.2)         │
│                                                                                    │
│  ② Dual-Pathway Pruning                                                           │
│       negative pass (delete) ──┐                                                  │
│                                ├──► union logic → pruned columns (9× ↓)          │
│       positive pass (keep)  ──┘                                                   │
│                                                                                    │
│  ③ Parallel Data Profiling  → per-table agents run role-specific SQL probes      │
│                                                                                    │
│  ④ Global Synthesis  → enforce topological connectivity → final schema D*        │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────── Pre-processing ──────────────────────────────────────┐
│  ⑤ Macro Plan Aggregation (consolidate reasoning paths)                          │
│  ⑥ Deterministic Tip Retrieval  → rule engine → 14-category library ℳ           │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────── STAGE 2: Agentic Loop ───────────────────────────────┐
│  max 40 actions · 56k token budget · force-trigger synthesis at 52k tokens       │
│                                                                                    │
│   ┌─────────────────────────────────────────────────────────────────┐             │
│   │ PROFILING → CONSOLIDATION → SQL SYNTHESIS → CONFIRMATION        │ ← loop     │
│   └─────────────────────────────────────────────────────────────────┘             │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
Answer Selection: 8 samples → majority vote → reward model tie-breaker
    │
    ▼
Final SQL
```

---

### APEX-SQL benchmark results

| Benchmark | N | Model | EX | Pass@8 |
|:----------|--:|:------|---:|-------:|
| Spider 2.0-Snow | 547 | DeepSeek-R1 | **53.03%** | **68.44%** |
| BIRD-Dev | 1 534 | GPT-4o | **70.7%** | — |
| Spider 2.0-Snow schema linking | 120 | GPT-4.1 | **88.33% SRR** | — |
| Spider 2.0-Snow (leaderboard) | 547 | — | **~73%** | — |

*The leaderboard 73% and the paper's 53.03% differ because the leaderboard uses a different model
(likely a stronger frontier model) and/or a different evaluation script. The paper's 53.03% is
the controlled research number; 73% is the current leaderboard position.*

**Exploration effect — oracle schema baseline (N=120, Spider 2.0-Snow):**

| Model | With exploration EX | Without exploration EX | Δ |
|:------|:-------------------:|:----------------------:|:-:|
| DeepSeek-V3.2 | 57.50% | 39.17% | **+18.33%** |
| GPT-4o | 41.67% | 36.67% | +5.00% |
| Kimi-k2-instruct | 48.33% | 38.33% | +10.00% |

"Rich get richer": stronger models benefit proportionally more from exploration.

**The identified bottleneck:** Pass@8 = 68.44% vs. voted EX = 53.03% — **15 pp lost to answer
selection**. The paper names selection as the primary next frontier.

---

## 6. Other SOTA methods

### DivSkill-SQL (Spider 2.0-Lite SOTA, 73.1%)

Attacks the weakness of naive ensembling — candidates that all fail the same way:

- **Residual Skill Optimization:** builds a library of complementary "skills" (prompt + strategy
  recipes). Each *new* skill is trained on examples the current ensemble *still gets wrong*,
  explicitly maximising **Pass@K** (probability ≥1 of K candidates is correct).
- At test time, several skill-guided agents each solve the query via different interaction
  patterns; results are selected across them.
- *Effect: 3× fewer hallucinated schema/function references vs. stochastic ensembling.*

**Why it outperforms random Best-of-N:** the skills are *failure-targeted* — each new skill
specifically covers the failures the existing ensemble leaves unsolved.

---

### DSR-SQL (Dual-State Reasoning, mid-tier)

Splits the problem into two states:

1. **Adaptive context** (schema compression): an LLM writes exploratory SQL and samples the
   schema 3× at high temperature, then merges — keeping only relevant tables/columns.
2. **Progressive generation** (4-state machine):
   - **Extend**: generates the next sub-question step
   - **Revise**: fixes the last step if execution was wrong
   - **Explore**: probes real data when results look suspicious
   - **Rephrase**: finalises the complete query

   Each intermediate result is *executed* before the next step — incremental verification.
   Also includes **evidence extraction** that compresses external documents to the few facts/
   formulas the specific question needs.

---

### ReFoRCE (prior academic SOTA on Snow, 31% with o1-preview)

Four mechanisms:

1. **Table compression:** groups near-duplicate/date-sharded tables; prunes columns so 3 000+
   column schemas fit in context.
2. **Column exploration:** runs probe SQL before answering — `SELECT DISTINCT`, value samples,
   nested-field inspection — feeding real values/formats back to grounding.
3. **Format restriction:** derives the expected answer's column structure and types; forces
   output to match (fewer wrong-shape / wrong-column answers).
4. **Self-refinement + self-consistency voting:** multiple candidates, each repaired on execution
   feedback, then voted — the answer multiple independent attempts agree on wins.

*Comparison to APEX-SQL's schema linking: ReFoRCE achieves 35% SRR vs. APEX-SQL's 97.5% SRR
on the same 120-case Spider 2.0-Snow protocol — a 62 pp gap attributable to APEX-SQL's
hypothesis-driven dual-pathway pruning and data-profiling verification.*

---

## 7. Databricks Genie — commercial architecture

**Product:** AI/BI Genie — conversational NL-to-SQL, part of the Databricks Data Intelligence
Platform. GA: June 2025. 4 000+ customers in preview.
**No public Spider 2.0 / BIRD benchmark numbers published.**

**Fundamental design philosophy:** the *opposite* of APEX-SQL. Instead of runtime schema
exploration, Genie invests **up-front expert curation** to narrow the LLM's task. Domain experts
build a Genie Space; at query time the LLM selects from pre-verified patterns rather than
reasoning over the full schema from scratch.

### The Genie Space — configuration unit

| Resource | Hard limit | Contents |
|:---------|:----------:|:---------|
| Tables / views | **30** | Unity Catalog tables scoped to this domain |
| Knowledge store snippets | **200** | Column descriptions, SQL expressions (KPIs, filters, row transforms), join definitions, synonyms |
| Instructions | **100** | Parameterized example SQL + general text rules (effective up to ~20 lines) |
| Entity matching | **120 columns, 1 024 values each** | Pre-sampled distinct-value lists for categoricals |

### 5-layer schema linking

1. **Unity Catalog metadata** (always active): schema, types, PK/FK, column descriptions.
2. **Intelligent filtering** (mechanism not disclosed — likely BM25/embedding): selects relevant
   columns and example SQL from the Space at query time.
3. **Entity matching**: pre-sampled distinct-value lists. Resolves *"Florida"* → `WHERE state = 'FL'`;
   *"first level support"* → exact DB code. Filters appear as dropdowns in the UI.
4. **Join relationships**: explicit definitions in the knowledge store; auto-suggested from FK metadata.
5. **Genie Ontology** (workspace-wide): automatic semantic map built from tables, dashboards,
   notebooks, and connected external docs (Google Drive, SharePoint via MCP).

### LLM backbone — compound AI with model rotation

| Mode | Primary model |
|:-----|:-------------|
| Standard Genie Spaces | **Azure OpenAI** (primary); Anthropic Claude (opt-in) |
| Agent Mode / Research Agent | **Anthropic Claude Sonnet** (explicitly documented) |
| Partner AI disabled | Databricks-hosted open-weight fallback |

Databricks rotates models without disclosure; two upgrades documented (Nov 2024, Sep 2025).

### Agentic evolution

| Date | Capability added |
|:-----|:----------------|
| Feb 2025 | Chain-of-Thought reasoning in text-to-SQL model |
| Sep 2025 | **Self-reflection** — checks own SQL before execution |
| Nov 2025 | Research Agent (Agent Mode) — single unified reasoning agent; uses Claude Sonnet |
| Dec 2025 | Semantic ambiguity detection — asks clarifying questions |
| Apr 2026 | Agent Mode public preview (UI only; not exposed via API) |

### Trusted Assets — the key structural differentiator

- **Parameterized example SQL:** if user question matches a trusted asset's NL question, Genie
  runs the verified SQL directly (parameter-substituted), **bypassing LLM generation entirely**.
  Response is flagged "Trusted."
- **SQL UDFs (Unity Catalog):** exact function logic executes, always "Trusted."
- For high-frequency or high-stakes queries, the LLM is completely eliminated from the path.

### Key tradeoff

Genie trades *setup cost* (expert curation of knowledge stores, entity lists, trusted assets)
for *runtime reliability and governance*. APEX-SQL and ReFoRCE do zero setup and explore
at runtime; Genie offloads that cost to up-front domain investment.

---

## 8. SOTA ranked (Spider 2.0-Snow execution accuracy)

| # | System | Snow EX | Open? | Key technique |
|--:|:-------|--------:|:-----:|:--------------|
| 1 | Genloop Sentinel Agent v2 Pro | **96.7%** | ✗ | Frontier model + heavy multi-agent ensemble |
| 2 | Native (usenative.ai) | 96.5% | ✗ | Proprietary agent |
| 3 | QUVI-3 + Gemini-3-pro | 94.2% | ✗ | Frontier model + agent |
| 4 | Tencent TCDataAgent | 94.0% | ✗ | Contextual-scaling agent |
| 5 | Paytm Prism | 90.5% | ~ | Multi-agent consensus/debate |
| 6 | **APEX-SQL** | **~73%** | ✅ | Hypothesis→verify→refine + data profiling |
| — | **Ours** | **~50–60%¹** | ✅ | Exploration + Best-of-N + deterministic format fixes |
| 7 | ReFoRCE (o3) | ~63% | ✅ | Column exploration + self-consistency (strong model) |
| 8 | ReFoRCE (o1-preview) | 31% | ✅ | Column exploration + self-consistency (original) |
| 9 | Spider-Agent | low | ✅ | Sandboxed execution agent, no ensembling |
| — | Databricks Genie | not published | ✗ | Curated RAG + trusted assets |

¹ *Not directly comparable: our number is on a curated conventional subset with lenient scoring.
Full-benchmark official scoring would be lower.*

**Lite track (for reference):** DivSkill-SQL **73.1%** (#1), SOMA-SQL 72.0%, DecisionX 71.8%.

---

## 9. Three-way comparison: our system vs. APEX-SQL vs. Databricks Genie

| Dimension | **Our system** | **APEX-SQL** (KDD 2026) | **Databricks Genie** |
|:----------|:--------------|:------------------------|:---------------------|
| **Design** | Dynamic RAG + deterministic compiler | Agentic hypothesis-verify | Curated RAG + trusted assets |
| **Schema scope** | Full DB via vector/BM25 + SchemaSlice | Full DB; runtime agent exploration | Hard limit: **30 tables** per Space |
| **Schema linking** | Hybrid retrieval (BM25 + embedding, RRF k=60) + SchemaSlice | 4-stage: hypothesis → dual-path pruning → parallel profiling → synthesis | 5-layer: UC metadata + filtering + entity matching + join defs + Ontology |
| **SQL generation** | Plan → SQL compiler (LLM owns intent, compiler owns syntax) | 4-action agentic loop, 56k token budget | CoT + self-reflection; trusted assets bypass generation entirely |
| **Self-correction** | Category-classified repair (8 Snowflake error categories) | SYNTHESIS re-enters loop on error | Self-reflection pre-execution + "Ask for Review" |
| **Best-of-N / voting** | Best-of-N with 6 structural strategies; multi-signal selector | 8 samples; majority vote + reward model | Not disclosed |
| **Large schema handling** | Partition collapsing (366 GA360 tables → 1 card); VARIANT enrichment | 9× column compression; 40-action budget caps runtime | Forces 30-table scope; multiple Spaces for large orgs |
| **Domain adaptation** | Index rebuild on schema change | Pre-processing runs + curated tip library ℳ | High up-front curation |
| **Governance** | None (research) | None (research) | Unity Catalog; per-user permissions; read-only SQL |
| **LLM** | Provider-neutral; `gpt-5.4-mini` today | Any OAI-compatible; best: DeepSeek-R1 | Azure OpenAI standard; Claude Sonnet for Agent Mode |
| **Spider 2.0-Snow EX** | ~50–60% (subset, lenient)¹ | **53.03%** (paper) / **73%** (leaderboard) | Not published |
| **Cost / correct answer** | **$0.022–$0.11** (measured) | Not reported | Not reported |
| **Code** | This repo | github.com/Tencent/APEX-SQL-Project | Closed source |

---

## 10. Where we stand and what closing the gap requires

**The gap to APEX-SQL is architectural, not tuning.**

APEX-SQL's advantage on Spider 2.0-Snow comes from data-grounded exploration during schema
linking — its 97.5% SRR vs our retrieval-based approach on the same schemas. Our Best-of-N
+ deterministic compiler is effective on conventional schemas but doesn't resolve the fundamental
"which column actually means what" ambiguity in enterprise DBs.

**Specific gaps and what fills them:**

| Gap | What APEX-SQL does | Our path |
|:----|:-------------------|:---------|
| Schema ambiguity resolution | Parallel data profiling — runs SQL to validate column roles | Wire exploration front-end into schema-linking stage (currently post-linking) |
| Large column space | Dual-pathway pruning (9× compression, 97.5% SRR) | SchemaSlice is partial; add hypothesis-driven filtering |
| SQL format correctness | 14-category deterministic tip library | Plan→SQL compiler already addresses this; extend to geo |
| Answer selection | Reward model + 8 candidates | Learned verifier (training code exists; not trained yet) |
| Geospatial queries | Not specialised for geo | `PlanGeoJoin`/`PlanGeoFilter` compiler extension |

**Common winning ingredients across all leading systems:**
1. **Strong base model** — gates everything (slide 8: gpt-5.4-mini = no gain, gpt-5.4 = +400%)
2. **Ground in real data** — exploration / parallel profiling (APEX stage 1, ReFoRCE exploration, our front-end)
3. **Many diverse candidates + smart selection** (our Best-of-N −80 pp; APEX Pass@8 gap −15 pp)
4. **Iterative verify-and-refine** with execution feedback
5. **Schema linking / pruning / compression** for 3 000+ column schemas
6. **Dialect discipline** — deterministic enforcement, not soft prompting

*Our ablation confirms #3 is dominant. Our contribution is #2 (value/format grounding) and #6
(deterministic compiler). The leaders' edge is primarily #4 (full hypothesis-verify loops) and
heavier #3.*
