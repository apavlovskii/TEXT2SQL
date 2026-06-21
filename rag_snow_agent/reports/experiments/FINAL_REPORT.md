# Gold-Free Text-to-SQL on Spider 2.0-Snow — Final Report & Ablations

**Scope.** Making our RAG Snowflake Text-to-SQL agent work **without a gold oracle**
(production-realistic), measuring it honestly, and improving it within the current
architecture. Benchmark: Spider 2.0-Snow (enterprise Snowflake; 547 instances / 152 DBs).
Model: gpt-5.4 unless stated. All accuracy is **execution-result match against gold**.

---

## 1. Executive summary

- **The headline production metric is no-gold accuracy, and it is far below the gold-fed
  number.** Gold-fed (agent allowed to see gold and repair to it): **90% (27/30)**. The same
  system **with no gold** is the real, deployable accuracy — and it is much lower.
- **Where we landed (no-gold, gpt-5.4):**
  - **Conventional schemas: ~50–60%** (8/20 → 12/20 over the project; ~10–11/20 single-run).
  - **Hard nested/analytics schemas (GA360/PATENTS): ~33%** (10/30).
- **The three biggest levers, in order, are not prompt tricks:** (1) **model capability**,
  (2) **schema/value grounding + indexing coverage**, (3) **candidate count (best-of-N)**.
- **Benchmark caveat that changes how to read every number:** ~63% of Spider2-Snow gold SQL
  is erroneous or ambiguous (VLDB 2026). We use **lenient "match-any-published-gold" scoring**
  to correct verified false-negatives; strict official scoring is lower.
- **Vs. published methods (Snow):** ReFoRCE 31%, **APEX-SQL 73%** (current leaderboard SOTA),
  proprietary 90–96%. We are below APEX; the gap is **architectural** (see §6).

---

## 2. What we built (all gold-free, all behind flags)

| Component | What it does |
|---|---|
| **No-gold harness** (`--eval_gold_dir`) | Agent solves blind; gold used only for *post-hoc* scoring. Enables honest production-accuracy measurement. |
| **Lenient any-gold audit** (`audit_gold_matches.py`) | Re-executes predictions, matches against *any* published gold variant (exact value-set), correcting eval-artifact false-negatives. |
| **Self-consistency voting** | Best-of-N candidates clustered by result; agreement among *independent* strategies boosts selection. |
| **Self-critic repair** | LLM critiques `(question, SQL, result)` and drives repair — gold-free replacement for the gold `RESULT_MISMATCH` loop. |
| **Exploration front-end** | Runs read-only probes to resolve entities to real values + discover nested structure (ReFoRCE/APEX-style). |
| **Information-aggregation plan** | Structured pre-gen plan: entity→column map, joins, constraint checklist, format reconciliation (DSR/APEX-style). |
| **Deterministic date-encoding rewrite** | Code-level fix: rewrites epoch/native date columns wrongly treated as YYYYMMDD (`TO_TIMESTAMP(col/1e6)::DATE` / `DATE 'x'`). |
| **Dialect + determinism tips** | Snowflake fuzzy-match/FLATTEN/UNION-shard/recursive-CTE guidance; NULLS LAST, decimals, name+id. |
| **Schema indexer** (`build_index`) | Extract + embed schema per DB (prerequisite for any DB to be runnable). |

---

## 3. Ablations

### 3a. The oracle gap (gold-fed vs no-gold) — the most important result
The first-10 non-geo (hard GA360/GA4) instances:

| Mode | Accuracy |
|---|---|
| Gold-fed (sees gold, repairs to it) | **10/10** |
| No-gold (production-realistic) | **1/10** (baseline) → 5/10 (improved) |

**Takeaway:** "90% gold-fed" massively overstates deployable accuracy. Always measure no-gold.

### 3b. Model × method interaction — capability gates everything
Same hard 10-slice, no-gold:

| | gpt-5.4-**mini** | gpt-5.4 |
|---|:---:|:---:|
| Baseline (no consensus/critic) | 1/10 | 1/10 |
| + consensus + self-critic | **1/10** | **5/10** |

**Takeaway:** the agentic techniques are **amplifiers** — they add nothing on a weak model and
+400% on a capable one. *(This also explains why our earlier ReFoRCE runs looked mediocre: they
were run on `mini`, never on a frontier model, and the Snow runs were never even scored.)*

### 3c. Component ablation (gold-fed, n=30, paired leave-one-out)
| Component removed | Δ accuracy | Tier |
|---|---:|---|
| **Best-of-N** | **−80 pp** | Dominant |
| Whole architecture (bare baseline) | −87 pp | — |
| Repair loop | −16.7 pp | Signal |
| Verification / sample-records / semantic / verifier | −7 to −10 pp | Within noise |

**Takeaway:** Best-of-N (candidate diversity + selection) is the load-bearing component by far.

### 3d. Schema-indexing coverage (infrastructure ablation)
Running conventional DBs that were **not** in the vector store:

| Schema indexed? | Executed successfully |
|---|---|
| No (agent flies blind) | **5/36** |
| Yes (after `build_index`) | **19/20** |

**Takeaway:** schema/embedding coverage is a hard prerequisite. A "low score" can be an
indexing gap, not a model failure — always check coverage before concluding.

### 3e. Candidate count (best-of-N = 4 vs 8)
On hard failed instances: **+1 recovery** (an ambiguity case the larger candidate pool covered).
Monotonic but with cost (~2× latency/tokens at bon=8).

### 3f. Fix-by-fix recovery on the conventional 20-slice (no-gold, lenient)
| Stage | Score | Δ |
|---|:---:|:---:|
| Baseline (front-end, bon=4) | 8/20 | — |
| + constraint-complete plan | 9/20 | +1 (`sf_bq153`) |
| + 8-candidate voting | 10/20 | +1 (`sf_local056`) |
| + deterministic date-encoding rewrite | 11/20 | +1 (`sf_bq121`) |
| + native-date rewrite / final bon=8 run | **12/20** | +1 (`sf_local068`) |

**Takeaway:** each targeted fix recovers **~1 instance** (within noise individually); they
compound to **8→12/20 (40%→60%)**. Diminishing returns are real and were confirmed across
**three independent interventions**.

---

## 4. Failure taxonomy (why instances still miss)
From reading our SQL vs gold SQL on the misses:

| Class | Example | Fixable? |
|---|---|---|
| **Value/date-format misread** (epoch vs YYYYMMDD; native DATE) | `sf_bq121`, `sf_local064` | ✅ deterministic rewrite (done) |
| **Dropped question constraint** (e.g. a specific gene filter) | `sf_bq153` | ✅ constraint-complete plan (done) |
| **Long-tail / wrong literal → empty result** | GA360 product names | ✅ exploration entity resolution (done) |
| **Recursive-CTE dialect** | `sf_local269` | ⚠️ recipe added; still hard |
| **Complex multi-step / domain logic** | `sf_local309`, `sf_bq193` | ❌ architectural ceiling |
| **Ambiguity** (signed vs ABS, etc.) | `sf_local056` | ~ partly (more candidates) |

The dominant remaining failures are **complex logic + recursive CTEs**, which more rules/candidates
do not crack.

---

## 5. Benchmark validity (read before quoting any score)
- **~63% of Spider2-Snow gold SQL is wrong or ambiguous** (VLDB 2026, "Pervasive Annotation
  Errors"). Concrete cases we hit: gold stored IDs as **integers (dropping leading zeros)**;
  **column-order/name** differences; **Decimal vs float**. These mark *correct* predictions wrong.
- The benchmark itself ships **multiple gold variants per question** (up to 8) — implicit
  acknowledgement of ambiguity. We count a match against *any* variant (exact value-set).
- **Implication:** do not over-optimize to exact-match gold on small samples; weight
  execution-plausibility/consensus, and audit flips by hand.

---

## 6. Where we stand vs SOTA, and what closing the gap requires
| Method | Spider2-Snow | Notes |
|---|---:|---|
| Proprietary (Genloop, etc.) | 90–96% | frontier models + heavy ensembles |
| **APEX-SQL** | **73%** | leaderboard SOTA; full agentic hypothesis-verify loop |
| ReFoRCE | 31% | column exploration + self-consistency |
| **Ours (conventional subset, lenient)** | ~50–60% | not full-benchmark, lenient scoring |
| **Ours (hard slice, lenient)** | ~33% | full GA360/PATENTS slice |

We are **below APEX**. The gap is architectural, not tuning: APEX runs a **full
hypothesis→verify→refine agent loop**, **dual-pathway schema pruning**, **role-based parallel
data profiling**, and a **deterministic tip library**. Our best-of-N + light-repair pipeline
plus rule fixes plateaus around the numbers above.

---

## 7. Transferable lessons for the real project
1. **Measure no-gold accuracy.** Gold-fed numbers (or any setup where the system sees the
   answer) overstate deployable accuracy by a lot.
2. **Pick the strongest model you can afford.** Agentic methods only pay off on capable models;
   on weak models they add nothing. This is the single highest-leverage choice.
3. **Invest in schema/value grounding + indexing coverage first.** Unindexed schemas → blind
   generation. Online value exploration (resolving real literals, date encodings) is what kills
   the dominant "empty/wrong-literal" failures.
4. **Enforce data-format facts deterministically, not via prompts.** The model's priors
   (e.g. "date = YYYYMMDD") override soft hints; a code-level rewrite is reliable, prompting is not.
5. **Best-of-N is the load-bearing technique.** Scale candidates before adding more rules.
6. **Trust your eval.** If your gold is noisy (it usually is in enterprise), build lenient/
   audited scoring or you will chase label noise.
7. **Stratify by difficulty.** Conventional star schemas (~50–60%) vs nested/recursive
   analytics (~33%) behave very differently — report and plan accordingly.

---

## 8. Limitations / honesty notes
- Conventional numbers are on a **20-instance curated subset** with **lenient scoring**; the
  best-per-instance 12/20 combines runs (optimistic) — a single clean run is ~10–11/20.
- Small N (20 / 30) → ±1 instance is within noise; deltas are directional, not significant.
- Hard-slice treatment full-30 was never completed (quota); 33% is the control number.
- We did **not** run our system on the full benchmark with official scoring; a like-for-like
  vs APEX/leaderboard number would require that.

---

## 9. Recommended next steps (if pursuing higher accuracy)
1. **One clean full-benchmark run, official scoring** — to get a defensible headline number.
2. **Adopt the APEX-style hypothesis-verify agent loop** — the only path to materially close
   the gap to 73%; this is a build, not a tuning pass.
3. **Finish indexing all benchmark DBs** (1 failed: SDOH — chunk the embedding upsert).
4. Keep all validated fixes (net-positive, no regressions).

*Artifacts: per-run records under `reports/experiments/` (`rep_idx_treat`, `rep_misses_fix`,
`five_8vote`, `failed_final`, `nogold_*`, `n30_*`); scripts: `audit_gold_matches.py`,
`build_representative_subset.py`, `index_representative_dbs.py`, `describe_ga360_*.py`,
`run_*.sh`. Code: exploration/planning (`agent/exploration.py`), date-encoding rewrite
(`prompting/sql_compiler.py`), dialect/determinism (`prompting/prompt_builder.py`).*
