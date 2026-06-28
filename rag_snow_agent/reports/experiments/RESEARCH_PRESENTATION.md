# Text-to-SQL Research — Findings, Value, and the Path to Higher Accuracy

*Enterprise NL→SQL on Spider 2.0-Snow (Snowflake; 547 real workflows, 152 DBs).*
*Companion data: `GOLD_FED_ABLATION_REPORT.md`, `GOLD_FED_100Q_REPORT.md`, `FINAL_REPORT.md`.*

---

## 1. Why this is hard (and why it matters)

- Enterprise Text-to-SQL = NL question → correct SQL over **real, messy warehouses**: 3000+
  columns, nested VARIANT/JSON, date-sharded tables, dialect quirks, ambiguous business terms.
- Far harder than classic Spider: top *proprietary* systems reach 90–96%; strong *academic*
  systems sit at 31–73%.
- **Business value:** every point of accuracy = analyst questions answered without a data engineer.

---

## 1a. What the questions actually look like (real samples, non-geo)

**Moderate — conventional schemas (we solve these):**
- **THELOOK_ECOMMERCE** — *"From Jan 2019–Apr 2022, how many users are at the youngest age and at the oldest age, for each gender?"* → ✅
- **IPL (cricket)** — *"Names of players who scored ≥100 runs in a match while playing for the team that **lost** that match."* → ✅

**Complex — multi-step / domain / data-format:**
- **STACKOVERFLOW** — *"Average reputation and number of badges by complete years of membership, for users who joined on/before Oct 1, 2021."* (`creation_date` is an epoch-microsecond integer) → ✅ *after our date-encoding fix*
- **PANCANCER_ATLAS** — *"For each histology type, the average per-patient avg of log10(normalized_count+1) of the **IGF2** gene among LGG patients."* → ✅ *after constraint-completeness fix*
- **GITHUB_REPOS** — *"Non-empty, non-comment lines from README.md across repos; frequency of each line + comma-list of languages per repo, ordered by frequency."* → ✗

**Very complex — the hard tail (mostly unsolved):**
- **GA360** — *"First half 2017, by hits product revenue: which traffic source had the highest total product revenue, and its max daily/weekly/monthly revenues (in millions)?"* (nested ARRAY → double FLATTEN + 3 time grains) → ✗
- **GA360** — *"Apr–Jul 2017: classify sessions as purchase vs non-purchase via hits productRevenue + totals.transactions; compare avg pageviews/visitor per group, by month."* → ✗
- **ORACLE_SQL** — *"Average total quantity across final packaging combinations, leaf-level items only, after fully expanding nested packaging relationships."* (recursive bill-of-materials explosion) → ✗

> These illustrate the spread: clean star-schema questions are tractable; nested-VARIANT analytics,
> recursive logic, and multi-grain aggregation are the genuine hard tail.

---

## 2. The single most important distinction: **gold-fed vs no-gold**

| Mode | What it means | Use |
|---|---|---|
| **Gold-fed** | System may compare to the answer key and repair until it matches | Research ceiling |
| **No-gold** | System never sees the answer (real deployment) | **Deployable accuracy** |

> **The headline lesson: a "90%" gold-fed number overstates deployable accuracy by ~40 points.**
> Always measure no-gold.

---

## 3. What we built (all gold-free, all measured)

- **Best-of-N + self-consistency voting** — diverse candidates, select by result agreement
- **Self-critic repair** — LLM judges `(question, SQL, result)`; gold-free replacement for the
  gold repair signal
- **Exploration front-end** — probe real data to resolve entities & discover formats
- **Information-aggregation planning** — structured entity→column / join / constraint plan
- **Deterministic data-format enforcement** — code-level rewrite of date/encoding mistakes
- **No-gold measurement harness + lenient audit** — honest scoring against noisy gold

---

## 4. Result A — Gold-fed ceiling (the research upper bound)

| Slice | Accuracy |
|---|---|
| 30-query (first non-geo) | **90%** |
| **100-query (representative, 3 runs)** | **84%** (81–87%) |
| 100-query, **reliably** solved (3/3 runs) | 76% |
| 100-query, **never** solved | 10% |

- Strong domains: GitHub, patents, NOAA weather, GA360, healthcare.
- **Weak domain: geospatial** (ST_* / distance queries) — the hard tail.

---

## 5. Result B — Which components add the most value (ablation)

*Paired leave-one-out, gold-fed, n=30:*

| Component | Net impact | Verdict |
|---|---:|---|
| **Best-of-N** (candidate diversity + selection) | **+80 pp** | **Dominant — the franchise** |
| Repair loop | +16.7 pp | Clear #2 (weak) |
| Sample-records / verification / semantic / verifier | +7–10 pp | Within noise at n=30 |

> **Invest in candidate diversity + selection. Everything else is marginal *given* good selection.**

---

## 6. Result C — Deployable (no-gold) accuracy

| Slice (gpt-5.4, no-gold) | Accuracy |
|---|---|
| Conventional schemas | **~50–60%** |
| Hard nested/analytics (GA360/PATENTS) | **~33%** |

- Our improvements moved the conventional slice **40% → 60%** over the project.
- This is *in line with academic SOTA on Snow* and well above older baselines — but below the
  current leaderboard leader (see slide 11).

---

## 7. Reconciling the numbers — gold *signal* vs repair *loop*

- **Gold is a *signal*; the repair loop is a *mechanism* that uses it.** Gold helps in **two**
  places: **selection** (pick the candidate that matches the answer) and **repair** (fix toward it).
- The ablation's "+16.7 pp for repair" was measured **with gold-selection still on**.
- Removing gold entirely (production) loses **both** — and the **bigger loss is selection (~40 pp)**.

> **Implication: the #1 production lever is a better *selection / stopping signal* (a learned
> verifier), because the candidates needed to reach 84% are already being generated.**

---

## 8. Key finding — model capability *gates* every technique

*Same hard slice, no-gold:*

| | gpt-5.4-**mini** | gpt-5.4 |
|---|:---:|:---:|
| Baseline | 1/10 | 1/10 |
| + consensus + self-critic | **1/10** | **5/10** |

> Agentic techniques are **amplifiers** — ~0 gain on a weak model, +400% on a capable one.
> **Pick the strongest model you can afford; then the architecture pays off.**

---

## 9. Key finding — grounding & infrastructure beat prompt tricks

- **Schema/index coverage is a prerequisite:** unindexed DBs → **5/36** executable; indexed → **19/20**.
- **Value/format grounding** (probe real data) kills the dominant "wrong literal / empty result"
  failures.
- **Deterministic enforcement > prompting:** the model's priors (e.g. "date = YYYYMMDD") override
  soft hints; a **code-level rewrite** is reliable, prompting is not.

---

## 10. Key finding — the benchmark's gold is ~63% noisy

- VLDB 2026: **~63% of Spider2-Snow gold SQL is erroneous or ambiguous.**
- Concrete cases we hit: IDs stored as ints (dropped leading zeros), column-order/name diffs,
  Decimal vs float — all mark *correct* answers wrong.
- **Don't over-optimize to exact-match gold;** use lenient/audited scoring and weight
  execution-plausibility + consensus.

---

## 11. The landscape — what the best systems do to raise accuracy

| Approach | Used by | Why it helps |
|---|---|---|
| **Candidate diversity + voting / ensembling** | All (ReFoRCE, DivSkill, ours) | The dominant lever (our ablation: +80 pp) |
| **Column/value exploration** (probe real data) | ReFoRCE, APEX, DSR-SQL | Resolves entities, formats, nested structure → fixes empty/wrong-literal |
| **Hypothesis → verify → refine agent loop** | **APEX-SQL (SOTA)** | Grounds reasoning in data; iterative correction |
| **Residual / complementary skills** | DivSkill-SQL | Diversity targets *failures*, not random → 3× fewer hallucinations |
| **Schema pruning / compression** | DSR-SQL, APEX | Handles 3000-column schemas without noise |
| **Format restriction** (constrain output) | ReFoRCE | Right columns/shape, fewer mismatches |
| **Dialect tip libraries** (fuzzy match, FLATTEN, recursive CTE) | ReFoRCE, APEX, DSR | Avoids silent dialect errors |
| **Multi-agent consensus / debate** | Paytm Prism | Resolves disagreement vs naive voting |

---

## 11a. SOTA review (1/3) — top tier: proprietary leaders & APEX-SQL

**Proprietary leaders** (Genloop Sentinel **96.7%**, Paytm Prism **90.5%** — Snow)
- **Frontier models** + **multi-agent consensus/debate**: specialized agents (schema, joins,
  business logic) each propose, then **negotiate to one answer** rather than majority-vote —
  resolving disagreements by reasoning. Heavy ensembling + business-context handling. Code closed.

**APEX-SQL** (Snow leaderboard **73%**, paper EX **53%** with DeepSeek-R1 — current academic/open SOTA).
*Paper: arXiv:2602.16720, KDD 2026. Code: github.com/Tencent/APEX-SQL-Project.*

A full **hypothesis → verify → refine** agent that grounds every decision in real data.
Two-stage pipeline: **Stage 1** finds the right schema; **Stage 2** generates and validates SQL.

### Stage 1 — Schema Linking (4 steps)

1. **Schema-agnostic logical planning** — generates 2 reasoning plans in natural language ("filter
   by X, aggregate Y over Z") with *no column names*, at temperature 0.8, then merges them at
   temperature 0.2. Decouples intent parsing from schema vocabulary so the model doesn't latch onto
   superficially similar column names. *Removing this step: −5 pp SRR.*

2. **Dual-pathway schema pruning** — two simultaneous LLM passes over all columns:
   - **Negative pass**: tag columns to *delete* (clearly irrelevant)
   - **Positive pass**: tag columns to *keep* (clearly needed)
   - **Union logic**: a column survives unless *both* passes reject it — high precision without sacrificing recall.
   - Compresses ~3 430 columns → ~383 columns (**9× reduction**, ~7 k tokens).
   - *Ablation (120-case pilot, SRR):* deletion only 80.8%; selection only 93.3%; **both: 97.5%**

3. **Parallel data profiling** — independent agents per table run role-specific exploratory SQL
   against the live database to validate each column's *actual* role: filter key, join key, metric,
   nullable flag. Results >30 rows compressed to top-10 + aggregate statistics. This is not generic
   sampling — each probe is *role-driven*. *Removing: −9.2 pp SRR.*

4. **Global synthesis** — integrates cross-table observations, enforces **topological connectivity**
   (all join paths in the output subgraph must be traversable). Includes a semantic linking sub-step
   that hypothesises table/column roles *before* profiling to direct the probes.
   *Removing all agentic verification: −21.7 pp SRR (55.8% vs. 77.5%).* Baseline ReFoRCE schema
   linking: **35% SRR** on the same protocol.

### Stage 2 — SQL Generation (pre-processing + agentic loop + selection)

*Two pre-processing steps run once before the agent loop:*

5. **Macro plan aggregation** — multiple reasoning paths from Stage 1 are consolidated into a
   single unified master plan injected into the agent's initial context. Prevents the agent from
   starting blind.

6. **Deterministic "tip library"** — a **rule engine** (not embedding retrieval) maps operational
   keywords from the plan to one of **14 directive categories** (Join Strategy, NULL inspection,
   String Matching, Aggregation, Sorting, Recursive CTE, …) stored in a hand-crafted library ℳ.
   >95% recall in pilot. Adding tips: **+3.75–4.27% EX@8** on GPT-4o / GPT-5. Reliable and
   noise-free vs. fuzzy retrieval. Can be disabled via `--no_tips`.

*Agentic exploration loop — bounded at max 40 actions and 56 k token budget:*

| Action | What happens |
|:-------|:-------------|
| **PROFILING** | Generates exploratory SQL — distributions, NULL checks, FK validation, value ranges. Compresses results >30 rows to top-10 + statistics. |
| **CONSOLIDATION** | Periodic state compression: prunes history to exploratory queries + execution results + latest consolidated plan. Prevents context bloat within the 56 k budget. |
| **SQL SYNTHESIS** | Maps accumulated evidence to physical SQL. If execution fails, re-enters the loop. SQL synthesis is *force-triggered* at 52 k tokens to ensure a candidate exists before budget exhaustion. |
| **CONFIRMATION** | Secondary semantic check: does the SQL logic match user intent, accumulated observations, and the current plan? Finalises output only when conditions pass. |

*Answer selection:*
- **8 samples** (Pass@8), majority voting on execution results.
- **Reward model** (`ContextualAI/ctx-bird-reward-250121`) as tie-breaker when voting fails to reach consensus.
- `--revote` flag re-runs selection on cached samples without regenerating.

**Key numbers:**

| Benchmark | N | Model | EX | Pass@8 |
|:----------|--:|:------|---:|-------:|
| Spider 2.0-Snow | 547 | DeepSeek-R1 | **53.03%** | **68.44%** |
| BIRD-Dev | 1 534 | GPT-4o | **70.7%** | — |
| Spider 2.0-Snow schema linking | 120 | GPT-4.1 | **88.33% SRR** | — |

**Exploration effect (oracle schema, N=120):** DeepSeek-V3.2 with exploration **57.50%** vs.
without **39.17%** — **+18.3 pp**. "Rich get richer": stronger models benefit proportionally more.

**The identified bottleneck:** Pass@8 = 68.44% vs. voted EX = 53.03% — **15 pp lost to answer
selection**. The authors explicitly name selection as the next frontier. Directly mirrors our
finding: Best-of-N ceiling 84%, deployable with naive selection ~50–60%.

- *Why it scores: resolve ambiguity by **querying the data**, enforce correctness deterministically.*

---

## 11a-2. APEX-SQL — architecture diagram (reference)

```
NL question
    │
    ▼
┌──────────────────────────── STAGE 1: Schema Linking ─────────────────────────────┐
│                                                                                    │
│  ① Hypothesis Generation ──► 2 schema-agnostic plans (T=0.8) ──► merge (T=0.2) │
│                                                                                    │
│  ② Dual-Pathway Pruning                                                           │
│       negative pass (delete) ──┐                                                  │
│                                ├──► union logic ──► pruned column set (9× ↓)    │
│       positive pass (keep)  ──┘                                                   │
│                                                                                    │
│  ③ Parallel Data Profiling  ──► per-table agents run role-specific SQL probes    │
│                                                                                    │
│  ④ Global Synthesis  ──► enforce topological connectivity ──► final schema D*    │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────── Pre-processing ──────────────────────────────────────┐
│  ⑤ Macro Plan Aggregation (preprocess_macro_plans.py)                            │
│  ⑥ Deterministic Tip Retrieval — rule engine → 14-category library ℳ            │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────── STAGE 2: Agentic Loop (max 40 actions, 56k tokens) ─┐
│                                                                                    │
│   ┌──────────────────────────────────────────────────────┐                        │
│   │  PROFILING → CONSOLIDATION → SQL SYNTHESIS → CONFIRM │ ← loop until done     │
│   └──────────────────────────────────────────────────────┘                        │
│       SQL SYNTHESIS force-triggered at 52k tokens                                 │
└────────────────────────────────────────────────────────────────────────────────────┘
    │
    ▼
Answer Selection: 8 samples ──► majority vote ──► reward model tie-breaker
    │
    ▼
Final SQL
```

---

## 11b. SOTA review (2/3) — DivSkill-SQL & DSR-SQL

**DivSkill-SQL** (Lite **73%**, #1 on the Lite track) — attacks the weakness of naive ensembling
(candidates that all fail the same way):
- **Residual Skill Optimization** — builds a library of **complementary "skills"** (prompt+strategy
  recipes); each *new* skill is trained on the exact examples the current ensemble **still gets
  wrong**, explicitly maximizing **Pass@K** (chance that ≥1 of K candidates is right).
- At test time, several **skill-guided agents** solve the same query via *different* interaction
  patterns; results are selected across them.
- *Why it scores: diversity engineered to cover failures, not random — **3× fewer hallucinated
  schema/function references** vs. stochastic ensembling.*

**DSR-SQL** (Dual-State Reasoning — academic, mid-tier) — splits the problem into two states:
1. **Adaptive context** — an LLM does **schema linking/compression by writing exploratory SQL**
   and sampling the schema 3× at high temperature, then merging — keeping only the relevant
   tables/columns (de-noises huge enterprise schemas).
2. **Progressive generation** — a **4-state machine** (Extend = next sub-question · Revise = fix
   last step · Explore = probe data when results look wrong · Rephrase = finalize) builds the query
   **step-by-step, executing each intermediate result**. Plus **evidence extraction** that
   compresses external docs to the few facts/formulas the question needs.
- *Why it scores: hard queries are decomposed and verified incrementally, not guessed whole.*

---

## 11c. SOTA review (3/3) — ReFoRCE & the baseline

**ReFoRCE** (Snow **31%** with o1-preview — prior academic SOTA). Four mechanisms together:
1. **Table compression** — groups near-duplicate / date-sharded tables and prunes columns so a
   3000-column schema fits in context without drowning the model in noise.
2. **Column exploration** — *before* answering, writes probe SQLs (`SELECT DISTINCT …`, value
   samples, nested-field inspection) and feeds the **real results** back, grounding generation in
   actual values/formats rather than guesses.
3. **Format restriction** — derives the expected answer's **column structure + types** and forces
   the output to match it (fewer wrong-shape / wrong-column answers).
4. **Self-refinement + self-consistency voting** — several candidates, each repaired on execution
   feedback, then **voted** — the answer multiple independent attempts agree on wins.
- *Why it scores: exploration removes value/format errors; voting removes one-shot variance.*

**Spider-Agent** (xlang, ICLR'25 — the original baseline)
- Text-to-SQL as an **agent in a sandboxed environment** (Gymnasium loop): issue actions (inspect
  schema, run SQL, read docs), see results, iterate in **Docker**.
- Establishes the execution-feedback paradigm but, single-agent with no ensembling, accuracy is
  low — the floor the others build on.

---

## 11d. Common winning ingredients (synthesis of the field)

Every leading system combines the same recurring ingredients:
1. **Strong base model** (frontier) — gates everything.
2. **Ground in real data before generating** — exploration / parallel profiling.
3. **Many diverse candidates + smart selection** — voting / consensus / residual skills.
4. **Iterative verify-and-refine** with execution feedback.
5. **Schema linking / pruning / compression** for huge schemas.
6. **Dialect & data-format discipline** — tip libraries, deterministic enforcement.

> Our ablation independently confirms **#3 (Best-of-N) is dominant**; our project adds **#2
> (value/format grounding)** and **#6 (deterministic format enforcement)**. The leaders' edge is
> primarily **#4 (full hypothesis-verify loops)** + heavier **#3** — our clearest roadmap.

---

## 11e. Commercial benchmark — Databricks Genie

**Product:** AI/BI Genie — Databricks' conversational NL-to-SQL product, part of the Data
Intelligence Platform. GA: June 2025. 4 000+ customers adopted during preview.
**No public Spider 2.0 / BIRD benchmark numbers.** Architected for enterprise governance, not
academic benchmarking.

**Core architectural philosophy:** the *opposite* of APEX-SQL. Rather than exploring the schema at
runtime, Genie invests **up-front human curation** to narrow the LLM's task. At query time, the
LLM selects from pre-verified patterns rather than reasoning over the full schema.

### The Genie Space — configuration unit

| Resource | Hard limit | Contents |
|:---------|:----------:|:---------|
| Tables / views | **30** | Unity Catalog tables scoped to this domain |
| Knowledge store snippets | **200** | Column descriptions, SQL expressions (KPIs, filters, row transforms), join definitions, synonyms |
| Instructions | **100** | Parameterized example SQL + general text rules (effective up to ~20 lines) |
| Entity matching | **120 columns**, **1 024 values each** | Pre-sampled distinct-value lists for categoricals |

### 5-layer schema linking

1. **Unity Catalog metadata** (always active) — schema, data types, PK/FK, column descriptions.
2. **Intelligent filtering** (mechanism not disclosed — likely BM25/embedding) — selects relevant
   columns and example SQL from the Space at query time.
3. **Entity matching** — resolves *"Florida"* → `WHERE state = 'FL'`; *"first level support"* →
   exact DB code. Filters appear as dropdowns populated from stored distinct values.
4. **Join relationships** — explicit definitions in knowledge store; auto-suggested from UC FK
   metadata. Prevents join hallucination.
5. **Genie Ontology** (workspace-wide, newer) — automatic semantic map built from tables,
   dashboards, notebooks, and connected external docs (Google Drive, SharePoint via MCP).

### LLM backend — compound AI, model rotation without disclosure

| Mode | Model |
|:-----|:------|
| Standard Genie Spaces | **Azure OpenAI** (primary); Anthropic Claude (opt-in) |
| Agent Mode / Research Agent | **Anthropic Claude Sonnet** (explicitly documented) |
| Partner AI disabled | Databricks-hosted open-weight fallback |

Databricks: *"a managed service that continuously evaluates models from multiple providers and uses
the most performant and accurate options."* Model versions not disclosed; upgraded at least twice
(Nov 2024, Sep 2025). **DBRX** (Databricks' own MoE LLM) exists but is *not confirmed* to power
Genie.

### Agentic evolution timeline

| Date | Capability added |
|:-----|:----------------|
| Feb 2025 | Chain-of-Thought reasoning in text-to-SQL model |
| Sep 2025 | **Self-reflection** — generates SQL, checks its own output before execution |
| Nov 2025 | Research Agent (Agent Mode) — single unified reasoning agent; multi-hypothesis → single reasoner |
| Dec 2025 | Clarifying questions on semantic ambiguity |
| Apr 2026 | Agent Mode public preview (UI only — not available via API) |

**Research Agent loop (Agent Mode, Claude Sonnet):**
parse → plan hypotheses → execute SQL per hypothesis → reflect on each result → iterate →
generate report with citations and visualisations. Scales reasoning depth to question complexity.

### Trusted Assets — key differentiator

- **Parameterized example SQL**: if the user's question matches a trusted asset's associated
  question, Genie runs the *verified* SQL directly (parameter-substituted), **bypassing LLM
  generation entirely**. Response is flagged "Trusted."
- **SQL UDFs (Unity Catalog)**: exact function logic executes; always "Trusted."
- Effect: for frequently-asked or high-stakes queries, the LLM is completely eliminated from the
  path. Equivalent to our `TraceMemory` roadmap item — but fully implemented.

### Knowledge mining (human-in-the-loop learning)
Thumbs-up on a response → Genie suggests new SQL expressions and join definitions for the
knowledge store. Iterative improvement without automated fine-tuning. Databricks: *"we do not
train foundation models on your data."*

### Constraints (also features)
Read-only SQL (SELECT only). Per-user Unity Catalog permissions enforced at execution time.
Compute credentials embedded at Space creation.

**Key tradeoff:** Genie trades *setup cost* for *reliability and governance*. Zero-setup research
systems (ReFoRCE, APEX-SQL) explore at runtime; Genie offloads that cost to domain-expert curation.

---

## 11f. Three-way comparison — our system vs. APEX-SQL vs. Genie

| Dimension | **Our system** | **APEX-SQL** (KDD 2026) | **Databricks Genie** |
|:----------|:--------------|:------------------------|:---------------------|
| **Design** | Dynamic RAG + deterministic compiler | Agentic hypothesis-verify | Curated RAG + trusted assets |
| **Schema scope** | Full DB via vector/BM25 index + SchemaSlice | Full DB; agent explores at runtime | Hard limit: **30 tables** per Space |
| **Schema linking** | Hybrid retrieval (BM25 + embedding, RRF k=60) + SchemaSlice post-processing | 4-stage: hypothesis → dual-path pruning → parallel profiling → global synthesis | 5-layer: UC metadata + filtering + entity matching + join defs + Ontology |
| **SQL generation** | Plan → SQL **compiler** (deterministic); LLM owns intent, compiler owns syntax | 4-action agentic loop; force-triggers synthesis at 52 k tokens | CoT + self-reflection; trusted assets bypass generation entirely |
| **Self-correction** | Category-classified repair loop (8 Snowflake error categories) | SYNTHESIS re-enters loop on execution error | Self-reflection pre-execution + human "Ask for Review" |
| **Best-of-N / selection** | Best-of-N with 6 structural strategy variants; multi-signal selector | 8 samples; majority vote + reward model tie-breaker | Not disclosed; single-pass + reflection |
| **Large schema handling** | Partition collapsing (366 GA360 tables → 1 card); VARIANT enrichment; 1-hop join-graph expansion | 9× column compression at schema-linking time; 40-action budget caps runtime cost | Forces 30-table scope; multiple Spaces for large orgs |
| **Domain adaptation** | Index rebuild on schema change (minutes for 20 Snow DBs) | Pre-processing runs per task; 14-category tip library curated once | High up-front: curate knowledge store, entity lists, trusted assets, example SQL |
| **Governance** | None (research) | None (research) | Unity Catalog; per-user permissions; read-only SQL enforced |
| **LLM** | Provider-neutral; today `gpt-5.4-mini` | Any OAI-compatible; best: DeepSeek-R1, GPT-4.1 | Compound AI — Azure OpenAI primary; Claude Sonnet for Agent Mode |
| **Spider 2.0-Snow EX** | ~50–60% conventional subset¹ | **53.03%** full (paper) / **73%** (leaderboard) | Not published |
| **Cost / correct answer** | **$0.11** (measured) | Not reported | Not reported |
| **Open source** | This repo | github.com/Tencent/APEX-SQL-Project | Closed source |

¹ *Our number is on a curated conventional subset with lenient scoring; not directly comparable
to the full-benchmark official protocols used for APEX-SQL and the leaderboard.*

**Why our deterministic compiler is an architectural differentiator vs. APEX-SQL:**
APEX-SQL's agentic loop still lets the LLM own SQL *formatting*. Our Plan→SQL compiler closes the
entire class of Snowflake-specific syntax errors (`LATERAL FLATTEN`, VARIANT access, quoting) by
separating intent from syntax. APEX-SQL's 14-category tip library addresses this partially but the
LLM still emits raw SQL.

**Why partition collapsing matters vs. APEX-SQL:**
APEX-SQL's runtime exploration encounters 366 GA360 daily tables as separate entities — schema
linking noise makes GA360 queries near-unreachable without partition-awareness. Our index
collapses them at build time into one representative card; retrieval is reliable regardless of
shard count.

**Where Genie wins and what that implies for our roadmap:**
- Trusted assets = guaranteed correctness for frequently-asked queries (LLM bypassed). Our
  `TraceMemory` read path is the architectural equivalent; wiring it into the query path is a
  roadmap item (current activation rate: 0%).
- Unity Catalog governance: enterprise feature we don't attempt; not a research benchmark concern.

---

## 12. SOTA ranked — best → lowest (Spider2-Snow)

| # | System | Snow | Open? | What gets them there |
|--:|---|---:|:--:|---|
| 1 | Genloop Sentinel Agent v2 Pro | **96.7%** | ✗ | frontier model + heavy multi-agent ensemble |
| 2 | Native (usenative.ai) | 96.5% | ✗ | proprietary agent |
| 3 | QUVI-3 + Gemini-3-pro | 94.2% | ✗ | frontier model + agent |
| 4 | Tencent TCDataAgent | 94.0% | ✗ | contextual-scaling agent |
| 5 | Paytm Prism | 90.5% | ~ | multi-agent **consensus/debate** |
| 6 | **APEX-SQL** | **73%** | ✅ | **hypothesis→verify→refine + data profiling** (academic SOTA) |
| — | **Ours** | **~50–60%¹** | ✅ | exploration + best-of-N + deterministic format fixes |
| 7 | ReFoRCE | 31% | ✅ | column exploration + self-consistency voting |
| 8 | Spider-Agent (baseline) | low | ✅ | single sandboxed agent, no ensembling |

¹ *Not directly comparable: ours is a curated conventional subset with lenient scoring; ~33% on
the hard nested/analytics slice. The rest are full-benchmark, official scoring.*

**Lite track (easier benchmark, for reference):** DivSkill-SQL **73.1%** (#1), SOMA-SQL 72.0%,
DecisionX 71.8%, Databao 69.7%.

**Reading:** the **top tier is proprietary + frontier models**; **APEX-SQL is the open SOTA** and
the realistic target; we sit **above ReFoRCE / older baselines, below APEX**. The gap to APEX is
**architectural** (full data-grounded verify loop), not parameter tuning.

---

## 13. Highest-value investments (ranked)

1. **Strongest available model** — gates everything (slide 8).
2. **Learned verifier / better selection** — closes most of the gold→no-gold gap; candidates
   already reach 84% (slide 7).
3. **Value/format grounding + full schema indexing** — kills the dominant failure classes (slide 9).
4. **Scale Best-of-N** — the proven dominant component (slide 5).
5. **(Bigger build) APEX-style hypothesis-verify loop** — the path toward 73%+ (slide 11).

---

## 14. Summary

- **Gold-fed ceiling ~84%; deployable (no-gold) ~50–60% conventional / ~33% hard** — measured honestly.
- **Best-of-N + selection is the dominant value driver;** the gold→no-gold gap is mostly a
  **selection / stopping-signal** problem → a **learned verifier** is the top production lever.
- **Model capability, data grounding, and indexing coverage** outweigh prompt-level tricks.
- Our improvements (grounding, deterministic format enforcement, constraint-complete planning,
  voting) moved conventional accuracy **40% → 60%** with no regressions.
- The leaders (APEX, proprietary) win via **heavier agentic loops + exploration + ensembling** —
  a clear, evidence-backed roadmap for closing the gap.

---

## 15. Appendix — methodology & honesty notes

- Numbers are on **subsets** (n = 20/30/100); one instance = 3–5 pp → small deltas are noise.
- No-gold conventional uses **lenient any-gold scoring** (corrects verified eval artifacts);
  strict official is lower.
- Gold-fed ablation holds gold constant — repair's +16.7 pp ≠ value of gold (slide 7).
- Best-per-instance aggregates across stochastic runs are optimistic; single-run is a few pts lower.
- Sources: Spider 2.0 leaderboard; ReFoRCE (arXiv 2502.00675); APEX-SQL (arXiv 2602.16720);
  DivSkill-SQL (arXiv 2605.21792); "Pervasive Annotation Errors" (arXiv 2601.08778, VLDB 2026).
