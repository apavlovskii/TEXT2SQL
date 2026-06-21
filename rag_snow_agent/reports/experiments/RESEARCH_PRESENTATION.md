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

## 11a. SOTA review (1/3) — the agentic baseline & ReFoRCE

**Spider-Agent** (xlang, ICLR'25 — the original baseline)
- Agent↔environment loop (Gymnasium), **Docker-sandboxed execution**, tool use.
- Established the *execution-feedback agent* paradigm for Spider2; modest accuracy alone.

**ReFoRCE** (Snow **31%** — prior academic SOTA)
- **Column exploration** — writes probe SQLs to learn real values & nested structure *before* answering.
- **Format restriction** — derives the expected output schema and constrains generation to it.
- **Self-refinement + self-consistency voting** over N≈8 candidates with execution feedback.
- **Table compression** to fit 1000s of columns in context.
- *Accuracy drivers: exploration + voting.* (Gold-free in its loop.)

---

## 11b. SOTA review (2/3) — DSR-SQL & DivSkill-SQL

**DSR-SQL** (Dual-State Reasoning)
- **Adaptive context state**: LLM-driven schema compression/linking via exploratory SQL + 3-round sampling.
- **Progressive generation state**: a **4-state machine** (Extend / Revise / Explore / Rephrase) that builds the query **sub-question by sub-question with per-step execution**.
- **Evidence extraction**: compress external knowledge docs to the question-relevant facts/formulas.
- *Accuracy drivers: structured decomposition + schema grounding.*

**DivSkill-SQL** (Lite **73%**, #1)
- **Residual Skill Optimization**: build *complementary* skills, each optimized on the cases the current ensemble **fails** (maximize Pass@K) — not random/stochastic diversity.
- Test-time: multiple skill-guided agents attack the same query via different interaction patterns.
- *Result: 3× fewer hallucinated schema refs — diversity that targets failures, not surface variation.*

---

## 11c. SOTA review (3/3) — APEX-SQL (current SOTA) & proprietary

**APEX-SQL** (Snow **73%** — current academic SOTA)
- **Hypothesis → verify → refine** agent loop, grounded in real data at every step.
- **Logical planning** that is *schema-agnostic* first (avoids string-similarity hallucination).
- **Dual-pathway schema pruning** (keep a column unless confidently noise AND not task-critical).
- **Parallel data profiling** — validate each column's **role** (filter / join / aggregate) against real values, formats, distributions.
- **Deterministic "tip library"** (14 categories: string-matching, NULL handling, joins, pitfalls) via keyword→directive mapping.
- *Accuracy driver: data-grounded hypothesis verification — the recipe to beat.*

**Proprietary** (Genloop Sentinel **96.7%**, Paytm Prism **90.5%**)
- Frontier models + **multi-agent consensus/debate** (specialized agents negotiate one answer) over naive voting; heavy ensembles; business-context handling. Code closed.

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

## 12. Where we stand vs SOTA (Spider2-Snow)

| System | Snow accuracy | Notes |
|---|---:|---|
| Proprietary (Genloop, Native, …) | 90–96% | frontier models + heavy ensembles |
| **APEX-SQL** | **73%** | full hypothesis-verify agent loop (academic SOTA) |
| **Ours (conventional / lenient)** | ~50–60% | curated subset, lenient scoring |
| ReFoRCE | 31% | column exploration + self-consistency |

- We are **above older academic baselines, below APEX.** The gap is **architectural**, not tuning.

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
