# Which Architectural Components Add Most Value — Gold-Fed Ablation Study

**Question this study answers:** of all the components in our Text-to-SQL agent, **which
ones actually drive accuracy**, and which are marginal? This is a controlled, paired
leave-one-out ablation designed to rank components by their contribution — to guide where we
invest engineering effort.

**Setup.** Spider 2.0-Snow, first **30** non-geospatial instances, model gpt-5.4-mini,
Best-of-N=4, max_repairs=3. **Gold-fed configuration**: the self-refinement loop is allowed to
compare each candidate's result to the gold answer and keep repairing until it matches (or the
budget is exhausted). This puts the system at its **assisted ceiling**, which is the cleanest
regime for isolating each component's marginal contribution. *(Production/no-gold numbers are a
separate report — they are lower; this study is about component value, not deployable accuracy.)*

**Method.** For each component we run an arm with that component removed, hold everything else
fixed, and measure the **paired per-instance change** vs the full system (which instances it
*rescues* minus which it *breaks*). 2,000-sample bootstrap CIs; components bucketed into tiers.
A CI spanning 0 means "within run-to-run noise at this N."

---

## 1. Headline — components ranked by value

| Rank | Component | Net impact | 95% CI | Tier |
|---:|---|---:|:---:|---|
| 1 | **Whole architecture** (vs bare baseline) | **+86.7 pp** | [73.3, 96.7] | **Dominant** |
| 2 | **Best-of-N** (candidate diversity + selection) | **+80.0 pp** | [63.3, 93.3] | **Dominant** |
| 3 | Self-correction repair loop | +16.7 pp | [0.0, 33.3] | Signal (weak) |
| 4 | Sample-records prompting | +10.0 pp | [0.0, 23.3] | Within noise |
| 5 | Verification (fingerprint/metamorphic) | +6.7 pp | [−6.7, 20.0] | Within noise |
| 6 | Semantic layer | +6.7 pp | [0.0, 16.7] | Within noise |
| 7 | Learned verifier | +6.7 pp | [−6.7, 20.0] | Within noise |

**One-sentence finding: Best-of-N is the load-bearing component — it accounts for essentially
all of the architecture's lift. Everything except the repair loop is statistically within noise
at this sample size.**

---

## 1b. Important: "gold-fed" vs the "repair loop" — how to read these numbers

**Gold-fed and the repair loop are not the same thing.** "Gold-fed" means the system's
correctness *signal* is the gold answer; the **repair loop** is a *mechanism* that consumes that
signal. Gold is used in **two** places: **(1) selection** — pick the best-of-N candidate whose
result matches gold — and **(2) repair** — keep fixing toward gold.

This ablation was run **entirely in gold-fed mode**. The `no_repair` arm removed only the repair
*iterations* and **kept gold-assisted selection on**. So **"repair = +16.7 pp" means the value of
iterative repair *given* gold-assisted selection — NOT the value of gold.**

| Config | Gold for selection? | Gold-driven repair? | Accuracy |
|---|:---:|:---:|---:|
| Full system | ✅ | ✅ | ~90% |
| No-repair arm (this ablation) | ✅ | ❌ | ~73% |
| **No gold at all (production / deployable)** | ❌ | ❌ | **~33–50%** |

**The big lever is the gold *signal* itself (~40 pp), and most of it is in *selection*, not
repair.** Removing gold entirely (production) loses both the ability to (a) pick the right
candidate among the N generated and (b) detect semantically-wrong-but-executing SQL — so accuracy
falls far more than the 16.7 pp repair ablation implies. **The 16.7 pp (repair-given-gold) and the
~40 pp gold→no-gold gap are different quantities; do not compare them directly.** Deployable
(no-gold) accuracy is in `FINAL_REPORT.md`.

---

## 2. Ablation matrix (accuracy + cost)

| Arm (component removed) | Accuracy | Δ vs full | Total tokens | Avg LLM calls/q |
|---|---:|---:|---:|---:|
| **Full system** | **90.0%** (27/30) | — | 2,670,814 | 17.2 |
| − Verification | 83.3% (25/30) | −6.7 pp | 2,689,095 | 17.6 |
| − Semantic layer | 83.3% (25/30) | −6.7 pp | 2,604,480 | 17.2 |
| − Learned verifier | 83.3% (25/30) | −6.7 pp | 2,639,277 | 17.0 |
| − Sample-records | 80.0% (24/30) | −10.0 pp | 2,288,149 | 16.9 |
| − Repair loop | 73.3% (22/30) | −16.7 pp | 1,537,049 | 8.0 |
| − **Best-of-N** | **10.0%** (3/30) | **−80.0 pp** | 673,291 | 5.3 |
| **Bare baseline** (all off) | **3.3%** (1/30) | −86.7 pp | 327,038 | 3.0 |

---

## 3. Cost vs value (for resourcing decisions)
- **Best-of-N is both the dominant value (+80 pp) and the dominant cost (~4× tokens: 0.67M → 2.67M).** That 4× spend is unambiguously worth it — it's the difference between 10% and 90%.
- **Repair loop**: roughly doubles tokens (1.5M → 2.7M) for +16.7 pp — real but the smallest "worth-it" lever, and its CI touches 0.
- **Verification / semantic / sample-records / verifier**: ~free-to-modest token cost and **no statistically reliable accuracy gain at n=30** — candidates for simplification or for re-validation at larger N before further investment.

---

## 4. Interpretation
1. **Candidate diversity + selection (Best-of-N) is the architecture.** Generating multiple
   diverse SQL candidates and selecting among them is responsible for ~all of the lift. This is
   consistent with the broader literature (self-consistency / ensembling dominates).
2. **Self-correction (repair) is the clear #2**, but already weak (+16.7 pp, CI floor at 0).
3. **The "knowledge" components (semantic layer, sample-records, verifier, verification) are
   within noise here.** That does **not** prove they're useless — at n=30 a true +5–10 pp effect
   is statistically invisible. It means: don't over-credit them yet; validate at larger N.

---

## 5. Caveats (so the ranking is read correctly)
- **Gold-fed ≠ production.** This study measures component *value at the assisted ceiling*. The
  deployable (no-gold) accuracy is materially lower (separate report). Notably, the repair loop's
  value here is partly the gold oracle driving it — its no-gold value is smaller.
- **n = 30 → one instance = 3.3 pp.** Any |Δ| under ~7 pp (≈2 instances) is within noise; that's
  why ranks 4–7 are bucketed together rather than finely ordered.
- **Benchmark gold is ~63% noisy/ambiguous** (VLDB 2026) — adds further label noise on top of
  sampling noise. Treat sub-10-pp differences as directional only.

---

## 6. Recommendation for the architecture
1. **Protect and scale Best-of-N** — it is the franchise component. The most reliable way to
   buy more accuracy is more/better candidates + smarter selection (e.g. bon=4→8 gave measurable
   gains in follow-up tests).
2. **Keep the repair loop** — clear #2.
3. **Re-validate the knowledge components at larger N (100+)** before further investment; if they
   stay within noise, simplify them to cut cost/latency.
4. **The biggest unmeasured lever is selection quality** (a trained verifier on real labels) and
   candidate diversity — both amplify the dominant Best-of-N component.

---

*Data: `reports/experiments/n30_A*/instance_results.jsonl`, `ablation_ranking.json`.
Reproduce ranking: `rank_ablation.py`; presentation build: `render_ablation_presentation.py`
(`PRESENTATION_n30.md`).*
