# Gold-Fed Performance at Scale — 100-Query Benchmark (Best-of-8 + Repair)

**Purpose.** A larger-N validation of the gold-fed system ceiling, complementing the 30-query
component ablation. This measures how the **full system with gold-assisted self-refinement**
performs across **100 queries**, with a **3-run variance estimate** and a domain breakdown of
where it's strong vs weak.

**Setup.** Spider 2.0-Snow, **100 instances** (broader than the 30-query ablation — **includes
geospatial DBs**). Model gpt-5.4-mini. **Best-of-N = 8, max_repairs = 4.** **Gold-fed**: the
self-refinement loop compares each candidate's result to gold and repairs until it matches
(assisted ceiling). Three independent repetitions (`benchmark_run_10/11/12`).

---

## 1. Headline — gold-fed ceiling ≈ 84%

| Run | Accuracy |
|---|---:|
| benchmark_run_10 | 87% (87/100) |
| benchmark_run_11 | 81% (81/100) |
| benchmark_run_12 | 84% (84/100) |
| **Mean** | **84.0%** (range 81–87%) |

**The gold-assisted full system solves ~84% of a representative 100-query Snow set, with ~±3 pp
run-to-run variance.** This is the realistic ceiling number (vs the 90% on the easier first-30
ablation slice, which over-represents well-handled domains).

---

## 2. Stability across the 3 runs (per-instance)

| Outcome | Instances |
|---|---:|
| **Always correct** (3/3 runs) | **76** |
| Flaky (1–2 of 3 runs) | 14 |
| **Never correct** (0/3 runs) | **10** |

**Read:** a **stable solved core of ~76%**, a **hard core of ~10%** the system never solves, and
**~14% flaky** (stochastic — sometimes solved). The flaky band is exactly why single-run numbers
swing ±3 pp; the honest "reliable" capability is ~76%, the "best-case" ~87%.

---

## 3. Where it's strong vs weak (per-DB, majority of 3 runs)

| Database | Solved | |
|---|---:|---|
| GITHUB_REPOS | 15/15 | ✅ |
| NOAA_DATA | 12/12 | ✅ |
| PATENTS | 14/15 | ✅ |
| GA360 | 10/12 | ✅ |
| CMS_DATA | 7/7 | ✅ |
| GITHUB_REPOS_DATE | 6/6 | ✅ |
| PATENTS_GOOGLE | 4/4 | ✅ |
| CENSUS_BUREAU_ACS_2 | 2/4 | ⚠️ |
| **GEO_OPENSTREETMAP** | **3/6** | ❌ |
| **NEW_YORK_NOAA** | **1/3** | ❌ |
| **NEW_YORK_CITIBIKE_1** | **1/3** | ❌ |

**Domain insight: the weak spots are geospatial DBs** (GEO_OPENSTREETMAP, NEW_YORK_NOAA,
NEW_YORK_CITIBIKE) — spatial predicates (`ST_*`, distance/within) are the hardest class here.
Conventional analytics (patents, GitHub, weather, healthcare, GA360) are handled well.

---

## 4. How this connects to the component ablation
- The 30-query ablation showed **Best-of-N is the dominant component (+80 pp)** and the **repair
  loop is the #2 lever (+16.7 pp)**. This 100-query run is that exact recipe at its strong
  setting (**Best-of-8 + 4 repairs**), and it lands at the **84% gold-fed ceiling** — consistent:
  scaling the dominant component (N=4→8) plus repair sustains high accuracy across a broader,
  harder set.
- The **10 never-solved** instances are the architectural hard core (geo + complex logic) that
  neither more candidates nor gold-assisted repair cracks.

---

## 4b. "Gold-fed" vs the "repair loop" — reconciling 84% with the ~50% no-gold number

A natural question: *if removing the repair loop only costs ~17 pp in the ablation, why does the
no-gold (production) number fall from ~84% to ~50%?* Because **gold-fed ≠ repair loop**:

- **Gold is the correctness *signal*; the repair loop is just one *mechanism* that uses it.**
  Gold is consumed in **two** places — **selection** (pick the best-of-N candidate matching gold)
  and **repair** (keep fixing toward gold).
- The ablation toggled the **repair mechanism with gold still on**, so its 17 pp is "value of
  repair *given* gold-assisted selection."

| Config | Gold for selection? | Gold-driven repair? | Accuracy |
|---|:---:|:---:|---:|
| Full (this run) | ✅ | ✅ | **~84%** |
| No-repair (ablation arm, 30-q) | ✅ | ❌ | ~73% |
| **No gold (production / deployable)** | ❌ | ❌ | **~33–50%** |

**Going to no-gold removes the gold signal from *both* selection and repair at once — and the
bigger loss is selection** (you can no longer tell which of the N candidates is correct, nor
detect wrong-but-executing SQL). That ~40 pp gold→no-gold gap is a *different quantity* from the
17 pp repair-given-gold figure; they should not be compared directly. **84% here is the
gold-assisted ceiling, not deployable accuracy** (deployable = `FINAL_REPORT.md`).

---

## 5. Caveats (so the 84% is read correctly)
- **Gold-fed = assisted ceiling, NOT deployable accuracy.** The self-refinement loop uses the
  gold answer to know when to stop/repair. Production (no-gold) accuracy is materially lower
  (separate report). Use 84% as "what the architecture can reach with a perfect stopping signal,"
  i.e. the target a good no-gold verifier would chase.
- **Model is gpt-5.4-mini** here (these runs predate our gpt-5.4 work) — a stronger model would
  likely lift this further.
- **~63% of Snow gold is noisy/ambiguous** (VLDB 2026); these runs use the official scorer, so
  some "misses" are gold errors and the true ceiling is likely a few points higher.
- **Token/cost not captured** in these runs (telemetry predates token tracking); cost can't be
  reported here.

---

## 6. Takeaways for the project
1. **Architecture ceiling on a representative 100-query Snow set is ~84%** (reliable core ~76%,
   best-case 87%), with Best-of-8 + repair.
2. **Geospatial is the weakest domain** — if the real workload is geo-heavy, that's where to
   invest (spatial-function guidance, geo-aware schema linking).
3. **The ~84% gold-fed vs the lower no-gold number is the value of a good verifier/stopping
   signal** — closing that gap (a learned verifier trained on these gold labels) is the highest-
   value production lever, since the candidates to reach 84% are already being generated.

*Data: `reports/experiments/benchmark_run_{10,11,12}/instance_results.jsonl` (+ manifests).
Companion reports: `GOLD_FED_ABLATION_REPORT.md` (component value, n=30), `FINAL_REPORT.md`
(no-gold / production accuracy + improvements).*
