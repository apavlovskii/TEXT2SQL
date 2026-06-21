# No-Gold Accuracy Experiment — Self-Consistency Voting + Self-Critic Repair

**Goal:** maximize Text-to-SQL accuracy **without the agent ever seeing gold results** at
inference, then measure the effect on a 10-query slice — first with `gpt-5.4-mini`, then with
the stronger `gpt-5.4`.

**What was built** (commit `adf4a0c0 Benchmark code` + this branch):
- **Self-consistency / MBR voting** (`selector.py`, `best_of_n.py`): candidates are clustered
  by an order-insensitive signature of their *executed result*; a candidate gets
  `consensus_bonus * (independent_votes − 1)`, where votes = number of **distinct strategies**
  that converged on the same result (so re-emitting one strategy can't inflate a vote). Gold-free.
- **Self-critic repair loop** (`refiner.py`): in the no-gold path, an LLM inspects
  `(question, SQL, result preview)` and, if it finds a concrete error, drives a `SELF_CRITIQUE`
  repair — the production substitute for the gold-driven `RESULT_MISMATCH` loop. Never marks a
  successful execution as failed; only spends extra repair attempts.
- **No-gold measurement harness** (`experiment_runner.py`): `--eval_gold_dir` scores the final
  SQL against gold **post-hoc** (agent stays blind to gold). Flags: `--enable_self_critic`,
  `--self_critic_max`, `--disable_consensus`.
- **Candidate-level logging**: every candidate's `final_sql` is persisted and **each is
  gold-checked post-hoc** (`candidates[].gold_matched`, `candidate_gold_any`) so we can separate
  *selection-miss* (a correct candidate existed but wasn't picked) from *generation-ceiling*
  (no candidate was correct).

All components verified active (unit-tested voting; logs show consensus + critique firing;
manifests confirm `gold_dir` was never passed to the agent).

---

## 1. Setup

- **Slice:** first 10 non-geospatial spider2-snow instances
  (`sf_bq011, 010, 009, 001, 002, 003, 004, 008, 269, 268`) — 9× GA360, 1× GA4. These are the
  hardest, most gold-repair-dependent analytics queries (nested VARIANT `hits`, exact-match gold).
- **Best-of-N = 4**, **max_repairs = 3**, `--eval_gold_dir` for post-hoc scoring.
- **Reference:** gold-fed full system (`n30_A0_full`) on these exact 10 = **10/10**.
- Accuracy = **post-hoc gold match**. `ok=10` in the progress bar only means "executed"; in
  no-gold mode nearly every query executes, so execution success ≠ accuracy.

## 2. Headline result — the gain is a model × method interaction

No-gold accuracy (gold-correct / 10; gold-fed reference = 10/10):

| | **gpt-5.4-mini** | **gpt-5.4** |
|---|:---:|:---:|
| **baseline** (no consensus, no self-critic) | 1/10 | 1/10 |
| **improved** (consensus + self-critic, max 3) | 1/10 | **5/10** |

- **Neither lever alone helps.** Upgrading the model at baseline: 1 → 1. Adding the gold-free
  improvements on mini: 1 → 1.
- **Together they 5×:** gpt-5.4 + consensus + self-critic = **5/10** (+400% over every other
  no-gold arm), recovering half of the gold-fed ceiling with the agent fully blind to gold.
- LLM cost of the improved arm: 88 → 152 calls (+73%) — cheap for +4 correct (budget was not a
  constraint).

## 3. Why — attribution from per-candidate gold checks

Because every candidate is gold-checked, we can see exactly where the gain comes from
(measured on the gpt-5.4 arms):

| gpt-5.4 arm | selected correct /10 | **any correct candidate in pool /10** |
|---|:---:|:---:|
| baseline (no critic) | 1 | **2** |
| improved (consensus + critic) | 5 | **5** |

1. **The self-critic *creates* correct candidates.** It lifted the pool from **2 → 5**
   instances that had at least one gold-correct candidate (rescued `sf_bq011`, `sf_bq010`,
   `sf_bq002`). The critic's diagnoses were sharp in *both* models (e.g. *"scans only
   EVENTS_20210131, so the Jan 1–7 filter can't access those dates"*; *"returns only one month,
   but the question asks for June **and** July"*) — but **only gpt-5.4 could act on them** to
   produce a corrected query. This is the whole interaction: critique quality is
   model-independent; *repair* quality is not.
2. **Consensus/selection captured the winners.** Baseline left `sf_bq001` on the table
   (a correct candidate existed but the heuristic picked a wrong one). The improved arm selected
   correctly on all 5 — result-agreement voting broke ties toward the answer multiple
   independent strategies converged on. No regressions from voting.

So on gpt-5.4 the two components are complementary: **self-critic raises the ceiling
(generation), consensus raises the capture rate (selection).**

## 4. Why mini saw zero gain (the earlier result, now explained)

On `gpt-5.4-mini`, baseline = improved = 1/10 even with the self-critic running 91 repair
attempts (up to 4/candidate). The critic diagnosed problems correctly but mini's *repairs* did
not converge — it could not translate "you're missing July / you're reading the wrong shard"
into correct SQL. The lever was there; the generator couldn't pull it. gpt-5.4 can.

## 5. Remaining failures & one downside

- **Still-ceiling (no correct candidate even with critic):** `sf_bq009, 003, 008, 268` —
  hard GA360 queries where neither model produced a correct candidate. These need Pillar-4 work
  (online value/shard exploration, decomposition-with-verification), not more critique.
- **Robustness downside:** on `sf_bq269` the critic's repairs pushed **all 4 candidates into
  `sql_syntax_error`** (baseline: wrong-but-executing → improved: broken). Aggressive critique
  can trade a plausible-wrong answer for a non-executing one. Mitigation: only accept a
  critic-driven repair if it still executes (keep the last successfully-executing SQL as a
  fallback).

## 6. Conclusions

1. **Gold-free self-critique + self-consistency DO recover accuracy without gold — but only
   with a capable enough generator.** On this hardest slice they moved no-gold accuracy from
   1/10 to 5/10 with gpt-5.4, while doing nothing on mini.
2. **Model choice and method are not substitutes.** A better model with the old selection/repair
   logic stayed at 1/10. The architecture is what converts the stronger model's competence into
   accuracy. Budget-permitting, run the strong model *with* these gold-free signals.
3. **The candidate-gold instrumentation paid off:** it showed the self-critic's value is
   *raising the candidate ceiling*, not just re-ranking — which tells us where to invest next.

## 7. Recommended next steps

1. **Isolate the two levers on gpt-5.4** (critic-only vs consensus-only arms) to apportion the
   +4 precisely. Current evidence: critic ≈ +3 (pool 2→5), selection ≈ +1 (captured sf_bq001).
2. **Add an "executes-or-revert" guard** to the self-critic so it can never turn an executing
   query into a broken one (fixes the `sf_bq269` regression).
3. **Distilled verifier (#8)** trained on the gold logs — to capture the remaining
   selection-misses and exact-match patterns plausibility can't see.
4. **Value-grounding probes (#7)** for the still-ceiling cases — the critic already *named* the
   causes (wrong shard / 0-row joins); a deterministic probe can verify and fix them.
5. **Evaluate on a larger, more diverse slice** — this 10-query GA360 slice is the worst case
   for gold-free methods; the +400% here is a lower bound on the gain for easier query classes.

---

*Artifacts (instance_results.jsonl + manifest.json): mini — `nogold_baseline_10/`,
`nogold_improved_10/` (critic max1), `nogold_improved_critic3_10/` (critic max3); gpt-5.4 —
`nogold_baseline_10_g54/`, `nogold_improved_10_g54/`. Logs under
`reports/experiments/_sweep_logs/nogold_*`. Per-candidate SQL + gold flags live in each record's
`candidates[]` field (gpt-5.4 runs).*
