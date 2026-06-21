# Implementation Plan — Explore → Plan front-end + Dialect/Determinism prompts

Bundle: **#1 online value/entity exploration · #2 structured pre-generation plan
(Information Aggregation) · #7 dialect-discipline prompts · #8 determinism tips.**

Goal: add a gold-free "explore → plan → generate" front-end that attacks our two
measured failure modes — (1) wrong/long-tail literals → empty results, and
(2) complex multi-step logic / generation ceiling — without disturbing the
existing best-of-N + consensus + self-critic back-end.

All flags default OFF so current behavior is unchanged until explicitly enabled.

---

## 0. Current flow (for reference)

```
experiment_runner.main loop
  build_schema_slice()                         # retrieval + variant descriptions
  retrieve semantic_context / sample_context
  solve_instance(...)
    └─ _solve_best_of_n → run_best_of_n
         generate_candidate_sqls()  ── per strategy: build_plan_prompt_with_strategy → plan → SQL
         refine_sql() per candidate (+ self-critic in no-gold)
         verification pass → score (heuristic+shape+metamorphic+verifier+consensus) → select
```

New phases (exploration, planning) run **once per instance inside `solve_instance`**,
before candidate generation, and their outputs thread into the generation prompts
exactly like the existing `semantic_context` / `sample_context` params.

```
solve_instance
  exploration_context = explore(...)            # NEW #1  (executes probes)
  plan_context        = build_information_plan(...) # NEW #2 (1 LLM call)
  → _solve_best_of_n / _solve_single (pass both contexts)
      → generate_candidate_sqls / run_pipeline (pass both)
          → build_plan_prompt_with_strategy(... + dialect #7 + determinism #8)
```

---

## 1. New module: `src/rag_snow_agent/agent/exploration.py`

Holds both #1 (exploration) and #2 (planning). ~200 lines.

### Data structures
```python
@dataclass
class ExplorationResult:
    evidence_text: str          # compact, prompt-ready: each probe SQL + truncated result
    probes: list[dict]          # [{sql, ok, rows, note}] for telemetry/logging
    n_probes: int
```

### #1 `explore(...)`
```python
def explore(
    question: str,
    db_id: str,
    schema_slice: SchemaSlice,
    executor: SnowflakeExecutor,
    model: str,
    max_probes: int = 6,
    max_rows: int = 20,
    max_evidence_chars: int = 4000,
    max_tokens: int = 800,
) -> ExplorationResult
```
Steps:
1. Prompt the LLM (see `get_exploration_prompt`) with the question + schema slice
   to emit ≤`max_probes` **read-only** probe SQLs (DISTINCT / ILIKE fuzzy / FLATTEN
   structure / COUNT vs COUNT(DISTINCT)). Parse the ```sql``` blocks.
2. **Safety guard** `_is_safe_select(sql)`: allow only single statements starting
   with `SELECT`/`WITH`; reject `;`-chained, DML/DDL/`CALL`/`MERGE`/`COPY`/`PUT`/
   `CREATE`/`DROP`/`ALTER`/`GRANT`/`INSERT`/`UPDATE`/`DELETE`/`TRUNCATE`. Inject a
   `LIMIT max_rows` if no LIMIT present (wrap as subquery if needed).
3. Execute each probe with `executor.execute(sql, sample_rows=max_rows)`. On error,
   one bounded self-correct attempt (reuse `refiner._attempt_repair` style or a small
   inline fix prompt); else drop the probe.
4. Build `evidence_text`: for each successful probe →
   `-- <note>\n<sql>\n→ <truncated result rows>`; cap total to `max_evidence_chars`.
Return `ExplorationResult`.

Concurrency: probes can run sequentially (≤6) — keep simple first; parallelize later
if latency matters.

### #2 `build_information_plan(...)`
```python
def build_information_plan(
    question: str,
    schema_slice: SchemaSlice,
    exploration_evidence: str,
    evidence: str | None,          # compressed external knowledge if available
    model: str,
    max_tokens: int = 1200,
) -> str                            # structured plan text (<= ~1500 chars after trim)
```
One LLM call (see `get_information_plan_prompt`) producing a **structured plan**:
- table roles, **entity→column mapping** (with exact nested access paths, e.g.
  `h.value:product.productRevenue`), **resolved literal values** (from exploration),
  **join/FLATTEN paths**, derived metrics/formulas, output grain & columns.
Return as compact text; trim to a char cap. This is *guidance*, not a hard constraint —
candidates may deviate.

---

## 2. Prompts: `src/rag_snow_agent/prompting/prompt_builder.py`

### New prompt builders
- `get_exploration_prompt(api, schema_text, question) -> messages` — instructs ≤N
  read-only probes; embeds #7 dialect discipline (fuzzy ILIKE, FLATTEN "inspect
  `f.value` first", DISTINCT, COUNT vs COUNT DISTINCT).
- `get_information_plan_prompt(question, schema_text, exploration_evidence, evidence) -> messages`.
- `get_dialect_discipline(api) -> str` (#7): Snowflake block —
  - Fuzzy match: *"Don't match strings you aren't sure of; explore first. Use
    `WHERE col ILIKE '%a%b%'`, replace spaces with %."*
  - Nested: FLATTEN access patterns + "inspect `f.value` when structure unknown."
  - Date shards: *"UNION ALL all matching day-tables, then filter — list tables
    explicitly."*
  - *"Use values/knowledge from the database, not your own."*
- `get_determinism_tips() -> str` (#8): `ORDER BY ... NULLS LAST`; secondary sort key
  on ties; default 4 decimals if unspecified; return both name and id when ambiguous.

### Inject into generation
In `build_plan_prompt_with_strategy(...)` (currently ~line 300-347) add optional
params `exploration_context`, `plan_context`, and append (order):
`plan_context` → `exploration_context` → existing semantic/sample/memory → schema →
`get_dialect_discipline(api)` → `get_determinism_tips()`.
Token budget: cap each injected block (plan ≤1500, exploration ≤4000, dialect/determinism ≤600 chars).

### Inject into repair
In `refiner._build_repair_prompt` / `_build_empty_result_repair_prompt`, append
`get_dialect_discipline(api)` (esp. fuzzy-match + nested) so empty-result repairs use
the same discipline.

---

## 3. Threading the contexts (mirror existing `sample_context` pattern)

Add `exploration_context: str | None = None`, `plan_context: str | None = None` to:
- `prompting/prompt_builder.build_plan_prompt` / `build_plan_prompt_with_strategy`
- `agent/candidate_generator.generate_candidate_sqls`
- `prompting/...run_pipeline` (single-candidate path)
- `agent/best_of_n.run_best_of_n`
- `agent/agent.solve_instance`, `_solve_best_of_n`, `_solve_single`

In `solve_instance` (agent.py:72), after `_eval_standards` setup and before branching:
```python
exploration_context = plan_context = None
if enable_exploration:
    er = explore(instruction, db_id, schema_slice, executor, model, ...)
    exploration_context = er.evidence_text
    if enable_planning:
        plan_context = build_information_plan(instruction, schema_slice,
                                              exploration_context, evidence=semantic_context, model=model)
```
Pass both into `_solve_best_of_n` / `_solve_single` → down to generation.

(Exploration runs once per instance and is shared across all N candidates — cheap relative to best-of-N.)

---

## 4. Runner + config: `src/rag_snow_agent/eval/experiment_runner.py`

- CLI flags: `--enable_exploration`, `--enable_planning`, `--exploration_max_probes` (default 6).
- `apply_cli_toggles`: `features["exploration"] / features["planning"]`,
  `config["exploration"]["max_probes"]`.
- Derive in main loop (near other feature flags ~line 555):
  `exploration_enabled`, `planning_enabled`, `exploration_max_probes`.
- Pass to `solve_instance(... enable_exploration=..., enable_planning=..., exploration_max_probes=...)`.
- Add to `write_manifest` toggles.

Config defaults (config yaml): `features.exploration: false`, `features.planning: false`,
`exploration: {max_probes: 6, max_rows: 20, max_evidence_chars: 4000}`.

---

## 5. Telemetry (`observability/instance_telemetry.py`)
Mark `exploration_used`, set `exploration_probes`, `exploration_probe_errors`,
`planning_used`. Record `exploration_context`/`plan_context` lengths in the per-instance record (`experiment_runner` record dict) for analysis.

---

## 6. Cheap validation BEFORE any gpt-5.4 run (mandatory)

Run a 1-instance dry check (no full A/B) and assert:
1. `explore()` returns ≥1 successful probe and non-empty `evidence_text`; print probes.
2. For `sf_bq010` ("Youtube ... Henley") the exploration evidence contains the resolved
   exact product string(s) — proves entity resolution works.
3. `build_information_plan()` returns a plan mentioning the right columns/paths.
4. Build the candidate prompt and assert it **contains**: plan text, exploration
   evidence, dialect block (`ILIKE`), determinism block (`NULLS LAST`).
5. Safety: feed a malicious probe (`DROP TABLE`) to `_is_safe_select` → rejected.

Only proceed to the full run once all five pass (we wasted a gpt-5.4 run last time on
un-wired descriptions — this gate prevents that).

---

## 7. Evaluation protocol
- Model: **gpt-5.4** (techniques are amplifiers; mini is meaningless).
- Mode: no-gold (`--eval_gold_dir`), candidate persistence ON.
- A/B on the same 10 (and a spot-checked subset): 
  - Control = current improved arm (consensus + self-critic).
  - Treatment = + exploration + planning + dialect + determinism.
- Report: gold_matched delta, **and** `candidate_gold_any` (did exploration/plan raise
  the *ceiling*?) — that's the metric that proves #1/#2 worked, independent of selection.
- Caveat: gold is ~63% noisy → also eyeball the flipped instances manually; don't trust
  raw deltas on n=10.

---

## 8. Risks & mitigations
| Risk | Mitigation |
|------|-----------|
| Probe latency/cost (N executions + 2 LLM calls/instance) | ≤6 probes, shared across all candidates, gpt-5.4 budget OK; make probe gen model configurable |
| Unsafe probe execution | `_is_safe_select` allowlist + LIMIT injection + executor timeout (already 120s) |
| Exploration misleads (bad probes) | bounded self-correct; evidence is *hints*, plan is *guidance*, candidates may deviate |
| Plan over-constrains | phrase as guidance; keep best-of-N strategy diversity intact |
| Token bloat | per-block char caps (plan 1.5k / evidence 4k / dialect+determinism 0.6k) |

---

## 9. Phasing & effort
1. **Prompts #7+#8** (`get_dialect_discipline`, `get_determinism_tips`) + inject into
   generation/repair. ~0.5 day. Independently testable; lowest risk.
2. **Exploration #1** (`exploration.py: explore` + safety guard) + thread
   `exploration_context`. ~1 day.
3. **Planning #2** (`build_information_plan`) + thread `plan_context`. ~0.5 day.
4. **Runner flags + telemetry + validation gate.** ~0.5 day.
5. **gpt-5.4 A/B + analysis.** (run-time)

Total ~2.5 days eng + run time. Each phase is independently shippable behind its flag.

---

## 10. Acceptance criteria
- All current tests pass; flags OFF → byte-identical behavior to today.
- Validation gate (§6) passes on `sf_bq010` (entity resolved) + safety check.
- On gpt-5.4 A/B: treatment raises `candidate_gold_any` over control on ≥2/10 (ceiling
  lift) — the primary success signal — with no regressions in execution success.
