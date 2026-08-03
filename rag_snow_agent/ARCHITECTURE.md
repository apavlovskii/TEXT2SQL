# SnowRAG-Agent — Solution Architecture

A custom-built (no LangChain/LangGraph) RAG pipeline that converts natural-language
analytics questions into executable Snowflake SQL. This document describes every
component in the pipeline and the specific engineering tricks that make it work,
for anyone extending or debugging the system.

**Final accuracy: 90%** on the representative evaluation set (30 instances spanning
GitHub, NOAA, patents, healthcare, and web-analytics schemas).

---

## 1. Pipeline at a glance

```
NL question
    │
    ▼
┌─────────────────────────┐
│ 1. Schema retrieval      │  hybrid BM25 + embeddings, RRF fusion, SchemaSlice trim
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 2. Exploration front-end │  read-only probes: entity values, date/VARIANT formats
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 3. Best-of-N generation  │  N candidates, N structurally distinct prompt strategies
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 4. Deterministic fixes   │  LISTAGG/NULLIF, epoch-date casts, VARIANT access, ...
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 5. Repair loop           │  execution-feedback + self-critique repair, capped budget
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 6. Selection             │  consensus/self-consistency voting + LLM tie-break
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│ 7. Verification          │  fingerprint check, metamorphic check, learned verifier
└─────────────────────────┘
    │
    ▼
Final SQL + answer
```

Core code lives in `src/rag_snow_agent/`:

| Directory | Responsibility |
|:----------|:----------------|
| `retrieval/` | Hybrid schema retrieval, SchemaSlice trimming/expansion |
| `chroma/` | ChromaDB vector store, schema/sample-record/semantic cards |
| `prompting/` | Prompt construction, plan schema, SQL compiler, deterministic SQL correction |
| `agent/` | Orchestration — candidate generation, Best-of-N, selection, repair |
| `eval/` | Benchmark runner, gold verification, ablation baselines |

---

## 2. Schema retrieval

**Hybrid index:** BM25 + `text-embedding-3-large`, combined with Reciprocal Rank
Fusion (k = 60), over a purpose-built entity-oriented index in ChromaDB. Card types:
`TableCard`, `ColumnCard`, `JoinCard`, `SemanticCard` (business-term → column
mappings), `SampleRecord` (representative row values, for value grounding),
`SnowflakeSyntax` (dialect tips: `LATERAL FLATTEN`, `VARIANT` access, `NULLS LAST`,
date casts).

**Partition collapsing:** temporally-sharded sources (e.g. GA360's 366 daily
tables) are collapsed at index-build time into one representative `TableCard`.
Without this, the shards drown out retrieval ranking for the tables that actually
matter.

**SchemaSlice post-processing** (`retrieval/schema_slice.py`), applied after
retrieval:
- Protected identifiers (join columns, time columns) resist trimming even under a
  tight token budget.
- 1-hop join-graph expansion: pull in tables reachable via one FK hop from any
  retained table, so a correct join target isn't dropped just because retrieval
  ranked it low.
- VARIANT enrichment: inject `SnowflakeSyntax` cards for tables with nested
  JSON/ARRAY columns.

**Measured retrieval recall:** 86.0% Schema Retrieval Rate at top-8 (all
gold-referenced tables present in the retrieved slice), 88.4% at top-20 — achieved
with zero LLM completion tokens in the schema-linking step.

---

## 3. Exploration front-end

Before generation, the agent runs read-only SQL probes against the live database
(`agent/agent.py: run_pregeneration_frontend`):
- **Entity resolution** — `SELECT DISTINCT col FROM table LIMIT 20` to discover
  real filter values (product names, category codes, ID formats) instead of
  guessing.
- **Format detection** — probes whether a date column is epoch-encoded or
  `YYYYMMDD`, what VARIANT field names actually look like, how deep nested arrays
  go.
- **Existence checks** — confirms candidate tables are non-empty and the columns
  the plan wants actually exist.

Results are injected into the prompt as grounding facts, replacing model guesses
with real evidence. Schema indexing is a prerequisite for this to work at all: an
unindexed database drops success from 19/20 to 5/36 on the same instances — a "low
accuracy" run on an unindexed DB is an infrastructure gap, not a model failure.

An **information-aggregation planning** step runs alongside exploration, building
an explicit entity→column map, join path, and constraint checklist before SQL is
generated — this is what eliminates dropped filter constraints that a single-shot
generation would silently omit.

---

## 4. SQL generation — raw SQL + deterministic correction

**Current default** (`agent/candidate_generator.py`, `use_deterministic_compiler=False`):
the LLM writes SQL directly against the retrieved schema and exploration context,
then a deterministic correction layer (`prompting/sql_correction.py`) validates and
repairs it — no intermediate structured plan.

A structured `QueryPlan → compile_plan()` path still exists (`use_deterministic_compiler=True`)
and is kept only for ablation/comparison: forcing the LLM into a rigid plan/template
format was found to fight the LLM's natural generation rather than help it — it
measured **~70% execute / ~10% gold-match**, versus **~95% / ~35%** for raw SQL on
the same instances. Don't reintroduce the compiler path as the default without new
evidence; this was a deliberate, tested decision.

**`validate_raw_sql`** parses arbitrary LLM-authored SQL with `sqlglot` (so it works
regardless of the model's quoting/casing style) and scope-resolves every SELECT
(including inside CTEs/subqueries/UNION branches) against its *own* FROM/JOIN
sources — a bare column that's actually an upstream CTE's output column is never
mistaken for a real table's column just because that table appears elsewhere in
the query.

**`check_type_mismatches_raw`** flags filter comparisons between a `NUMBER`-family
column and a date-shaped string literal — the classic epoch/`YYYYMMDD`-encoded
column being compared to a human-readable date. One repair round-trip
(`build_fix_sql_prompt`) is attempted before falling back to the unfixed candidate.

---

## 5. Deterministic post-generation fixes (tips & tricks)

These are applied unconditionally after generation, regardless of what the LLM
produced — the philosophy throughout is: **the LLM owns intent, deterministic code
owns correctness-critical mechanics that prompting alone can't reliably guarantee.**

- **LISTAGG / NULLIF wrap** (`prompting/sql_compiler.py: rewrite_listagg_nullif`) —
  Snowflake's `LISTAGG` returns an **empty string**, not `NULL`, when every value in
  a group is `NULL` (e.g. after a `LEFT JOIN` with no match). Every `LISTAGG(...)`
  call is wrapped in `NULLIF(..., '')` so downstream `NULL` comparisons work as
  expected. A prompt-only instruction to do this was tried first and found
  unreliable (0/8 candidates applied it live even though every system prompt
  contained it) — replaced with a mechanical, always-safe rewrite instead of
  relying on LLM compliance.
- **Epoch / date-shard rewriting** — detects integer epoch-microsecond columns
  being cast as if they were native dates and rewrites to
  `TO_TIMESTAMP(col/1e6)::DATE`; also rewrites naive casts on native date columns
  to proper `DATE 'YYYY-MM-DD'` literals.
- **VARIANT access normalization** — `:field` vs `['field']` vs `GET_PATH(col,
  'field')`, normalized per dialect convention.
- **Change/difference ambiguity** (`agent/candidate_generator.py:
  _detect_unqualified_change_ambiguity`) — a question using "change"/"difference"/
  "delta"/"variation" *without* a directional qualifier ("increase", "decrease",
  ...) is genuinely ambiguous between a **signed** value and an **absolute
  magnitude**. Confirmed on a real instance (`sf_local056`) where gold used
  `ABS(delta)` and the pipeline computed a signed delta — both are defensible
  readings. Rather than guess, the pipeline deliberately forces two of the
  Best-of-N candidates to commit to each interpretation, so selection gets a real
  choice instead of every candidate independently guessing the same way.
- **Lossy-join risk hint** (`agent/refiner.py: _detect_lossy_join_risk`) — flags an
  `INNER` (or bare) `JOIN` to a table whose name suggests derived/computed
  standings-or-ranking data, on a question asking for a result across an enumerated
  domain ("for each year/month..."). Such tables often cover a narrower range than
  the base entity table; an `INNER JOIN` silently drops periods they don't cover
  rather than erroring. Surfaced as a hint to the self-critique reviewer, not an
  automatic rewrite — the join may be intentional.
- **All-NULL column / sort-direction checks** (`agent/refiner.py`) — two
  zero-LLM-judgment signals that short-circuit straight to "flagged problem": a
  result column that's `NULL` in every row (the value was never actually computed),
  and a sort order that contradicts explicit superlative wording ("highest",
  "lowest", ...) in the question.
- **Empty-result consensus trap fix** (`agent/best_of_n.py`) — candidates that
  independently return zero rows used to cluster into a false "consensus" and
  out-score the one candidate with a real result. Fixed: empty-result candidates
  score with `consensus_votes = 0` instead of winning by trivial agreement.

---

## 6. Best-of-N generation with structural strategy diversification

`agent/candidate_generator.py` generates N candidates, each biased toward a
different reasoning entry point (one extra instruction prepended to the same
plan-generation system prompt):

| Strategy | Bias | Targets |
|:---------|:-----|:--------|
| `default` | none | general queries |
| `flatten_first` | identify VARIANT/ARRAY columns needing `LATERAL FLATTEN` first | nested/semi-structured data |
| `cte_first` | break the question into sequential steps, one CTE per step | multi-step aggregation, ranking |
| `join_first` | identify correct JOIN relationships first, build outward | multi-table joins |
| `metric_first` | identify the target metric/aggregation first, trace back to sources | COUNT/SUM/AVG questions |
| `time_first` | identify date/time filters or grouping first | time-series, date ranges |
| `geo_first` | identify spatial predicates first (`ST_WITHIN`, `ST_DWITHIN`, ...) | spatial/location queries |

This produces **structural** diversity (different reasoning paths), not just
stochastic resampling of the same prompt — temperature also ramps from 0.2
(candidate 1) to 0.3 (candidates 2+) on top of the structural hint. Best-of-N is,
by a wide margin, the single most impactful component in the whole pipeline:
removing it alone costs the largest accuracy drop of any single-component ablation.

---

## 7. Selection: consensus voting + LLM tie-break

**Self-consistency / MBR voting** (`agent/selector.py`, `agent/best_of_n.py`):
candidates are clustered by an order-insensitive signature of their *executed
result* (not SQL text) — two queries with different SQL but identical result sets
are the same answer. A candidate's consensus bonus counts only **distinct
strategies** that agree, so re-emitting the same strategy twice can't inflate its
own vote.

**LLM tie-break for close calls** (`agent/best_of_n.py: _find_tiebreak_pair` /
`_llm_tiebreak`): plurality voting alone can be fooled when several strategies
share the same mistake, forming a false majority whose sheer vote-count bonus
buries a single correct outlier. The pipeline compares clusters on their
*intrinsic* score (score with the consensus bonus subtracted back out) — if the
runner-up cluster's intrinsic score is within a calibrated ratio (0.75) of the
leader's, an LLM is asked to adjudicate directly between the two result sets. This
threshold was calibrated against a real confirmed failure (`sf_bq059`): a lone
correct candidate lost outright to a 6-vote wrong cluster purely because of
vote-count bonus, at an intrinsic ratio of ≈0.82 — the threshold is set safely
below that observed value. When the disagreement is plausibly the change/
difference ambiguity described above, the tie-break prompt is sharpened with
reasoning specific to that ambiguity rather than a generic "which is more
correct?" framing.

---

## 8. Repair loop

After a candidate executes (success or error), a repair loop
(`agent/refiner.py`) inspects `(question, SQL, execution result or error)` and
attempts targeted fixes, budgeted to a fixed number of attempts per candidate.

- **Category-classified repair:** 8 Snowflake-specific error classes
  (`SYNTAX_ERROR`, `COLUMN_NOT_FOUND`, `TYPE_MISMATCH`, `RESULT_MISMATCH`,
  `EMPTY_RESULT`, `WRONG_AGGREGATION`, `WRONG_JOIN`, `VARIANT_ACCESS_ERROR`), each
  with a category-specific repair prompt.
- **Self-critique repair** (gold-free): an LLM inspects `(question, SQL, result
  preview)` and only spends repair budget when it diagnoses a *specific, concrete*
  problem — it never marks a successfully-executing query as failed on stylistic
  grounds. Diagnostic quality is largely model-independent, but translating a
  correct diagnosis into a fixed query is model-dependent (a stronger model closes
  the loop; a weaker model can identify the same bug and still fail to fix it).
- **Robustness guard:** if a repair attempt makes things worse (pushes a
  wrong-but-executing candidate into a syntax error), the loop reverts to the last
  successfully-executing SQL rather than keeping the broken repair.

---

## 9. Verification

Two lightweight post-execution checks on the selected candidate
(`agent/verifier.py`, `agent/metamorphic.py`):
- **Fingerprint check** — hashes the executed result and compares against a cached
  fingerprint for the same question, catching exact repeats of a known-wrong
  result.
- **Metamorphic check** — runs a semantically equivalent reformulation of the
  question and checks both produce consistent results, catching silent semantic
  errors that execute cleanly but answer the wrong thing.
- **Learned verifier** — a trained classifier scores candidates using real
  `gold_matched` ground truth (built via
  `eval/train_verifier.py: build_verifier_dataset_from_experiment_results`, not the
  older `is_best` heuristic label). Fixed a path bug where the model file could
  never be located (an off-by-one directory-traversal depth), silently falling
  back to a heuristic score instead of the trained model. Current model: 86.3% test
  accuracy vs. 77.5% majority-class baseline.

---

## 10. Known limitations

- **Geospatial queries** (`ST_*` predicates) are the dominant weak domain —
  `PlanGeoJoin`/`PlanGeoFilter` compiler support exists but isn't fully wired in;
  this only affects the legacy plan-compiler path, not the raw-SQL default.
  Recursive-CTE dialect translation is also still hard.
- **Generation ceiling on ambiguous multi-step business logic** — the remaining
  misses are dominated by genuinely complex, multi-step domain reasoning rather
  than formatting or retrieval bugs.
- **Model dependency for repair** — self-critique diagnoses are model-independent,
  but only a strong enough base model can reliably turn a correct diagnosis into a
  fixed query; a weaker model gets no benefit from the same repair loop.
- Spider 2.0-Snow's published gold SQL has a non-trivial rate of annotation
  errors/ambiguity (integer-vs-string IDs, column-order mismatches, decimal vs.
  float representations) — treat single-run accuracy numbers on this benchmark as
  somewhat noisy rather than exact.
