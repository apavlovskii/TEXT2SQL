# RAG retrieval — follow-up notes

Forwarded answers to follow-up questions about the `rag_snow_agent` retrieval
stack (collections, query construction, staged outputs). All references are
file paths inside `/home/raskin/TEXT2SQL/rag_snow_agent/`.

---

## 1) What is in a semantic card and how it's used

### Source model (`src/rag_snow_agent/semantic_layer/models.py`)

A `SemanticCard` is a small, normalized fact about a database object —
**not** a free-text description. Its fields are:

| Field | Meaning |
|---|---|
| `db_id` | Which database the fact belongs to (`GA4`, `GA360`, `PATENTS`, …). |
| `fact_type` | The category of the fact (closed vocabulary, see below). |
| `subject` | Qualified name the fact is about — usually a column (`GA4.EVENTS.EVENT_TIMESTAMP`) but can be a table. |
| `confidence` | 0.0–1.0; how strong the inference is (higher for FK-style evidence, lower for heuristic). |
| `source_types` | Where the fact came from: `metadata`, `docs`, `probes`, `traces`. |

The **document that gets embedded** (`SemanticCard.document`, models.py:29-37)
is the 4-line block:

```
Semantic: <fact_type>
Subject:  <qualified_name>
Confidence: <0..1>
Sources: <metadata|docs|probes|traces>
```

That's it — no narrative text. The dense vector for this card is essentially
an embedding of `fact_type + qualified_name`. Comments, sample values, and
example rows are stored in **metadata only**, not in the document.

### Fact types and how they're produced

Categories (from `SemanticProfile` in models.py:53-77 and the per-DB
breakdown in `chroma/COLLECTIONS.md`):

| `fact_type` | Source | Meaning |
|---|---|---|
| `nested_container_column` | metadata | VARIANT/OBJECT/ARRAY column — signal that LATERAL FLATTEN or `:` access may be needed. |
| `date_format_pattern` | metadata | Date-like column whose format was detected (`YYYYMMDD` as NUMBER vs VARCHAR). |
| `dimension_candidate` | metadata | String column that looks like a grouping key (low-cardinality strings, `country_code`, `source`, …). |
| `identifier_column` | metadata | `*_id`/`*_key` / FK-shaped column. |
| `filter_value_hints` | **live Snowflake probe** | Top-5 distinct values for a STRING column (e.g. `['US','GB','DE','FR','JP']`). |
| `sample_rows` | **live Snowflake probe** | 5 representative rows from a unique table schema — shows real value formats and NULL patterns. |

The cards are built once per database by
`semantic_layer.build_semantic_layer` (see `COLLECTIONS.md` build commands).
The `metadata`-sourced facts come from inspecting Snowflake's
`INFORMATION_SCHEMA`; the `probes`-sourced facts cost real query budget
(default `--max_probe_budget 20` per DB).

### How a card is used at solve time

1. **Retrieval** — `retrieve_semantic_context()` in
   `src/rag_snow_agent/retrieval/semantic_retriever.py:12-43` calls
   `SemanticLayerStore.query_semantic_cards(db_id, instruction, top_k=8)`
   (semantic_layer/store.py:66-102). That issues a **single, pure dense**
   Chroma query — no BM25, no RRF, only an embedding cosine search on
   `query_texts=[instruction]` filtered to `db_id`. The retrieval target is
   the embedded 4-line document, so dense match is mostly driven by
   `fact_type` + qualified-name overlap with the question's vector.
2. **Rendering** — only a one-liner per card is rendered into the prompt
   (semantic_retriever.py:34-43):

   ```
   Semantic context:
   - [primary_time_column] GA4.EVENTS.EVENT_TIMESTAMP (conf=0.92)
   - [date_format_pattern] GA360.GA_SESSIONS_*.DATE (conf=0.85)
   - [filter_value_hints] GA360.GA_SESSIONS_*.COUNTRY (conf=0.80)
   ```
   The `value` field on `SemanticFact` (e.g. the actual top-5 country
   codes) is **not** propagated into this rendering. Only fact_type,
   subject and confidence are shown. So at prompt time the card behaves
   like a *typed pointer* — "look at this column, it's a time / dimension /
   probe-confirmed identifier" — not as a content payload.
3. **Where it lands in the prompt** — `prompting/prompt_builder.py:225-243`
   inserts the rendered block *above* the schema block and question in the
   plan-generation user message (and analogously in SQL generation at
   `prompt_builder.py:356-374`). It runs **only if**
   `semantic_layer.enabled = true` in config and the
   `semantic_cards` collection isn't empty.

So the card is best thought of as: a typed hint that "this column has
this semantic role in this DB," computed once during indexing
(metadata + occasional Snowflake probes), and surfaced as a compact
bullet list ahead of the schema.

---

## 2) Is the entire user question compared against the schema cards (cosine), even if it is large and has multiple sections?

**Yes. The full `instruction` string is sent verbatim as one embedding
query.** No chunking, no section splitting, no sub-question routing
happens *before* retrieval.

Evidence:

- `eval/experiment_runner.py:389,422` —
  `instruction = instance.get("instruction", "")` and immediately
  `build_schema_slice(query=instruction, …)`.
- `retrieval/debug_retrieve.py:124-125` —
  `retriever.retrieve_tables(query, db_id, …)` and
  `retriever.retrieve_columns(query, db_id, …)` are passed that same
  `query` string.
- `retrieval/hybrid_retriever.py:74-80` —
  `self.collection.query(query_texts=[query], n_results=…, where=…)`.
  Chroma embeds whatever you hand it; the OpenAI embedding function in
  `chroma/chroma_store.py:21-53` will call the API once for the whole
  string and return one 3072-d vector. Cosine similarity is computed
  between that single vector and every TableCard / ColumnCard vector
  in the filtered subset.

Practical scale (from `Spider2/spider2-snow/spider2-snow.jsonl`):
typical instructions are 100–250 chars (~30–80 tokens), the largest
is ~1.2k chars (~250–300 tokens). All well under
`text-embedding-3-large`'s 8191-token input limit, so they go through
in a single request and get one vector each.

Caveats worth flagging:

- **No query rewriting before retrieval.** The question is not
  paraphrased, summarized, or split into sub-queries before the
  embedding call. The "decomposition" feature
  (`prompting/question_decomposition.py`, gated by
  `agent.decompose_questions`) runs *after* retrieval and only feeds
  the LLM planner — it does not re-run schema retrieval per sub-question.
- **`external_knowledge` is excluded.** When a Spider2-Snow instance
  has an `external_knowledge` file (e.g. a doc explaining a metric
  formula), that file is later loaded into the prompt
  (`experiment_runner.py:485-495`) but is *not* concatenated to the
  retrieval query. So if the question is "Compute metric M as defined
  in spec.md", only the question text — not the metric definition —
  shapes the embedding.
- **Lexical channel is even narrower.** Recall that the BM25-style step
  matches `tokenize_identifier(query)` against the column/table
  `qualified_name` (hybrid_retriever.py:96-105). For a multi-sentence
  question, every content word becomes a token in the query set, but
  only words that happen to match identifier fragments contribute
  (e.g. "transaction revenue" → `{transaction, revenue}` will boost
  any column whose name contains either token).
- **One embedding per call, two calls per question.** Tables and
  columns are retrieved as two **independent** RRF runs (different
  `object_type` filters). Both reuse the same `instruction` vector
  conceptually, but Chroma re-embeds per call.

So: yes, even a long multi-section question becomes one vector and is
matched in one cosine pass against each filtered subset.

---

## 3) On first iteration, the `snowflake_syntax` collection is not searched?

**Correct — on the first iteration it is not touched.** The
syntax collection is **only** queried during the repair loop, after a
SQL failure.

The only call site is `agent/refiner.py:574`, inside `_attempt_repair`:

```python
syntax_guidance = _get_syntax_guidance(error_msg, current_sql, chroma_store)
```

`_attempt_repair` itself is invoked from the repair branches inside
the EXPLAIN/execute loop (`refiner.py:371-375`, `:418-424`) — those
branches run **only after** the previous attempt produced an error.

So the lifecycle is:

| Iteration | Retrieval calls that actually fire |
|---|---|
| Initial plan + SQL | `schema_cards` (RRF×2: tables and columns), `semantic_cards` (dense), `trace_memory` (dense, if `memory.enabled`), `sample_records` (metadata `get`, not a query). `snowflake_syntax`: **not searched.** |
| EXPLAIN succeeds, executor succeeds | Pipeline ends. `snowflake_syntax` was never searched. |
| EXPLAIN/exec fails → repair attempt 1 | `snowflake_syntax.query(query=f"{error_msg[:100]} {sql[:100]}", top_k=2)` fires inside `_attempt_repair`. |
| Each subsequent repair attempt | Same: another `snowflake_syntax` query with the new error/SQL. |

Two related details:

- The syntax query **is dense only** (snowflake_syntax.py:168-189),
  no RRF.
- It has **no `db_id` filter** — syntax chunks are dialect-level (LATERAL
  FLATTEN, QUALIFY, PIVOT, …) and shared across all DBs.
- There is **one** retrieval-like call inside `refiner` that does run on
  the first iteration *before* EXPLAIN: the pre-execution column
  validation (`refiner.py:301-333`) does a metadata-only
  `collection.get(…)` against `schema_cards` to check whether each
  column in the candidate SQL exists. That's a filter, not a vector
  search.

---

## 5) Example: what each stage renders, including `format_for_prompt()`

Walking through a concrete instance (`sf_bq009`, `db_id=GA360`):

### Stage 0 — raw input

```
instance_id: sf_bq009
db_id:       GA360
instruction: Which traffic source has the highest total transaction
             revenue for the year 2017, and what is the difference in
             millions (rounded to two decimal places) between the
             highest and lowest monthly total transaction revenue for
             that traffic source?
```

### Stage 1 — embedding query

The full `instruction` is embedded as one vector by
`text-embedding-3-large` and used twice (tables, columns) and a third
time for the semantic cards. The embedding model sees the literal
text above; no preprocessing.

### Stage 2 — dense retrieval (per RRF call)

Chroma returns up to `min(top_k*2, 200)` IDs nearest the question
vector inside `{db_id="GA360", object_type="table"}`, ordered nearest
→ farthest. Example (ranks shown):

```
1  table:GA360.GA_SESSIONS_2017.GA_SESSIONS_*
2  table:GA360.GA_SESSIONS_2017.GA_SESSIONS_20170101
3  table:GA360.GA_SESSIONS_2016.GA_SESSIONS_*
…
```

### Stage 3 — lexical re-ranking on the same candidate set

`tokenize_identifier("Which traffic source has the highest…")` →
`{which, traffic, source, has, the, highest, total, transaction,
revenue, for, year, 2017, and, what, is, …}`. Each candidate's
`qualified_name` is tokenized; ranking key = `|query_tokens ∩
name_tokens|`. So `GA360.GA_SESSIONS_*.TRAFFICSOURCE` (token overlap
≈ 2: `traffic`, `source`) ranks above
`GA360.GA_SESSIONS_*.GEONETWORK` (overlap 0). Ties keep dense order.

### Stage 4 — RRF fusion

For each ID, score = Σ `1 / (60 + rank_i)` over the two ranked lists.
Output: `[(id, rrf_score)]` sorted descending. The retriever wraps the
top-`top_k` into `ScoredItem`s carrying `dense_rank`, `lexical_rank`,
`fused_rank`, and `rrf_score`. Debug view (`debug_retrieve.py:266-271`):

```
--- Top tables (retrieved 25) ---
  rank=  1  dense=  1  lex=  3  rrf=0.0322  GA360.GA_SESSIONS_*.GA_SESSIONS_*
  rank=  2  dense=  4  lex=  1  rrf=0.0306  GA360.GA_SESSIONS_*.TRAFFICSOURCE
  rank=  3  dense=  2  lex=  6  rrf=0.0298  GA360.GA_SESSIONS_*.TOTALS
  …
```

### Stage 5 — SchemaSlice assembly

Columns from the column-RRF list bucket under their parent table;
VARIANT enrichment adds known sub-paths; connectivity/join-graph
rounds pull in any missing joinable tables; budget trim drops
low-`fused_rank` items until `max_schema_tokens` (default 10000) is
respected.

### Stage 6 — `SchemaSlice.format_for_prompt()` (schema_slice.py:61-100)

The trimmed slice is rendered into the compact block injected into
the plan and SQL prompts. For this example it would look like:

```
-- Database: GA360
TABLE GA360.GA_SESSIONS_*.GA_SESSIONS_*  -- Sharded daily session table
  DATE STRING — date as string (YYYYMMDD), compare with string literals e.g. >= '20170201'
  TOTALS VARIANT OBJECT — access with t."TOTALS":"field"::TYPE [fields: transactionRevenue, transactions, visits]
  TRAFFICSOURCE VARIANT OBJECT — access with t."TRAFFICSOURCE":"field"::TYPE [fields: source, medium, campaign]
  FULLVISITORID STRING
  VISITID INTEGER
  CHANNELGROUPING STRING
TABLE GA360.GA_SESSIONS_*.GA_SESSIONS_20170101
  …
```

Key formatting rules (from `format_for_prompt`):

- VARIANT columns get an inline access hint (ARRAY → `LATERAL FLATTEN`,
  OBJECT → `t."col":"field"::TYPE`) plus a `[fields: …]` list of up to
  8 known sub-paths from `variant_fields`.
- Time columns get a format hint (`date as string (YYYYMMDD)` vs
  `date as integer (YYYYMMDD)`) when one was inferred.
- Each column line is `  <name> <type><annotation> -- <comment>`.

### Stage 7 — semantic context (separate, dense-only)

```
Semantic context:
- [primary_time_column] GA360.GA_SESSIONS_*.DATE (conf=0.95)
- [date_format_pattern] GA360.GA_SESSIONS_*.DATE (conf=0.90)
- [filter_value_hints] GA360.GA_SESSIONS_*.CHANNELGROUPING (conf=0.80)
- [nested_container_column] GA360.GA_SESSIONS_*.TOTALS (conf=0.85)
- [nested_container_column] GA360.GA_SESSIONS_*.TRAFFICSOURCE (conf=0.85)
…
```

### Stage 8 — assembled plan-generation user message

`prompt_builder.py:233-244` concatenates (in this order: memory →
decomposition → semantic → sample → schema/question):

```
Semantic context:
- [primary_time_column] GA360.GA_SESSIONS_*.DATE (conf=0.95)
- …

Schema:
-- Database: GA360
TABLE GA360.GA_SESSIONS_*.GA_SESSIONS_*  -- Sharded daily session table
  DATE STRING — date as string (YYYYMMDD), …
  TOTALS VARIANT OBJECT — access with t."TOTALS":"field"::TYPE [fields: transactionRevenue, transactions, visits]
  TRAFFICSOURCE VARIANT OBJECT — access with t."TRAFFICSOURCE":"field"::TYPE [fields: source, medium, campaign]
  …

Question: Which traffic source has the highest total transaction
revenue for the year 2017, and what is the difference in millions
(rounded to two decimal places) between the highest and lowest
monthly total transaction revenue for that traffic source?

Return the plan as JSON only.
```

### Stage 9 — syntax retrieval (only on repair)

If the first SQL EXPLAIN failed with e.g.
`SQL compilation error: Invalid argument type for LATERAL FLATTEN`,
the repair step issues:

```python
syntax_store.query(
    query_text="SQL compilation error: Invalid argument typ… SELECT t.totals:transactionRev…",
    top_k=2,
)
```

returning ≤2 chunks (e.g. `LATERAL_FLATTEN/part1`,
`SNOWFLAKE_IDENTIFIERS/part2`), which are appended to the repair
prompt's system content (refiner.py:577).

### Recap of which stages fire per pipeline run

| Stage | When | Collection | Method |
|---|---|---|---|
| dense+lex+RRF tables | every initial call | `schema_cards` | HybridRetriever |
| dense+lex+RRF columns | every initial call | `schema_cards` | HybridRetriever |
| metadata get (VARIANT fields, fallback cols) | every initial call | `schema_cards` | `collection.get` filter |
| join graph / connectivity | every initial call | `schema_cards` (joins) | `collection.get` filter |
| semantic context | if `semantic_layer.enabled` | `semantic_cards` | dense only |
| trace memory | if `memory.enabled` and traces exist | `trace_memory` | dense only |
| sample records | if `sample_records.enabled` | `schema_cards` (metadata) | `collection.get` filter |
| **snowflake syntax** | **only after a failed attempt** | `snowflake_syntax` | dense only |
| plan-guided expansion | when planner names missing tables | `schema_cards` | HybridRetriever (single-shot) |

> Note: the user's numbering skipped a "4)" — only 1, 2, 3, 5 were
> asked, and only those are answered above.

---

## 6) How a ColumnCard is searched — detailed steps

`HybridRetriever.retrieve_columns(query, db_id, top_k=100)` is just a
thin wrapper around `retrieve(query, db_id, object_type="column",
top_k=100)` (`hybrid_retriever.py:136-139`). The column path differs
from the table path only in the `object_type` filter and the (larger)
default `top_k`. Below is the exact, step-by-step trace.

### Pre-step — what's in the haystack

Before any query is issued, the `schema_cards` collection already holds
one ColumnCard per concrete column in every indexed Snowflake table.
Each card carries (`chroma/schema_cards.py:63-102`):

- **`chroma_id`** = `"column:" + qualified_name` (e.g.
  `"column:GA360.GA_SESSIONS_*.GA_SESSIONS_*.TRAFFICSOURCE"`).
- **`document`** (the text that gets embedded):
  ```
  Column: <DB.SCHEMA.TABLE.COLUMN>
  Type:   <data_type>
  Description: <comment>          # only if non-empty
  Nullable: <YES|NO>
  ```
- **`metadata`** (used for filtering and downstream rendering):
  `db_id`, `object_type="column"`, `qualified_name`,
  `table_qualified_name`, `data_type`, `source`, `token_estimate`,
  optional `comment`.
- **Embedding vector**: 3072-d cosine vector produced by
  `text-embedding-3-large` on the four-line `document` above. Indexed
  in Chroma's per-collection HNSW graph (`hnsw:space="cosine"`, set in
  `chroma_store.py:67-73`).

VARIANT sub-fields exist as a separate flavour of ColumnCard whose
`data_type="VARIANT_FIELD"` — they live in the same `schema_cards`
collection and *can* show up in the dense top-k for a column query,
but `build_schema_slice` skips them when building the slice
(`debug_retrieve.py:143-144`) because their info is already carried by
the parent VARIANT column's `variant_fields` list.

### Step 1 — input shaping

`retrieve` receives `query=<the raw NL instruction>`, `db_id="GA360"`,
`object_type="column"`, `top_k=100` (default from
`config/defaults.yaml`'s `retrieval.top_k_columns`). The string is
passed through with no normalization, no truncation, no
sub-question splitting (see Q2 above).

### Step 2 — dense retrieval inside the column subset

```python
dense_results = self.collection.query(
    query_texts=[query],
    n_results=min(top_k * 2, 200),         # = min(200, 200) = 200 for the default
    where={"$and": [{"db_id": "GA360"}, {"object_type": "column"}]},
    include=["metadatas", "distances"],
)
```
What Chroma actually does (`hybrid_retriever.py:74-80`):

1. Calls the registered `OpenAIEmbeddingFunction` once on `query` →
   one 3072-d query vector.
2. Applies the `where` predicate to restrict candidates to ColumnCards
   for that database — for GA360, this filters out the ~92 GA4 columns,
   the PATENTS columns, etc., leaving roughly 5.5k GA360 ColumnCards
   (per the breakdown in `chroma/COLLECTIONS.md`).
3. Runs HNSW cosine search inside that filtered subset and returns the
   200 nearest ColumnCard IDs, ordered nearest → farthest, with their
   metadata and distances. (We pass `top_k*2` to over-fetch — 100
   candidates would not give the lexical stage enough material to
   re-rank.)

`ids[0]` is the ordered ID list; `metadatas[0]` is parallel. The code
builds:

- `meta_by_id`: dict mapping each Chroma ID to its metadata, used in
  later steps.
- `dense_ranked = list(ids)`: the dense ordering, position 0 = nearest.

If `ids` is empty (no ColumnCard for this `db_id` at all), the
function returns `[]` immediately (`hybrid_retriever.py:84-85`).

### Step 3 — lexical re-ranking on the 200 candidates

```python
query_tokens = tokenize_identifier(query)
for cid, meta in meta_by_id.items():
    qname   = meta.get("qualified_name", "")
    n_toks  = tokenize_identifier(qname)
    overlap = len(query_tokens & n_toks)
    scored_lex.append((cid, overlap))
scored_lex.sort(key=lambda x: x[1], reverse=True)   # stable sort
lexical_ranked = [cid for cid, _ in scored_lex]
```

Two key points specific to columns:

- The lexical signal is computed on `qualified_name` =
  `DB.SCHEMA.TABLE.COLUMN`. So the schema and table names contribute
  tokens too, not just the column name. For
  `GA360.GA_SESSIONS_*.GA_SESSIONS_*.TRAFFICSOURCE`,
  `tokenize_identifier` produces
  `{ga360, ga, sessions, trafficsource}`. (Split rule:
  `[_.\s]+|(?<=[a-z])(?=[A-Z])` — splits on `_`, `.`, whitespace and
  lowercase→Uppercase boundaries. `GA_SESSIONS` → `{ga, sessions}`;
  `fullVisitorId` → `{full, visitor, id}`; `TRAFFICSOURCE` stays as
  one token because it's all uppercase.)
- Column-card lexical *re-rank* never goes outside the 200 dense
  candidates — it is **not** a true BM25 over the 5.5k GA360 columns.
  A column whose name perfectly matches the question but whose dense
  embedding ranks it at position 201 will never appear in the lexical
  list.

Ties (all candidates with `overlap = 0`, which is the majority case for
multi-word natural-language questions) keep their dense order because
Python's `sort` is stable. So the lexical ranking *only reorders the
candidates that share at least one token with the question*; the rest
trail in dense order.

### Step 4 — RRF fusion

```python
fused = reciprocal_rank_fusion([dense_ranked, lexical_ranked], k=60)
```
(`hybrid_retriever.py:26-38`, `rrf_k=60` from
`config/defaults.yaml`.) For each candidate ID:

`rrf_score(id) = 1 / (60 + dense_rank) + 1 / (60 + lexical_rank)`

with ranks **1-based** (the implementation uses
`1.0 / (k + rank_0 + 1)`). Since every ID in `meta_by_id` appears in
both lists, both terms always contribute — there are no "missing from
one list" zeros here, only different ranks. The output is sorted
descending by score.

Worked numbers (illustrative for the
`"Which traffic source has the highest total transaction revenue…"`
query, GA360):

| Column | dense_rank | lex_rank | RRF score = 1/(60+d)+1/(60+l) |
|---|---:|---:|---:|
| `GA_SESSIONS_*.TRAFFICSOURCE` | 4 | 1 | `1/64 + 1/61 = 0.0320` |
| `GA_SESSIONS_*.TOTALS` | 2 | 12 | `1/62 + 1/72 = 0.0300` |
| `GA_SESSIONS_*.CHANNELGROUPING` | 8 | 6 | `1/68 + 1/66 = 0.0299` |
| `GA_SESSIONS_*.DATE` | 11 | 88 | `1/71 + 1/148 = 0.0209` |
| `GA_SESSIONS_*.SESSIONQUALITYDIM` | 14 | 200 | `1/74 + 1/260 = 0.0174` |

(Exact numbers vary by run because the dense ranking depends on the
embedding model's response; the structure — RRF rescues
identifier-matched columns even when they're not the densest — is the
invariant.)

### Step 5 — slice top-`top_k` and wrap as `ScoredItem`s

```python
for fused_rank_0, (cid, rrf_score) in enumerate(fused[:top_k]):   # top 100
    items.append(ScoredItem(
        chroma_id     = cid,
        object_type   = "column",
        qualified_name = meta.get("qualified_name", cid),
        metadata      = meta,
        dense_rank    = dense_rank_map[cid],   # 1-based, from dense list
        lexical_rank  = lex_rank_map[cid],     # 1-based, from lex list
        fused_rank    = fused_rank_0 + 1,      # 1-based, final
        rrf_score     = rrf_score,
    ))
```
(`hybrid_retriever.py:114-128`.) The result of `retrieve_columns` is a
list of up to 100 `ScoredItem`s, each carrying all three ranks so
later stages and the debug CLI can inspect why something made the cut.

### Step 6 — bucketing into tables

`build_schema_slice` (`retrieval/debug_retrieve.py:127-156`) immediately
groups column items by their parent table using the
`table_qualified_name` metadata:

```python
cols_by_table: dict[str, list[ScoredItem]] = defaultdict(list)
for ci in column_items:
    tqn = ci.metadata.get("table_qualified_name", "")
    cols_by_table[tqn].append(ci)
```

A column is then attached to a table **only if that table also made
the table-RRF top-k**. Concretely: the outer loop is over `table_items`
(from `retrieve_tables`), not over `cols_by_table.keys()`. So a column
that ranks well but whose parent table wasn't returned by the table
retriever is **dropped here** — unless connectivity expansion later
pulls in that table for graph reasons.

For each retrieved table, the code skips VARIANT sub-field columns
(`raw_dtype == "VARIANT_FIELD"`) and builds a `ColumnSlice`
(`debug_retrieve.py:145-156`):

- `name`        = last segment of `qualified_name`.
- `data_type`   = from metadata (defaults to `VARCHAR` if missing).
- `comment`     = from metadata.
- `original_name` = preserved exact case from the qualified name —
  important for double-quoted Snowflake identifiers (`"fullVisitorId"`).
- `token_estimate` = pre-computed cl100k_base token count of the card
  (from `chroma_metadata`).
- `fused_rank`  = the **column** RRF rank from Step 5.
- `is_variant`  = `True` if `data_type` is VARIANT / OBJECT / ARRAY.

Then `classify_column` (`budget.py:30-54`) adds:

- `is_join_key`     — true when the name matches `(^ID$|_ID$|_KEY$)`.
- `is_time_column`  — true when type or name contains
  `DATE|TIME|TIMESTAMP`.
- `variant_kind`    — defaults VARIANT to `ARRAY` (FLATTEN required)
  unless the metadata says otherwise.
- `date_format`     — `"YYYYMMDD integer"` or `"YYYYMMDD string"` when
  the column looks date-shaped on a numeric/string type.

These flags are what protects columns during the budget trim, and
what shapes the inline hints in `format_for_prompt()`.

### Step 7 — zero-RRF-column fallback

If a top-RRF table had **no** columns in the RRF column list (rare —
happens when a table is dense-near to the question but none of its
columns ranked in the top 100), the slice does a metadata-only fetch
of all that table's columns (`debug_retrieve.py:158-184`):

```python
all_cols = retriever.collection.get(
    where={"$and": [
        {"db_id": db_id},
        {"object_type": "column"},
        {"table_qualified_name": qname},
    ]},
    include=["metadatas"],
)
```

These columns are assigned `fused_rank = 999` so they're the **first**
to be dropped by the budget trim. This is a deterministic safety net,
not retrieval — the table won't appear in the prompt as a bare header
with no columns.

### Step 8 — VARIANT sub-field enrichment

`_enrich_variant_fields` (`debug_retrieve.py:40-109`) does **another
metadata-only** `collection.get` per table to pull every
`data_type="VARIANT_FIELD"` row for that table, then parses the
sub-paths (e.g. `"totals":pageviews` → parent `totals`, field
`pageviews`) and attaches them onto the parent VARIANT column's
`variant_fields` list. It also flips `variant_kind` to `ARRAY` (needs
`LATERAL FLATTEN`) vs `OBJECT` (use `:`) based on whether the
sub-field's comment says "array element".

This is why a VARIANT column comes out of column retrieval looking
like `TOTALS VARIANT OBJECT … [fields: transactionRevenue,
transactions, visits]` in the final prompt — the column itself was
RRF-retrieved, but the field list comes from a follow-up
metadata-only get.

### Step 9 — connectivity / join-graph expansion (may pull new columns)

`expand_connectivity` and `expand_join_graph_neighbors`
(`debug_retrieve.py:201-207`) can add tables that weren't in the
table-RRF result but are reachable via JoinCards. When that happens,
the newly-added table's columns are *not* pulled through the
hybrid retriever — they come in via a metadata `collection.get`,
the same way Step 7 does, with `fused_rank=999`.

### Step 10 — budget trim (column-level)

`trim_to_budget` (`budget.py:57-137`) is where the column RRF rank
finally pays out:

1. Apply `max_columns_per_table` if set. Protected columns
   (join keys, time columns) are kept first; the rest are kept by
   ascending `fused_rank` until the cap is hit.
2. While `schema_slice.token_estimate > max_schema_tokens` (default
   10000): repeatedly drop the **unprotected** column with the
   **highest `fused_rank`** (worst RRF). Break ties by preferring
   columns from worse-ranked tables. If the only candidates left are
   protected, those start getting dropped too.
3. If a table loses all its columns it's removed entirely.
4. Finally, if still over budget, drop whole worst-ranked tables.

A column's chance of surviving into the prompt is therefore a joint
function of (a) its column-RRF rank, (b) its parent table's RRF rank,
and (c) whether `classify_column` flagged it as a join/time column.

### Step 11 — render into the prompt

`SchemaSlice.format_for_prompt()` (`schema_slice.py:61-100`) emits one
line per surviving column:

```
  <COLUMN_NAME> <data_type><annotation> -- <comment>
```

Column-specific annotations come from the flags set in Steps 6/8:

- `is_variant` + `variant_kind == "ARRAY"` → appends
  `ARRAY — use LATERAL FLATTEN(input => t."<col>") alias, then alias.value:"field"::TYPE`.
- `is_variant` + `variant_kind == "OBJECT"` → appends
  `OBJECT — access with t."<col>":"field"::TYPE`.
- `variant_fields` (≤ first 8) listed as `[fields: a, b, c]`.
- `is_time_column` + `date_format` → appends a comparison-style hint
  (`date as integer (YYYYMMDD), compare with integers e.g. >= 20170201`
  or string variant).

After Step 11 the column has finished its journey: hybrid-retrieved →
RRF-ranked → bucketed under its table → flagged/classified →
budget-trimmed → rendered with inline hints — and now sits inside
the schema block of the plan and SQL prompts.

### Quick reference — order of operations for column retrieval

1. NL question → string `query`.
2. Chroma dense `query` with `where={db_id, object_type="column"}`,
   `n_results=200` → `dense_ranked` (5.5k → 200).
3. Tokenize `query` and each candidate's `qualified_name`;
   sort by `|overlap|` (stable) → `lexical_ranked` (still 200 IDs).
4. RRF fuse the two lists with `k=60` → ranked top 100 →
   `ScoredItem`s carrying dense/lex/fused ranks.
5. Bucket by `table_qualified_name`; keep only columns of
   table-RRF-retrieved tables; drop `VARIANT_FIELD`s.
6. For tables with no matched columns, metadata-fetch their entire
   column list at `fused_rank=999`.
7. Metadata-fetch `VARIANT_FIELD` rows to populate `variant_fields`
   and correct `variant_kind` on the parent VARIANT columns.
8. Connectivity / join-graph expansion may add more tables (and
   columns at `fused_rank=999`).
9. `classify_column` sets join/time/VARIANT/date-format flags.
10. Budget trim evicts unprotected columns by descending
    `fused_rank` until token budget is satisfied.
11. `format_for_prompt()` renders the survivors with inline Snowflake
    access hints into the schema block of the LLM prompt.

---

## 7) Join-graph expansion — exactly which columns are added

There are **three** distinct "connectivity-related" expansion functions
in `src/rag_snow_agent/retrieval/connectivity.py`. They serve different
purposes, but the answer to "which columns of the added table show up
in the slice?" is essentially the same across all three:
**all columns of the added table** (with one minor exclusion in one
path), all stamped with `fused_rank = 999`.

| Function | When it fires | Source of bridge candidates | Column subset added |
|---|---|---|---|
| `expand_connectivity` | Always (1 round, `connectivity_rounds=1` default) | Heuristic: scan all TableCards in the DB, pick a table whose **document text** ("Columns: …" line) names join-ish keys (`^ID$ \| _ID$ \| _KEY$`) shared by 2+ already-selected tables | **All columns** of the chosen bridge table |
| `expand_connectivity_with_join_graph` | Defined but *not* wired into `build_schema_slice` — only invoked from tests/debug paths | Real graph from JoinCards; BFS-shortest-bridge between disconnected components | **All columns** of each bridge table on the path |
| `expand_join_graph_neighbors` | Always (called unconditionally after the heuristic round), but gated internally by `_GEO_QUESTION_RE` on the instruction | 1-hop neighbors of selected tables via JoinCards, **only** when neighbor has a geo-shaped column (`lat`, `lon`, `geometry`, `state_name`, `zip_code`, …) | **All columns except `VARIANT_FIELD`s** of each qualifying neighbor |

Below is the per-function breakdown. Line numbers refer to
`connectivity.py`.

### 7.1 `expand_connectivity` (heuristic bridge — the one that actually runs)

This is the function called by `build_schema_slice` at
`debug_retrieve.py:202-204` (the `connectivity_rounds=1` default).

Selection logic (`connectivity.py:45-153`):

1. If there are < 2 tables in the slice, do nothing.
2. Compute, for every pair `(i, j)` of selected tables, whether they
   share a join-ish column name (`_tables_share_join_key`, lines
   34-42). The match is **case-insensitive equality on the column
   name** — e.g. both tables having a column called `ORDER_ID`
   counts as a shared key. The actual data types or FK relationships
   are not consulted.
3. If every pair already shares at least one such key, return early
   — no bridge needed.
4. Otherwise: pull every TableCard for the `db_id` from Chroma
   (`collection.get(where={db_id, object_type="table"})` — metadata
   filter, no embedding) and for each candidate parse its
   `document` string's `"Columns: A, B, C"` line. From those column
   names, pick out the subset matching `(^ID$|_ID$|_KEY$)`.
5. Count how many selected-table indices each bridge candidate
   touches via shared join-ish names. Keep the candidate touching the
   most tables (≥ 2 required).

If a winner is found, the function then loads its columns
(`connectivity.py:120-141`):

```python
bridge_cols_result = collection.get(
    where={"$and": [
        {"db_id": schema_slice.db_id},
        {"object_type": "column"},
        {"table_qualified_name": qname},
    ]},
    include=["metadatas"],
)
for cm in bridge_col_metas:
    col_name = cm.get("qualified_name", "").rsplit(".", 1)[-1]
    cs = ColumnSlice(
        name           = col_name,
        data_type      = cm.get("data_type", "VARCHAR"),
        token_estimate = cm.get("token_estimate", 5),
        fused_rank     = 999,                # ← lowest priority
        is_join_key    = bool(_JOIN_RE.search(col_name)),
    )
    col_slices.append(cs)
```

So for this path the answer is:

- **All columns of the bridge table are added**, not only the join keys.
- `VARIANT_FIELD` sub-fields are **not** filtered here — they will sit
  in the slice with their parent until budget trim or
  `format_for_prompt`-side handling drops them. (In practice the
  geo/FK bridge tables this picks tend to have no VARIANT columns, so
  this rarely matters.)
- `comment` is **not** copied through here (unlike the geo-neighbor
  path) — the bridge columns enter the slice without their
  description.
- Every bridge column is given `fused_rank = 999`, which puts it at
  the very back of the budget-trim eviction order. So:
  - The bridge table itself is kept (it's the structural reason we
    added it).
  - Its **join-shaped columns** (`*_ID`, `*_KEY`) are flagged
    `is_join_key=True` and thus *protected* by `trim_to_budget`
    (`budget.py:25-27, 109-113`).
  - All its **other columns** are unprotected at rank 999 and will be
    the first things dropped if the slice exceeds
    `max_schema_tokens`.

Net effect: structurally the bridge table arrives with its full column
list, but in the rendered prompt you usually see only its join keys
plus whatever date/time columns survive — exactly the columns needed
to write the join, nothing else.

The bridge table's own `fused_rank` is set to `len(tables) + 1`
(line 146), i.e. it ranks last among tables. If the slice is over
budget, *whole-table* eviction happens after column eviction
(`budget.py:131-135`), so the bridge generally survives.

### 7.2 `expand_connectivity_with_join_graph` (defined but currently unused)

This is the "proper" join-graph version that uses JoinCards
(confidence-weighted BFS over `JoinGraph.shortest_bridge_tables`,
`join_graph.py:103-184`). It's defined in `connectivity.py:156-241`
but **is not called** by `build_schema_slice` — verified by grepping
the codebase; the only callers are debug/test scripts. The default
production flow uses 7.1 + 7.3 instead.

If it were used, the column-add logic is identical to 7.1: same
metadata `collection.get`, same `fused_rank=999`, same
`is_join_key` flagging via regex, same omission of `comment`.

Bridge selection here is meaningfully different though: it finds the
**shortest** path (in JoinCard edges) between disconnected components
of selected tables, ties broken by maximum minimum-confidence; every
intermediate table on the chosen path becomes a bridge. So multiple
bridges can be added in one call, not just one.

### 7.3 `expand_join_graph_neighbors` (the geo-targeted one)

Called unconditionally by `build_schema_slice`
(`debug_retrieve.py:207`), but its body returns immediately unless
the instruction matches `_GEO_QUESTION_RE`:

```python
\b(within|radius|distance|nearby|near|miles?|kilometers?|km\b|mi\b|
   boundary|boundar|polygon|geospatial|spatial|latitude|longitude|
   lat\b|lon\b|zip\s*code|state\b|county)
```

When it does fire (`connectivity.py:277-340`):

1. For every table already in the slice, walk its 1-hop neighbors
   in the JoinGraph (constructed from JoinCards for that DB).
2. For each neighbor not already in the slice, fetch the neighbor's
   full column list (metadata-only get).
3. Check whether **any** column name matches `_GEO_COLUMN_RE`:
   ```python
   ^lat$ | ^lon$ | ^latitude$ | ^longitude$ |
   ^geom$ | ^geometry$ | ^geography$ |
   state_name | state_code | zip_code | zip_code_geom
   ```
4. If yes → add **all** the neighbor's columns to the slice, with two
   small differences from path 7.1:
   - `data_type == "VARIANT_FIELD"` columns are skipped
     (`connectivity.py:311-312`).
   - `comment` and `original_name` are propagated through
     (`connectivity.py:316-317`).

Everything else is the same: `fused_rank=999`, `is_join_key` set by
regex, neighbor's table-level `fused_rank = len(tables)+1`.

Note: the geo-column check is a **gate** ("does this neighbor have
*any* lat/lon/zip column?"), not a **filter** ("keep only the
lat/lon columns"). Once the gate passes, the entire neighbor table
goes in.

### 7.4 What survives into the prompt

Putting steps 7.1/7.3 together with the budget trim (Section 6,
Step 10):

- **Always rendered**: the bridge / neighbor table itself, plus any of
  its columns flagged `is_join_key` (matched the `_ID|_KEY` regex) and
  any flagged `is_time_column`. These are *protected* — they survive
  even tight budgets.
- **Likely rendered**: a handful of other columns when there's slack
  in the token budget. Eviction order is by descending
  `fused_rank` (so 999s go before RRF-ranked columns), then by
  descending table rank. With `max_schema_tokens=10000` (default),
  small bridge tables typically keep most of their columns; on a
  fat GA360 slice they get aggressively trimmed.
- **Never enriched**: VARIANT sub-fields are not attached to bridge
  table VARIANT columns (the enrichment in step 8 of the column-search
  path runs **before** connectivity expansion in
  `build_schema_slice`, so bridge tables added afterwards don't get
  the `[fields: …]` annotation).

### 7.5 Quick answer

> Are all columns of the joined table added, or only some specific ones?

**All of them**, in every join-graph path that's currently wired up.
The only filtering at insertion time is "drop `VARIANT_FIELD`
sub-fields" in the geo-neighbor path, and that's a structural cleanup,
not a relevance filter. What ends up in the final prompt is
overwhelmingly the **join keys** of the added table — but that's a
consequence of the *budget trim* (which protects `is_join_key` /
`is_time_column` and evicts everything else at `fused_rank=999`
first), not of the join-graph step itself.

---

## 8) The BM25 formula from THESIS_EN.md §4.3 — step by step, and how it applies in our project

### 8.0 The formula in the thesis

From `docs/THESIS_EN.md:299-305` (Section 4.3, "Sparse Retrieval"):

$$
\mathrm{BM25}(D, Q) = \sum_{t \in Q} \mathrm{IDF}(t)\cdot
\frac{f(t,D)\cdot(k_1+1)}
     {f(t,D)+k_1\cdot\bigl(1 - b + b\cdot\dfrac{|D|}{\mathrm{avgdl}}\bigr)}
$$

with parameters declared as $k_1 = 1.5$, $b = 0.75$ (the standard
Okapi BM25 defaults).

This is the textbook Robertson/Zaragoza form. Below: what each piece
means, then a worked walkthrough on one of our actual ColumnCards,
then a candid note on **how much of this is actually implemented**
in the production retriever versus only described in the thesis.

### 8.1 The vocabulary the formula operates on

- $Q$ — the **query**, after tokenization. In our setting $Q$ is the
  user's NL instruction (e.g. "Which traffic source has the highest
  total transaction revenue for 2017…"), tokenized into a multiset
  of lowercased terms.
- $D$ — a single **document** in the index. In our setting a
  document is the embedded text of one schema card:
  `Column: GA360.GA_SESSIONS_*.TRAFFICSOURCE\nType: VARIANT\nDescription: …\nNullable: NO`
  for a ColumnCard (or the analogous block for a TableCard).
- $t \in Q$ — each **unique term** in the query (BM25 sums per-term,
  not per-occurrence; query-side TF is handled implicitly).
- $f(t, D)$ — the **term frequency** of $t$ in document $D$ (how many
  times the token appears inside that one card's text).
- $|D|$ — the **document length** in tokens (how many tokens the
  card's text has after tokenization).
- $\mathrm{avgdl}$ — the **average document length** across the
  entire indexed collection (e.g. across all 7,862 ColumnCards plus
  465 TableCards plus JoinCards in `schema_cards`).
- $\mathrm{IDF}(t)$ — **inverse document frequency** of term $t$.
  The standard BM25 variant is
  $$\mathrm{IDF}(t) = \ln\!\left(\frac{N - n(t) + 0.5}{n(t) + 0.5} + 1\right)$$
  where $N$ is the total number of documents in the index and
  $n(t)$ is the number of documents containing $t$ at least once.
  Rare terms get a large positive IDF; ubiquitous terms get a small
  (near-zero) IDF. The "+1" inside the log is the BM25+ smoothing
  that prevents negative weights for terms appearing in more than
  half of the corpus.
- $k_1, b$ — the two tunable knobs:
  - $k_1$ controls how quickly term frequency saturates. As
    $k_1 \to 0$, the formula degenerates to binary "term present /
    absent" (any extra occurrences add nothing). As
    $k_1 \to \infty$, TF is essentially linear (the second, third,
    fourth occurrences each count almost as much as the first). At
    $k_1 = 1.5$ saturation kicks in fast — the 2nd hit is worth
    roughly $1.5/3.5 \approx 43\%$ of the first.
  - $b$ controls how aggressively document length is normalized.
    $b = 0$ means "ignore length, never penalize long docs";
    $b = 1$ means "fully normalize by length". At $b = 0.75$ a doc
    that is 2× longer than average has its TF effectively divided
    by $1 - 0.75 + 0.75 \cdot 2 = 1.75$ — a noticeable but not
    crushing penalty.

### 8.2 Reading the formula left-to-right

$$
\mathrm{BM25}(D, Q) = \sum_{t \in Q}\;
\underbrace{\mathrm{IDF}(t)}_{\text{rarity weight}}
\;\cdot\;
\underbrace{\frac{f(t,D)\cdot(k_1+1)}
                  {f(t,D)+k_1\cdot\bigl(1 - b + b\cdot\dfrac{|D|}{\mathrm{avgdl}}\bigr)}}_{\text{TF with saturation + length normalization}}
$$

Per-term, the score is the product of two factors:

1. **IDF — how informative is this term globally?**
   A term that appears in 80% of cards (`column`, `varchar`) carries
   almost no signal. A term that appears in 0.1% of cards
   (`trafficsource`, `wikidata`, `osm_id`) carries a huge signal.
2. **Normalized TF — how present is this term in *this* card,
   penalized if the card is artificially long?**
   The numerator $f(t,D)\cdot(k_1+1)$ grows with TF; the denominator
   $f(t,D) + k_1 \cdot L$ caps it (where
   $L = 1 - b + b \cdot |D|/\mathrm{avgdl}$ is the length
   normalizer). Crucially the ratio asymptotes — going from 5 hits
   to 50 hits gives a much smaller jump than going from 0 to 5.

The sum over $t \in Q$ aggregates the per-term scores. Documents
that match many distinct query terms beat documents that match the
same term many times.

### 8.3 Step-by-step computation on one ColumnCard

To make this concrete, let's compute BM25 for the query

> "Which traffic source has the highest total transaction revenue
> for 2017?"

against the GA360 ColumnCard for `TRAFFICSOURCE`. The card's document
is (`schema_cards.py:75-80`):

```
Column: GA360.GA_SESSIONS_*.GA_SESSIONS_*.TRAFFICSOURCE
Type: VARIANT
Description: Traffic source attribution for the session
Nullable: NO
```

**Step 1 — tokenize the query.**
Lowercase, split on whitespace/`_`/`.`/`:`, and (per the thesis,
§4.3.2) preserve both the whole identifier and its sub-parts. The
query terms become roughly:

```
{which, traffic, source, has, the, highest, total, transaction,
 revenue, for, 2017}
```

(`the`, `for`, `has`, `which` are kept here for clarity; in a real
implementation they would typically be stopworded or simply earn an
IDF near zero.)

**Step 2 — tokenize the document, count TF.**
The card's text tokenizes to (with the thesis's compound-identifier
split — `GA_SESSIONS` → `{ga, sessions}`, `TRAFFICSOURCE` kept as a
single token because it has no uppercase→lowercase boundary):

```
{column, ga360, ga, sessions, ga, sessions, trafficsource,
 type, variant, description, traffic, source, attribution, for,
 the, session, nullable, no}
```

(18 tokens; some duplicates because `GA_SESSIONS_*` appears twice in
the qualified name. So $|D| \approx 18$.)

Term frequencies inside this card:

| term | $f(t, D)$ |
|---|---:|
| `traffic` | 1 |
| `source` | 1 |
| `for` | 1 |
| `the` | 1 |
| `2017` | 0 |
| `revenue` | 0 |
| `transaction` | 0 |
| `total` | 0 |
| `highest` | 0 |
| `which` | 0 |
| `has` | 0 |

So only `traffic`, `source`, `for`, `the` actually contribute — the
other query terms have $f = 0$ and drop out of the sum.

**Step 3 — estimate the IDFs.**
Assume the indexed collection is the ~7.8k ColumnCards + ~465
TableCards + ~17k JoinCards in `schema_cards`, so $N \approx 25{,}000$.
Plausible document counts $n(t)$ on this corpus:

| term | $n(t)$ | $\mathrm{IDF}(t) = \ln\!\bigl(\tfrac{N - n + 0.5}{n + 0.5} + 1\bigr)$ |
|---|---:|---:|
| `the` | ~22,000 | $\ln(3{,}000.5 / 22{,}000.5 + 1) \approx \ln 1.14 \approx 0.13$ |
| `for` | ~20,000 | ~0.22 |
| `source` | ~600 | $\ln(24{,}400 / 600 + 1) \approx \ln 41.7 \approx 3.73$ |
| `traffic` | ~80 | $\ln(24{,}920 / 80 + 1) \approx \ln 312 \approx 5.74$ |
| `revenue` | ~120 | ~5.34 |
| `2017` | ~250 | ~4.61 |
| `transaction` | ~90 | ~5.62 |

(Numbers are illustrative — what matters is the ordering: `the` and
`for` contribute almost nothing; `traffic`, `revenue`,
`transaction` are highly informative.)

**Step 4 — estimate the length normalizer.**
The thesis-prescribed defaults are $k_1 = 1.5$, $b = 0.75$. If the
average ColumnCard document is $\mathrm{avgdl} \approx 14$ tokens,
then for our 18-token document:

$$
L \;=\; 1 - b + b \cdot \frac{|D|}{\mathrm{avgdl}}
\;=\; 0.25 + 0.75 \cdot \frac{18}{14}
\;\approx\; 1.214
$$

**Step 5 — TF-saturation factor per term.**
The denominator is $f + k_1 \cdot L = f + 1.5 \cdot 1.214 \approx f + 1.82$.
The numerator is $f \cdot (k_1 + 1) = 2.5 \cdot f$. So for the four
terms with $f = 1$:

$$
\frac{f \cdot (k_1+1)}{f + k_1 \cdot L}
\;=\; \frac{2.5}{1 + 1.82}
\;=\; \frac{2.5}{2.82}
\;\approx\; 0.887
$$

(Every term with $f = 1$ in this document contributes the same
saturation factor 0.887, because TF and length are document
properties, not term properties.)

**Step 6 — final score for this document.**

$$
\mathrm{BM25}(D, Q)
\;\approx\;
0.887 \cdot \bigl(\mathrm{IDF}(\text{traffic}) + \mathrm{IDF}(\text{source})
+ \mathrm{IDF}(\text{for}) + \mathrm{IDF}(\text{the})\bigr)
$$
$$
\;\approx\; 0.887 \cdot (5.74 + 3.73 + 0.22 + 0.13)
\;\approx\; 0.887 \cdot 9.82
\;\approx\; 8.71
$$

The takeaway: ~98% of this card's BM25 score comes from
`traffic` + `source` (the two rare terms it shares with the query).
The stopword-ish matches (`the`, `for`) and the unmatched terms
(`revenue`, `2017`, `transaction`) collectively contribute almost
nothing for this single card.

**Step 7 — how this card ranks against its peers.**
Now apply Step 1-6 to every other ColumnCard in the GA360 subset.
Cards that **also** contain `traffic` or `source` (e.g.
`GA_SESSIONS_*.TRAFFICSOURCE_*` variants, `SOURCE` column on other
tables) get comparable rare-term mass. Cards that match `revenue` or
`transaction` instead (e.g.
`GA_SESSIONS_*.TOTALS:transactionRevenue` if it were a top-level
column, or `TOTALS` directly via description hits) climb the ranking
through *those* terms. Cards that match only `the`/`for` rank near
zero because IDF crushes them.

The output is `lexical_ranked = [card_id sorted by BM25 desc]`,
later fed into RRF together with `dense_ranked` (Section 6, Step 3).

### 8.4 The three Text-to-SQL adaptations from §4.3

The thesis calls out three project-specific tweaks (lines 309-311):

1. **Compound identifier splitting.** `revenue_amount_usd` is indexed
   both as the whole token and as `{revenue, amount, usd}`. So both
   "show revenue" (which only contains the natural-language fragment)
   and "filter by revenue_amount_usd" (which contains the exact
   identifier) can fire matches. This works through the indexer's
   tokenization rule, not through the BM25 formula itself.
2. **Case-insensitivity with originals preserved.** The tokenizer
   lowercases and treats `_`, `.`, `:` as separators, but stores the
   original form alongside. So `fullVisitorId` is matched whether
   the user writes it as `fullvisitorid`, `full_visitor_id`, or the
   exact camelCase — and the original is what later gets rendered in
   the prompt.
3. **Rare-term dominance.** This is exactly what step 3 above
   demonstrated numerically: tokens like `wikidata`, `osm_id`,
   `Q1095` have $n(t) \le 5$ on a 25k-doc corpus, so $\mathrm{IDF}$
   pushes their per-term contribution past 8, dwarfing the
   contribution of common business words. That is the property
   compensating for the dense pass's weakness with short identifiers
   and codes (§4.2, "Where dense retrieval is weaker").

### 8.5 How the BM25 result feeds the rest of the pipeline

Per the thesis (§4.3 final paragraph and §4.4):

1. `_lexical_query()` returns a list of `ScoredItem`s ordered by
   descending `bm25_score`, with the per-card 1-based
   `lexical_rank` attached.
2. `_dense_query()` returns the parallel list ordered by cosine
   similarity, with `dense_rank` attached.
3. RRF (§4.4, k = 60) merges the two by position, not by raw score
   — exactly because the BM25 scale (0…tens) and cosine scale
   (0…1) are not directly addable. RRF gives the thesis the
   "robustness across 20 domains" property it claims at line 331.
4. The fused list flows into the same `SchemaSlice` post-processing
   already covered in Sections 6/7 above: bucketing by table,
   VARIANT enrichment, join-graph expansion, budget trim, and
   `format_for_prompt()`.

### 8.6 ⚠ Implementation reality check (important)

This is the part worth flagging carefully — the thesis's §4.3
description does **not** match what the code on this branch
actually executes:

- The thesis references `rank_bm25`. **It is not a project
  dependency** — grep on `pyproject.toml` and `uv.lock`:
  zero matches for `rank_bm25` / `rank-bm25` / `BM25`.
- The thesis names the methods `_dense_query()` and
  `_lexical_query()`. **They do not exist in the codebase.**
  `HybridRetriever` exposes a single `retrieve()` method
  (`hybrid_retriever.py:66-129`) with the dense and lexical passes
  inline. No `_dense_query`/`_lexical_query` symbols anywhere
  under `rag_snow_agent/src`.
- The thesis says `ScoredItem` carries `bm25_score` and
  `lexical_rank`. **`bm25_score` does not exist** — the dataclass
  (`hybrid_retriever.py:44-53`) has `dense_rank`, `lexical_rank`,
  `fused_rank`, `rrf_score` but no `bm25_score`.
- The lexical stage in `retrieve()` is **not BM25**. It is a
  set-intersection count:
  ```python
  overlap = len(tokenize_identifier(query) & tokenize_identifier(qname))
  ```
  No TF (each token contributes 0 or 1), no IDF (every matched
  token weighs the same), no length normalization, no
  $k_1$/$b$ knobs. It also runs only against `qualified_name` from
  metadata, not against the embedded document text where comments
  and descriptions live. And it only re-ranks the top-200 dense
  candidates, never the full collection.

So in practice, the thesis's §4.3 formula is a **specification of
what the lexical channel is supposed to do**, with the production
code currently shipping a much cheaper proxy that captures the
"identifier-token match" intent but loses the rarity-weighting,
TF-saturation, and length-normalization properties. The
RRF/cosine/embedding parts of §4.3-§4.5 match the implementation
faithfully; only the BM25 step diverges.

### 8.7 What changing the implementation to real BM25 would buy

If the lexical pass were swapped to a true BM25 (e.g. via
`rank_bm25.BM25Okapi` over the embedded card documents, not just
their qualified names), three concrete behaviours from the thesis
would actually materialize:

- Cards whose **description** (not name) mentions a rare query
  term — e.g. a column whose comment says "primary GEO key derived
  from OSM" hit by a query about `osm`, even though the column
  name is opaque — would surface. The current set-overlap pass
  cannot see comments at all.
- Multi-occurrence emphasis: a card mentioning `traffic` three times
  in its description would outrank a card mentioning it once,
  bounded by the $k_1$ saturation. The current pass treats both as
  "overlap = 1".
- Per-database calibration would still be unnecessary — that's the
  RRF property, and it's already in place.

This is a known gap, not a bug — the production retriever's
identifier-token overlap is fast and good enough for short
question/identifier queries; full BM25 would help for the comment-
and description-driven matches that GA360 and PATENTS rely on.

---

## 9) Defending the 4-way benchmark with 4 different models (§6.2)

### 9.1 The thing the commission will object to

Section 6.2 of `THESIS_EN.md` (lines 503-513) compares four systems on
the same 25-query Spider 2.0 — Snow subset, but each runs on a
different LLM:

| System | Model |
|---|---|
| DSR-Lite | DeepSeek |
| Spider-Agent | GPT-4o |
| ReFoRCE | GPT-5-mini |
| **SnowRAG-Agent (Run 9)** | **GPT-5.4** |

The obvious commission question: *"How do you know your 92 % vs.
ReFoRCE's 36 % comes from architecture and not from the fact that
you used a stronger model?"* This is a legitimate methodological
concern — by itself the §6.2 table is **not** a model-controlled
experiment.

### 9.2 The load-bearing defense: §6.3.1 (Run 12) is the real comparison

The strongest counter is that the thesis **does** include a clean,
model-controlled experiment — §6.3.1 (Run 12), lines 555-585. There,
SnowRAG-Agent and ReFoRCE are run on:

- the **same model** (`gpt-5.4-mini`),
- the **same 100 queries**,
- the **same Snowflake connection**,
- the **same gold-match harness**.

Result: **84 % vs. 20 %** — a 4.2× accuracy gap that cannot be
attributed to model choice because the model is held constant. The
thesis explicitly states this on line 571:

> "Crucially, both systems here operate on the same model
> (`gpt-5.4-mini`) and process the same 100 queries. Thus any
> divergences between them follow exclusively from architectural
> differences. This rules out the hypothetical counter-argument
> that the SnowRAG-Agent advantage might be explained by a stronger
> LLM."

**Defense tactic for the commission:** treat §6.2 as a *naturalistic*
comparison (each system on the model its authors paired it with —
the most charitable interpretation of each baseline) and §6.3.1 as
the *controlled* comparison. The 4.2× gap survives model
normalization.

The set-intersection result on line 577-582 makes the §6.3.1
argument even more pointed: **every query ReFoRCE got right,
SnowRAG-Agent also got right.** There is no overlap of failures
that "tilts the other way" — i.e. SnowRAG-Agent never loses to
ReFoRCE on a query, regardless of model. That zero-regression
property is hard to reconcile with a "they only won because GPT-5.4
is stronger" story.

### 9.3 The supporting defense: §7.3 "no-RAG GPT-5.4" baseline

Section 7.3 (lines 705-711) is the second pillar. The thesis runs
**the same GPT-5.4 model with no RAG** — i.e. the full DDL pushed
into the prompt — and gets **36 %**, the same level as ReFoRCE.

What that proves to the commission:

- GPT-5.4 alone does **not** carry the system. Strip out the RAG and
  the accuracy collapses from 92 % to 36 %.
- 36 % is the same number ReFoRCE on GPT-5-mini reached in §6.2.
  So a stronger model **without** the architecture gives no
  improvement over a weaker model **with** ReFoRCE's architecture.
- The remaining 56 pp gap (36 % → 92 %) is what the architecture
  contributes, with the model held constant on the SnowRAG side.

This is the cleanest single sentence to drop in the defense:
*"Same model, no RAG: 36 %. Same model, full system: 92 %. Same
56-point gap is exactly the architectural delta."*

### 9.4 Why DSR-Lite's 0 % is not about DeepSeek being weak

The thesis pre-empts the commission on DSR-Lite at line 521:

> "The contrast with DSR-Lite deserves special attention. The model
> used in that system (DeepSeek) is weaker than current alternatives
> in generation strength, but the decisive factor is not the model
> but the architecture: the full DDL of GA360 with its hundreds of
> tables simply does not fit into the context window, and so
> analysis of the query is impossible before any reasoning algorithm
> or correction loop has a chance to act."

In other words: even if you swapped DSR-Lite onto GPT-5.4, GA360's
full DDL would still fail to fit. The 0 % is structural, not a model
deficiency. The commission can verify this empirically: count GA360
columns × tokens-per-column-line and compare against context
windows. The architecture caps the achievable accuracy below where
a stronger model could rescue it.

### 9.5 Why Spider-Agent on GPT-4o is a fair baseline

GPT-4o was a contemporary frontier model at publication — not a
crippled choice. Spider-Agent reached 12 %. The thesis attributes
this to GPT-4o's ability to "guess" table structures from names
(line 523), which is itself a model-strength advantage that
*helped* the baseline. Even with that help, the baseline tops out
at 12 % because it has no schema retriever returning "exactly the
slice required by the current query."

So the commission can't argue "you ran Spider-Agent on an
obsolete model." The model used is the one Spider2 authors used in
the official benchmark report [4] — i.e. the canonical reference
result.

### 9.6 Why ReFoRCE on GPT-5-mini in §6.2 is the right baseline pairing

ReFoRCE's own paper [8] and reference implementation were validated
on the GPT-5-mini family. Pairing them differently would actually be
methodologically *worse*, because you'd be benchmarking a
non-canonical configuration. §6.2's model choice respects how each
upstream system is designed to be used; §6.3.1's choice (force them
both onto `gpt-5.4-mini`) is the model-controlled counterpart that
follows up.

### 9.7 The cost-efficiency angle (orthogonal to "stronger model")

The token / cost columns in §6.2 deflate the "stronger model"
objection from a different direction:

- SnowRAG-Agent uses **177K tokens per correct answer**;
  ReFoRCE uses **833K**.
- In §6.3.1 (same model), SnowRAG-Agent's **cost per correct
  answer is \$0.11 vs ReFoRCE's \$2.03** — 18× cheaper.

If our advantage were "we just used a bigger model," our cost
would go *up* per correct answer, not down by 18×. The cost
efficiency is incompatible with a model-strength explanation; it
only makes sense if the architecture is delivering more correct
answers per LLM call. That is itself an architectural property,
independent of which model fills the LLM slot.

### 9.8 The ablation chapter (§7) is the third pillar

Chapter 7 explicitly varies one component at a time, all on the
same GPT-5.4 model. Tier A (descriptions, partition collapsing,
Plan → SQL compiler) contributes the bulk of the accuracy lift;
Tier D (`trace_memory`, `semantic_cards`, `sample_records`) each
contributes <10 pp. If the commission still doubts whether the
architecture matters, the ablation table directly attributes
accuracy to specific architectural components on a fixed model.

### 9.9 Suggested phrasing for the defense

A defensible three-sentence answer when the commission challenges
the §6.2 table:

> "Section 6.2 reports each system on the model its upstream authors
> validated it with — that's the comparison that gives each baseline
> the best chance. The model-controlled experiment is Section 6.3.1
> (Run 12): same `gpt-5.4-mini`, same 100 queries, same harness —
> SnowRAG-Agent reaches 84 % against ReFoRCE's 20 %, a 4.2× gap that
> by construction cannot be explained by model choice. Section 7.3
> reinforces this from the other direction: GPT-5.4 with no RAG
> scores 36 %, the same as ReFoRCE — so a stronger model on its own
> does not close the gap; the architecture does."

Optional add-on if the commission keeps pressing:

> "We also note that on the §6.3.1 comparison, the set of queries
> ReFoRCE solves is a strict subset of the set SnowRAG-Agent solves
> — 20 of 20 ReFoRCE correct answers are also SnowRAG-Agent correct
> answers. There is no model-attributable failure direction left to
> explain."

### 9.10 What we could still do to harden the defense

If time permits before the defense, the cheapest improvements
would be:

1. **Run DSR-Lite on `gpt-5.4-mini`** for the 25-query subset and
   add a row to §6.2. Expected result: still very low accuracy
   (because of the context-window argument), but it removes the
   "DeepSeek was the bottleneck" objection at the cost of one
   benchmark run.
2. **Run Spider-Agent on `gpt-5.4-mini`** for the 25-query subset.
   Same logic — the model becomes a constant and we read off the
   pure architectural delta.
3. Cross-reference these new rows in §6.2 with a short note like
   "to verify the result is not driven by model choice we additionally
   ran each baseline on the same `gpt-5.4-mini`." That sentence
   alone closes the methodological gap that the §6.2 table opens.

If those reruns are not feasible, leaning on §6.3.1 + §7.3 as
described above is already sufficient — the §6.2 table can be
explicitly framed as "best-effort reproduction of each upstream
system's intended setup," with §6.3.1 carrying the controlled
comparison.

### 9.11 ⚠ The "DSR-Lite used DeepSeek" claim is not reproducible from the artifacts

A finding that needs to be addressed before the defense.

**What the thesis says** (§6.2, line 507):

> "Model: DSR-Lite → DeepSeek"

**What the committed code actually defaults to.**
`DSR-SQL/DSR_Lite/utils/Prompt.py:1-2` (the file that defines every
solver-stage class — `Fine_grained_Exploration`,
`Information_Aggregation`, `GenerateSQLBeginning`,
`ContinueSQLWriting`, `Simple_Fix`):

```python
BASE_MODEL='gpt-4o-mini'
Reasoning_model='gpt-4o-mini'
```

Each stage class reads `self.model = Reasoning_model` (or
`BASE_MODEL` for `Simple_Fix`), so the solving pipeline as committed
runs on **`gpt-4o-mini`**, not on DeepSeek.

**Where `deepseek-*` actually appears in the DSR tree** (and what it's
used for, in case it was overridden at runtime):

| Location | Value | Used for |
|---|---|---|
| `script/Knowledge_Compression.sh` | `deepseek-chat` | Offline knowledge-compression preprocessing — not the solver |
| `LLM/LLM_config.json` | endpoint placeholder | Just lists DeepSeek as one of four available API endpoints |
| `LLM/LLM_OUT.py:5` | `model="deepseek-reasoner"` | Function-signature default (fires only if no model is passed in) |
| `LLM/DeepSeek_LLM.py:6` | `model="deepseek-reasoner"` | Same — DS wrapper signature default |

So the only two `deepseek-*` IDs that could have powered the solver
in any execution are **`deepseek-reasoner`** and **`deepseek-chat`**.

**What the run logs record about the model.**
The benchmark run referenced by the thesis is at
`DSR-SQL/DSR_Lite/logs/run_snow_2026-03-21_164927/` — 23 attempted
instances, 0 correct, 4.21M tokens (matches the §6.2 numbers exactly).
But:

- `outcome/token_usage_summary.json`: only timestamps + token counts,
  no model field.
- `log/*/status_*.jsonl`: step name, token counts, SQL, errors —
  **no model field**.
- `log/*/main_*.log`: prose log messages — no model name in the lines
  we sampled.

**Git history.** `git log -- DSR-SQL/DSR_Lite/utils/Prompt.py` shows
one commit (`b3a37179`, 2026-03-21 22:15) introducing the file
already at `gpt-4o-mini`. The run completed earlier the same day
(17:57 onwards), so we can't reconstruct what `Prompt.py` looked like
at run time from the repo history alone.

**Net conclusion.** The thesis label "DeepSeek" is **not
reproducible** from what is currently in the repo. There are three
possible interpretations:

1. The run actually used `gpt-4o-mini` (matches the committed code)
   and the thesis row is mislabeled.
2. The run used `deepseek-reasoner` or `deepseek-chat` (matches the
   only two `deepseek-*` strings in the code), and `Prompt.py` was
   edited locally just before the run and reverted before committing
   — but no log records this.
3. The run used some other model via a runtime override that is also
   not captured in any committed artifact.

**What this means for the defense.** This is a real risk the
commission can spot in 30 seconds with `cat utils/Prompt.py`. Two
safe paths forward:

- **Path A (lowest effort).** Change the §6.2 row to
  `"gpt-4o-mini (DSR-Lite default)"`. The accuracy (0 %), token
  count (4.21M), and the architectural argument all still hold —
  in fact the framing gets cleaner, because the 0 % result is now
  attributable purely to architecture, not to "the DSR authors
  picked DeepSeek." This also makes the §6.2 table partially
  model-controlled (Spider-Agent on GPT-4o, DSR-Lite on
  GPT-4o-mini — both OpenAI, similar generation).
- **Path B (if confident DeepSeek was used).** Re-run DSR-Lite with
  `Reasoning_model='deepseek-reasoner'` (or whichever was used)
  set explicitly in `Prompt.py`, regenerate the row, and add a
  sentence to §6.2 naming the exact model ID. Cost: one short run.
  This restores the original framing but with a reproducible
  artifact behind it.

Either path forecloses the question. The current state — thesis
says one model, repo says another, logs say nothing — does not.

**Bottom line for the question "which DeepSeek model was used?":**
**Cannot be answered from the artifacts.** The two candidates are
`deepseek-reasoner` and `deepseek-chat` (the only `deepseek-*` IDs
in the code), but the committed solver defaults to `gpt-4o-mini` and
no run log records the actual model name.

---

## 10) Текст доклада для защиты ВКР (≈15 минут)

Целевая длительность: 14–15 минут устной речи, темп ~125 слов/мин,
итого ~1800–2000 слов. Каждый раздел соответствует одному слайду
презентации. В квадратных скобках — ориентировочное время на слайд.

> **Примечание перед защитой.** Я обнаружил несколько расхождений
> между числами на слайдах и в §6 ВКР — они выписаны в самом конце
> этого раздела (после текста доклада). До защиты эти числа стоит
> сверить и привести к одному источнику истины. В тексте доклада
> ниже использованы цифры **из слайдов**, потому что именно они
> будут перед глазами комиссии.

---

### Слайд 1. Титульный лист [≈30 сек]

Уважаемый председатель и члены аттестационной комиссии! Меня
зовут Павловский Александр, я представляю свою выпускную
квалификационную работу на тему «Разработка ИИ-ассистента для
аналитики больших данных с использованием динамического RAG и
передовых методов NLP в области ML4CODE». Научный руководитель —
Кантонистова Елена Олеговна, соруководитель — Шапкин Антон
Алексеевич. Прошу разрешения начать доклад.

---

### Слайд 2. Проблематика [≈1 мин]

Работа с большими объёмами и сложными схемами промышленных
данных сегодня требует привлечения ИТ-специалистов, владеющих
SQL. Одновременно эксперты доменной области — аналитики,
маркетологи, продуктовые менеджеры — глубоко понимают, какие
именно вопросы нужно задать данным, но не владеют техническим
языком запросов. В результате задача автоматической трансляции
запросов с естественного языка в SQL приобретает высокую
практическую ценность: она снимает с инженеров рутинную нагрузку
и одновременно открывает данные для тех, кто формулирует
бизнес-вопросы. Именно эта потребность лежит в основе бурного
развития направления Text-to-SQL за последние пять лет.

---

### Слайд 3. Анализ предметной области [≈2 мин]

Эволюция методов Text-to-SQL прошла несколько ясных этапов, и
каждый из них упирался в один и тот же предел.

Первое поколение — **zero-shot-подходы и промпт-инжиниринг**. Они
прекрасно работали на академическом бенчмарке Spider 1.0, где
схема умещается в контекстное окно, но столкнулись с тремя
ограничениями: рост сложности схемы и необходимость длинного
контекста, галлюцинации имён таблиц и атрибутов и высокая
чувствительность к формулировке запроса.

Второе поколение — **агентные подходы**, такие как DAIL-SQL,
CHESS и ReFoRCE. Они вводят многошаговое рассуждение, компрессию
схемы, исследование столбцов, генерацию нескольких кандидатов и
голосование с самокоррекцией. Это даёт прирост точности, но за
счёт многократного вызова LLM, что радикально увеличивает
стоимость и задержку.

Третье направление — **обучение с подкреплением**, в частности
методы GRPO, в которых модель получает частичные награды за
привязку к схеме базы данных, проверку синтаксиса и
семантическую близость к эталону. Это перспективное направление,
но оно требует доступа к большим объёмам размеченных данных и
закрытой инфраструктуры обучения.

Параллельно происходил **рост сложности бенчмарков**: от Spider
1.0 к BIRD и далее к Spider 2.0. Spider 2.0 — это уже задачи
промышленного уровня: сотни таблиц, нестандартные типы данных,
длинный контекст, реальные облачные хранилища. Здесь все
вышеперечисленные подходы упёрлись в одну и ту же преграду:
LLM-агенты в их классическом виде с этим масштабом не справляются
— необходимы новые архитектурные решения.

---

### Слайд 4. Проблемы существующих решений [≈1 мин]

Если систематизировать упомянутые проблемы, они сводятся к
четырём пунктам.

Первое — сотни таблиц и тысячи атрибутов промышленных схем
просто не помещаются в контекстное окно модели. Второе — связи
между объектами баз данных часто сложны и плохо
документированы. Третье — попытка компенсировать это агентной
архитектурой превращается в многократные итерации, которые
кратно увеличивают стоимость и задержку ответа. И четвёртое —
усложнение инференса ведёт к существенному росту расхода токенов
и, как следствие, высоким вычислительным затратам.

Таким образом, наращивание объёма вычислений на этапе инференса —
так называемое test-time scaling — не является самодостаточной
стратегией: без качественного отбора метаданных оно просто
многократно прогоняет модель по той же самой неполной или,
наоборот, избыточной схеме.

---

### Слайд 5. Постановка задачи исследования [≈1 мин]

Из этого диагноза вытекают три цели работы.

Первая — рассмотреть альтернативную возможность использования
архитектуры Retrieval-Augmented Generation для оптимизации
контекста LLM, то есть подавать в модель не всю схему, а только
её релевантный фрагмент.

Вторая — снизить общее потребление токенов за счёт отсечения
информации, нерелевантной для пользовательского запроса.

И третья — повысить точность ответов за счёт использования
структурированной базы знаний о схеме, в которой ключевые
семантические сведения собраны заранее, а не выводятся моделью
заново при каждом запросе.

Все три цели проверяются на едином объективном инструменте —
бенчмарке Spider 2.0 — Snow, который содержит 547 запросов к
двадцати промышленным базам данных в облаке Snowflake.

---

### Слайд 6. Научная новизна [≈1 мин]

Научная новизна работы состоит из трёх взаимосвязанных
компонентов.

Во-первых, это первая для Spider 2.0 демонстрация
RAG-архитектуры как инструмента **снижения стоимости** решения
при сохранении и даже росте качества генерации. До этой работы
RAG использовался для повышения точности, но не как средство
экономии токенов на промышленных схемах.

Во-вторых, введены новые архитектурные элементы:
**детерминированный компилятор Plan → SQL**, который выносит
рутину форматирования из LLM в код; **многоступенчатая
постобработка SchemaSlice** с расширением через граф связей;
и **сворачивание схемы для партиционированных по дате таблиц** —
шаг, который снимает «шум» от шардирования и решает типичную
проблему GA360 с его 366 ежедневными партициями.

В-третьих, всё это интегрировано в полноценный программный
комплекс с открытым исходным кодом, который и был использован
для всех экспериментов в работе.

---

### Слайд 7. Архитектура агента генерации SQL-запросов [≈1,5 мин]

Архитектура решения построена вокруг четырёх ключевых блоков.

Первый блок — **векторное хранилище схемы и метаданных** на базе
ChromaDB. В нём хранятся не текстовые фрагменты, как в классическом
RAG, а атомарные сущности — карточки таблиц, столбцов, связей,
семантических фактов и примеров строк. Принцип «один объект —
одна запись» обеспечивает чистоту извлечения.

Второй блок — **гибридный поиск с RRF-фьюжном**. Запрос
пользователя одновременно отрабатывается плотным семантическим
поиском по эмбеддингам и лексическим поиском по идентификаторам;
два ранжированных списка сливаются методом Reciprocal Rank
Fusion. Это даёт устойчивость как к естественно-языковым
формулировкам, так и к запросам с точными именами полей.

Третий блок — **детерминированный компилятор Plan → SQL**. LLM
генерирует только высокоуровневый план запроса в виде JSON, а
форматирование самого SQL — алиасы, цитирование, синтаксис
`LATERAL FLATTEN` для VARIANT-полей, порядок CTE — выполняет
компилятор на стороне кода. Это убирает целый класс
синтаксических ошибок ещё до запуска запроса.

Четвёртый блок — **цикл самокоррекции**, в котором ошибки
Snowflake классифицируются по семи категориям, и под каждую
категорию подбирается специализированный промпт ремонта. На
практике типичные ошибки устраняются за 1–2 итерации.

Поверх этого добавлен механизм **Best-of-N с диверсификацией
стратегий**, который генерирует несколько кандидатов и выбирает
лучший по выученной оценке. Такая комбинация даёт качество
агентного решения при существенно меньшем расходе токенов.

---

### Слайд 8. Коллекции в векторном хранилище [≈1,5 мин]

Векторное хранилище состоит из пяти специализированных коллекций,
каждая из которых отвечает за свой тип знания о данных.

**Schema Cards** — это базовая коллекция структуры. Внутри неё
три подтипа: **TableCard** содержит имя таблицы, её описание на
естественном языке и список столбцов; **ColumnCard** — описания
столбцов, полученные через профилирование реальных данных, с
такими полями, как доля пустых значений, диапазон значений,
число уникальных значений и структура вложенных объектов;
**JoinCard** — карточки связей между таблицами, формируемые из
внешних ключей и из эталонных SQL-запросов.

**Semantic Cards** — коллекция автоматически выведенных
семантических фактов: какие столбцы являются временными, какие
играют роль метрик, какие — измерений, и какие реальные значения
встречаются в категориальных столбцах.

**Sample Records** — несколько репрезентативных строк из каждой
уникальной таблицы. Они показывают модели формат данных,
паттерны NULL и реальные диапазоны значений лучше любой
текстовой документации.

**Trace Memory** — компактные траектории успешно решённых
запросов: формулировка вопроса, план решения, ключевые таблицы.
Они служат few-shot-примерами для новых запросов того же
домена.

**Snowflake Syntax** — справочник по специфике диалекта Snowflake:
`LATERAL FLATTEN`, доступ к VARIANT, окна `QUALIFY`. Эта
коллекция подключается только в момент ремонта ошибки.

Принцип «один объект — одна запись» позволяет точно фильтровать
извлекаемую информацию по типу и подавать в LLM ровно то, что
нужно текущему запросу.

---

### Слайд 9. Процесс извлечения информации — Retrieval Pipeline [≈1,5 мин]

Процесс извлечения построен в три этапа.

**Этап 1 — плотный семантический поиск.** Вопрос пользователя
кодируется моделью `text-embedding-3-large` в вектор размерности
3072, и поиск идёт по косинусной близости в коллекции
`schema_cards` с фильтром по идентификатору базы и типу объекта.
Этот этап хорошо ловит синонимы и парафразы — например, «sales»,
«revenue» и «income» окажутся рядом в пространстве эмбеддингов.

**Этап 2 — лексический поиск по формуле BM25.** Он компенсирует
слабые места плотного поиска: точные коды, идентификаторы вроде
`fullVisitorId` или `osm_id`, ключевые слова, которые в
эмбеддинговом пространстве растворяются. BM25 учитывает три
фактора: насколько редким является термин по корпусу,
насколько часто он встречается в конкретной карточке и насколько
длинна сама карточка. Низкочастотные термины получают высокий
вес и доминируют в ранжировании.

**Этап 3 — слияние двух списков методом Reciprocal Rank Fusion.**
Оценка элемента вычисляется как сумма обратных позиций по двум
спискам с константой сглаживания k, равной 60. Преимущество RRF
в том, что он работает только с позициями, не зависит от
абсолютных величин — а косинусная близость и BM25-оценка имеют
разную природу и шкалу. Это делает решение робастным к
особенностям конкретной базы данных и не требует калибровки
весов под каждый домен.

Финальный список проходит через постобработку: расширение
графом связей (если запрос требует географических соседей —
добавляются их таблицы), обогащение известными вложенными
полями VARIANT-столбцов и обрезание по бюджету токенов. На выходе
формируется компактный объект `SchemaSlice`, который и
встраивается в промпт для LLM.

---

### Слайд 10. Результаты бенчмарков (1) — выборка из 25 запросов [≈1,5 мин]

Перейду к экспериментальным результатам. На первой подвыборке
из 25 запросов бенчмарка Spider 2.0 — Snow я сравнил предложенную
систему — SnowRAG-Agent — с тремя базовыми реализациями
открытого исходного кода.

DSR-Lite на модели DeepSeek с двадцатью итерациями исправлений
дал точность **8 %** — 2 правильных ответа из 25.

Spider-Agent на GPT-4o с пятнадцатью итерациями исправлений —
**12 %**, около 3 правильных ответов.

ReFoRCE на GPT-5-mini с восемью кандидатами и тремя итерациями —
**44 %**, или 11 правильных ответов.

SnowRAG-Agent на GPT-5-mini с восемью кандидатами и тремя
итерациями — **92 %**, 23 правильных ответа из 25.

Ключевой результат — точность 92 %, что более чем в два раза выше
ближайшего базового решения. Но не менее важна стоимость:
SnowRAG-Agent тратит **177 тысяч токенов на один правильный
ответ**, тогда как ReFoRCE — 833 тысячи. Это выигрыш почти в
4,7 раза по эффективности использования модели.

Особо отмечу контраст с DSR-Lite. Дело здесь не в слабости
модели DeepSeek, а в архитектуре: полная DDL базы GA360 с её
сотнями таблиц просто не умещается в контекстное окно, и анализ
запроса становится невозможным до того, как алгоритм
рассуждения или коррекции вообще получит шанс сработать.

---

### Слайд 11. Результаты бенчмарков (2) — выборка из 100 запросов [≈1,5 мин]

Для подтверждения воспроизводимости преимущества был проведён
ключевой эксперимент — прямое сравнение SnowRAG-Agent и
upstream-реализации ReFoRCE на **одной и той же модели**
`gpt-5.4-mini`, на одних и тех же 100 запросах, с одинаковыми
параметрами: 8 кандидатов, 4 цикла коррекции.

Этот эксперимент специально устроен так, чтобы исключить
гипотетическое возражение «вы выиграли просто потому, что взяли
более сильную модель». Модель здесь одна и та же.

Результаты следующие. Точность SnowRAG-Agent — **84 %**, точность
ReFoRCE — **40 %**. Соотношение — **в 2,1 раза в нашу пользу**
по точности.

По расходу токенов разрыв ещё больше: входных токенов 17,6
миллионов против 133,4 — в 7,6 раза меньше; выходных — 2,35
миллиона против 3,62; в сумме 19,9 миллионов против 137,0 — в
6,9 раза меньше токенов.

В деньгах это выливается в **18-кратное снижение стоимости одного
правильного ответа**: 11 центов против 2 долларов 3 центов у
ReFoRCE.

Этот результат принципиально важен методологически: он показывает,
что доминирующим фактором качества является не вычислительный
бюджет и не мощность модели, а структурный отбор метаданных.

---

### Слайд 12. Анализ вклада компонентов (ablation study) [≈1,5 мин]

Чтобы количественно разнести вклад отдельных архитектурных
решений, был проведён ablation study — двенадцать конфигураций,
каждая из которых отключает по одному компоненту, все на одной
и той же модели.

Компоненты разбиваются на три уровня по величине вклада.

**Уровень A — критические компоненты, потеря более 30 пунктов
точности.** Сюда входят: описания столбцов, сгенерированные LLM
из реального профилирования (вклад −68 пунктов); сворачивание
партиций (−84 пункта — самый сильный отдельный эффект на этой
базе); и детерминированный компилятор Plan → SQL (−36 пунктов).

**Уровень B — значимые компоненты, потеря от 20 до 30 пунктов.**
Это обогащение VARIANT-полями (−45 пунктов), базовая LLM без RAG
с полной DDL в промпте (−48 пунктов), Best-of-N (−28 пунктов) и
цикл самокоррекции (−21 пункт).

**Уровень D — компоненты с вкладом менее 10 пунктов.** Это
коллекции `trace_memory`, `semantic_cards`, `sample_records` и
расширение графом связей. Каждая по отдельности даёт небольшой
прирост, но в сумме они стабилизируют поведение на пограничных
формулировках.

Главный вывод этого исследования — три ключевых архитектурных
компонента: качественные описания, сворачивание партиций и
детерминированный компилятор — обеспечивают основную долю
качества. И именно структурный отбор метаданных, а не количество
итераций или мощность модели, является доминирующим фактором.

---

### Слайд 13. Выводы [≈1 мин]

Подведу итоги работы.

Первое: путём тонкой работы с метаданными можно добиться
существенного повышения качества генерируемых SQL-запросов на
промышленных схемах. Конкретно — рост с 36 % у ближайшего
аналога до 84–92 % на бенчмарке Spider 2.0 — Snow.

Второе: использование векторной базы данных и семантического
поиска позволяет избежать перегрузки контекста и улучшить
качество генерации одновременно. Средняя длина промпта в
SnowRAG-Agent составляет 4,7 тысяч токенов против 26,7 тысяч у
ReFoRCE — это сокращение в 5,6 раз.

Третье: технология RAG в применении к Text-to-SQL позволяет
добиться существенного снижения потребления токенов и стоимости
одного правильного ответа — в 18 раз дешевле на одинаковой
модели. Это переводит решение из категории исследовательского
прототипа в категорию пригодного для промышленного развёртывания.

Все три гипотезы работы подтверждены экспериментально.
Программный комплекс опубликован под лицензией MIT, что
обеспечивает полную воспроизводимость результатов.

В приложениях к презентации показаны архитектура веб-приложения,
структура API и пользовательский интерфейс, через которые
система доступна конечному аналитику.

Спасибо за внимание! Готов ответить на ваши вопросы.

---

### ⚠ Расхождения между слайдами и текстом ВКР, которые стоит
сверить до защиты

Я заметил несколько противоречий в числах между презентацией и
§6 ВКР. До защиты их нужно привести к одному источнику истины,
иначе комиссия может задать неудобный вопрос.

| Что сравнивается | Значение на слайдах | Значение в ВКР §6 / THESIS_EN.md |
|---|---|---|
| DSR-Lite, точность (25 запросов) | **8 % (2/25)** | **0 % (0/25)** |
| ReFoRCE, точность (25 запросов) | **44 % (11/25)** | **36 % (9/25)** |
| Модель SnowRAG-Agent в Run 9 | **GPT-5-mini** | **GPT-5.4** |
| ReFoRCE, точность (100 запросов, Run 12) | **40 %** | **20 % (20/100)** |
| Соотношение SnowRAG/ReFoRCE на 100 | **× 2,1** | **× 4,2** |

Также сохраняется вопрос §9.11 ниже: на слайдах для DSR-Lite
указана модель **DeepSeek**, но в committed-коде
`DSR-SQL/DSR_Lite/utils/Prompt.py` дефолт стоит на `gpt-4o-mini`,
и логи прогона модель не фиксируют.

**Рекомендация.** Перед защитой выбрать один источник истины
(скорее всего ВКР, поскольку она консервативнее по числам) и
привести слайды в соответствие. В тексте доклада выше использованы
цифры со слайдов — если решите оставить цифры из ВКР, нужно будет
прокатать раздел «Результаты бенчмарков (1)» и «(2)» с правкой
конкретных процентов и соотношений.

---

## 11) How Best-of-N is implemented in the project

Best-of-N entry point: `agent/best_of_n.py:run_best_of_n`, dispatched
from `agent/agent.py:107-108` whenever the caller passes `best_of_n
> 1`. Default `N` in the full-system ablation preset is **2**
(`config/ablations/full_system.yaml:13`); the ablation that switches
Best-of-N off uses `N = 1` and falls through to the single-candidate
flow. The thesis-level §6.2/6.3 runs use **N = 8** passed in by the
runner.

The pipeline is four sequential phases:
**generate → execute+repair → verify → score-and-select.**

### 11.1 Phase 1 — generate N diverse candidates

Implementation: `agent/candidate_generator.py:generate_candidate_sqls`.

The diversification mechanism is a **rotation over a fixed strategy
list** (`STRATEGIES` at `candidate_generator.py:28-36`):

```python
STRATEGIES = [
    "default",
    "flatten_first",
    "cte_first",
    "join_first",
    "metric_first",
    "time_first",
    "geo_first",
]
```

For candidate `i` (zero-based), the strategy is
`strategies[i % len(strategies)]`. The first two non-default slots
(`flatten_first`, `cte_first`) are placed early so they fire even at
small `N = 3`.

Each strategy injects a **system-prompt hint** appended to the
standard planner system message
(`prompting/prompt_builder.py:300-347`, function
`build_plan_prompt_with_strategy`). Examples:

- `flatten_first` — "START by identifying VARIANT/ARRAY columns that
  need LATERAL FLATTEN ... add a flatten_ops entry ..."
- `cte_first` — "Break the question into sequential steps. Each step
  becomes a CTE in the 'ctes' array. Step 1 filters base data, Step 2
  aggregates, ..."
- `join_first` — "START by identifying the correct JOIN
  relationships between tables."
- `metric_first` — "START by identifying the target metric
  (COUNT, SUM, AVG)."
- `time_first` — "START by identifying date/time filters or
  time-based grouping."
- `geo_first` — "START by identifying any geospatial relationships
  ... use `geo_joins` for spatial JOIN predicates, `geo_filters`
  for spatial WHERE ..."
- `default` — empty hint (the standard planner prompt).

A second diversification axis is **temperature**
(`candidate_generator.py:134`):
```python
temp = temperature if i == 0 else min(temperature + 0.1, 0.8)
```
The first candidate uses the configured temperature (default 0.2);
candidates 2..N add +0.1, capped at 0.8. So you get one "best-guess"
candidate at low temperature plus more exploratory candidates at
slightly higher temperature.

Per candidate, the planner produces a `QueryPlan` JSON. The plan is
parsed with one fix-on-failure retry
(`_try_parse_plan`, `candidate_generator.py:59-79`). If the plan
parses but has empty `selected_tables`, the candidate is retried once
with explicit feedback in the conversation
(`candidate_generator.py:151-178`). The valid plan is then handed to
the **deterministic compiler** `compile_plan(plan, schema_slice)` to
emit SQL — this is where the system avoids most syntax errors before
they ever reach Snowflake. Only if the compiler degenerates to
`SELECT 1` does it fall back to LLM-direct SQL generation
(`candidate_generator.py:181-192`).

Result of Phase 1: a list of `CandidateItem(candidate_id, strategy,
plan, sql, generation_notes)`.

### 11.2 Phase 2 — execute and repair each candidate independently

Implementation: `best_of_n.py:131-181`.

Every candidate independently runs through the full self-correction
loop `refiner.refine_sql` (the same `EXPLAIN → execute → repair`
loop described earlier — pre-execution column validation, EXPLAIN
phase, error classification, snowflake_syntax retrieval on failure,
up to `max_repairs` repair iterations, early termination on
repeated errors).

So Best-of-N is **N parallel** (logically; the loop is sequential
in the current implementation) instances of the single-candidate
flow. There is no shared repair state between candidates — each
gets its own repair_trace and final SQL.

Special case: a candidate whose generated SQL starts with `SELECT 1`
(i.e. the planner failed completely) is short-circuited at
`best_of_n.py:140-158` with `success=False, execution_success=False,
score=0.0` — no Snowflake call wasted on it.

`_candidate_to_result` (`best_of_n.py:24-79`) packages each
candidate's outcome with one important subtlety: **`execution_success`
is true whenever the SQL actually ran on Snowflake and returned rows,
even if gold-match verification later marked `exec_result.success =
False`.** This is what allows the selector to still prefer a
"ran-but-gold-mismatched" candidate over a "didn't-run-at-all"
candidate during benchmark replays.

### 11.3 Phase 3 — verification pass (signal collection)

For every executed candidate, four signal sources are gathered
(`best_of_n.py:183-227`):

**(a) Expected shape** — computed once per instance from the NL
instruction by `shape_inference.infer_expected_shape`
(`agent/shape_inference.py:45-78`). It's a rule-based parser:

| Trigger phrases | Inferred shape flag |
|---|---|
| `top N`, `highest`, `lowest`, `most`, `best`, `worst` | `expect_small_result = True` |
| `monthly` / `per month` / `by month` | `expect_time_series, grain="month"` |
| `daily`, `weekly`, `yearly` | analogous grains |
| `for each`, `grouped by`, `by <dim>`, `per <dim>`, `breakdown` | `expect_grouped_output = True` |
| `how many`, `count`, `total number`, `sum of`, `average` | `expect_aggregate_output = True` |

The shape is attached to every candidate so the scorer sees the
same expectation.

**(b) Result fingerprint** — `result_fingerprint.build_result_fingerprint`
records `row_count`, `column_count`, `column_names`, per-column
`null_ratios`, and `numeric_stats` (min/max/mean) from the actual
execution result. Used by the verifier and for debugging
score breakdowns.

**(c) Metamorphic checks** — `metamorphic.run_metamorphic_checks`,
bounded by `max_metamorphic_checks` (default 2), runs **only on
candidates whose SQL actually executed**. Two checks in v1:

- *Limit expansion*: if the SQL has `LIMIT N`, derive a variant with
  `LIMIT N*2` and re-execute. The check returns a `score_delta`
  reflecting whether the derived query still ran cleanly.
- *Shape consistency*: pure data check comparing `row_count`
  against `expected_shape` (e.g. "grouped output expected but
  only 1 row returned" → −15 pp).

The metamorphic deltas are accumulated into `candidate.metamorphic.score_delta`.

**(d) Verifier score** — `verifier.score_candidate_semantics`
(`agent/verifier.py:46-60`). A learned LogisticRegression on
`verifier_features.extract_candidate_features` lives at
`models/verifier.joblib` if trained (see `train_verifier.py`); if
the artifact is absent the function returns 0.0. So in the default
distribution this signal is a no-op.

### 11.4 Phase 4 — score and select

Implementation: `agent/selector.py:score_candidate` and
`explain_candidate_score`, called from `best_of_n.py:226-227`.

Scoring is a **linear sum of independent terms** with parameters
overridable via the config's `scoring` dict. Default values from
`selector.py:22-35`:

| Term | Default | When applied |
|---|---:|---|
| `execution_success` | **+100** | Candidate executed on Snowflake (or returned rows despite gold mismatch) |
| `repair_penalty` | **−10 × repairs** | Each repair iteration the candidate needed |
| `empty_result` | **−20** | `row_count == 0` |
| `small_output_bonus` | **+10** | `expect_small_result` and `row_count ≤ 5` |
| `grouped_output_bonus` | **+5** | `expect_grouped_output` and `row_count > 1` |
| `grouped_single_row_penalty` | **−15** | `expect_grouped_output` but `row_count == 1` |
| `aggregate_single_row_bonus` | **+10** | `expect_aggregate_output` (non-grouped) and `row_count == 1` |
| `time_series_bonus` | **+10** | `row_count` falls in plausible band per grain (e.g. monthly → 6–24 rows) |
| `error_penalty` | **−30** | Final `error_type ∈ {object_not_found, invalid_identifier}` |
| `error_penalty` | **−15** | Final `error_type == aggregation_error` |
| `metamorphic_delta` | sum | Σ score_delta from metamorphic checks |
| `verifier_score` | `× 20` | Trained verifier output if model loaded, else 0 |

So the score is dominated by execution success (+100 vs 0), modulated
by shape-fit signals (±10–15 each), penalized for repairs and
hopeless error types, and nudged by metamorphic and (optionally)
learned-verifier deltas.

Selection: `candidate_results.sort(key=lambda c: c["score"],
reverse=True); best = candidate_results[0]`
(`best_of_n.py:230-231`). Plain argmax — no voting, no consensus.

A human-readable `selection_reason` is composed
(`best_of_n.py:233-260`): which strategy, the numeric score, whether
the candidate executed, and which shape-fit bonuses fired. This
becomes part of the persisted instance record.

### 11.5 Output

`run_best_of_n` returns a dict
(`best_of_n.py:262-269`):

```python
{
  "best_candidate_id": <int>,
  "best_sql":          <str>,
  "best_success":      <bool>,
  "selection_reason":  <str>,
  "expected_shape":    {...},
  "candidates":        [<full per-candidate dict>, ...],
}
```

The caller (`agent.py:_solve_best_of_n`) wraps that into an
`InstanceResult` with `best_of_n_used=True`,
`candidate_count=N`, `selection_reason=...`, and a list of
`candidate_summaries` so the benchmark report can show all N
attempts side-by-side.

### 11.6 Differences from ReFoRCE-style voting

ReFoRCE's Best-of-N variant is a voting-with-self-correction
ensemble: N candidates vote, and the most popular SQL wins (see the
§6.3.1 failure analysis at lines 583-584 of the thesis where this
mechanism mis-selects two queries). SnowRAG-Agent's Best-of-N is
**not voting** — it is **multi-signal scoring + argmax** on a
fixed-weight linear combination. Concrete consequences:

- **Diversity is structural, not random.** Each candidate has a
  different planning strategy injected into the system prompt, so
  the N candidates explore distinct interpretations
  (flatten-first vs cte-first vs join-first vs ...). Temperature
  only nudges sampling within each strategy.
- **Selection uses ground-truth-ish signals**, not popularity.
  The +100 for "actually executed" is by far the largest term in
  the score, so any successfully-executing candidate strictly
  dominates any that failed. Shape signals discriminate between
  multiple successful candidates.
- **Cheap by design.** N is small in practice (2 in the full-system
  ablation; 8 in §6.2/§6.3 runs). Combined with the deterministic
  compiler producing valid SQL on the first try most of the time,
  the average cost per question stays low — this is the
  18× cost-per-correct-answer gap reported in §6.3.1.

### 11.7 Quick reference — Best-of-N call chain

```
solve_instance(best_of_n=N)              agent/agent.py:107
  → _solve_best_of_n                     agent/agent.py:266
    → run_best_of_n                      agent/best_of_n.py:82
      1. generate_candidate_sqls         candidate_generator.py:82
         · strategy rotation (STRATEGIES)
         · temperature ramp
         · build_plan_prompt_with_strategy   prompt_builder.py:350
         · plan parse + fix retry        candidate_generator.py:59
         · compile_plan (deterministic)  prompting/sql_compiler.py
         · LLM-SQL fallback if compile→SELECT 1
      2. refine_sql per candidate        agent/refiner.py
         · EXPLAIN-then-execute + repair loop
      3. verification pass               best_of_n.py:183
         · infer_expected_shape          shape_inference.py:45
         · build_result_fingerprint      result_fingerprint.py
         · run_metamorphic_checks        metamorphic.py
         · score_candidate_semantics     verifier.py
      4. score_candidate + argmax        selector.py:38
         · linear weighted sum
         · best = sorted desc by score → [0]
    ← returns InstanceResult with candidate_summaries
```

### 11.8 Is gold-match part of Best-of-N scoring? — clarification

**Short answer: no, not directly.** The scorer is deliberately
gold-blind. Gold-match enters the final score only through two
indirect side channels, and even those are bounded.

**What the scorer actually reads** (`selector.score_candidate`,
`selector._build_breakdown`):

```
execution_success, repairs_count, row_count, error_type,
expected_shape, metamorphic.score_delta, verifier_score
```

None of these is "result matches gold." The scorer cannot distinguish
"executed successfully and matched the reference" from "executed
successfully, no reference available, may or may not be correct."

But gold-match **is** consulted inside `refine_sql`
(`refiner.py:391-448`) when the caller passes `gold_dir`
(benchmark mode). That check leaks into the score in two ways:

**Channel 1 — gold PASS routes through the regular
`execution_success` bonus.**
When gold matches, `refine_sql` returns immediately with
`exec_result.success=True`. The scorer awards the standard
**+100** `execution_success` bonus. So passing gold *does* improve
the score — but indistinguishably from "ran cleanly without gold
verification."

**Channel 2 — gold FAIL hurts only via `repair_penalty`.**
When gold doesn't match, `refine_sql` treats the result as an error
(`RESULT_MISMATCH`), bumps `repairs_count`, and loops again. The
scorer deducts **−10 × repairs**. With `max_repairs=2` the worst
gold-mismatch can do is −20.

**Crucially, gold-failure is explicitly masked from the binary
`execution_success` flag.** `_candidate_to_result`
(`best_of_n.py:42-50`):

```python
if exec_result and exec_result.success:
    execution_success = True
elif exec_result and row_count is not None and row_count > 0:
    # Gold-match failure: SQL executed and returned rows, but results
    # didn't match gold.  Treat as execution success for scoring purposes.
    execution_success = True
```

So even after `refine_sql` flips `success=False` because gold
mismatched, the scorer still grants the +100 — because rows were
returned. A "ran-but-wrong-result" candidate keeps its +100 and only
pays the −10×repairs penalty.

Additionally, `RESULT_MISMATCH` is **not** in the `error_type`
penalty list in `_build_breakdown` (`selector.py:125-129`), which
penalizes only `object_not_found`, `invalid_identifier`, and
`aggregation_error`. So the error-type channel also doesn't penalize
gold mismatch.

**Why this design.** Best-of-N has to work in two modes:

- **Benchmark mode** (`gold_dir` set) — gold exists. Making scoring
  depend on it would be cheating the benchmark: "select the
  candidate that matches gold" is not selection, it's reading the
  answer key.
- **Production / UI mode** (no `gold_dir`) — there is no gold. The
  scorer must work identically here.

So the scorer was deliberately built **gold-blind** and gold-match
only leaks in via the bounded repair channel.

**Net effect on the score.** Sorting candidates by score, with N=8
and `max_repairs=2`:

| Candidate outcome | Bonus | Repair penalty | Net |
|---|---:|---:|---:|
| Gold PASS (no repairs needed) | +100 | 0 | **+100** |
| Gold PASS after 1 repair | +100 | −10 | **+90** |
| Gold FAIL after max_repairs (=2), rows returned | +100 | −20 | **+80** |
| Ran cleanly, no gold available | +100 | 0 | **+100** |
| SQL never executed | 0 | 0 or more | **0 or worse** |

A gold-failing candidate that produced rows ends up at +80;
a gold-passing one at +90 to +100; a never-executed one at ≤ 0.
So gold-failure is detectable in the score, but the gap is much
smaller than the +100 between "executed" and "didn't execute" —
and shape signals (±10–15 each) and the verifier (if trained,
×20) can swing the final ranking around the gold-mismatch signal.

**Practical implication for the §6 benchmark.** Because gold-failure
is partially absorbed by the score, in benchmark replays Best-of-N
may pick a "ran-but-wrong" candidate over an alternate "ran-but-also-wrong"
candidate based on shape signals (small result expected, time series
plausible, etc.) — not based on which one matched gold. This is what
the §11.6 contrast with ReFoRCE's voting captures: SnowRAG-Agent
selects on *what the result looked like*, not on *whether it agreed
with the reference*.

(This also explains the §6.3.1 finding that every query ReFoRCE
solved, SnowRAG-Agent also solved: ReFoRCE's voting can pick the
wrong one out of multiple correct ones; SnowRAG-Agent's shape-driven
scoring is less likely to do that, because a correctly-shaped result
beats a wrongly-shaped result regardless of which other candidates
voted for it.)

---

## 12) Слайд «Направления дальнейших исследований» — 5 тезисов

Каждый тезис опирается либо на конкретный нерешённый кейс из §6/§8
ВКР, либо на реальный архитектурный задел, обнаруженный в коде, и
формулируется так, чтобы можно было сразу назвать **эксперимент** и
**измеряемую метрику**. Под каждым тезисом — короткая «защитная
обвязка» для слайда и для устного ответа.

### Тезис 1. Обучение верификатора кандидатов и его включение в Best-of-N

**Формулировка для слайда.**
Дообучение модели логистической регрессии на накопленных логах
кандидатов и интеграция её выходной вероятности в скоринг отбора
лучшего кандидата вместо текущего эвристического веса 0.

**Почему это разумная следующая работа.**
В архитектуре уже реализована полная инфраструктура: модуль
`verifier_features` извлекает 15 числовых признаков из каждого
кандидата (флаги ошибок, бакет числа строк, согласованность с
ожидаемой формой результата, метаморфические дельты, сложность
SQL), модуль `train_verifier` обучает `LogisticRegression`, а
`trace_logger` автоматически складывает все Best-of-N кандидаты в
JSONL. Не хватает только тренировочного прогона на накопленных
логах. Эффект включения в скоринг даёт прибавку до +20 баллов за
кандидата при `verifier_weight=20`.

**Эксперимент:** собрать кандидатные логи с прогонов Run 10/12,
обучить LogReg, заменить нулевой стаб на обученный артефакт и
сравнить точность Best-of-N с N = 2…8 на той же 100-запросной
подвыборке.

**Метрика:** прирост gold-match при фиксированной модели и N.

### Тезис 2. Доменно-адаптированная модель эмбеддингов

**Формулировка для слайда.**
Замена универсальной модели `text-embedding-3-large` на
доменно-специализированную: либо Snowflake Cortex Embed, либо
дообученную через contrastive learning на парах
«естественный вопрос ↔ карточка схемы», извлечённых из траекторий
успешных решений.

**Почему это разумная следующая работа.**
Плотный поиск — единственный канал, формирующий начальный набор
кандидатов в RAG-конвейере (см. §11 в notes — лексический этап
работает только на этих 200 кандидатах), и одновременно
единственный компонент, использующий неспециализированную модель
общего назначения. Из ablation в §7 ВКР видно, что качество
описаний столбцов даёт +68 пунктов точности — то есть смысловая
близость текста к вопросу критически важна. При этом
`text-embedding-3-large` обучена на веб-корпусе, где слова
«revenue» и «sales» близки в общеязыковом смысле, но в контексте
GA360 они относятся к разным VARIANT-полям с разными
формулами расчёта. Доменная модель учится именно этой разнице.

Дообучение поверх contrastive-датасета из логов даёт **самонастройку
под конкретное хранилище** заказчика: чем дольше система работает,
тем точнее эмбеддинги отражают семантику его именно метрик. Это
также прямо упоминается в §8 ВКР как одно из перспективных
направлений.

**Эксперимент:** собрать пары «вопрос ↔ итогово-использованные
карточки» из логов Run 10/12 (порядка нескольких сотен пар),
обучить bi-encoder поверх Sentence-BERT-подобной архитектуры,
заменить им эмбеддинг-функцию в `chroma_store.py` и прогнать тот
же 100-запросный бенчмарк.

**Метрика:** Recall@k для нужных таблиц/столбцов на верхнем
ранге RRF-списка и итоговая точность gold-match при прочих
равных.

### Тезис 3. Специализированный субагент для геопространственных задач

**Формулировка для слайда.**
Выделенный модуль обработки геозапросов с собственным набором
примитивов плана (S2/H3-индексы, расширенный набор Snowflake
spatial-функций, библиотека готовых пространственных шаблонов).

**Почему это разумная следующая работа.**
В §6 ВКР зафиксировано, что из 16 нерешённых задач на 100-выборке
большинство — геопространственные сценарии: запросы по радиусу,
проверка попадания точки в полигон, дистанции между объектами.
Текущая стратегия `geo_first` подсказывает планировщику только
семь общих рекомендаций, но не предоставляет ему пространственных
структур данных и не индексирует геообъекты. Целевой субагент
закроет эту нишу, не утяжеляя основной конвейер для остальных
доменов.

**Эксперимент:** прогон на геоподвыборке (GEO_OPENSTREETMAP,
NEW_YORK_NOAA, NOAA_DATA_PLUS) с включённым и выключенным
гео-субагентом.

**Метрика:** gold-match на геоподвыборке и доля «гео» среди
нерешённых задач полной 100-выборки.

### Тезис 4. Самопополняющаяся trace_memory через пользовательские сессии

**Формулировка для слайда.**
Автоматическое накопление коллекции `trace_memory` за счёт
успешно завершённых пользовательских сессий с фиксацией признака
«пользователь принял ответ» и непрерывное самообучение системы во
времени.

**Почему это разумная следующая работа.**
По §6 коллекция `trace_memory` сейчас почти пуста (5 элементов),
и в ablation её вклад ограничен −4 пунктами — не потому, что
механизм слабый, а потому, что нечего извлекать. Если включить
автоматическую запись успешных кейсов из веб-приложения, через
несколько недель работы коллекция накопит сотни траекторий, и
few-shot-канал по аналогичным вопросам начнёт стабильно срабатывать.
Это превращает систему из статического snapshot-а в обучающуюся
во времени.

**Эксперимент:** синтетический бэкфил `trace_memory` из логов
Run 10/12 для воссоздания «зрелого» состояния, повторный прогон
бенчмарка с включённой и выключенной памятью.

**Метрика:** прирост точности при росте размера коллекции и
средняя экономия токенов на повторяющихся типах запросов.

### Тезис 5. Перенос архитектуры на BigQuery / Redshift / DuckDB

**Формулировка для слайда.**
Оценка переносимости подхода на другие облачные хранилища и
SQL-диалекты, выделение диалект-зависимых компонентов и
формализация интерфейса диалекта.

**Почему это разумная следующая работа.**
Бо́льшая часть архитектуры — векторное хранилище, гибридный
поиск, RRF, ablation-исследование — диалект-независима. К Snowflake
жёстко привязаны три компонента: компилятор `Plan → SQL` (синтаксис
`LATERAL FLATTEN`, кавычки), коллекция `snowflake_syntax` и
коннектор `executor`. Перенос на BigQuery даст ответ на ключевой
открытый вопрос: насколько преимущество в стоимости и точности
устойчиво к смене диалекта, и сколько работы требует адаптация.
Spider 2.0 — Lite уже размечен для BigQuery, что снимает вопрос
с эталонной разметкой.

**Эксперимент:** портирование `SQL-компилятора` и `snowflake_syntax`
на BigQuery, прогон тех же 100 задач из Spider 2.0 — Lite.

**Метрика:** разница в gold-match и стоимости на правильный
ответ между Snowflake- и BigQuery-вариантами.

### Краткая сводка для слайда (одна строка на тезис)

> 1. **Обученный LogReg-верификатор** — закрытие готового задела в
>    скоринге Best-of-N.
> 2. **Доменно-адаптированные эмбеддинги** — самонастройка
>    плотного поиска под конкретное хранилище через
>    contrastive-обучение на логах.
> 3. **Гео-субагент** — закрытие основной зоны провалов (16 из 100).
> 4. **Самонаполняющаяся trace_memory** — переход от статического
>    snapshot-а к обучению на сессиях пользователей.
> 5. **Кросс-диалектная переносимость (BigQuery / Redshift)** —
>    проверка устойчивости результата за пределами Snowflake.

### Что отвечать, если спросят «а почему этого нет в работе»

Все пять направлений — это либо архитектурные заделы, не доведённые
до полноразмерного эксперимента в рамках текущего цикла, либо
естественные обобщения, требующие отдельной эталонной разметки.
Это нормальная зона будущей работы: тема ВКР — «продемонстрировать
жизнеспособность подхода», и она закрыта; полное обогащение
обвязки и кросс-диалектная проверка — следующий шаг.

---

## 13) Как сейчас работает trace_memory — полный путь

### 13.1 Реальное состояние коллекции

Согласно живой `.chroma`-базе (проверка через `uv run` на момент
этого ответа):

```
schema_cards:      579985
semantic_cards:      5762
sample_records:        10
trace_memory:         123
snowflake_syntax:      62
```

То есть `trace_memory` **уже не пуста** — в ней 123 записи,
накопленные за прогоны Run 10/12. Документ
`chroma/COLLECTIONS.md` устарел в части «0 items».

### 13.2 Структура записи

Реализация — `chroma/trace_memory.py` + `agent/memory.py`.

**Что *вычисляется* при сохранении** (`memory.py:make_trace_record`):
богатый `TraceRecord` с 14 полями:

| Поле | Источник |
|---|---|
| `trace_id` | sha256 хэш от `instance_id:db_id:final_sql[:100]`, обрезан до 16 hex-символов |
| `instance_id`, `db_id`, `instruction` | как есть |
| `instruction_summary` | `instruction[:200]` |
| `schema_slice_summary` | "DB: …" + список таблиц с первыми 8 столбцами каждой |
| `plan_summary` | сжатый план: «Tables: T1, T2; Joins: 2; Aggs: COUNT(x); Group: a, b; Filters: 3» |
| `final_sql` | усечён до 500 символов |
| `repair_summary` | «error_type→repair_action; …» по цепочке исправлений |
| `verification_summary` | «exec:OK, rows:5, meta_delta:+10.0» |
| `tables_used` | список qualified_name из `schema_slice` |
| `key_columns_used` | только join-ключи + временны́е столбцы |
| `join_conditions` | «T1.a = T2.b» из плана |
| `column_access_patterns` | VARIANT-обращения, извлечённые regex-ом из финального SQL: `"trafficSource":"source"::STRING` |

**Что реально сохраняется в Chroma** (`trace_memory.py:31-49`):

```python
doc = (instruction_summary or "") + "\n" + (plan_summary or "")
col.upsert(
    ids=[trace_id],
    documents=[doc],                         # ← эмбеддится только это
    metadatas=[{
        "db_id":          ...,
        "instance_id":    ...,
        "tables_used":    ",".join(tables_used),
        "token_estimate": ...,
    }],
)
```

**Существенно:** из 14 полей `TraceRecord` в Chroma попадают только
**два** (`instruction_summary` + `plan_summary`) в эмбеддинг и
**четыре** (`db_id`, `instance_id`, `tables_used`, `token_estimate`)
в метаданные. Остальные 10 полей — `final_sql`, `repair_summary`,
`verification_summary`, `key_columns_used`, `join_conditions`,
`column_access_patterns`, `schema_slice_summary`, и вторичные
`instruction` — **вычисляются и теряются** при upsert-е. Это
важный задел для оптимизации.

### 13.3 Когда трейс записывается

Триггер — `agent.py:_persist_trace`. Вызывается в двух местах:

1. **После Best-of-N** (`agent.py:131-139`):
   ```python
   if memory_enabled and result.success:
       _persist_trace(instance_id, db_id, instruction,
                      schema_slice, final_sql=result.final_sql,
                      chroma_dir=chroma_dir)
   ```
   ⚠ Заметьте: в Best-of-N ветке `plan`, `repair_trace` и
   `candidate_record` **не передаются**, поэтому `plan_summary`,
   `repair_summary` и `verification_summary` в записи окажутся
   пустыми. Сохраняется минимальный вариант.

2. **После одиночного кандидата** (`agent.py:162-172`):
   ```python
   if memory_enabled and result.success:
       _persist_trace(instance_id, db_id, instruction,
                      schema_slice,
                      plan=result.pipeline_result.plan,
                      final_sql=result.final_sql,
                      repair_trace=result.repair_trace,
                      chroma_dir=chroma_dir)
   ```
   Здесь передаётся всё, что нужно для богатой записи. Но
   как мы только что увидели — большая часть полей всё равно
   потеряется на стороне `upsert_trace`.

Гейт `result.success` — это финальный успех инстанса. В
benchmark-режиме с `gold_dir` это означает, что результат сошёлся
с эталоном; в production-режиме — что SQL выполнился без ошибки.

### 13.4 Когда трейсы читаются — два независимых пути

**Путь A — прямой dense-запрос в момент решения новой задачи**
(`chroma/trace_memory.py:51-79`):

```python
def query_traces(self, db_id, instruction, top_k=3):
    return col.query(
        query_texts=[instruction],
        n_results=top_k,
        where={"db_id": db_id},
        include=["metadatas", "documents", "distances"],
    )
```

— чистый плотный поиск по эмбеддингу всей формулировки нового
вопроса, фильтр по `db_id`, top_k = 3 по умолчанию. Без BM25, без
RRF (как и `semantic_cards` и `snowflake_syntax` — гибридный поиск
только над `schema_cards`).

Далее `prompting/prompt_builder.py:build_memory_context` рендерит
эти 3 трейса в компактный блок:

```
Prior successful queries on this database:
- [a3b1c2d4] <instruction_summary>\n<plan_summary> | tables: T1,T2,T3 | sql: <sql_preview[:120]>
- [9f7e2c1d] ...
```

— с бюджетом 800 токенов и token-aware обрезанием.

**⚠ Latent bug в рендеринге.** Строка
`sql_preview = t.get("metadata", {}).get("sql_preview", "")`
читает поле `sql_preview` из метаданных трейса. Но
`upsert_trace` **не пишет `sql_preview` в метаданные** — поле
вообще не упоминается в `chroma_metadata`. То есть колонка
`| sql: ...` в рендере **всегда пустая**. Реальный few-shot
получает только `instruction_summary + plan_summary + tables_used`,
финальный SQL до LLM не доезжает.

Этот блок втыкается в промпт в `prompt_builder.py:242-243`:
```python
if memory_context:
    user_content = memory_context + "\n\n" + user_content
```

— то есть выше схемы, выше семантического контекста, выше самого
вопроса. Это самая первая часть промпта.

И вот критически важная находка:

**⚠ Путь A *не подключён* к production-конвейеру бенчмарка.**
В `eval/experiment_runner.py` нигде не вызывается `query_traces`,
и параметр `memory_context` ни разу не передаётся в `solve_instance`
/ `run_pipeline`. Параметр существует в сигнатурах
`run_pipeline` (`plan_sql_pipeline.py:88`) и `build_plan_prompt`
(`prompt_builder.py:225`), но в `agent.py:_solve_single` он
**не пробрасывается** при вызове `run_pipeline`.

То есть инфраструктура полная, рендеринг работает, прямо в `chroma`
лежат 123 трейса — а до промпта они **никогда не доходят** во время
бенчмарковых прогонов. Это объясняет, почему вклад `trace_memory` в
ablation (§7 ВКР) ограничен −4 пунктами: измерялся не сам канал,
а его отсутствие при пустой коллекции; а сейчас он не подключён даже
при непустой коллекции.

**Путь B — косвенный, через semantic layer**
(`semantic_layer/infer_from_traces.py`):

```python
def infer_from_traces(db_id, trace_store):
    col = trace_store.collection()
    results = col.get(where={"db_id": db_id}, limit=100,
                      include=["metadatas", "documents"])
    # ...
```

Делает **metadata-only `get`** (не векторный запрос) первых 100
трейсов для базы. Из них вытаскивает:

- **`frequently_used_table`** — таблицы, упомянутые в `tables_used`
  ≥ 2 раз, попадают как SemanticFact с confidence
  = min(0.5 + 0.1·count, 0.9).
- **`variant_access_pattern`** — VARIANT-обращения, извлечённые из
  `document` regex-ом `"(\w+)\s*:\s*"?(\w+)`, тоже с порогом
  ≥ 2 повторений.

Эти факты добавляются в коллекцию `semantic_cards` во время
запуска `semantic_layer/build_semantic_layer.py` (точка вызова —
строка 68 этого файла). И тут второе ограничение:

**⚠ `infer_from_traces` срабатывает только при ребилде semantic
layer-а.** Это офлайн-индексирующий шаг, который запускается
вручную до бенчмарка. Трейсы, накопленные во время самого
бенчмарка, в `semantic_cards` не попадают — только при
последующем ребилде. То есть «обучение во времени» сейчас не
происходит: цикл feedback не замкнут.

### 13.5 Сводка: что есть и чего не хватает

| Компонент | Реализован? | Подключён к benchmark-конвейеру? |
|---|---|---|
| Сборка `TraceRecord` (14 полей) | ✅ | ✅ — вызывается при `success` |
| `upsert_trace` в Chroma | ✅ — но сохраняет только 2+4 поля | ✅ |
| `query_traces` (dense top-3) | ✅ | ❌ **не вызывается из experiment_runner** |
| `build_memory_context` (рендер few-shot) | ✅ — но `sql_preview` всегда пуст | ❌ |
| `memory_context` в `run_pipeline` / `build_plan_prompt` | ✅ — параметр существует | ❌ — не пробрасывается из `_solve_single` |
| `infer_from_traces` → `semantic_cards` | ✅ | ⚠ только при ручном ребилде semantic layer |

### 13.6 Что это значит для ablation и для будущей работы

В § 7 ВКР вклад `trace_memory` оценён как −4 пункта. Это число
честное в том смысле, что измерялась **текущая конфигурация
системы**, в которой:

- путь A неактивен (нет вызова из experiment_runner),
- путь B активен, но только на сильно усечённой версии данных
  (только частоты таблиц и VARIANT-паттерны, без полного SQL и
  без структуры плана).

Это даёт сильный фактический довод в пользу **Тезиса 4** будущих
исследований (самопополняющаяся trace_memory, §12 этой заметки):
выигрыш не в том, чтобы построить новую функциональность с нуля —
а в том, чтобы **замкнуть три обрыва** (`experiment_runner` →
`query_traces`, `upsert_trace` → сохранять все 14 полей,
`infer_from_traces` → онлайн-режим) и измерить эффект полностью
подключённого канала. Это маленький патч с потенциально большой
отдачей и подходящая по объёму задача для следующего шага.
