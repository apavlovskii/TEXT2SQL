# ChromaDB Collections Reference

> All collections use `text-embedding-3-large` (OpenAI, 3072 dimensions) with cosine similarity.
> Persistence path: `rag_snow_agent/.chroma/`
>
> **Last updated:** 2026-04-01

---

## Overview

| Collection | Items | Purpose |
|:-----------|------:|:--------|
| `schema_cards` | 25,307 | Tables, columns (incl. VARIANT sub-fields), and join edges |
| `semantic_cards` | 5,762 | Auto-induced semantic facts: time columns, metrics, dimensions, sample rows, filter values |
| `snowflake_syntax` | 55 | Chunked Snowflake SQL syntax reference documentation |
| `trace_memory` | 0 | Successful solution traces for few-shot retrieval (populated during runs) |

---

## 1. `schema_cards` — 25,307 items

**Purpose:** Database schema metadata for retrieval during SQL generation.

### Card breakdown

| Card Type | Count | ID Format |
|:----------|------:|:----------|
| TableCard | 465 | `table:DB.SCHEMA.TABLE` |
| ColumnCard | 7,862 | `column:DB.SCHEMA.TABLE.COLUMN` |
| JoinCard | 16,980 | `join:LEFT.COL->RIGHT.COL` |

**Special entries:**
- 58 VARIANT sub-field columns (`data_type=VARIANT_FIELD`) — nested paths from GA4/GA360 VARIANT columns
- 234 gold SQL join edges (`source=gold_sql`, `confidence=1.0`) — extracted from Spider2 gold SQL files

### Per-database breakdown

| Database | Tables | Columns | VARIANT Sub-fields | Join Edges | Total |
|:---------|-------:|--------:|-------------------:|-----------:|------:|
| GA4 | 92 | 2,137 | 21 | 16,744 | 18,973 |
| GA360 | 366 | 5,559 | 37 | 0 | 5,925 |
| PATENTS | 3 | 79 | 0 | 5 | 87 |
| PATENTS_GOOGLE | 4 | 87 | 0 | 4 | 95 |
| Other (gold joins) | — | — | — | 227 | 227 |

### Metadata fields

- **TableCard:** `db_id`, `object_type="table"`, `qualified_name`, `source`, `token_estimate`
- **ColumnCard:** `db_id`, `object_type="column"`, `qualified_name`, `table_qualified_name`, `data_type`, `token_estimate`, `source`
- **JoinCard:** `db_id`, `object_type="join"`, `left_table`, `right_table`, `left_column`, `right_column`, `confidence`, `source`

### Built by

```bash
uv run python -m rag_snow_agent.chroma.build_index --db_id <DB> --credentials snowflake_credentials.json
uv run python -m rag_snow_agent.chroma.ingest_gold_joins --gold_dir Spider2/spider2-snow/evaluation_suite/gold/sql/
```

---

## 2. `semantic_cards` — 5,762 items

**Purpose:** Automatically induced semantic facts about databases — what columns mean, what values they contain, which are time/metric/dimension columns.

### Fact type breakdown

| Fact Type | Count | Source | Description |
|:----------|------:|:-------|:------------|
| `nested_container_column` | 3,280 | metadata | VARIANT/OBJECT/ARRAY columns identified |
| `date_format_pattern` | 1,204 | metadata | Date columns with detected format (YYYYMMDD as NUMBER/VARCHAR) |
| `dimension_candidate` | 839 | metadata | String columns likely used for grouping (source, status, country) |
| `identifier_column` | 374 | metadata | Join key / ID columns (*_id, *_key) |
| `filter_value_hints` | 60 | **probes** | Top 5 distinct values from live Snowflake queries |
| `sample_rows` | 5 | **probes** | 5 representative rows per unique table schema |

### Sample rows by database

| Database | Sample Rows | Filter Hints | Total Cards | Notes |
|:---------|:------------|:-------------|:------------|:------|
| GA4 | 1 | 4 | 1,937 | 92 partition tables → 1 representative |
| GA360 | 2 | 8 | 3,670 | 366 partition tables → 2 unique schemas |
| PATENTS | 1 | 24 | 76 | 3 tables; PUBLICATIONS too large for sample probe |
| PATENTS_GOOGLE | 1 | 24 | 79 | 4 tables; same limitation |

### Probe-derived facts (from live Snowflake queries)

- **`filter_value_hints`**: Top 5 distinct values for STRING columns (e.g., `country_code: ['US', 'GB', 'DE', 'FR', 'JP']`)
- **`sample_rows`**: First 5 rows from each unique table, showing actual data formats, NULL patterns, and value ranges

### Metadata fields

- `db_id`, `object_type="semantic"`, `fact_type`, `subject`, `confidence`, `source`, `token_estimate`

### Built by

```bash
uv run python -m rag_snow_agent.semantic_layer.build_semantic_layer \
  --db_id <DB> --credentials snowflake_credentials.json --max_probe_budget 20
```

---

## 3. `snowflake_syntax` — 55 items

**Purpose:** Chunked Snowflake SQL syntax reference for retrieval during query repair.

### Topics

| Topic | Chunks | Key Content |
|:------|-------:|:------------|
| `LATERAL_FLATTEN` | 6 | LATERAL JOIN, FLATTEN for VARIANT/ARRAY/OBJECT, chaining |
| `SNOWFLAKE_IDENTIFIERS` | 6 | Case sensitivity, double-quoting, VARIANT colon access |
| `QUALIFY` | 6 | Window function filtering (Snowflake extension) |
| `PIVOT_UNPIVOT` | 6 | PIVOT, dynamic PIVOT, UNPIVOT |
| `SET_OPERATORS` | 5 | UNION, INTERSECT, EXCEPT/MINUS |
| `SUBQUERY_OPERATORS` | 5 | EXISTS, IN, ALL/ANY |
| `WHERE_FILTER` | 5 | WHERE clause, NULL handling, ILIKE |
| `GROUP_BY` | 4 | GROUP BY, GROUP BY ALL, ROLLUP, CUBE |
| `JOIN` | 4 | INNER/LEFT/RIGHT/FULL/CROSS/NATURAL |
| `ORDER_BY_LIMIT` | 4 | ORDER BY, NULLS FIRST/LAST, LIMIT/OFFSET |
| `WITH_CTE` | 4 | CTEs, recursive CTEs |

### Built by

```bash
uv run python -m rag_snow_agent.chroma.ingest_syntax
```

---

## 4. `trace_memory` — 0 items

**Purpose:** Compact traces of successfully solved Spider2-Snow instances for few-shot retrieval during plan generation.

Populated automatically during benchmark runs when:
- `memory.enabled: true` in config
- A query's results match gold (when `--gold_dir` is set)

### Record structure

Each trace stores: instruction summary, plan summary, final SQL (truncated), tables used, key columns, join conditions, VARIANT access patterns.

### Built by

Automatically via `_persist_trace()` in `agent.py` during successful gold-matched solves.

---

## Rebuild All Collections

```bash
cd rag_snow_agent

# 1. Wipe and rebuild schema cards
rm -rf .chroma
for db in GA4 GA360 PATENTS PATENTS_GOOGLE; do
  uv run python -m rag_snow_agent.chroma.build_index --db_id "$db" --credentials snowflake_credentials.json
done

# 2. Gold SQL joins
uv run python -m rag_snow_agent.chroma.ingest_gold_joins \
  --gold_dir Spider2/spider2-snow/evaluation_suite/gold/sql/

# 3. Snowflake syntax reference
uv run python -m rag_snow_agent.chroma.ingest_syntax

# 4. Semantic layer (with live probes)
for db in GA4 GA360 PATENTS PATENTS_GOOGLE; do
  uv run python -m rag_snow_agent.semantic_layer.build_semantic_layer \
    --db_id "$db" --credentials snowflake_credentials.json --max_probe_budget 20
done
```
