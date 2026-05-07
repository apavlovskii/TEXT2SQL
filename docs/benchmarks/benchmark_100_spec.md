# Specification: Benchmark on First 100 Spider2-Snow Queries

## 1. Overview

This document specifies all activities required to extend the SnowRAG-Agent benchmark from 25 queries to the first 100 queries of Spider2-Snow. The current system supports 4 databases (GA360, GA4, PATENTS, PATENTS_GOOGLE) with 92% accuracy on 25 queries. Expanding to 100 queries introduces 16 additional databases with diverse schemas, geospatial data types, and heavily partitioned tables.

---

## 2. Database Inventory

### 2.1 Currently indexed (4 databases, 25 queries)

| Database | Queries | Tables in Snowflake | Indexed in ChromaDB | Profiled | Status |
|:---------|--------:|--------------------:|:------|:---------|:-------|
| GA360 | 12 | 366 (→1 collapsed) | Yes | Yes (GPT-5.4) | Ready |
| GA4 | 1 | 92 (→1 collapsed) | Yes | Yes (GPT-5.4) | Ready |
| PATENTS | 15 | 3 | Yes | Yes (GPT-5.4) | Ready |
| PATENTS_GOOGLE | 4 | 4 | Yes | Yes (GPT-5.4) | Ready |

### 2.2 New databases to index (16 databases, 75 queries)

| Database | Queries | Tables | Partitioned | Estimated Rows | Partition Action |
|:---------|--------:|-------:|------------:|---------------:|:-----------------|
| GITHUB_REPOS | 15 | 6 | 0 | 7.6M | None needed |
| NOAA_DATA | 12 | 218 | 186 | 742M | Collapse GSOD yearly tables |
| CMS_DATA | 7 | 52 | 0 | 2.2B | None needed |
| GITHUB_REPOS_DATE | 6 | 5,173 | 5,167 | 528M | Collapse daily tables (5173→~6) |
| GEO_OPENSTREETMAP | 6 | 10 | 0 | 217M | None needed |
| CENSUS_BUREAU_ACS_2 | 4 | 296 | 0 | 4.3M | None — year-based table names are actual different datasets |
| PATENTSVIEW | 3 | 59 | 0 | 126M | None needed |
| NEW_YORK_CITIBIKE_1 | 3 | 117 | 96 | 233M | Collapse partitioned tables |
| NEW_YORK_NOAA | 3 | 119 | 113 | 1.8B | Collapse partitioned tables |
| PATENTS_USPTO | 2 | 46 | 0 | 670M | None needed |
| NOAA_DATA_PLUS | 2 | 234 | 186 | 742M | Collapse GSOD yearly tables |
| PYPI | 1 | 2 | 0 | 86.9M | None needed |
| NOAA_GSOD | 1 | 97 | 96 | 174M | Collapse yearly tables (97→1) |
| NOAA_GLOBAL_FORECAST_SYSTEM | 1 | 11 | 0 | 218M | None needed |
| NEW_YORK_GEO | 1 | 38 | 17 | 1.6B | Collapse partitioned tables |
| GEO_OPENSTREETMAP_BOUNDARIES | 1 | 26 | 0 | 217M | None needed |

**Totals:** 16 new databases, 6,524 Snowflake tables (→~540 after partition collapse), ~7.8 billion rows.

---

## 3. Schema Indexing Activities

For each of the 16 new databases, execute:

### 3.1 Build ChromaDB index

```bash
uv run python -m rag_snow_agent.chroma.build_index \
  --db_id <DB_ID> --credentials ./snowflake_credentials.json --chroma_dir .chroma/
```

The `build_index` pipeline:
1. Connects to Snowflake, extracts all tables and columns from `INFORMATION_SCHEMA`
2. Extracts join edges from FK constraints + heuristic name matching
3. Discovers VARIANT sub-fields via `OBJECT_KEYS()` and `LATERAL FLATTEN + OBJECT_KEYS()` fallback
4. Collapses partition tables (date-suffix dedup) into one representative per group
5. Creates TableCard, ColumnCard, and JoinCard entries in ChromaDB `schema_cards`

**Databases requiring partition collapse:**

| Database | Pattern | Tables | Expected After Collapse |
|:---------|:--------|-------:|------------------------:|
| NOAA_DATA | GSOD_YYYY | 186 | ~1 per schema |
| GITHUB_REPOS_DATE | _YYYYMMDD | 5,167 | ~6 |
| NEW_YORK_CITIBIKE_1 | various date suffixes | 96 | ~10 |
| NEW_YORK_NOAA | TLC_FHV/YELLOW_YYYY | 113 | ~10 |
| NOAA_DATA_PLUS | GSOD_YYYY | 186 | ~1 per schema |
| NOAA_GSOD | GSOD_YYYY | 96 | 1 |
| NEW_YORK_GEO | various | 17 | ~5 |

### 3.2 Clean up stale entries

After `build_index` runs, clean up old partition table entries that were replaced by collapsed representatives (same process as was done for GA360/GA4).

### 3.3 Estimated indexing output

| Metric | Existing (4 DBs) | New (16 DBs) | Total |
|:-------|------------------:|-------------:|------:|
| Table cards | 9 | ~540 | ~549 |
| Column cards | 204 | ~3,000 | ~3,204 |
| Join cards | 9 | ~200 | ~209 |
| VARIANT sub-fields | 130 | ~500 | ~630 |

---

## 4. Data Profiling Activities

For each new database, run the data profiling pipeline:

```bash
uv run python scripts/profile_data.py \
  --credentials ./snowflake_credentials.json \
  --db_ids GITHUB_REPOS NOAA_DATA CMS_DATA GITHUB_REPOS_DATE GEO_OPENSTREETMAP \
           CENSUS_BUREAU_ACS_2 PATENTSVIEW NEW_YORK_CITIBIKE_1 NEW_YORK_NOAA \
           PATENTS_USPTO NOAA_DATA_PLUS PYPI NOAA_GSOD NOAA_GLOBAL_FORECAST_SYSTEM \
           NEW_YORK_GEO GEO_OPENSTREETMAP_BOUNDARIES \
  --output data/table_column_descriptions.json \
  --model gpt-5.4 \
  --merge \
  --enrich --chroma_dir .chroma/
```

The `--merge` flag preserves existing GA360/GA4/PATENTS/PATENTS_GOOGLE descriptions while adding new ones.

### Profiling pipeline per database:
1. Connect to Snowflake
2. Extract 100 sample rows per table (one partition per group for partitioned tables)
3. For each column: compute null rate, unique count, value range, sample values, VARIANT structure
4. Send column profiles + sample rows to GPT-5.4 for natural language description generation
5. Save to `table_column_descriptions.json`
6. Enrich ChromaDB `schema_cards` with generated descriptions

> **Important: Sampling strategy for large tables.** Several databases contain tables with hundreds of millions or billions of rows (e.g., NEW_YORK_NOAA ~1.8B, CMS_DATA ~2.2B, GEO_OPENSTREETMAP HISTORY_NODES ~110M). Using `SELECT * ... LIMIT 100` on such tables triggers a full sequential scan in Snowflake, which is slow and expensive. Instead, use Snowflake's built-in `SAMPLE` clause:
>
> ```sql
> SELECT * FROM table SAMPLE (100 ROWS)
> ```
>
> or, for more deterministic results:
>
> ```sql
> SELECT * FROM table TABLESAMPLE BERNOULLI (0.001) LIMIT 100
> ```
>
> The `profile_data.py` script must be updated to use `SAMPLE` instead of `LIMIT` for tables exceeding a configurable row threshold (e.g., 1M rows). This avoids full table scans and reduces Snowflake compute credits consumed during profiling.

### Estimated profiling cost

| Database | Tables to Profile | Estimated LLM Calls | Estimated Tokens |
|:---------|------------------:|--------------------:|-----------------:|
| GITHUB_REPOS | 6 | 6 | ~15K |
| NOAA_DATA | ~32 (after dedup) | 32 | ~80K |
| CMS_DATA | 52 | 52 | ~130K |
| GITHUB_REPOS_DATE | ~6 | 6 | ~15K |
| GEO_OPENSTREETMAP | 10 | 10 | ~25K |
| CENSUS_BUREAU_ACS_2 | ~296 | 296 | ~740K |
| PATENTSVIEW | 59 | 59 | ~150K |
| NEW_YORK_CITIBIKE_1 | ~21 | 21 | ~55K |
| NEW_YORK_NOAA | ~6 | 6 | ~15K |
| Others (7 DBs) | ~60 | 60 | ~150K |
| **Total** | **~548** | **~548** | **~1.4M** |

Estimated profiling cost: ~$20 (GPT-5.4 at ~$15/M tokens)

---

## 5. External Knowledge Documents

15 external knowledge files referenced by the 100 queries. All files exist at `ReFoRCE/spider2-snow/resource/documents/`:

| File | Used By | Content Type |
|:-----|:--------|:-------------|
| `patents_info.md` | PATENTS, PATENTS_GOOGLE | IPC code handling, text embeddings, originality |
| `sliding_windows_calculation_cpc.md` | PATENTS | Exponential moving average calculation |
| `google_analytics_sample.ga_sessions.md` | GA360 | Full GA360 schema documentation |
| `ga360_hits.eCommerceAction.action_type.md` | GA360 | eCommerce action type codes |
| `ga4_obfuscated_sample_ecommerce.events.md` | GA4 | GA4 events schema |
| `lang_and_ext.md` | GITHUB_REPOS | Programming language to file extension mapping |
| `functions_st_distance.md` | NOAA_DATA | Snowflake ST_DISTANCE geospatial function |
| `functions_st_dwithin.md` | GEO_OPENSTREETMAP, NEW_YORK_NOAA | ST_DWITHIN geospatial function |
| `functions_st_within.md` | NEW_YORK_CITIBIKE_1, NOAA_DATA_PLUS, NOAA_GLOBAL_FORECAST_SYSTEM | ST_WITHIN function |
| `functions_st_intersects.md` | GEO_OPENSTREETMAP_BOUNDARIES | ST_INTERSECTS function |
| `functions_st_intersects_polygon_line.md` | GEO_OPENSTREETMAP | ST_INTERSECTS for polygon/line |
| `functions_st_contains.md` | NEW_YORK_GEO | ST_CONTAINS function |
| `forward_backward_citation.md` | PATENTSVIEW | Forward/backward citation counting |
| `avg_vulnerable_weights.md` | CENSUS_BUREAU_ACS_2 | Average vulnerable population weights |
| `total_vulnerable_weights.md` | CENSUS_BUREAU_ACS_2 | Total vulnerable population weights |

**Action required:** Index external knowledge documents into a dedicated ChromaDB collection or inject into prompts based on `external_knowledge` field per instance.

---

## 6. Special Considerations

### 6.1 Geospatial queries (21 queries)
Databases: NOAA_DATA, GEO_OPENSTREETMAP, NEW_YORK_CITIBIKE_1, NEW_YORK_NOAA, NOAA_DATA_PLUS, NOAA_GLOBAL_FORECAST_SYSTEM, NEW_YORK_GEO, GEO_OPENSTREETMAP_BOUNDARIES

These queries use Snowflake geospatial functions: `ST_DISTANCE`, `ST_DWITHIN`, `ST_WITHIN`, `ST_CONTAINS`, `ST_INTERSECTS`. The system must:
- Include geospatial function syntax in the Snowflake guidance prompt
- Handle GEOGRAPHY/GEOMETRY data types in schema descriptions
- Add geospatial function reference chunks to `snowflake_syntax` collection

### 6.2 Massive partition collapse (GITHUB_REPOS_DATE: 5,173 tables)
This database has 5,167 daily-partitioned tables. The `build_index` partition collapse must handle this efficiently. Expect ~6 representative tables after collapse.

### 6.3 Large schema databases (CENSUS_BUREAU_ACS_2: 296 tables, CMS_DATA: 52 tables, PATENTSVIEW: 59 tables)
These have many tables with similar names. Retrieval accuracy depends on column descriptions to disambiguate. Profiling all tables is critical.

### 6.4 Shared schemas across databases
Several databases share tables from the same Snowflake schemas (e.g., `GEO_US_BOUNDARIES`, `GEO_OPENSTREETMAP`, `NEW_YORK`). The indexer must handle this correctly — each `db_id` gets its own set of cards.

---

## 7. SQLite Mirror Update

After indexing and profiling, rebuild the SQLite mirror with the expanded schema:

```bash
uv run python scripts/build_sqlite_mirror.py
```

This requires re-extracting sample records for the 16 new databases:

```bash
uv run python scripts/extract_sample_records.py \
  --credentials ./snowflake_credentials.json \
  --db_ids GITHUB_REPOS NOAA_DATA CMS_DATA ... \
  --output data/sample_records.json
```

---

## 8. Execution Plan

### Phase 1: Schema Indexing (~30 min)
Run `build_index.py` for all 16 new databases. Clean up partition duplicates.

### Phase 2: Data Profiling (~60 min, ~$20)
Run `profile_data.py` with `--merge` for all 16 databases using GPT-5.4.

### Phase 3: External Knowledge Indexing (~10 min)
Index the 15 external knowledge markdown files into ChromaDB.

### Phase 4: Sample Records + SQLite Mirror (~15 min)
Extract samples and rebuild mirror.db.


---

## 9. Risk Assessment

| Risk | Impact | Mitigation |
|:-----|:-------|:-----------|
| Snowflake timeout on large tables | Profiling fails | Set statement_timeout |
| GITHUB_REPOS_DATE collapse (5173 tables) | Slow indexing | Already handled by `_collapse_partition_tables` |
| Geospatial function support | Wrong SQL | Add geospatial syntax to Snowflake guidance |
| CENSUS_BUREAU_ACS_2 (296 tables) | Retrieval noise | Strong descriptions critical for disambiguation |
| External knowledge not indexed | Missing context for 15+ queries | Implement external knowledge retrieval before benchmark |
