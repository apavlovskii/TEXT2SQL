# Benchmark Run 11

## What We Built

Same core agent as Run 10, with geospatial query support added to address the primary failure mode from Run 10 (7/13 failures were geospatial).

### Architecture updates since Benchmark Run 10

1. **PlanGeoJoin model** (`plan_schema.py`) — New plan element for spatial JOINs with geospatial predicates (ST_WITHIN, ST_CONTAINS, ST_INTERSECTS) in the ON clause. Unlike PlanJoin (equality only), PlanGeoJoin accepts an arbitrary `on_expression` that is emitted verbatim by the compiler with table alias substitution.

2. **PlanGeoFilter model** (`plan_schema.py`) — New plan element for geospatial WHERE predicates (ST_DWITHIN, ST_DISTANCE, ST_CONTAINS). The `expression` field is a complete SQL boolean predicate emitted verbatim in the WHERE clause.

3. **Compiler geospatial support** (`sql_compiler.py`) — `_compile_geo_joins()` generates spatial JOIN clauses, `_substitute_aliases()` replaces full table names with t1/t2/t3 aliases in raw expressions. Both CTE and single-block paths pass geo_joins and geo_filters through compilation.

4. **Geospatial prompting guidance** (`prompt_builder.py`) — Added to `_SNOWFLAKE_GUIDANCE`: ST_POINT, ST_MAKEPOINT, TO_GEOGRAPHY, ST_WITHIN, ST_CONTAINS, ST_DWITHIN, ST_DISTANCE, ST_INTERSECTS syntax with examples. Distance unit guidance (meters, miles conversion). Added geo_joins and geo_filters to plan JSON schema with usage documentation.

5. **"geo_first" candidate strategy** (`prompt_builder.py`, `candidate_generator.py`) — New strategy in Best-of-N rotation that prioritizes identifying geospatial relationships, guides use of geo_joins/geo_filters, and instructs on coordinate ordering (lon first) and unit conversion.

6. **Geospatial syntax reference** (`snowflake_syntax.py`) — New GEOSPATIAL_FUNCTIONS topic with point construction, spatial predicates, distance measurement, common patterns (spatial JOIN, distance filter, point-in-polygon), and key rules.

7. **Per-instance external knowledge injection** (`experiment_runner.py`) — Reads the `external_knowledge` field from each spider2-snow.jsonl instance, loads the referenced markdown file, and injects its full content into `semantic_context` before prompting. This ensures geospatial function docs (functions_st_within.md, etc.) are directly available to the LLM for relevant instances.

## Benchmark Parameters

- Run first 100 candidate tests from Spider2-Snow
- Use self refinement loop with up to 4 iterations
- Generate 8 candidate queries on each iteration
- Use GPT-5.4-mini model
- ChromaDB with GPT-5.4 profiled descriptions for all 20 databases
- Gold verification against execution output
- All Run 10 features active + geospatial support + external knowledge injection

## Expected Impact

Run 10 had 13 failures:
- 7 geospatial (sf_bq050, sf_bq426, sf_bq291, sf_bq208, sf_bq048, sf_bq348, sf_bq254) — targeted by this update
- 3 complex multi-step logic (sf_bq010, sf_bq270, sf_bq222) — model capability limit
- 2 complex schema / 296 tables (sf_bq073, sf_bq410) — retrieval noise
- 1 API error (sf_bq127) — transient

Optimistic target: 90-93% (fix 3-6 geospatial failures)
Conservative target: 88-89% (fix 1-2 geospatial + no regressions)

## Execution Command

```bash
cd rag_snow_agent
uv run python -m rag_snow_agent.eval.experiment_runner \
  --split_jsonl ../Spider2/spider2-snow/spider2-snow.jsonl \
  --credentials ./snowflake_credentials.json \
  --experiment benchmark_run_11 \
  --limit 100 \
  --model gpt-5.4-mini \
  --best_of_n 8 \
  --max_repairs 4 \
  --gold_dir ../Spider2/spider2-snow/evaluation_suite/gold/ \
  --chroma_dir .chroma/
```

## Pre-run Checklist

- [ ] Verify geospatial changes in plan_schema.py, sql_compiler.py, prompt_builder.py
- [ ] Verify "geo_first" in candidate_generator.py STRATEGIES list
- [ ] Verify GEOSPATIAL_FUNCTIONS in snowflake_syntax.py
- [ ] Verify external knowledge injection in experiment_runner.py
- [ ] Re-index Snowflake syntax: `python -m rag_snow_agent.chroma.ingest_syntax`
- [ ] Benchmark run completed on first 100 Spider2-Snow test cases
- [ ] Token usage summary produced
- [ ] Final accuracy computed
- [ ] Detailed error and issue review produced
- [ ] Comparison report produced (vs Run 10)
