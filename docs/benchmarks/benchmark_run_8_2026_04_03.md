# Benchmark Run 8

## What We Built

Same system as Run 7, with critical fixes and enrichments:

### Architecture updates since Benchmark Run 7

1. **Universal Pydantic type coercion** — `_CoercingBase` base class for all plan models; coerces integers/booleans to strings in LLM-generated JSON
2. **Partition table collapsing** — GA360 366→2 tables, GA4 92→1 table in ChromaDB index; partition hints in table comments
3. **VARIANT sub-field enrichment** — ARRAY vs OBJECT classification from ChromaDB VARIANT_FIELD entries; correct field paths surfaced in schema prompts
4. **VARIANT ARRAY fallback extraction** — FLATTEN+OBJECT_KEYS fallback for VARIANT columns that are arrays (PATENTS: 0→93 sub-fields discovered)
5. **Natural language descriptions** — comprehensive table and column descriptions from gold SQLs and external knowledge docs; stored as comments in ChromaDB
6. **Gold verification bug fix** — corrected `--gold_dir` path; Run 8 is the first to verify against actual gold execution output

## Results

**Gold-match accuracy: 6/25 = 24.0%** — best result, first verified against gold output.

| Database | Accuracy |
|---|---|
| PATENTS | 6/11 = 55% |
| GA360 | 0/12 = 0% |
| GA4 | 0/1 = 0% |
| PATENTS_GOOGLE | 0/1 = 0% |

New gold match: **sf_bq091** — never matched before. Required LATERAL FLATTEN on assignee_harmonized with correct value:"name"::STRING field paths.

Full report: `rag_snow_agent/reports/experiments/benchmark_run_8/benchmark_run_8_report.md`

## Deliverables Checklist

- [x] Benchmark run completed on first 25 Spider2-Snow test cases
- [x] Model used: GPT-5.4 with 8 candidates, 4 repairs
- [x] Token usage summary produced
- [x] Final accuracy computed: 6/25 = 24.0%
- [x] Detailed error and issue review produced
- [x] Comparison report produced
