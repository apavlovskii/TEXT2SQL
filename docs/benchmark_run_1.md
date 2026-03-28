# Benchmark Run 1

## What We Built

We built an agentic Text-to-SQL system targeting the Spider2-Snow benchmark.

- Retrieves a compact, relevant subset of database schema (SchemaSlice) using a vector database, instead of sending full schemas to the LLM.
- Generates SQL via a structured plan-first approach to improve correctness and reduce hallucinations.
- Executes queries against Snowflake and automatically classifies/repairs errors through a bounded refinement loop.
- Generates multiple candidate SQL queries (Best-of-N) to increase the chance of finding a correct solution.
- Applies semantic verification using result fingerprints, expected output shape, and consistency checks.
- Stores successful query traces and reuses them through a memory retrieval mechanism.
- Uses a join graph to improve schema connectivity and multi-table query reasoning.
- Uses a learned verifier to improve candidate selection from past execution signals.

Overall goal: achieve high accuracy on Spider2 while significantly reducing token usage and improving efficiency compared to existing approaches like ReFoRCE.

## What Needs To Be Done Now

1. Perform all necessary setup and execute a benchmark using our solution on the first 25 candidate tests from Spider2-Snow.
2. Use the GPT-4o model.
3. Produce a token usage summary and final accuracy calculation.
	- Accuracy definition: number of successfully completed tests where result matched gold results, divided by number of test cases executed (25 in this run).
4. Produce a detailed review of typical errors and issues during query generation.
5. Produce a comparison report of our solution vs ReFoRCE, DSR, and Spider2 test results.
6. Use these references for ReFoRCE and DSR test descriptions:
	- README_DSRLITE.md
	- README_REFORCE.md
	- README_SPIDER2.md

## Deliverables Checklist

- [ ] Benchmark run completed on first 25 Spider2-Snow test cases
- [ ] Model used: GPT-4o
- [ ] Token usage summary produced
- [ ] Final accuracy computed (matched gold / 25)
- [ ] Detailed error and issue review produced
- [ ] Comparison report produced (Our solution vs ReFoRCE vs DSR vs Spider2)
