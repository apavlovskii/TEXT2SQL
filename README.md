# TEXT2SQL
Text 2 SQL Conversion

## Project Summary

This workspace is used to evaluate and compare three different text-to-SQL style benchmark pipelines:

- `DSR-SQL / DSR_Lite`
- `ReFoRCE`
- `Spider2 / spider-agent-snow`

The goal is not only to run each benchmark successfully, but also to understand how different agentic SQL-generation approaches behave on realistic database tasks across Snowflake, BigQuery, and local database settings.

By running all three projects in the same workspace, this repository is being used to:

1. measure end-to-end accuracy and execution success,
2. track token usage and cost efficiency,
3. analyze recurring SQL failure patterns,
4. compare recovery/repair behavior across approaches, and
5. identify practical improvement areas for more reliable text-to-SQL systems.

In short, this project is trying to determine which benchmarked approach is most effective, which failure modes are shared across systems, and what engineering changes are most likely to improve robustness and correctness.

## Project-Specific Guides

Detailed setup, credential, execution, output, and evaluation instructions are documented in the project-specific README files:

- `README_DSRLITE.md` — detailed instructions for setting up and running `DSR-SQL / DSR_Lite`
- `README_REFORCE.md` — detailed instructions for setting up and running `ReFoRCE`
- `README_SPIDER2.md` — detailed instructions for setting up and running `Spider2 / spider-agent-snow`
- [`rag_snow_agent/README.md`](rag_snow_agent/README.md) — **Analytics Insite**: our custom RAG-based Text-to-SQL agent with web UI, including full installation instructions (local dev, Docker Compose, Docker standalone), API reference, and benchmark execution guide

Use those files when you want the exact commands, environment setup steps, output locations, and run-specific notes for each benchmark.


For experiment setup and execution as well as current result review for DSR-SQL, ReFoRCE and SPIDER2 review README_DSRLITE.md, README_REFORCE.md and README_SPIDER2.md.

Cross-project comparison report (DSR vs ReFoRCE vs Spider+GPT-4o): `Cross_Project_Comparison_DSR_ReFoRCE_Spider4o.md`

See docs/SPEC_SPIDER2_SNOW_RAG.md for the implementation spec.