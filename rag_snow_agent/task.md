You are implementing the project described in docs/SPEC_SPIDER2_SNOW_RAG.md.

Rules:
1) Do not modify the vendored Spider2 code except for adding a new runner script if needed.
2) All new code goes under rag_snow_agent/.
3) Secrets must be read from rag_snow_agent/snowflake_credentials.json and/or environment variables. Never print secrets.
4) Prioritize correctness and reproducibility over speed.
5) Every major module must have a small unit test or smoke test.

First milestone:
- Implement Snowflake connectivity + schema extraction (INFORMATION_SCHEMA) for a given db_id
- Implement ChromaDB store + ingestion of TableCard/ColumnCard
- Provide a CLI: python -m rag_snow_agent.chroma.build_index --db_id <DB_ID> --credentials rag_snow_agent/snowflake_credentials.json
