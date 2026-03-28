# Architecture Update (2026-03-28 18:00)

## Problem Observed

From our benchmark run, we saw **106 occurrences** of this error:

- `Referenced column/field does not exist`

## Required Changes

1. Upload the **entire Snowflake schema** into the vector database.
2. Modify query-generation code so that each newly generated query is validated **before execution**.
3. During this pre-execution validation step, verify through the vector database that all referenced columns actually exist.
