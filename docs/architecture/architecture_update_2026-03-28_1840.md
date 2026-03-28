# Architecture Update (2026-03-28 18:40)

## Retrieval Flow Update

Instead of retrieving one `SchemaSlice` once, use this approach:

1. **Round 1:** Retrieve a very small initial slice.
2. Let the planner produce a draft plan.
3. Run a cheap expansion step that adds only missing tables/columns implied by unresolved joins, filters, or metrics.
4. Cap this to **one extra expansion round** to keep token usage low.

## Uncertainty Handling

Add **1–2 micro-probes** when the planner is uncertain, such as:

- checking top values for a candidate filter column,
- verifying whether a nested field exists,
- testing a join-key distribution.

## Relation-Aware Retrieval

Add relation-aware retrieval, not just table/column retrieval.

If the vector DB stores mostly table and column cards, it can retrieve the right nouns but still miss the right join path.

### Required Enhancements

- Store and retrieve `JoinCards` / relation cards as first-class objects.
- Rank relation cards jointly with schema cards.
- Require the planner to build from a connected relation graph.

### Data Source for Join Knowledge

To achieve this:

1. Scan Spider2 ground-truth SQLs in:
   - `Spider2/spider2-snow/evaluation_suite/gold/sql/`
2. Extract unique join conditions.
3. Store those join conditions in the vector database.
4. Use them during retrieval.
