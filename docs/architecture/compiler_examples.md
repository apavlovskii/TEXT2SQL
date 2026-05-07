# Deterministic SQL Compiler — Input/Output Examples

> Source: `rag_snow_agent/src/rag_snow_agent/prompting/sql_compiler.py`
> Schema: `rag_snow_agent/src/rag_snow_agent/prompting/plan_schema.py`

The LLM generates a JSON `QueryPlan`. The deterministic compiler converts it to
Snowflake SQL with **no LLM involvement** in the SQL string construction.

---

## Example 1: Two-Table Join with Aggregation

**Natural language question:**
> "Top 10 customers by total completed order amount"

### Input (QueryPlan JSON — what the LLM produces)

```json
{
  "selected_tables": ["DB.PUBLIC.ORDERS", "DB.PUBLIC.CUSTOMERS"],
  "joins": [
    {
      "left_table": "DB.PUBLIC.ORDERS",
      "left_column": "CUSTOMER_ID",
      "right_table": "DB.PUBLIC.CUSTOMERS",
      "right_column": "CUSTOMER_ID",
      "join_type": "INNER"
    }
  ],
  "filters": [
    {
      "table": "DB.PUBLIC.ORDERS",
      "column": "STATUS",
      "op": "=",
      "value": "'COMPLETED'"
    }
  ],
  "group_by": ["DB.PUBLIC.CUSTOMERS.NAME"],
  "aggregations": [
    {
      "func": "SUM",
      "table": "DB.PUBLIC.ORDERS",
      "column": "AMOUNT",
      "alias": "total_amount"
    },
    {
      "func": "COUNT",
      "table": "DB.PUBLIC.ORDERS",
      "column": "ORDER_ID",
      "alias": "order_count"
    }
  ],
  "order_by": [{"expr": "total_amount", "direction": "DESC"}],
  "limit": 10
}
```

### Output (Snowflake SQL — deterministic, no LLM)

```sql
SELECT
  t2."NAME",
  SUM(t1."AMOUNT") AS total_amount,
  COUNT(t1."ORDER_ID") AS order_count
FROM DB.PUBLIC.ORDERS AS t1
INNER JOIN DB.PUBLIC.CUSTOMERS AS t2 ON t1."CUSTOMER_ID" = t2."CUSTOMER_ID"
WHERE t1."STATUS" = 'COMPLETED'
GROUP BY t2."NAME"
ORDER BY total_amount DESC
LIMIT 10
```

### What the compiler does

1. **Alias assignment**: ORDERS → `t1`, CUSTOMERS → `t2` (by position in `selected_tables`)
2. **FROM clause**: Primary table first, then JOINs with ON conditions
3. **Column quoting**: All columns double-quoted (`"NAME"`, `"AMOUNT"`) to preserve case
4. **SELECT construction**: GROUP BY columns first, then aggregations with aliases
5. **Clause assembly**: WHERE → GROUP BY → ORDER BY → LIMIT

---

## Example 2: Simple COUNT(*)

**Natural language question:**
> "How many items are in the catalog?"

### Input

```json
{
  "selected_tables": ["DB.PUBLIC.ITEMS"],
  "aggregations": [
    {
      "func": "COUNT",
      "table": "DB.PUBLIC.ITEMS",
      "column": "*",
      "alias": "cnt"
    }
  ]
}
```

### Output

```sql
SELECT
  COUNT(*) AS cnt
FROM DB.PUBLIC.ITEMS AS t1
```

### What the compiler does

- Detects `column: "*"` → emits `COUNT(*)` instead of `COUNT(t1."*")`
- Single table → only `t1` alias, no JOINs

---

## Example 3: COUNT DISTINCT

**Natural language question:**
> "How many unique users have generated events?"

### Input

```json
{
  "selected_tables": ["DB.PUBLIC.EVENTS"],
  "aggregations": [
    {
      "func": "COUNT_DISTINCT",
      "table": "DB.PUBLIC.EVENTS",
      "column": "USER_ID",
      "alias": "unique_users"
    }
  ]
}
```

### Output

```sql
SELECT
  COUNT(DISTINCT t1."USER_ID") AS unique_users
FROM DB.PUBLIC.EVENTS AS t1
```

### What the compiler does

- `COUNT_DISTINCT` is a plan-level pseudo-function → compiled to `COUNT(DISTINCT ...)`

---

## Example 4: LATERAL FLATTEN on VARIANT Array

**Natural language question:**
> "Top 10 patent assignees by number of patents"

### Input

```json
{
  "selected_tables": ["PATENTS.PUBLIC.PUBLICATIONS"],
  "flatten_ops": [
    {
      "table": "PATENTS.PUBLIC.PUBLICATIONS",
      "variant_column": "assignee_harmonized",
      "alias": "ah",
      "extract_fields": ["name", "country_code"]
    }
  ],
  "group_by": ["ah.name"],
  "aggregations": [
    {
      "func": "COUNT",
      "table": "PATENTS.PUBLIC.PUBLICATIONS",
      "column": "publication_number",
      "alias": "patent_count"
    }
  ],
  "order_by": [{"expr": "patent_count", "direction": "DESC"}],
  "limit": 10
}
```

### Output

```sql
SELECT
  ah.value:"name",
  COUNT(t1."publication_number") AS patent_count
FROM PATENTS.PUBLIC.PUBLICATIONS AS t1
, LATERAL FLATTEN(input => t1."assignee_harmonized") ah
GROUP BY ah.value:"name"
ORDER BY patent_count DESC
LIMIT 10
```

### What the compiler does

1. **FLATTEN clause**: `PlanFlatten` → `, LATERAL FLATTEN(input => t1."assignee_harmonized") ah`
2. **Flatten alias detection**: `ah` is recognized as a flatten alias, not a table
3. **Field access**: `ah.name` → `ah.value:"name"` (Snowflake VARIANT path syntax)
4. **Mixed references**: Regular columns use `t1."col"`, flatten fields use `ah.value:"field"`

---

## Example 5: Nested VARIANT Field Access

**Natural language question:**
> "Page paths from GA360 hit data"

### Input

```json
{
  "selected_tables": ["GA360.GOOGLE_ANALYTICS.GA_SESSIONS"],
  "flatten_ops": [
    {
      "table": "GA360.GOOGLE_ANALYTICS.GA_SESSIONS",
      "variant_column": "hits",
      "alias": "h",
      "extract_fields": ["page.pagePath", "eCommerceAction.action_type"]
    }
  ],
  "group_by": ["h.page.pagePath"],
  "aggregations": [
    {
      "func": "COUNT",
      "table": "GA360.GOOGLE_ANALYTICS.GA_SESSIONS",
      "column": "*",
      "alias": "hit_count"
    }
  ],
  "order_by": [{"expr": "hit_count", "direction": "DESC"}],
  "limit": 20
}
```

### Output

```sql
SELECT
  h.value:"page":"pagePath",
  COUNT(*) AS hit_count
FROM GA360.GOOGLE_ANALYTICS.GA_SESSIONS AS t1
, LATERAL FLATTEN(input => t1."hits") h
GROUP BY h.value:"page":"pagePath"
ORDER BY hit_count DESC
LIMIT 20
```

### What the compiler does

- **Nested path**: `page.pagePath` → `h.value:"page":"pagePath"` (each dot-segment becomes a colon-quoted path)
- This is the Snowflake syntax for traversing nested VARIANT objects inside a flattened array

---

## Example 6: CTE Pipeline (Multi-Step Query)

**Natural language question:**
> "Daily revenue and unique visitors for Q1 2025, ordered by revenue"

### Input

```json
{
  "selected_tables": ["DB.SCHEMA.GA_SESSIONS"],
  "ctes": [
    {
      "name": "base_sessions",
      "description": "Filter to Q1 2025",
      "selected_tables": ["DB.SCHEMA.GA_SESSIONS"],
      "filters": [
        {
          "table": "DB.SCHEMA.GA_SESSIONS",
          "column": "date",
          "op": "BETWEEN",
          "value": "'20250101' AND '20250331'"
        }
      ]
    },
    {
      "name": "daily_revenue",
      "description": "Aggregate revenue per day",
      "selected_tables": ["base_sessions"],
      "group_by": ["base_sessions.date"],
      "aggregations": [
        {
          "func": "SUM",
          "table": "base_sessions",
          "column": "totalTransactionRevenue",
          "alias": "revenue"
        },
        {
          "func": "COUNT_DISTINCT",
          "table": "base_sessions",
          "column": "fullVisitorId",
          "alias": "unique_visitors"
        }
      ]
    }
  ],
  "order_by": [{"expr": "revenue", "direction": "DESC"}],
  "limit": 30
}
```

### Output

```sql
WITH base_sessions AS (
SELECT
  t1.*
FROM DB.SCHEMA.GA_SESSIONS AS t1
WHERE t1."date" BETWEEN '20250101' AND '20250331'
),
daily_revenue AS (
SELECT
  base_sessions."date",
  SUM(base_sessions."totalTransactionRevenue") AS revenue,
  COUNT(DISTINCT base_sessions."fullVisitorId") AS unique_visitors
FROM base_sessions AS base_sessions
GROUP BY base_sessions."date"
)
SELECT *
FROM daily_revenue
ORDER BY revenue DESC
LIMIT 30
```

### What the compiler does

1. **CTE iteration**: Each `PlanCTE` is compiled independently by `_compile_single_block()`
2. **Upstream CTE references**: `base_sessions` in the second CTE's `selected_tables` is recognized as a CTE name (not a real table) and used directly as its own alias
3. **Final SELECT**: No top-level aggregations → emits `SELECT * FROM daily_revenue` with ORDER BY and LIMIT
4. **Assembly**: `WITH cte1 AS (...), cte2 AS (...) + final SELECT`

---

## Example 7: Three-Table Join with LEFT JOIN

**Natural language question:**
> "All orders with customer name and optional shipping address"

### Input

```json
{
  "selected_tables": [
    "DB.PUBLIC.ORDERS",
    "DB.PUBLIC.CUSTOMERS",
    "DB.PUBLIC.ADDRESSES"
  ],
  "joins": [
    {
      "left_table": "DB.PUBLIC.ORDERS",
      "left_column": "CUSTOMER_ID",
      "right_table": "DB.PUBLIC.CUSTOMERS",
      "right_column": "CUSTOMER_ID",
      "join_type": "INNER"
    },
    {
      "left_table": "DB.PUBLIC.ORDERS",
      "left_column": "SHIPPING_ADDRESS_ID",
      "right_table": "DB.PUBLIC.ADDRESSES",
      "right_column": "ADDRESS_ID",
      "join_type": "LEFT"
    }
  ],
  "filters": [
    {
      "table": "DB.PUBLIC.ORDERS",
      "column": "CREATED_AT",
      "op": ">=",
      "value": "'2025-01-01'"
    }
  ],
  "order_by": [{"expr": "CREATED_AT", "direction": "DESC"}],
  "limit": 100
}
```

### Output

```sql
SELECT
  t1.*
FROM DB.PUBLIC.ORDERS AS t1
INNER JOIN DB.PUBLIC.CUSTOMERS AS t2 ON t1."CUSTOMER_ID" = t2."CUSTOMER_ID"
LEFT JOIN DB.PUBLIC.ADDRESSES AS t3 ON t1."SHIPPING_ADDRESS_ID" = t3."ADDRESS_ID"
WHERE t1."CREATED_AT" >= '2025-01-01'
ORDER BY CREATED_AT DESC
LIMIT 100
```

### What the compiler does

- **Multiple join types**: INNER and LEFT coexist naturally
- **Third alias**: ADDRESSES → `t3`
- **No aggregations/group_by**: Falls back to `t1.*`

---

## Compiler Guarantees

| Property | How |
|----------|-----|
| **Deterministic** | Same `QueryPlan` always produces identical SQL |
| **Case-safe** | All column names double-quoted; original casing from SchemaSlice preserved |
| **Alias-stable** | `t1, t2, t3...` assigned by position — no randomness |
| **FLATTEN-aware** | VARIANT arrays get proper `LATERAL FLATTEN` + `alias.value:"field"` syntax |
| **CTE-composable** | Each CTE block compiled independently, upstream CTEs used as table refs |
| **Fallback-safe** | Empty `selected_tables` → `SELECT 1` (detected by caller for retry) |

## What the LLM Does vs. What the Compiler Does

| Responsibility | LLM | Compiler |
|---------------|-----|----------|
| Choose tables | Yes | No |
| Choose join columns | Yes | No |
| Choose filters and values | Yes | No |
| Choose aggregation functions | Yes | No |
| Write SQL syntax | **No** | **Yes** |
| Handle quoting/casing | **No** | **Yes** |
| Format FLATTEN/CTE | **No** | **Yes** |
| Assign aliases | **No** | **Yes** |
