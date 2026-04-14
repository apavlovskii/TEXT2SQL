"""Snowflake SQL syntax reference collection for ChromaDB.

Stores chunked SQL syntax documentation for retrieval during query generation.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import chromadb

from .chroma_store import ChromaStore

log = logging.getLogger(__name__)

SYNTAX_COLLECTION = "snowflake_syntax"


@dataclass
class SyntaxChunk:
    """One chunk of Snowflake SQL syntax documentation."""

    chunk_id: str
    topic: str  # e.g. "JOIN", "LATERAL", "GROUP BY"
    section: str  # e.g. "syntax", "examples", "usage_notes"
    content: str
    token_estimate: int = 0

    def chroma_id(self) -> str:
        return f"syntax:{self.chunk_id}"

    def chroma_metadata(self) -> dict:
        return {
            "object_type": "syntax",
            "topic": self.topic,
            "section": self.section,
            "token_estimate": self.token_estimate,
        }


def _estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token."""
    return len(text) // 4


def _make_chunk_id(topic: str, section: str, idx: int = 0) -> str:
    raw = f"{topic}:{section}:{idx}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def chunk_markdown_by_sections(
    text: str,
    topic: str,
    max_chunk_tokens: int = 600,
) -> list[SyntaxChunk]:
    """Split markdown text into chunks by ## headers, respecting token budget.

    Each chunk gets a topic (e.g. "JOIN") and a section name derived from
    the heading.  If a section exceeds max_chunk_tokens, it is split further
    at paragraph boundaries.
    """
    # Split on ## headings (keep the heading with its content)
    sections: list[tuple[str, str]] = []
    current_heading = "overview"
    current_lines: list[str] = []

    for line in text.split("\n"):
        if line.startswith("## "):
            if current_lines:
                sections.append((current_heading, "\n".join(current_lines).strip()))
            current_heading = re.sub(r"[^a-z0-9_]+", "_", line[3:].strip().lower()).strip("_")
            current_lines = [line]
        elif line.startswith("# ") and not sections:
            # Top-level title — use as overview heading
            current_heading = "overview"
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_heading, "\n".join(current_lines).strip()))

    # Build chunks, splitting large sections at paragraph boundaries
    chunks: list[SyntaxChunk] = []
    for section_name, section_text in sections:
        if not section_text.strip():
            continue

        tokens = _estimate_tokens(section_text)
        if tokens <= max_chunk_tokens:
            chunks.append(SyntaxChunk(
                chunk_id=_make_chunk_id(topic, section_name, len(chunks)),
                topic=topic,
                section=section_name,
                content=section_text,
                token_estimate=tokens,
            ))
        else:
            # Split at double-newlines (paragraphs) or code blocks
            paragraphs = re.split(r"\n\n+", section_text)
            buffer: list[str] = []
            buffer_tokens = 0
            sub_idx = 0

            for para in paragraphs:
                para_tokens = _estimate_tokens(para)
                if buffer_tokens + para_tokens > max_chunk_tokens and buffer:
                    chunks.append(SyntaxChunk(
                        chunk_id=_make_chunk_id(topic, section_name, sub_idx),
                        topic=topic,
                        section=f"{section_name}_part{sub_idx + 1}",
                        content="\n\n".join(buffer),
                        token_estimate=buffer_tokens,
                    ))
                    sub_idx += 1
                    buffer = []
                    buffer_tokens = 0
                buffer.append(para)
                buffer_tokens += para_tokens

            if buffer:
                chunks.append(SyntaxChunk(
                    chunk_id=_make_chunk_id(topic, section_name, sub_idx),
                    topic=topic,
                    section=f"{section_name}_part{sub_idx + 1}" if sub_idx > 0 else section_name,
                    content="\n\n".join(buffer),
                    token_estimate=buffer_tokens,
                ))

    return chunks


class SnowflakeSyntaxStore:
    """Manages the snowflake_syntax ChromaDB collection."""

    def __init__(self, chroma_store: ChromaStore) -> None:
        self.client = chroma_store.client
        self._embedding_fn = chroma_store._embedding_fn

    def collection(self) -> chromadb.Collection:
        kwargs: dict = {
            "name": SYNTAX_COLLECTION,
            "metadata": {"hnsw:space": "cosine"},
        }
        if self._embedding_fn is not None:
            kwargs["embedding_function"] = self._embedding_fn
        return self.client.get_or_create_collection(**kwargs)

    def upsert_chunks(self, chunks: list[SyntaxChunk]) -> int:
        if not chunks:
            return 0
        col = self.collection()
        batch = 100
        for i in range(0, len(chunks), batch):
            chunk_batch = chunks[i : i + batch]
            col.upsert(
                ids=[c.chroma_id() for c in chunk_batch],
                documents=[c.content for c in chunk_batch],
                metadatas=[c.chroma_metadata() for c in chunk_batch],
            )
        log.info("Upserted %d syntax chunks", len(chunks))
        return len(chunks)

    def query(self, query_text: str, top_k: int = 3) -> list[dict]:
        """Retrieve most relevant syntax chunks for a query."""
        col = self.collection()
        results = col.query(
            query_texts=[query_text],
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )
        items = []
        ids = results.get("ids", [[]])[0]
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]
        for i, cid in enumerate(ids):
            items.append({
                "chunk_id": cid,
                "topic": metas[i].get("topic", "") if i < len(metas) else "",
                "section": metas[i].get("section", "") if i < len(metas) else "",
                "content": docs[i] if i < len(docs) else "",
                "distance": dists[i] if i < len(dists) else 1.0,
            })
        return items

    def count(self) -> int:
        return self.collection().count()


# ── Built-in syntax reference content ────────────────────────────────────────

SNOWFLAKE_SYNTAX_DOCS: dict[str, str] = {}


def _register(topic: str, content: str) -> None:
    SNOWFLAKE_SYNTAX_DOCS[topic] = content


_register("JOIN", """\
# Snowflake JOIN Syntax

## Syntax Forms

### INNER JOIN
```sql
SELECT ... FROM table1 INNER JOIN table2 ON table1.col = table2.col
```

### LEFT/RIGHT/FULL OUTER JOIN
```sql
SELECT ... FROM table1 LEFT OUTER JOIN table2 ON table1.col = table2.col
SELECT ... FROM table1 RIGHT OUTER JOIN table2 ON table1.col = table2.col
SELECT ... FROM table1 FULL OUTER JOIN table2 ON table1.col = table2.col
```

### CROSS JOIN
```sql
SELECT ... FROM table1 CROSS JOIN table2
-- Cannot use ON clause; use WHERE for filtering
```

### NATURAL JOIN
```sql
SELECT ... FROM table1 NATURAL INNER JOIN table2
-- Joins on all columns with matching names; cannot use ON
```

### USING clause
```sql
SELECT ... FROM table1 JOIN table2 USING (shared_column_name)
```

## Usage Notes
- Default join type is INNER if not specified
- CROSS JOIN prohibits ON clause
- NATURAL JOIN prohibits ON clause
- Omitting ON for non-CROSS joins creates Cartesian product
- Column names are case-insensitive when unquoted; use double-quotes to preserve case

## Example
```sql
SELECT t1.col1, t2.col1
FROM t1 INNER JOIN t2 ON t2.col1 = t1.col1
ORDER BY 1, 2;
```
""")

_register("LATERAL_FLATTEN", """\
# Snowflake LATERAL JOIN and FLATTEN

## LATERAL JOIN Syntax
```sql
SELECT ...
FROM left_table, LATERAL (inline_view)
```
Or explicitly:
```sql
FROM left_table INNER JOIN LATERAL (subquery)
```

The inline view can reference columns from left_table. Executes like a loop: for each row in left_table, evaluate the inline view.

## FLATTEN Syntax (for VARIANT/ARRAY/OBJECT)
```sql
SELECT col.value:field::TYPE
FROM table_name,
LATERAL FLATTEN(input => table_name."variant_column") col
```

## Chaining FLATTEN for Nested Data
```sql
SELECT id,
    f1.value:type::STRING AS contact_type,
    f1.value:content::STRING AS contact_details
FROM persons p,
  LATERAL FLATTEN(INPUT => p."contacts", PATH => 'contact') f,
  LATERAL FLATTEN(INPUT => f.value:business) f1;
```

## Key Rules
- Cannot use ON, USING, or NATURAL JOIN with lateral table functions
- VARIANT column names must be double-quoted if mixed-case: "trafficSource", "customDimensions"
- Access nested fields with colon syntax: "column":"field"::TYPE
- Use value:field for FLATTEN output access

## Common Pattern for VARIANT Arrays
```sql
-- Flatten an array column and extract fields
SELECT
    t.id,
    item.value:"name"::STRING AS item_name,
    item.value:"price"::NUMBER AS item_price
FROM my_table t,
LATERAL FLATTEN(input => t."items") item;
```
""")

_register("WITH_CTE", """\
# Snowflake WITH Clause (CTEs)

## Syntax
```sql
WITH
    cte_name1 AS (SELECT ...),
    cte_name2 AS (SELECT ...)
SELECT ... FROM cte_name1 JOIN cte_name2 ...
```

## Recursive CTE
```sql
WITH RECURSIVE cte_name (col1, col2) AS (
    -- Anchor
    SELECT initial_value, 0
    UNION ALL
    -- Recursive
    SELECT next_value, iteration + 1
    FROM cte_name
    WHERE iteration < max
)
SELECT * FROM cte_name;
```

## Rules
- A CTE can reference earlier CTEs but not later ones
- Column list is required for recursive CTEs
- UNION ALL (not UNION) required in recursive CTEs
- Recursive clause cannot use aggregates, GROUP BY, ORDER BY, LIMIT, DISTINCT
""")

_register("QUALIFY", """\
# Snowflake QUALIFY Clause

## Purpose
Filter results of window functions (like HAVING filters aggregates).

## Execution Order
FROM → WHERE → GROUP BY → HAVING → WINDOW → QUALIFY → DISTINCT → ORDER BY → LIMIT

## Syntax
```sql
SELECT col, ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY sort_col) AS rn
FROM table
QUALIFY rn = 1;
```

## Key Rules
- Requires at least one window function in SELECT or QUALIFY predicate
- Can reference column aliases
- Not part of ANSI SQL standard (Snowflake extension)

## Common Patterns
```sql
-- Top-1 per group
SELECT * FROM table
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY score DESC) = 1;

-- Using alias
SELECT *, RANK() OVER (ORDER BY amount DESC) AS rnk
FROM sales
QUALIFY rnk <= 10;
```
""")

_register("GROUP_BY", """\
# Snowflake GROUP BY

## Syntax
```sql
SELECT col, AGG_FUNC(measure) FROM table GROUP BY col
SELECT col, AGG_FUNC(measure) FROM table GROUP BY ALL
SELECT col, AGG_FUNC(measure) FROM table GROUP BY 1  -- by position
```

## Extensions
- GROUP BY GROUPING SETS: multiple GROUP BY in one query
- GROUP BY ROLLUP: hierarchical subtotals
- GROUP BY CUBE: all dimensional combinations

## Rules
- Every non-aggregate SELECT column must be in GROUP BY
- Column aliases can be used in GROUP BY
- GROUP BY ALL = all non-aggregate SELECT items
- When a name matches both a column and alias, the column takes precedence
""")

_register("WHERE_FILTER", """\
# Snowflake WHERE Clause

## Syntax
```sql
SELECT ... FROM ... WHERE predicate
```

## NULL Handling
- NULL = NULL returns NULL, not TRUE
- Use IS NULL / IS NOT NULL for NULL checks
- Rows with NULL predicate are filtered out

## Expression Limit
- Max 200,000 expressions in a list (e.g. IN clause)
- For larger lists, use JOIN with a lookup table

## Common Patterns
```sql
WHERE col = 'value'
WHERE col IN ('a', 'b', 'c')
WHERE col BETWEEN 10 AND 20
WHERE col LIKE '%pattern%'
WHERE col ILIKE '%case_insensitive%'  -- Snowflake-specific
WHERE col IS NOT NULL
```
""")

_register("ORDER_BY_LIMIT", """\
# Snowflake ORDER BY and LIMIT

## ORDER BY
```sql
SELECT ... ORDER BY col ASC NULLS LAST
SELECT ... ORDER BY col DESC NULLS FIRST
SELECT ... ORDER BY 1, 2  -- by position
SELECT ... ORDER BY ALL    -- all SELECT columns
```

## LIMIT / OFFSET
```sql
SELECT ... LIMIT count
SELECT ... LIMIT count OFFSET start
SELECT ... FETCH FIRST count ROWS ONLY  -- ANSI syntax
```

## Rules
- ORDER BY without LIMIT: results non-deterministic
- LIMIT NULL = unlimited
- TOP n and LIMIT n are equivalent
- Keep ORDER BY and LIMIT at same query level
""")

_register("SET_OPERATORS", """\
# Snowflake Set Operators

## UNION
```sql
SELECT ... UNION SELECT ...          -- removes duplicates
SELECT ... UNION ALL SELECT ...      -- keeps duplicates
SELECT ... UNION ALL BY NAME SELECT ... -- match by column name
```

## INTERSECT
```sql
SELECT ... INTERSECT SELECT ...      -- rows in both
```

## EXCEPT / MINUS
```sql
SELECT ... EXCEPT SELECT ...         -- rows in first but not second
SELECT ... MINUS SELECT ...          -- same as EXCEPT
```

## Rules
- Each query must select same number of columns (except BY NAME)
- INTERSECT has higher precedence than UNION/EXCEPT
- Output column names come from first query
""")

_register("SUBQUERY_OPERATORS", """\
# Snowflake Subquery Operators

## EXISTS / NOT EXISTS
```sql
WHERE EXISTS (SELECT 1 FROM other_table WHERE other_table.id = main_table.id)
WHERE NOT EXISTS (SELECT 1 FROM other_table WHERE ...)
```

## IN / NOT IN
```sql
WHERE col IN (SELECT col FROM other_table)
WHERE col NOT IN (SELECT col FROM other_table)
```

## ALL / ANY
```sql
WHERE col > ALL (SELECT col FROM other_table)
WHERE col = ANY (SELECT col FROM other_table)
```

## Rules
- Correlated subqueries only supported in WHERE
- Cannot use correlated subqueries with OR
- IN is shorthand for = ANY
- NOT IN is shorthand for != ALL
""")

_register("PIVOT_UNPIVOT", """\
# Snowflake PIVOT

## Syntax
```sql
SELECT * FROM table
PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
ORDER BY id;
```

## Dynamic Pivot
```sql
SELECT * FROM table
PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
ORDER BY id;
```

## Default for NULLs
```sql
PIVOT(SUM(amount) FOR quarter IN (ANY) DEFAULT ON NULL (0))
```

## Rules
- Supported aggregates: AVG, COUNT, MAX, MIN, SUM
- Dynamic PIVOT (ANY) not supported in stored procedures
- Only one aggregate per PIVOT; use UNION for multiple

## UNPIVOT (inverse)
```sql
SELECT * FROM wide_table
UNPIVOT(value FOR quarter IN (q1, q2, q3, q4));
```
""")

_register("GEOSPATIAL_FUNCTIONS", """\
# Snowflake Geospatial Functions

## Point Construction
```sql
-- From longitude, latitude columns (lon comes first!)
ST_POINT(longitude, latitude)
ST_MAKEPOINT(longitude, latitude)

-- From WKT string (POINT takes lon lat order)
TO_GEOGRAPHY('POINT(51.5 26.75)')
```

## Converting Stored Geometry
```sql
-- Convert a stored GEOGRAPHY/VARIANT column to GEOGRAPHY type
TO_GEOGRAPHY(column_name)
TO_GEOGRAPHY("zip_code_geom")
```

## Spatial Predicates (return BOOLEAN)
```sql
-- Point-in-polygon: is the point inside the polygon?
ST_WITHIN(point, polygon)
ST_WITHIN(ST_POINT(t."lon", t."lat"), TO_GEOGRAPHY(z."zip_code_geom"))

-- Polygon-contains-point (reverse of ST_WITHIN)
ST_CONTAINS(polygon, point)
ST_CONTAINS(TO_GEOGRAPHY(p."geometry"), TO_GEOGRAPHY(pt."geometry"))

-- Distance-within: are two geographies within N meters?
ST_DWITHIN(geo1, geo2, distance_in_meters)
ST_DWITHIN(ST_MAKEPOINT(t."lon", t."lat"), ST_MAKEPOINT(-73.764, 41.197), 32186.8)
-- 20 miles = 20 * 1609.34 = 32186.8 meters

-- Intersection: do two geographies share any space?
ST_INTERSECTS(geo1, geo2)
ST_INTERSECTS(TO_GEOGRAPHY(a."geometry"), TO_GEOGRAPHY(b."geometry"))
```

## Distance Measurement (returns FLOAT in meters)
```sql
-- Geodesic distance in meters between two GEOGRAPHY values
ST_DISTANCE(geo1, geo2)
ST_DISTANCE(TO_GEOGRAPHY("geography"), TO_GEOGRAPHY('POINT(51.5 26.75)')) <= 5000
```

## Common Patterns

### Spatial JOIN (point-in-polygon via zip codes)
```sql
SELECT t.*, z."zip_code"
FROM trips t
JOIN zip_codes z
  ON z."state_code" = 'NY'
 AND ST_WITHIN(
       ST_POINT(t."longitude", t."latitude"),
       TO_GEOGRAPHY(z."zip_code_geom")
     )
```

### Distance filter in WHERE
```sql
SELECT *
FROM weather_stations w
WHERE ST_DWITHIN(
    ST_MAKEPOINT(w."longitude", w."latitude"),
    ST_MAKEPOINT(-73.764, 41.197),
    32186.8  -- 20 miles in meters
)
```

### Finding features within a polygon
```sql
SELECT p.*
FROM points p
JOIN polygons g ON ST_CONTAINS(TO_GEOGRAPHY(g."geometry"), TO_GEOGRAPHY(p."geometry"))
```

## Key Rules
- GEOGRAPHY distances are always in meters
- ST_POINT and ST_MAKEPOINT take (longitude, latitude) — lon first!
- TO_GEOGRAPHY('POINT(lon lat)') uses WKT format — also lon first
- Convert miles to meters: multiply by 1609.34
- Convert km to meters: multiply by 1000
- ST_WITHIN(a, b) is equivalent to ST_CONTAINS(b, a)
""")

_register("SNOWFLAKE_IDENTIFIERS", """\
# Snowflake Identifier Quoting Rules

## Case Sensitivity
- Unquoted identifiers are stored and resolved as UPPERCASE
- Double-quoted identifiers preserve exact case
- Column "fullVisitorId" is different from FULLVISITORID

## When to Double-Quote
- Always quote identifiers that are mixed-case: "fullVisitorId", "trafficSource"
- Always quote identifiers that match SQL keywords: "date", "order", "group"
- Always quote identifiers with special characters

## VARIANT Field Access
- Use colon syntax for nested fields: "variant_col":"field_name"
- Cast with :: operator: "variant_col":"field"::STRING
- Access array elements: "array_col"[0]

## Examples
```sql
-- Mixed-case column (MUST quote)
SELECT "fullVisitorId" FROM table;

-- VARIANT nested access (MUST quote parent if mixed-case)
SELECT "trafficSource":"source"::STRING FROM table;

-- Lowercase columns (MUST quote since Snowflake uppercases unquoted)
SELECT "publication_number", "filing_date" FROM patents;
```

## Common Mistakes
- Using FULLVISITORID instead of "fullVisitorId" → invalid identifier
- Using trafficSource instead of "trafficSource" → invalid identifier
- Using PUBLICATION_NUMBER instead of "publication_number" → invalid identifier
""")


def build_all_syntax_chunks(max_chunk_tokens: int = 600) -> list[SyntaxChunk]:
    """Build all syntax chunks from the built-in reference content."""
    all_chunks: list[SyntaxChunk] = []
    for topic, content in SNOWFLAKE_SYNTAX_DOCS.items():
        chunks = chunk_markdown_by_sections(content, topic, max_chunk_tokens)
        all_chunks.extend(chunks)
    return all_chunks
