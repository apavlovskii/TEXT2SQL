  Priority 1: Double-quote all column identifiers in SQL output
  - In sql_compiler.py, wrap every column reference in double-quotes: "fullVisitorId" not fullVisitorId
  - Also update repair prompts to instruct the LLM to use double-quoted identifiers                            

  Priority 2: Store original column casing in ChromaDB and SchemaSlice
  - Currently ColumnCards store the column name but the SchemaSlice/compiler doesn't preserve case
  - Store the exact case from DESCRIBE TABLE and thread it through to SQL generation
                                                                                                                                                            
  Priority 3: VARIANT column access syntax
  - When a column is data_type=VARIANT, the compiler should generate "col":"field" path syntax, not bare col
  - Add VARIANT-awareness to the plan schema: if a plan references a VARIANT column, require the sub-field path
                                                                                                                                                            
  Priority 4: Early termination on hopeless repairs
  - If the same identifier error repeats 3+ times across repair attempts, stop and try the next candidate instead of burning remaining repair budget 

  Priority 5: Teach LLM to use LATERAL FLATTEN
  - PATENTS queries on abstract_localized, inventor, assignee_harmonized need FLATTEN
  - Add FLATTEN examples to the Snowflake guidance in prompts

Priority 6:
Cache successful patterns: Use trace memory more aggressively to bootstrap from prior successes on the same database, especially column names.

Priority 7:
Snowflake query syntax reference uploaded to chromadb collection. Use it to verify the query syntax when there're doubts before submitting to snowflake.