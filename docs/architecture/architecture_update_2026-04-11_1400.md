Need to implement data profiling for our Snowflake tables and generate table and column descriptions. This information has to be added to our vector knowledge base and used during retrieval to improve quality of SQL generation.
- We already have sample rows extracted from the snowflake tables. Using that data generate table description that explains the purpose of the table and its data.
- for every table run a snowflake query to extract 100 rows and for every extracted column generate column description based on data seen in that column, description should explain data in that column and help LLM to generate better SQLs. For partitioned tables extract data only from one partition.
- Based on 100 rows identify columns with potentially low cardinality. For each of the identified columns run APPROX_COUNT_DISTINCT("<column name>") to verify the number of distinct values. If the number of distinct values is less than 25 extract those values and add them to the vector index.
While running approx_count_distinct queries mind the column case sensitivity in snowflake.
Some of the table columns have variant type with nested structures
For example, PATENTS.PATENTS.PUBLICATIONS has values in a structured format:
[
  {
    "language": "en",
    "text": "A new or improved alternating current control device",
    "truncated": false
  }
]

For such variant columns we need to calculate distinct values for each of the internal record fields and not for the whole record.