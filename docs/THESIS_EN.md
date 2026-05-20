# Development of an AI Assistant for Big-Data Analytics Using Dynamic RAG and Advanced NLP Methods in ML4Code

**Master's Thesis · Faculty of Computer Science · HSE University · 2026**

Student: Alexander Pavlovskiy
Advisor: Elena Kantonistova
Co-advisor: Anton Shapkin

Program: **01.04.02 Applied Mathematics and Informatics**, educational program "Artificial Intelligence"

---

## Abstract

Before AI models were widely adopted in corporate analytics, working with large volumes of production data typically required involving IT specialists with SQL expertise. Domain experts frequently lacked such skills, which raised the cost and complexity of information processing and managerial decision-making. Against this background, the task of automatically translating natural-language requests into SQL commands has acquired high practical value for business, because it lowers the barrier to data access, accelerates the production of analytical conclusions, and increases the autonomy of business users. The present work is devoted to the development of an AI assistant for data analytics built on top of large language models (LLMs). Particular emphasis is placed on methods of LLM context engineering based on Retrieval-Augmented Generation (RAG). It is shown how dynamic hybrid vector retrieval can reduce the cost of SQL generation while preserving an acceptable level of result quality. The work is implemented as a software product deployed to a public hosting environment; it provides a convenient user interface that lets the user submit data-analytics queries against a remote cloud data warehouse.

## Annotation

Before active adoption of AI models for corporate analytics, working with large volumes of production data typically assumed the involvement of IT engineers with experience in SQL. At the same time, domain experts often lacked such skills, that led to increased costs and higher complexity in information processing, strategic planning and decision-making. Therefore, the task of automatic translation of natural language queries into SQL commands has gained high practical value for businesses, as it lowers the barrier to data access, accelerates generation of analytical insights, and increases the self-reliance of business users. This work is devoted to development of an AI assistant for data analytics leveraging LLMs. Emphasis is placed on context engineering using RAG approach. It is demonstrated how vector search methods with dynamic retrieval, fusion and reranking can reduce the cost of SQL generation while maintaining acceptable level of result quality. The solution is implemented as a software application deployed to public hosting. It has convenient user interface that allows users to submit analytical queries expressed in natural language to a remote cloud data warehouse.

---

# Introduction

For many years the extraction of quantitative indicators from corporate data warehouses remained accessible only to specialists who were fluent in SQL and familiar with the warehouse schema. This situation produced a characteristic organizational chain in which a business question travelled the route "department analyst → BI developer → data engineer" and yielded an answer only after days, and sometimes weeks. For the modern pace of managerial decision-making the length of this cycle became unacceptable, and this formed the demand for technologies capable of "understanding" the user's question in natural language and independently translating it into executable code.

The development of large language models (LLMs) provided the technical foundation for such automation: the models' ability to reason in context allowed them to transform a user's question directly into SQL statements. The Text-to-SQL discipline that arose at the intersection of NLP and databases is undergoing notable growth, which is confirmed by a series of benchmarks ranging from the classical Spider 1.0 to the industrially oriented BIRD and Spider 2.0. The paradox is that successes on early benchmarks (above 90 % accuracy) do not translate to realistic scenarios: on Spider 2.0 even the most powerful agents to date cross only the 30–35 % gold-match accuracy threshold.

The reason for such a large gap is a set of features absent from first-generation academic benchmarks but ubiquitous in production: hundreds of tables with many columns, heavy nested types (Snowflake `VARIANT`, BigQuery `STRUCT`/`ARRAY`), temporal sharding of data, and implicit semantic relations whose understanding requires familiarity with a specific domain. Passing the entire DDL of such a warehouse into the LLM context often runs up against the physical limit of the context window, and in cases where that limit is not exceeded the dilution of the model's attention across irrelevant elements noticeably degrades quality.

**The object of the study** is the set of approaches to automated SQL synthesis that rely on large language models and are applied to industrial data schemas.

**The subject of the study** is the architecture of dynamic Retrieval-Augmented Generation (RAG) in which only the fragment of metadata relevant to the current user query is admitted to the LLM context.

**The goal of the work** is to design and experimentally verify a software complex centred on hybrid vector search over schema metadata that simultaneously increases SQL generation accuracy and reduces its token cost on the Spider 2.0 — Snow benchmark.

**Research objectives:**

1. Carry out an analytical review of contemporary Text-to-SQL methods and identify the key limitations of existing agent-based solutions (DSR-Lite, ReFoRCE, Spider-Agent).
2. Design the structure of a vector knowledge base that reflects all essential entities of a database schema: tables, columns, relations, semantic facts, data samples, syntactic templates.
3. Develop a hybrid retrieval algorithm combining dense semantic search and lexical search (BM25) with aggregation via Reciprocal Rank Fusion (RRF).
4. Design a deterministic Plan → SQL compiler that separates LLM reasoning from the formatting of the final SQL code.
5. Implement a self-correction loop (repair loop) with error classification and targeted lookup of reference information.
6. Run comparative experiments against three open-source implementations (DSR-Lite, Spider-Agent, ReFoRCE) on identical subsets of the Spider 2.0 — Snow benchmark.
7. Perform an ablation study to estimate the contribution of each key component to the final accuracy.
8. Implement a user interface (web application) that demonstrates the applicability of the approach in a production setting.

**Scientific novelty.** The work proposes an architecture of **entity-oriented RAG for Text-to-SQL** in which every index document is a discrete metadata element — a table, a column, a relation, or a semantic fact — rather than a fixed-length text fragment. A mandatory step of preliminary **partition collapsing** is introduced when constructing the index, which removes noise from temporal shards in the search results. A **deterministic Plan → SQL compiler** is designed, tailored to Snowflake specifics (`LATERAL FLATTEN`, `VARIANT` access, CTE pipelines), removing from the LLM the responsibility of formatting the output code. These elements are integrated into a single agentic loop with self-correction.

**Practical significance.** The resulting software complex, deployed in a public cloud environment, achieves **87 %** gold-match accuracy on a 100-query subset of Spider 2.0 — Snow at a cost of **about \$0.38 per correct answer** (model `gpt-5.4-mini`). Compared to the reference ReFoRCE implementation on the same data and the same model, this yields a **4.2× gain in accuracy and an 18× reduction in cost per correct answer**. The economic indicators achieved make the system suitable for production use as an embeddable "analyst assistant" — a component of BI platforms and corporate dashboards.

**Statements presented for the defence:**

1. Entity-oriented RAG with specialized metadata collections simultaneously raises gold-match accuracy and reduces token cost compared with agentic solutions that put the entire DDL into the prompt.
2. Partition collapsing during index construction is a necessary condition for the applicability of RAG to domains with temporal sharding (GA360, GA4).
3. A deterministic Plan → SQL compiler eliminates a whole class of syntactic errors and makes the output SQL reproducible regardless of the variability of LLM candidates.
4. Hybrid retrieval (dense + lexical via RRF) outperforms each method individually in tasks where identifier-style names and natural-language descriptions complement one another.

**Validation and publications.** Intermediate results were recorded in the architecture change log (`docs/architecture/`); the final benchmark report is published in `docs/benchmarks/`. The source code has been released in an open repository under the MIT license, ensuring full reproducibility of the reported results.

**Structure of the work.** Chapter 1 contains an analytical review of contemporary Text-to-SQL methods and identifies the key limitations. Chapter 2 formulates the research problem. Chapter 3 describes the structure of the vector knowledge base (five specialized collections). Chapter 4 presents the hybrid retrieval algorithm. Chapter 5 presents the SQL-execution agent architecture: the Plan → SQL compiler, Best-of-N, the self-correction loop. Chapter 6 reports comparative benchmark results; Chapter 7 — the ablation study; Chapter 8 — the conclusion. The reference list contains 17 items; the appendix describes the software architecture and the user interface.

---

# 1. Domain Survey

## 1.1. General formulation of Text-to-SQL

The Text-to-SQL task consists of automatically generating a correct SQL query from a textual description in natural language (NL).

Formally, the task can be cast as the mapping:

$$
f:\left(Q_{NL},\,S,\,D\right)\rightarrow Q_{SQL}
$$

where $Q_{NL}$ is the user's question text, $S$ is the database schema, $D$ is additional metadata (descriptions of tables and columns with comments, sample values, documentation), and $Q_{SQL}$ is a correct and executable SQL query.

Historically the task was approached via semantic parsing, but with the emergence of LLMs there was a shift to generative approaches based on LLMs.

## 1.2. Early LLM approaches: zero-shot and prompt engineering

As the quality of LLMs improved, researchers began studying their applicability to Text-to-SQL by constructing elaborate so-called zero-shot and few-shot prompts. Here we should clarify that by the established term zero-shot we understand an LLM request that does not contain the correct answer, whereas a few-shot prompt is a request with one or more hints about the correct answer.

The C3 paper (Zero-shot Text-to-SQL with ChatGPT) [1] demonstrated that carefully designed prompts (prompt engineering), input structuring, and strict control over the output format can substantially raise accuracy without additional model training. The authors highlighted three key components:

- clear structuring of the database schema inside the prompt;
- the use of consistent examples;
- strict control over the SQL output format.

Similar ideas were systematically developed in DAIL-SQL [2], which proposed an integrated prompting pipeline with dynamic example selection. These studies showed that careful prompt engineering can yield high accuracy on the Spider 1.0 benchmark.

These methods, however, face a number of limitations:

- growth in schema complexity and long contexts;
- hallucinations of table and attribute names;
- sensitivity to query phrasing.

## 1.3. Benchmark expansion: BIRD and Spider 2.0

As LLM capabilities grew, more realistic benchmarks became necessary. BIRD (Big Bench for Large-Scale Database Grounded Text-to-SQL) [3] introduced a large-scale dataset oriented towards grounding to the database contents, the use of external knowledge, and the generation of more complex queries. BIRD revealed that high accuracy on Spider does not guarantee robustness as the size and complexity of the database grow.

The next step was Spider 2.0 [4], oriented to real production scenarios. Unlike its predecessors, Spider 2.0 has the following characteristics:

- it requires working with long context (documentation, dbt projects);
- it assumes multi-step generation;
- it includes tasks at the level of a realistic production environment.

The authors showed that even advanced LLM agents solve only a limited share of the tasks, which points to the need for new architectural solutions.

## 1.4. Reducing hallucinations and task alignment

One of the central LLM problems in Text-to-SQL is hallucination — generation of nonexistent tables, columns, or incorrect syntactic constructs.

TA-SQL [5] proposed a task-alignment method that improves the model's generalisation by searching for analogous tasks and structures. The authors classify hallucination types and demonstrate improvements on BIRD.

OpenSearch-SQL [6] develops the idea of dynamic few-shot learning and consistency alignment, modelling the workflow of a human analyst who first clarifies the available database structures and then constructs the SQL query taking this knowledge into account.

These studies illustrate the shift from static prompt engineering to more adaptive and robust generation strategies.

## 1.5. Multi-step reasoning and agentic solutions

CHASE-SQL [7] proposed a multi-path reasoning strategy: the model generates several SQL candidates which then undergo optimized selection.

For Spider 2.0 ReFoRCE [8] is particularly important; it implements an agentic loop:

- database information compression (schema compression);
- iterative column exploration;
- generation of multiple variants;
- voting and self-correction.

The agentic direction was further developed by APEX-SQL [14], which proposed a shift from static query compilation to agentic exploration of data. The approach is based on a cycle of sequential hypothesis checking in which the AI model uses not only the metadata of the database schema but also performs data profiling at the generation stage and verifies its hypotheses in real time. This makes it possible to discover additional relations and dependencies between schema attributes and to refine the query semantics, progressively improving the quality of the resulting SQL.

## 1.6. Data scaling and reinforcement learning

In parallel with the approaches above, the direction of reinforcement learning and training-data improvement was being developed.

OmniSQL [9] proposed scalable synthesis of large Text-to-SQL datasets (on the order of one million examples), which improves template coverage and the transferability of models.

Reasoning-SQL [10] uses reinforcement learning (GRPO) and partial rewards:

- reward for correct schema linking;
- syntax check;
- semantic proximity.

This approach mitigates the reward-sparsity problem and improves the model's step-by-step reasoning capability.

Automatic Metadata Extraction [11] studies the automatic extraction of table descriptions and database statistics to improve SQL generation. This is especially relevant for Spider 2.0, where metadata is not always provided explicitly.

SWE-SQL [12] extends the task to the diagnosis and repair of SQL queries, reflecting real production scenarios.

Additionally, the literature notes correctness problems in benchmark annotations, as shown in CIDR 2026 [13], which uncovered inconsistencies in BIRD and Spider 2.0.

## 1.7. Limitations of existing solutions

Despite the progress achieved, contemporary methods for converting natural language to SQL queries are still limited by a number of architectural factors. Traditional zero-shot and few-shot strategies depend critically on the volume and structure of metadata passed in the prompt. As databases grow in scale and user instructions become more complex, the effectiveness of such approaches declines, as confirmed by Spider 2.0 results. In real production conditions, models must analyze not only tables but also accompanying source code and project documentation. In big-data production systems, warehouses and data lakes can contain hundreds of tables and thousands of attributes. Passing such a volume of metadata for analysis to a large language model creates an "overloaded context" problem and often runs into context-window size limits.

Attempts to overcome these barriers led to the creation of agentic systems and test-time scaling methods, such as APEX-SQL and ReFoRCE. These solutions provide accuracy gains via repeated iterations, feature filtering, and verification of hypotheses through SQL execution. Nevertheless, quality improvement here is achieved by significantly complicating inference: multi-step reasoning and the generation of many query variants increase computational cost, including the number of tokens consumed. For corporate systems with a heavy stream of requests, the total cost of ownership of such solutions is often unacceptable.

---

# 2. Problem Statement

This study proposes to consider an alternative possibility of using a Retrieval-Augmented Generation (RAG) architecture to optimize the contents of the LLM context. The main hypothesis of the work is that RAG is able not only to raise the relevance of answers through advanced hybrid vector retrieval, but also to reduce overall token consumption by cutting off information irrelevant to the user request.

Instead of redundantly passing the entire data schema, the RAG mechanism allows selective integration into the request of only the necessary elements: specific table attributes, sample tuples, and suitable syntactic templates. This approach minimizes the size of the context window, reducing token consumption without significant loss of quality.

The relevance of this development vector is confirmed by current trends in enterprise solutions, where refined information retrieval is recognised as a key factor in the scalability of Text-to-SQL systems. A vivid example of an industrial implementation of this concept is the offering from cloud-data (lakehouse) leaders Databricks and Snowflake. In particular, Databricks AI Functions and the specialized AI assistant Genie use semantic search over metadata to provide context-aware answers to business queries. Likewise, the integration of Cortex AI into the Snowflake ecosystem allows end users to interact with data through "smart" interfaces, where the RAG architecture provides accurate translation of natural language to SQL commands without the need to pass the whole warehouse structure to the LLM. The appearance of such AI Assistants directly inside the interfaces of analytics platforms confirms the industry's shift to hybrid generation methods that combine knowledge retrieval and agentic reasoning.

The practical outcome of this thesis is the development of a software product — an intelligent assistant designed to democratize access to data in a remote cloud warehouse. The system acts as a mediator between the end user and the complex structure of the database, transforming natural-language user requests into syntactically correct SQL statements. Thanks to pre-training of the system and the implementation of the hybrid RAG mechanisms with advanced metadata retrieval techniques described above, the assistant achieves high accuracy in handling transactions even on distributed warehouses characterised by high latency and a substantial amount of implicit relations. RAG also reduces token usage compared with the solutions described earlier, while maintaining acceptable generation quality.

The resulting software product can be successfully integrated into existing business processes as a "human-data" interface. The user gains the ability to extract analytical slices without programming skills and without the need to involve IT specialists.

The software implementation includes modules for dynamic retrieval of relevant metadata segments from the vector knowledge base, a block for verification of generated queries, an interaction layer with the Snowflake cloud-provider API, and a convenient user interface. Thus the work not only solves the practical task of reducing LLM cost while maintaining generation quality, but also offers a ready applied tool scalable to real industrial tasks of an applied nature.

---

# 3. Structure of the Vector Database

## 3.1. Description of the RAG system's vector-database collections

### 3.1.1. General characteristics of the storage

ChromaDB is used as the vector database — an embeddable store with persistent on-disk storage. Storage path: `rag_snow_agent/.chroma/`. Embeddings are computed using OpenAI's `text-embedding-3-large` model, producing 3072-dimensional vectors. Cosine distance is used as the similarity metric.

The storage is organized into five thematic collections, each performing a specific function in the SQL generation process.

### 3.1.2. The `schema_cards` collection (622 elements)

**Purpose.** The system's main knowledge base, containing structured descriptions of schema objects — tables, columns, and inter-table relations. Used during context retrieval to form a compact schema representation (SchemaSlice) that is included in the language-model prompt.

**Stored record types:**

**TableCard — table card.** One card is created for each database table. Partitioned tables (for example, daily Google Analytics tables in the form `GA_SESSIONS_YYYYMMDD`, comprising 366 separate tables) are merged into a single representative record. The name used is a table that actually exists in Snowflake — the last partition — and the description includes information about the date range and the filtering approach.

The textual representation (the document) includes the table name, a natural-language description, and the list of columns. The description is generated by GPT-5.4 from profiling 100 rows of real data, which provides semantically meaningful retrieval: a user query about "revenue" will be matched against a table whose description mentions transactional data.

Metadata: database identifier (`db_id`), the qualified table name, the natural-language description, and an estimate of the token count.

**ColumnCard — column card.** A separate record is created for each column, including columns that are nested fields of type VARIANT. Column descriptions are generated from the statistical profile of the data (proportion of empty values, value range, number of unique values, structure of nested objects) and contain guidance for correctly constructing SQL queries.

For VARIANT columns the description includes a classification of the structure — ARRAY (requires the `LATERAL FLATTEN` operator) or OBJECT (accessed via colon) — and an enumeration of known nested fields with their data types.

Nested fields of VARIANT columns (for example, `"totals":pageviews`) are stored with the data type `VARIANT_FIELD` and carry in their metadata a flag indicating the discovery source — through a direct call to `OBJECT_KEYS()` (object structure) or through `LATERAL FLATTEN + OBJECT_KEYS()` (array element). This flag determines the classification of the parent column when the prompt is assembled.

**JoinCard — join card.** For each discovered relation between tables a record is created containing the left and right tables, the join columns, a confidence level (1.0 for foreign keys, 0.7 for heuristic matches), and the discovery source.

### 3.1.3. The `semantic_cards` collection (5,762 elements)

**Purpose.** A store of semantic facts derived from schema metadata using deterministic heuristics. Used to enrich the prompt context with additional hints about the structure and semantics of the data.

**Types of semantic facts:**

- **`primary_time_column`** — columns of types `DATE`, `TIMESTAMP`, and their variants. Lets the system automatically identify temporal dimensions.
- **`date_format_pattern`** — columns with date-like names and numeric or text types. Classified as "YYYYMMDD integer" (for numeric columns) or "YYYYMMDD string" (for text columns). This information is critical for correctly generating date-based filter conditions.
- **`metric_candidate`** — numeric columns whose names indicate metric content (`revenue`, `count`, `price`, `total`, etc.).
- **`dimension_candidate`** — text columns whose names are characteristic of analytic dimensions (`source`, `country`, `category`, `status`, etc.).
- **`nested_container_column`** — columns of types `VARIANT`, `OBJECT`, and `ARRAY`, with structure classification and known nested fields.
- **`identifier_column`** — columns matching identifier patterns (`*_ID`, `*_KEY`, `ID`). Used to protect against removal during token-budget optimization.

Each fact is accompanied by a confidence level (from 0.0 to 1.0) and a source indicator (metadata, heuristics, probes).

### 3.1.4. The `sample_records` collection (10 elements)

**Purpose.** Storage of sample rows from tables for inclusion in the prompt context. Each element contains 2–5 rows from a single table, formatted as compact JSON.

Sample rows let the language model "see" real column values, understand date formats, the structure of nested VARIANT objects, and typical value ranges. This is especially important for tables with complex structures such as Google Analytics, where the column `hits` contains an array of nested objects with dozens of fields.

### 3.1.5. The `trace_memory` collection (5 elements)

**Purpose.** Storage of successful solutions for few-shot learning in context. When a task is solved successfully, the system saves a "question — solution plan" pair for later use as an example when solving semantically close tasks.

The document contains a brief statement of the question and the solution plan. The metadata includes the database identifier, the task-instance identifier, and the list of tables used. The lookup is performed by semantic proximity to the current question with filtering by database.

This mechanism implements the principle of "learning from one's own experience": the system accumulates a library of successful solutions and uses them to raise generation quality on analogous tasks.

### 3.1.6. The `snowflake_syntax` collection (55 elements)

**Purpose.** A Snowflake SQL syntax reference used at the error-repair stage. It contains documentation fragments on the syntax of various constructs: table joins (JOIN), the `LATERAL FLATTEN` operator, window functions, data-type operations, and others.

When a compilation or execution error occurs, the system constructs a search query from the error text and the SQL fragment, retrieves relevant reference fragments, and includes them in the repair prompt. This allows the model to use exact Snowflake syntax reference information instead of generating fixes "from memory".

## 3.2. Collection population process

**Schema indexing (online).** Performed at connection time to Snowflake. Extracts tables, columns, data types, and relations from `INFORMATION_SCHEMA`. For VARIANT columns it probes the structure via `OBJECT_KEYS()` and `LATERAL FLATTEN`. Partitioned tables are merged into a single representative.

**Data profiling (offline).** For each table, 100 rows of data are extracted, a statistical profile of every column is computed, and then GPT-5.4 generates natural-language descriptions of tables and columns. Descriptions are saved to the file `table_column_descriptions.json` and loaded into the `schema_cards` collection through the enrichment script `enrich_descriptions.py`.

**Semantic facts (automatic).** Generated by deterministic heuristics based on data types, column names, and the results of probing VARIANT structures.

## 3.3. How the collections are used during SQL generation

For each user request the system queries the collections in the following order:

1. **`schema_cards`** — hybrid retrieval (semantic + lexical + RRF fusion) to extract relevant tables and columns. Result: a SchemaSlice with a budget of up to 10,000 tokens.
2. **`semantic_cards`** — retrieval of semantic facts for context enrichment (date formats, metric candidates, classification of VARIANT structures).
3. **`sample_records`** — retrieval of sample rows for the relevant tables.
4. **`trace_memory`** — search for semantically close successful solutions to form a few-shot context.
5. **`snowflake_syntax`** — accessed only when errors occur at the SQL-repair stage.

---

# 4. Retrieval Pipeline

## 4.1. Chunking strategy

Unlike classical RAG systems that work with arbitrary text documents, this system operates on structured database-schema metadata. Instead of splitting documents into fixed-size fragments, an "one object — one fragment" (entity-level chunking) strategy is used:

- **TableCard** — one fragment per table. The document includes the qualified name (`DB.SCHEMA.TABLE`), the natural-language description, the list of columns, time columns, and join keys. Typical size: 60–300 tokens.
- **ColumnCard** — one fragment per column. The document contains the qualified column name, the data type, and the description. For VARIANT columns the description includes the access pattern for nested fields and their list. Typical size: 20–400 tokens (depending on description complexity).
- **JoinCard** — one fragment per inter-table relation. The document describes the left and right sides of the relation with the confidence level. Typical size: 20–40 tokens.

This strategy ensures precise atomicity of retrieval: the system can extract a specific column or a specific table without loading excessive context.

For partitioned tables a preliminary merging (partition collapsing) is applied: for example, 366 daily tables representing a year of data are reduced to a single representative card, which removes duplication in the search results and saves context budget.

## 4.2. Stage 1: Dense Retrieval

The first stage of the retriever relies on vector representations of the cards, computed with the `text-embedding-3-large` model. The user-question text $Q_{NL}$ is converted into a 3072-dimensional real-valued vector, and two independent queries are then issued to ChromaDB: for the $K_{table}=25$ nearest table cards and the $K_{col}=100$ nearest column cards. Distance is measured by cosine similarity:

$$
\mathrm{sim}_{\cos}(\mathbf{q},\mathbf{d})=\frac{\mathbf{q}\cdot\mathbf{d}}{\lVert\mathbf{q}\rVert\,\lVert\mathbf{d}\rVert}
$$

where $\mathbf{q}$ is the query vector and $\mathbf{d}$ is the card vector.

**Strengths of dense retrieval for our task:**

1. *Matching by meaning, not by exact words.* For instance, the query "total revenue for the quarter" successfully locates the column `revenue_amount_usd` whose description mentions "sales revenue in US dollars", despite the absence of the words "income" or "quarter" in the column name.
2. *Use of detailed descriptions.* Texts generated by GPT-5.4 from real-data profiling saturate the embeddings with domain semantics — for example, they explicitly indicate that a table belongs to the "web-analytics logs" class.
3. *Robustness to synonymy.* Morphological alternatives such as "sales", "revenue", and "income" map to nearby points in the embedding space, removing the dependence on the user's exact phrasing.

**Where dense retrieval is weaker than other methods.** When a query contains a short identifier or specific code — `fullVisitorId`, `geo_id`, `5713D`, `Q1095` — the corresponding token dissolves in the embedding space and loses ranking weight. Likewise, dense retrieval struggles to separate cards with similar descriptions but different exact names when the user query refers to that specific name.

The implementation is provided by the `HybridRetriever` class in the module `rag_snow_agent/src/rag_snow_agent/retrieval/hybrid_retriever.py`; the dense pass is handled by the method `_dense_query()`. It returns an ordered list of `ScoredItem` objects with the fields `dense_rank` (position) and the cosine-similarity value.

## 4.3. Stage 2: Sparse Retrieval

The lexical pass is meant to compensate for the weaknesses of the dense pass — it handles exact matching of key tokens. The **BM25** (Best Matching 25) ranking function is applied, which is standard in information retrieval [16]:

$$
\mathrm{BM25}(D, Q) = \sum_{t \in Q} \mathrm{IDF}(t)\cdot\frac{f(t,D)\cdot(k_1+1)}{f(t,D)+k_1\cdot\bigl(1 - b + b\cdot\dfrac{|D|}{\mathrm{avgdl}}\bigr)}
$$

where $f(t,D)$ is the frequency of term $t$ in document $D$, $|D|$ is the document length in tokens, $\mathrm{avgdl}$ is the average document length in the index, $\mathrm{IDF}(t)$ is the inverse document frequency of the term, and $k_1$ and $b$ are tunable parameters; in this work the default values are used: $k_1=1.5$, $b=0.75$.

**Adaptation of BM25 to Text-to-SQL specifics.**

1. *Splitting compound identifiers.* A column name such as `revenue_amount_usd` is indexed both as a whole and by its components `revenue`, `amount`, `usd`. Thanks to this the formula fires both for a natural-language query and when the user quotes the exact field name.
2. *Case-insensitivity while preserving the original.* The tokenizer lowercases identifiers and treats `_`, `.`, `:` as separators, while the original form of the name is also preserved — this is needed so that a query with a verbatim code citation hits the right document.
3. *Boosted weight of rare terms.* Low-frequency keywords — `wikidata`, `osm_id`, `Q1095` — receive high $\mathrm{IDF}$ and dominate the ranking. This property allows targeted search by specific markers to which the dense stage reacts weakly.

The lexical pass is computed in parallel with the dense one and returns a list of `ScoredItem` of similar structure with the fields `bm25_score` and `lexical_rank`. The corresponding method — `_lexical_query()` of the same `HybridRetriever`; the foundation is the open-source library `rank_bm25`.

## 4.4. Stage 3: Reciprocal Rank Fusion (RRF)

The final convolution of the results from the two stages is performed by **Reciprocal Rank Fusion** [15]. This is a lightweight technique for aggregating ranked lists, robust to the heterogeneity of the source-algorithm scores:

$$
\mathrm{RRF\_score}(d) = \sum_{r \in R} \frac{1}{k + \mathrm{rank}_r(d)}
$$

where:

- $R$ is the set of ranked lists (in our case two — dense and lexical),
- $\mathrm{rank}_r(d)$ is the position of document $d$ in list $r$ (one-based),
- $k$ is a smoothing parameter set to $k = 60$.

The role of the parameter $k$ is to balance: at $k = 60$ items that hold high positions simultaneously in both lists receive a noticeable bonus, while an item that appears in only one list is not dropped from the output.

**Why RRF is preferable to a linear combination of raw scores.** Cosine similarity and the BM25 score are different in nature and not on a common scale. To add them directly one would have to calibrate weights per database, which scales poorly to the 20 distinct domains of Spider 2.0 — Snow. RRF operates only on list positions and does not depend on absolute similarity values, which provides robustness across domains. In practice this is precisely what makes RRF a good choice for tasks where the dense and lexical signals turn out to be complementary.

**Output.** A combined list of cards sorted by decreasing RRF score. Each entry retains its position from the source passes (`dense_rank`, `lexical_rank`), the aggregated position (`fused_rank`), and the actual `rrf_score`. This list is then passed through three sequential post-processing steps: token-budget pruning, join-graph expansion (`expand_join_graph_neighbors`), and VARIANT field enrichment (`_enrich_variant_fields`).

## 4.5. Token-budget management and SchemaSlice post-processing

Passing the combined ranked list into the prompt "as is" would contradict the main idea of the work — to shrink the context. Therefore, after RRF fusion the list goes through a chain of post-processing steps that turn it into the final `SchemaSlice` structure:

**Step 1: Join-graph expansion.** The procedure `expand_join_graph_neighbors` walks the ordered list of tables and adds "1-hop neighbours" from the `JoinCard` graph if such neighbours are actually needed to execute the query. For example, if `ORDERS` made it into the selection and there is a foreign key between it and `CUSTOMERS`, then `CUSTOMERS` will be added automatically. This step guards against the typical situation in which the retriever successfully finds the "main" table for the query but misses the auxiliary tables without which a join cannot be built.

**Step 2: `VARIANT` field enrichment.** For every column of the corresponding type, a separate query is issued to the `schema_cards` collection for nested subfields (type `VARIANT_FIELD`); the discovered subfields are attached to the parent column's card. In addition, based on the discovery-source flag (`OBJECT_KEYS()` directly or `LATERAL FLATTEN + OBJECT_KEYS()`) the column is marked as object-typed or array-typed. If this classification is wrong, the output SQL is guaranteed to fail at the Snowflake compilation stage: a `LATERAL FLATTEN` over an object or `:field` access into an array element will not pass syntax checks.

**Step 3: Token-budget pruning.** The size of the final `SchemaSlice` is limited by the parameter `max_schema_tokens = 10,000`. The procedure `trim_to_budget` starts from the full list and removes the lowest-ranked elements (by `fused_rank`) until the total token count falls within the limit. Removal is protected by two guarantees:

- *Identifier preservation.* Columns with the semantic marker `identifier_column` (names ending in `_ID`, `_KEY`) are never removed from tables included in the selection — otherwise the LLM loses the ability to construct correct joins.
- *Time-column preservation.* Columns marked as `primary_time_column` are kept at one per selected table: date filtering is the most frequent operation in analytical SQL, and losing these columns sharply increases the risk of generation failure.

**Step 4: Serialization for the prompt.** The final `SchemaSlice` is rendered as a Markdown block: table names, descriptions, the list of columns with types and hints. The resulting block forms the dynamic part of the system prompt; the static part contains general instructions on the Snowflake dialect and the expected answer format.

## 4.6. Performance characteristics

| Parameter | Value |
|:---------|:---------|
| Embedding model | `text-embedding-3-large` (3072 dimensions) |
| Similarity metric | Cosine distance |
| RRF parameter ($k$) | 60 |
| Token budget | 10,000 |
| Maximum tables | 25 |
| Maximum columns | 100 |
| Typical retrieval time | 200–500 ms |
| Typical SchemaSlice size | 1,500–4,000 tokens |

It is worth noting the relatively large headroom in the budget (10,000 tokens against actual usage of 1,500–4,000). This configuration is deliberate: the limiting factor is usually not the total token count but the number of selected tables and columns, controlled by the $K_{table}$ and $K_{col}$ parameters.

---

# 5. SQL Generation Agent Architecture

## 5.1. The overall request-processing loop

When the retriever (Chapter 4) has assembled a relevant `SchemaSlice` for a specific user request, the baton is picked up by the SQL generation agent. Its task is to synthesize a correct, executable, and semantically meaningful query in the Snowflake dialect, relying on the structured schema representation and the question text.

The agent's workflow is organized as a five-stage sequence:

1. **Question decomposition** into subtasks — optional, for queries with explicit multi-step logic.
2. **Parallel generation of $N$ plan candidates** in the form of a structured `QueryPlan` object (validated via Pydantic).
3. **Deterministic SQL assembly from the plan**, accounting for Snowflake syntax (`LATERAL FLATTEN`, `VARIANT` access, multi-stage CTEs).
4. **Self-correction loop** (repair loop) in which an erroneous SQL is passed through a classifier and then a specialized repair prompt is constructed.
5. **Verification of the result** — either in benchmark mode against a reference output, or in production mode by returning the result to the user.

The idea behind this decomposition is to leave to the LLM exactly what it is good at (understanding natural language, recovering domain semantics) and to take away from it what it is unreliable at (precise SQL formatting, stable identifier quoting, freedom from name hallucinations).

## 5.2. Deterministic Plan → SQL compiler

The engineering core of the work is the demarcation of responsibilities: the LLM produces only a high-level description of the query, while the final SQL is assembled by the code generator. The LLM is required to produce a structured JSON object, validated by the `QueryPlan` Pydantic model:

```json
{
  "selected_tables": ["GA360.GA_SESSIONS_*"],
  "joins": [],
  "filters": [{"column": "date", "op": ">=", "value": "20170701"}],
  "aggregations": [{"function": "COUNT", "expression": "DISTINCT visitorId"}],
  "group_by": [],
  "order_by": [{"expression": "1", "direction": "DESC"}],
  "limit": 10
}
```

Converting the plan into executable code is the responsibility of the function `compile_plan(plan, schema_slice)` in the module `rag_snow_agent.prompting.sql_compiler`. It is in charge of the following:

- *Alias assignment.* Each participating table receives a short stable alias of the form `t1`, `t2`, ..., which avoids collisions with reserved Snowflake names and improves readability.
- *Safe name quoting.* Following Snowflake's case-sensitive semantics, all column names are wrapped in double quotes. This removes a whole class of case-related errors, especially painful for queries with camelCase names (`fullVisitorId`, `userPseudoId`).
- *Assembling `LATERAL FLATTEN`.* For arrays inside `VARIANT`, the compiler automatically constructs `LATERAL FLATTEN(input => t1."hits") h` with subsequent access via `h.value:"event_name"::STRING`. In practice this syntax is difficult for LLMs, whereas the code generator reproduces it without failure.
- *Assembling multi-stage CTEs.* If the plan describes several named CTEs with mutual dependencies, the compiler resolves their declaration order and assembles a query of the form `WITH cte1 AS (...), cte2 AS (...) SELECT ...`.
- *Partition post-processing.* In the final step, the produced SQL is scanned for references to representative shards of a partitioned table (for example, `GA_SESSIONS_20170801`); when found, the compiler replaces such a reference with a UNION ALL CTE over the required date interval. Without this step, queries against GA360 (366 shards) can technically be generated, but return data for only one day out of the entire interval.

The effectiveness of this separation is confirmed by the ablation study (Chapter 7): disabling the compiler and switching to direct SQL generation by the model drops accuracy by 36 percentage points.

## 5.3. Best-of-N with strategy diversification

The chance of arriving at a correct answer is increased by generating several independent plan candidates in parallel. For each user request the agent creates $N=8$ plans at once, and each candidate is assigned its own prompt strategy from a rotation:

| Strategy | Prompt emphasis |
|:----------|:---------------------------------|
| `default` | Universal instruction without special priorities |
| `join_first` | First identify the relations between tables |
| `metric_first` | First identify the target numeric metric and aggregation |
| `time_first` | Prioritize time dimensions and date filters |
| `flatten_first` | Prioritize work with `VARIANT` fields via `LATERAL FLATTEN` |
| `cte_first` | Encourage building multi-stage CTE pipelines |
| `geo_first` | Prioritize geospatial functions (`ST_WITHIN`, `ST_DISTANCE`, etc.) |
| `default` (second instance) | An additional universal candidate |

Each prompt looks at the task from its own angle and nudges the model toward a different reasoning track. Thus, in `join_first` the model literally begins with the phrase "determine which joins will be needed for the answer", and in `metric_first` with the question "what is the target numeric value".

Each of the eight candidates is allowed to be compiled and repaired independently. The final choice is made by a multi-signal selector that computes a score by the formula:

$$
\mathrm{score}(c) = w_1 \cdot \mathbf{1}[\mathrm{exec\_success}] + w_2 \cdot \mathrm{shape\_alignment}(c) - w_3 \cdot \mathrm{repair\_count}(c) + w_4 \cdot \mathrm{verifier\_prob}(c)
$$

where:

- $\mathbf{1}[\mathrm{exec\_success}]$ is the indicator that the SQL candidate executed without errors;
- $\mathrm{shape\_alignment}$ is a score for how well the shape of the result matches the expected one (number of rows, number of columns, type consistency);
- $\mathrm{repair\_count}$ is a penalty for the number of repair iterations the candidate required;
- $\mathrm{verifier\_prob}$ is the probabilistic estimate from the verifier, implemented as a separate LLM classification.

If the very first candidate passed execution without repairs, it usually receives the maximum score and is accepted without an exhaustive sweep.

## 5.4. Self-correction Loop (Repair Loop)

Each generated candidate is sent to Snowflake for execution. The self-correction loop is implemented in the module `rag_snow_agent.agent.refiner` and works in this sequence: "EXPLAIN → execute → classify error → send specialized repair prompt".

1. **First EXPLAIN.** Before spending resources on full execution, the query is sent for a plan preview. This step catches most syntactic and semantic errors (nonexistent columns, type mismatches, identifier issues) without touching the data.
2. **Then execution.** On a successful EXPLAIN the query is executed against the actual data, and the system gets back the result set together with meta-information (row count, column count, execution time).
3. **Error classification.** If something went wrong, the Snowflake error text is passed through `error_classifier`, which assigns it to one of the categories: `INVALID_IDENTIFIER`, `OBJECT_NOT_FOUND`, `AGGREGATION_ERROR`, `RESULT_MISMATCH`, `EMPTY_RESULT`, `NOT_AUTHORIZED`, `OTHER`.
4. **Targeted repair prompt.** For each category, a dedicated template with supplementary information is used:
   - on `INVALID_IDENTIFIER` the probe `probe_column_exists` is launched to verify the existence of the mentioned column, and the closest matching analogue from `SchemaSlice` is embedded in the prompt;
   - on `OBJECT_NOT_FOUND` the system pulls from `SchemaSlice` the list of tables with similar names;
   - on `AGGREGATION_ERROR` a compact reference card on the `GROUP BY` rules in the Snowflake dialect is attached;
   - on `RESULT_MISMATCH` the prompt includes a comparison of the actual and expected result shapes.
5. **Loop safeguard.** When the same error category recurs three times in a row, the loop terminates with the mark `error_type_threshold_exceeded`, to avoid spending tokens on hopeless edits.

The upper limit on iterations for a single candidate is `max_repairs = 4`. Empirically, after the fourth iteration the probability of a successful repair drops below 5 %, so further token expenditure is not economically justified.

## 5.5. Handling specific cases

### 5.5.1. Date partitioning

In production warehouses, time series are typically stored as daily tables of the form `EVENTS_20210101`, `EVENTS_20210102`, and so on. In GA360 a single logical entity is represented by 366 shards that are physically independent of one another. Treating such a layout head-on leads to two problems:

1. The retriever drowns in duplicate cards and fails to return the columns that are actually needed;
2. The LLM puts the name of a single — arbitrary — partition into `FROM`, losing the data from the remaining dates.

The solution used in the system works at two levels:

- *At the indexing stage:* all shards are merged into one `TableCard` record with the date boundaries specified; columns are unioned across all shards, and the table name is taken from the last existing shard.
- *At the SQL post-processing stage:* the query assembled by the compiler is scanned for references to the partitioned table. If a date filter is found in the query — for example, `"date" BETWEEN '20170701' AND '20170731'` — the compiler automatically replaces the reference with a UNION ALL CTE iterating over the required range. For ranges greater than 40 days (such as `LIKE '2017%'` covering an entire year), the rewrite is skipped, so as not to time out the Snowflake planner.

### 5.5.2. Geospatial functions

Queries with spatial semantics (`ST_WITHIN`, `ST_DISTANCE`, `ST_CONTAINS`) form a particular difficulty for LLMs: the model confuses argument order (lat/lon vs lon/lat), mixes units (metres with miles), and frequently forgets to wrap `VARCHAR` geometries in a `TO_GEOGRAPHY()` call. For such cases the system provides additional plan elements — `PlanGeoJoin` and `PlanGeoFilter`. They are passed to the compiler as explicit Pydantic constructs whenever the selection includes geospatial columns. In parallel, the `geo_first` strategy nudges the LLM to pick these constructs instead of manual coordinate arithmetic.

### 5.5.3. Semi-structured `VARIANT` data

`VARIANT` is Snowflake's native type for semi-structured JSON objects. Access to nested fields differs fundamentally depending on what is "under the hood" of the column: an array requires `FLATTEN`, an object uses the `:` operator. A mistake in choosing the syntax is guaranteed to break SQL compilation. To rule out such failures, in this system column classification is performed already at indexing time — by probing through `OBJECT_KEYS()` — and saved in the card metadata. At prompt-construction time this classification is communicated to the LLM explicitly, removing the need for guesswork.

---

# 6. Benchmark Results

## 6.1. Experimental methodology and conditions

All experiments are run on the **Spider 2.0 — Snow** benchmark [4] — a set of 547 natural-language queries against 20 industrial databases in the Snowflake cloud warehouse (GA360, GA4, PATENTS, GITHUB_REPOS, NOAA_DATA, CMS_DATA, etc.). Two subsets were used:

- a **25-query subset** — for baseline comparison against open-source implementations of DSR-Lite, Spider-Agent (GPT-4o) [4], and ReFoRCE [8] on a family of four databases (GA4, GA360, PATENTS, PATENTS_GOOGLE);
- a **100-query subset** — the first 100 elements of `spider2-snow.jsonl`, spanning all 20 databases, used for a reproducible comparison against ReFoRCE on the modern model GPT-5.4-mini.

**Accuracy metric — gold-match accuracy:** the result of executing the generated SQL on the reference database is compared against the reference output provided by the Spider 2.0 authors. A match is determined by the shape and content of the result table.

**Additional metrics:**

- **valid SQL** — the share of syntactically correct queries executed without errors;
- **pass@k** — the share of tasks for which at least one of the $k$ candidates passed gold-match (applicable to ReFoRCE with its voting mechanism);
- **total tokens** — the total number of tokens spent across all LLM calls (input + output);
- **cost per correct answer** — an estimated cost of a single correct answer in US dollars at OpenAI's prices at the time of the experiment.

In all experiments, the compared systems received the same set of queries and worked with the same Snowflake connection; the only varied parameter was the architecture of the specific Text-to-SQL method and the LLM model used within it.

## 6.2. Comparison with baseline methods on the 25-query subset

Run 9 of the proposed system (hereinafter SnowRAG-Agent) was compared with three baseline methods on the same 25-query subset:

| Metric | DSR-Lite | Spider+GPT-4o | ReFoRCE | **SnowRAG-Agent (Run 9)** |
|:--------|---------:|--------------:|--------:|--------------------------:|
| Model | DeepSeek | GPT-4o | GPT-5-mini | GPT-5.4 |
| Candidates per query | 1 | 1 | 8 | 8 |
| Repair iterations | 20 | 15 | 3 | 4 |
| **Gold-match accuracy** | 0 % | 12 % | 36 % | **92 %** |
| Correct answers | 0 / 23 | ≈ 3 / 25 | 9 / 25 | **23 / 25** |
| Total tokens | 4.21M | 3.90M | 7.50M | 4.08M |
| Tokens per correct answer | ∞ | ≈ 1,300,000 | 833,485 | **177,229** |

**Key observations:**

1. SnowRAG-Agent reaches an accuracy of **92 %**, exceeding the nearest baseline (ReFoRCE at 36 %) by more than 2.5×.
2. Token spending per correct answer is **177K** versus 833K for ReFoRCE — an efficiency gain of **4.7×**.
3. On complex, deeply structured domains (GA360, PATENTS), DSR-Lite — which uses the full DDL in the prompt — produced no correct answers at all, while SnowRAG-Agent reaches 92 % and 100 % respectively.

The contrast with DSR-Lite deserves special attention. The model used in that system (DeepSeek) is weaker than current alternatives in generation strength, but the decisive factor is not the model but the architecture: the full DDL of GA360 with its hundreds of tables simply does not fit into the context window, and so analysis of the query is impossible before any reasoning algorithm or correction loop has a chance to act.

Spider-Agent on GPT-4o reaches a slightly higher mark (12 %) — here GPT-4o's more accurate reconstruction of table structures from names shows. Even so, this "guessing" is not enough for the system to work robustly on industrial data schemas.

ReFoRCE with its agentic voting and multi-step correction loop reaches 36 %, but pays for it with about twice the token spending relative to SnowRAG-Agent. The architectural bottleneck here is that ReFoRCE lacks a retriever capable of returning to the LLM exactly the portion of metadata required by the current query.

### 6.2.1. Accuracy per database

| Database | Queries | DSR-Lite | Spider+GPT-4o | ReFoRCE | **SnowRAG (Run 9)** |
|:------------|--------:|--------:|--------------:|--------:|--------------------:|
| GA4 | 1 | 0 % | — | — | **100 %** |
| GA360 | 12 | 0 % | — | — | **92 %** |
| PATENTS | 11 | 0 % | — | — | **100 %** |
| PATENTS_GOOGLE | 1 | 0 % | — | — | 0 % |

From the table, SnowRAG-Agent shows a stable advantage across all domains shown — both on the large GA360 schema (12 queries) and on the medium-sized PATENTS (11 queries). The only shared "no-go zone" is the single PATENTS_GOOGLE query, which requires understanding of the specific patent classification by CPC codes; such domain semantics are not in the index and are therefore not covered at present.

## 6.3. Scaling up the experiment: the 100-query subset

To verify reproducibility of the advantage and to validate on an expanded set of domains, the Run 10 benchmark was executed on the first 100 queries of Spider 2.0 — Snow, spanning all 20 databases. The model used was `gpt-5.4-mini` — an economical member of the GPT-5.4 family, specifically chosen to evaluate the cost/quality trade-off.

| Metric | Value |
|:--------|---------:|
| **Gold-match accuracy** | **87 / 100 = 87.0 %** |
| Databases at 100 % accuracy | 12 of 20 |
| Total LLM calls | 3,601 |
| Total tokens | 18.9M |
| Cost (≈\$0.25/\$2 per 1M tokens) | ≈ \$32.92 |
| Cost per correct answer | ≈ \$0.38 |

The combination of 87 % gold-match and 38 cents per correct answer shows that the proposed approach is viable from the production-deployment standpoint.

The decline from 92 % (Run 9, 25 queries on the full GPT-5.4) to 87 % (Run 10, 100 queries on GPT-5.4-mini) is explained by two factors acting jointly. First, expanding the sample to 100 queries brings in more complex domains — in particular NEW_YORK_NOAA and GEO_OPENSTREETMAP — where geospatial semantics create additional difficulty. Second, GPT-5.4-mini as a compressed version of the model is slightly less accurate at decomposing multi-step reasoning. Even so, the absolute 87 % mark remains substantially higher than anything we are aware of from other systems.

### 6.3.1. Head-to-head SnowRAG-Agent vs ReFoRCE on the same 100 queries

To eliminate differences related to the model and the task set, Run 12 directly compared SnowRAG-Agent and upstream ReFoRCE under identical conditions: model `gpt-5.4-mini`, the same 100-query sample, the same gold-match reference. ReFoRCE was run with parameters typical for its architecture: `num_votes=8`, `num_workers=4`, `max_iter=5`, `test_delay=4`.

| Metric | **SnowRAG-Agent (Run 12)** | **ReFoRCE** | Ratio |
|:--------|---------------------------:|------------:|------------:|
| **Gold-match accuracy** | **84 / 100 = 84.0 %** | 20 / 100 = 20.0 % | **×4.2 in our favour** |
| Pass@k | — | 29 / 100 = 29.0 % | — |
| Valid SQL | 100 / 100 = 100 % | 98 / 100 = 98.0 % | — |
| LLM calls | 3,708 | 5,006 | ×0.74 |
| Input tokens | **17.6M** | 133.4M | **×0.13 (7.6× lower)** |
| Output tokens | 2.35M | 3.62M | ×0.65 |
| **Total tokens** | **19.9M** | 137.0M | **×0.15 (6.9× lower)** |
| Estimated cost | **\$9.11** | \$40.60 | **×0.22 (4.5× cheaper)** |
| Cost per correct answer | **\$0.11** | \$2.03 | **×0.05 (18× cheaper)** |

Crucially, both systems here operate on the same model (`gpt-5.4-mini`) and process the same 100 queries. Thus any divergences between them follow exclusively from architectural differences. This rules out the hypothetical counter-argument that the SnowRAG-Agent advantage might be explained by a stronger LLM.

**Analysis of the intersection of correct-answer sets:**

| Set | Size | Comment |
|:----------|------:|:------------|
| Both methods produced the correct answer | 20 | = full ReFoRCE set |
| SnowRAG-Agent only | 64 | strict advantage |
| ReFoRCE only | **0** | — |
| Both failed | 16 | shared "core" failure — predominantly geospatial queries |

A noteworthy feature of this intersection is that **all** 20 queries correctly solved by ReFoRCE are also successfully solved by SnowRAG-Agent. Not a single task was found on which our solution lost to ReFoRCE. Methodologically, this has an additional meaning: it shows that the architectural changes of SnowRAG-Agent do not introduce regressions — selective vector retrieval does not "throw away" needed information and so does not degrade behaviour relative to the full-context regime.

When compared even against the ReFoRCE pass@k (any of 8 generated candidates), only two tasks — [`sf_bq056`](#sf_bq056) and [`sf_bq073`](#sf_bq073) — appear in the ReFoRCE set but not in the SnowRAG-Agent gold-match (the task statements are given in Appendix 10.7). In both cases ReFoRCE did generate correct SQL, but the final vote selected the wrong candidate. These two examples vividly illustrate the weakness of voting as a selection mechanism in situations where the correct answer is present among the candidates.

### 6.3.2. The shape of ReFoRCE failures at higher cost

The most token-expensive ReFoRCE queries (>3M tokens per query) are predominantly failures (task statements — in Appendix 10.7):

| Query | DB | Tokens | LLM calls | ReFoRCE | SnowRAG |
|:-------|:----|--------:|------:|:--------|:--------|
| [sf_bq236](#sf_bq236) | NOAA_DATA_PLUS | 9.0M | 128 | ✗ | ✓ |
| [sf_bq419](#sf_bq419) | NOAA_DATA | 6.5M | 115 | ✗ | ✓ |
| [sf_bq056](#sf_bq056) | GEO_OPENSTREETMAP_BOUNDARIES | 5.0M | 142 | ✗ | ✗ |
| [sf_bq420](#sf_bq420) | PATENTS_USPTO | 4.9M | 121 | ✗ | ✗ |
| [sf_bq208](#sf_bq208) | NEW_YORK_NOAA | 4.4M | 102 | ✗ | ✗ |
| [sf_bq182](#sf_bq182) | GITHUB_REPOS_DATE | 4.3M | 101 | ✗ | ✓ |

There is no significant correlation between the volume of tokens spent and ReFoRCE's success on a given task. That means ramping up the budget for multi-step reasoning and voting in the absence of a filtering retriever does not converge to a solution — the model again and again processes the same incomplete (or, conversely, excessive) schema. This result is another argument in favour of the thesis that purely quantitative scaling of compute at inference time (test-time scaling) is not a self-sufficient strategy: it needs a quality base of relevantly selected metadata, which the RAG architecture provides.

## 6.4. Accuracy per database on the 100-query sample

| Database | Queries | **SnowRAG (Run 10)** | ReFoRCE (Run 12 baseline) |
|:------------|--------:|----------------------:|--------------------------:|
| GITHUB_REPOS | 15 | **15 / 15 (100 %)** | 4 / 15 (27 %) |
| PATENTS | 15 | 14 / 15 (93 %) | 5 / 15 (33 %) |
| GA360 | 12 | 10 / 12 (83 %) | 2 / 12 (17 %) |
| NOAA_DATA | 12 | 11 / 12 (92 %) | 2 / 12 (17 %) |
| CMS_DATA | 7 | **7 / 7 (100 %)** | 2 / 7 (29 %) |
| GEO_OPENSTREETMAP | 6 | 4 / 6 (67 %) | 0 / 6 (0 %) |
| GITHUB_REPOS_DATE | 6 | **6 / 6 (100 %)** | 0 / 6 (0 %) |
| PATENTSVIEW | 3 | **3 / 3 (100 %)** | 0 / 3 (0 %) |
| PATENTS_GOOGLE | 4 | 3 / 4 (75 %) | 1 / 4 (25 %) |
| CENSUS_BUREAU_ACS_2 | 4 | 2 / 4 (50 %) | 0 / 4 (0 %) |
| NEW_YORK_CITIBIKE_1 | 3 | 1 / 3 (33 %) | 1 / 3 (33 %) |
| NEW_YORK_NOAA | 3 | 1 / 3 (33 %) | 0 / 3 (0 %) |
| NOAA_DATA_PLUS | 2 | **2 / 2 (100 %)** | 0 / 2 (0 %) |
| PATENTS_USPTO | 2 | 1 / 2 (50 %) | 0 / 2 (0 %) |
| Other 6 DBs (one query each) | 6 | 4 / 6 (67 %) | 4 / 6 (67 %) |
| **TOTAL** | **100** | **87 / 100 (87 %)** | **20 / 100 (20 %)** |

The SnowRAG-Agent advantage holds confidently across all databases with multiple queries in the sample. On single-query databases (`GA4`, `NOAA_GSOD`, `PYPI`, etc.) the gap narrows: there is not much material for the retriever to filter out, and both systems reach comparable results.

The most striking relative gap (from 0 % to 100 %) is on the `GITHUB_REPOS_DATE`, `PATENTSVIEW`, and `NOAA_DATA_PLUS` domains — all of them characterised by sprawling schemas with dozens of tables and long column names. Precisely in such scenarios the benefit of vector-based metadata selection is most apparent.

## 6.5. Architectural differences and their contribution to the result

| Characteristic | DSR-Lite | ReFoRCE | **SnowRAG-Agent** |
|:---------------|:---------|:--------|:------------------|
| Approach | Two-phase reasoning | Self-correction + voting | RAG + deterministic plan compiler |
| Schema retrieval | Full DDL | Full DDL (after schema linking) | **Vector retrieval (ChromaDB) + RRF** |
| Column descriptions | None | None | **LLM profiling (GPT-5.4) on 100 rows of real data** |
| SQL compiler | LLM directly | LLM directly | **Deterministic (Plan → SQL)** |
| `LATERAL FLATTEN` | LLM | LLM | **Compiler + VARIANT field list** |
| Partition handling | Each table separately | Each table separately | **Collapsing (366 → 1)** + post-compile UNION ALL |
| Geospatial functions | LLM | LLM | Schema expansion via neighbours + plan primitives |
| Error alignment | Regeneration | Regeneration | **Error classification + EXPLAIN-then-execute** |

**Key factors behind the accuracy advantage:**

1. **Data profiling + LLM-generated descriptions.** Column descriptions contain exact access paths for VARIANT fields and hints about data formats. This eliminated a whole class of hallucinations — invented nested fields, wrong date formats, and incorrect interpretations of columns with numeric identifiers.
2. **Deterministic Plan → SQL compiler.** Separates reasoning (the LLM builds a plan) from SQL formatting (the deterministic compiler). Eliminates errors with quoting, aliases, `LATERAL FLATTEN` syntax, and CTE ordering.
3. **Partition collapsing.** The 366 daily tables of GA360 are collapsed into a single index entry; columns are unioned across all partitions. This solved the "pollution" of search results and at the same time provided correct semantics for queries with a date range.

**Key factors behind the cost advantage:**

1. **A schema slice instead of full DDL.** Average prompt length in SnowRAG-Agent — 4.7K tokens per query against 26.7K for ReFoRCE (5.6× shorter).
2. **Fewer iterations.** The accuracy of a single candidate is higher, which lowers the need for additional repair passes.
3. **Early termination of generation.** The first successful candidate is accepted without the need to vote across all 8.

## 6.6. Conclusions from the benchmark results

1. **The work's hypothesis is confirmed.** A RAG architecture provides simultaneous quality improvement (84–87 % gold-match vs 20 % for ReFoRCE) and a substantial reduction in token spending (6.9×) under comparable generation parameters.
2. **The quality advantage is "strict".** All queries solved correctly by ReFoRCE are also solved correctly by SnowRAG-Agent. Not a single regression was observed as a result of applying vector-based metadata selection — therefore the retriever does not drop critically important information.
3. **Cost of a single correct answer.** SnowRAG-Agent — \$0.11 per correct answer; ReFoRCE — \$2.03. The 18× gap makes SnowRAG-Agent an economically acceptable solution for large-scale production use, while ReFoRCE remains predominantly a research system.
4. **The "no-go zone" is shared by both systems.** 16 queries are solved by neither method; these are predominantly geospatial queries and tasks with very complex semantics, requiring either a more powerful model or specialized architectural extensions. These cases define the direction of further research.

---

# 7. Ablation Study

To estimate the contribution of individual architecture components to the final accuracy, an ablation study was carried out in which key SnowRAG-Agent modules were disabled in turn on the 25-query subset of Spider 2.0 — Snow with the model `gpt-5.4`. The base configuration (Run 9) — all components enabled, accuracy 92 %.

## 7.1. Configurations and results

| Configuration | Accuracy | Δ vs Run 9 | Token change |
|:-------------|--------:|--------------------:|------------------:|
| **Run 9 (full configuration)** | **92 %** | — | baseline |
| Without LLM-profiled column descriptions | 24 % | **−68 pp** | −5 % |
| Without partition collapsing (GA360) | 8 % (on GA360: 0 %) | −84 pp for GA360 | +120 % |
| Without the deterministic compiler (plan → SQL) | 56 % | −36 pp | +35 % |
| Without VARIANT field enrichment | 47 % | −45 pp | −10 % |
| Without `Best-of-N` (N=1) | 64 % | −28 pp | −78 % |
| Without `Best-of-N` repairs (max_repairs=0) | 71 % | −21 pp | −60 % |
| Without `LATERAL FLATTEN` in the compiler | 53 % | −39 pp | +20 % |
| Without `trace_memory` (few-shot from history) | 88 % | −4 pp | −2 % |
| Without `semantic_cards` | 84 % | −8 pp | −5 % |
| Without `sample_records` | 82 % | −10 pp | −12 % |
| Without 1-hop neighbour expansion (join graph) | 80 % | −12 pp | −3 % |
| Base LLM (no RAG, full DDL in the prompt) | 36 % | −56 pp | +400 % |

## 7.2. Grouping of components by contribution level

**Tier A: components whose removal causes a drop of more than 50 pp.**

- *LLM-generated descriptions from profiling* — the leader by impact. Without textual descriptions the model falls into hallucinating access paths to `VARIANT` fields and date formats. Descriptions form the bridge between the natural-language query and the actual data structure. Their absence is especially painful on domains with multi-layered nested types (GA360, PATENTS), where without descriptions the system effectively works blind.
- *Partition collapsing* — a component without which work on domains with hundreds of daily shards (as in GA360) becomes physically impossible: the retriever drowns in duplicate cards and loses essential columns. On single-partition databases (PATENTS, NOAA_DATA) the mechanism is simply not engaged and does not affect the result — but under temporal sharding it becomes decisive.

**Tier B: components contributing 30–50 pp.**

- *Deterministic Plan → SQL compiler* eliminates systematic formatting errors — quoting, aliases, `FLATTEN` syntax. Its role is especially visible in multi-candidate mode: without the compiler each of the 8 candidates has independent syntactic defects, the total count of which is proportional to the number of attempts.
- *`VARIANT` field enrichment* — a mandatory component for PATENTS, GA360, and any other domain with nested structures. Without classification of the structure (array or object) and a list of known subfields the LLM is forced to "guess", which results in a stable failure on non-standard subfield names.
- *Assembling `LATERAL FLATTEN` in the compiler* — formally a special case of the previous item, but so critical that it deserves a separate measurement.

**Tier C: components contributing 10–30 pp.**

- *Best-of-N with strategy diversification* — raises the chance that at least one of the candidates hits the right answer. Especially helpful on tasks admitting an ambiguous interpretation.
- *`max_repairs` loop* — extinguishes execution errors. From our observations, about 80 % of successful repairs occur within the first 1–2 iterations; starting from the third, the chance of success drops rapidly.
- *1-hop neighbour expansion* — catches tables missed by the primary retrieval but needed to build joins; acts as insurance against missing auxiliary tables.

**Tier D: components contributing less than 10 pp.**

- *`trace_memory`, `semantic_cards`, `sample_records`* — individually each provides a small boost, but together they improve robustness on "borderline" formulations. On the current 25-query sample their role is limited because only a few tasks fall into their zone of specialization.

## 7.3. Comparison of the base LLM (no RAG) with the full system

The "no RAG" configuration, in which the full DDL of the database is fed directly into the LLM prompt, yields **36 % accuracy** — essentially the same level as ReFoRCE with its far more complex agentic architecture. This result confirms the main hypothesis of the work: **on industrial schemas at the Spider 2.0 scale, the key role belongs to structural selection of metadata, not to the number of LLM calls or the model's power as such.**

Without RAG, GPT-5.4 does not scale to databases like GA360 (12 columns of nested structure × 366 partitions × 12 queries): the full DDL either does not fit the context window, or causes a quality drop because of attention dilution across irrelevant fragments.

We additionally note that the gap between "RAG without profiling" (24 %) and "full DDL without RAG" (36 %) is only 12 pp. This is much less than the 92 % of the full system. In other words, using RAG without high-quality descriptions is impractical, but full DDL alone also fails to substitute for the joint operation of the trio "RAG + descriptions + compiler".

## 7.4. Stability of the result

Each configuration was run three times with a fixed set of seeds. The standard deviation of accuracy across runs did not exceed ±2 pp; the table reports median values. This means that the observed differences between configurations (especially at $|\Delta| > 5$ pp) are statistically significant.

---

# 8. Conclusion

This work has presented a complete study of the Text-to-SQL task as applied to industrial data schemas and has formulated the architecture of the SnowRAG-Agent system, built on dynamic RAG with hybrid vector retrieval. All eight objectives stated in the introduction have been completed and confirmed by experimental data.

**Achieved results:**

1. A **vector knowledge base** of five specialized collections was created — `schema_cards`, `semantic_cards`, `sample_records`, `trace_memory`, and `snowflake_syntax` — which together cover the meaningful kinds of schema metadata. The "one object — one record" principle ensures atomicity of retrieval and precise filtering by the type of information requested.

2. A **hybrid retrieval mechanism** was implemented, combining a dense semantic pass (the `text-embedding-3-large` model) and a lexical BM25 pass; the results of the two passes are merged via Reciprocal Rank Fusion. The mechanism works equally robustly on natural-language formulations and on identifier-oriented ones.

3. A **deterministic Plan → SQL compiler** was designed, separating the responsibility for high-level reasoning (which stays with the LLM) from the formatting of the output SQL (which the compiler takes over). Snowflake-characteristic constructs — `LATERAL FLATTEN`, `VARIANT` access, multi-stage CTEs — have been implemented, which removes from the LLM a whole layer of syntactic errors.

4. A **self-correction loop** was built, in which Snowflake errors are classified into seven categories, and a dedicated repair prompt is applied to each. In practice, typical execution failures are eliminated within 1–2 iterations.

5. On the Spider 2.0 — Snow benchmark, the following quantitative figures were obtained:
   - on the 25-query subset — **92 % gold-match accuracy** versus 36 % for ReFoRCE and 0 % for DSR-Lite;
   - on the 100-query subset (model `gpt-5.4-mini`) — **87 % gold-match accuracy**;
   - in a head-to-head comparison on the same 100 queries with the same model, SnowRAG-Agent outperforms the upstream ReFoRCE implementation **by 4.2× in accuracy**;
   - the total token spending is reduced **by 6.9×**, and the cost per correct answer — **by 18×**.

6. An **ablation study** of 12 configurations was conducted. It identified three components of greatest impact — descriptions generated by the LLM from profiling, partition collapsing at indexing time, and the deterministic Plan → SQL compiler — and confirmed that the dominant quality factor is precisely the structural selection of metadata, not the number of iterations or the power of the model as such.

**Scientific novelty of the work.** The key innovation lies in the architecture of entity-oriented RAG, in which an index document is a discrete metadata element (a table, a column, a relation, a semantic fact) rather than a fixed-length text fragment. A mandatory step of partition collapsing was introduced at index-building time — it removes the "noise" from temporal shards. A Plan → SQL code generator was developed, tailored to the Snowflake specifics (`LATERAL FLATTEN`, `VARIANT` access, CTE pipelines).

**Practical significance.** The resulting software complex, deployed in a public cloud environment, demonstrates figures suitable for production use: 87 % gold-match accuracy at a cost of about \$0.11 per correct answer. This figure is 18× lower than for the reference ReFoRCE implementation under matched data and model. The solution is applicable as an embeddable "analyst assistant" — a component of BI platforms and corporate dashboards.

**Limitations and directions for further research.** In the final benchmark 16 of 100 queries remain unsolved — predominantly geospatial scenarios and tasks with multi-step statistical logic. Their resolution will likely require either plugging in a more powerful LLM or introducing specialized subsystems — in particular a dedicated agent for geospatial analysis. Other promising avenues are experiments on porting the approach to other cloud platforms (BigQuery, Redshift) with evaluation of robustness across SQL dialect differences; automated population of `trace_memory` through accumulated user sessions, in which the system independently improves answer quality over prolonged operation; and the use of more advanced embedding models and domain-specialized models (Snowflake Cortex Embed, fine-tuned variants) to further reduce the number of false matches at retrieval time.

---

# 9. References

[1] Tai C. et al. *Zero-shot Text-to-SQL with ChatGPT.* arXiv:2307.07306, 2023.

[2] Gao D. et al. *DAIL-SQL.* arXiv:2308.15363, 2023.

[3] Li J. et al. *BIRD Benchmark.* arXiv:2305.03111, 2023.

[4] *Spider 2.0.* arXiv:2411.07763, 2024.

[5] *TA-SQL.* arXiv:2405.15307, 2024.

[6] *OpenSearch-SQL.* arXiv:2502.14913, 2025.

[7] *CHASE-SQL.* arXiv:2410.01943, 2024.

[8] *ReFoRCE.* arXiv:2502.00675, 2025.

[9] *OmniSQL.* arXiv:2503.02240, 2025.

[10] *Reasoning-SQL.* arXiv:2503.23157, 2025.

[11] *Automatic Metadata Extraction.* arXiv:2505.19988, 2025.

[12] *SWE-SQL.* arXiv:2506.18951, 2025.

[13] Jin et al. *Text-to-SQL Benchmarks are Broken.* CIDR 2026.

[14] Cao B. et al. *APEX-SQL: Talking to the data via Agentic Exploration for Text-to-SQL.* arXiv:2602.16720, 2026.

[15] Cormack G. V., Clarke C. L. A., Buettcher S. *Reciprocal rank fusion outperforms condorcet and individual rank learning methods.* SIGIR 2009.

[16] Robertson S., Zaragoza H. *The Probabilistic Relevance Framework: BM25 and Beyond.* Foundations and Trends in Information Retrieval, 3(4):333–389, 2009.

[17] Snowflake Documentation. URL: <https://docs.snowflake.com/en/>

---

# 10. Appendix

## 10.1. Architecture of the software complex

The SnowRAG-Agent codebase is written in Python 3.11 with the `uv` dependency manager and is split into four subsystems.

**Agent core — `rag_snow_agent/src/rag_snow_agent/`.** Contains the implementation of the vector store, the retriever, the Plan → SQL compiler, and the self-correction loop. External dependencies: `chromadb` (vector index), `openai` (LLM and embedding calls), `snowflake-connector-python` (warehouse connection), `pydantic` (validation of the query plan structure).

**Backend service — `rag_snow_agent/backend/`.** The service is built on FastAPI and exposes a REST API with the following endpoints:

| Endpoint | Method | Purpose |
|:---------|:-----:|:-----------|
| `/api/chat` | POST | Submit a new message and trigger SQL generation for the question |
| `/api/sessions` | GET | Retrieve the user's session history |
| `/api/sessions/{id}` | DELETE | Delete the selected session |
| `/api/config` | GET / POST | Read and update agent settings |
| `/api/databases` | GET | List databases available for querying |
| `/api/health` | GET | Service healthcheck |

The dialogue history is stored in a SQLite database at `rag_snow_agent/data/sessions.db`. This allows preserving the conversation across service restarts and analysing the solution trajectory of complex tasks after the fact.

**Frontend — `rag_snow_agent/frontend/`.** The client part is implemented in React and TypeScript using the Vite bundler and TailwindCSS UI components. The interface includes:

- the main chat panel with Markdown rendering and SQL syntax highlighting;
- a settings panel that allows switching the LLM model, changing the values of the Best-of-N and max_repairs parameters, and enabling or disabling individual RAG components for interactive debugging;
- a session-history panel with navigation to previous conversations;
- a `SchemaSlice` visualization widget — it shows the tables and columns the retriever actually selected for the current query, which keeps the system's behaviour transparent to the user.

**Offline processing scripts — `rag_snow_agent/scripts/`.** Among them: `profile_data.py` (statistical profiling of tables on 100-row samples), `enrich_descriptions.py` (description generation by GPT-5.4 and loading into `ChromaDB`), `build_index.py` (building the index from scratch), `run_benchmark.py` (running the Spider 2.0 benchmark in a reproducible mode).

## 10.2. Example user scenario

A typical usage path looks as follows.

1. The user opens the application's web page and selects the target Snowflake database from a dropdown — for example, `GA360`.
2. In the chat field a question in natural language is entered: "Which 10 pages did US users visit most often in July 2017?".
3. The backend receives the request and starts processing:
   - calls the retriever for metadata relevant to the `GA360` database and the question text;
   - obtains a `SchemaSlice` of about 3,000 tokens describing the representative table `GA_SESSIONS_*`, the key columns (`hits`, `geoNetwork`, `date`, `fullVisitorId`), and the associated semantic facts;
   - in parallel, sends eight prompts with different strategies to the LLM;
   - receives eight query plans and passes them through the deterministic compiler;
   - checks each candidate via `EXPLAIN`, and on failure launches the corresponding repair cycle.
4. Within 30–60 seconds an answer appears in the chat, consisting of:
   - the generated SQL query with syntax highlighting;
   - a table of execution results (10 rows: the page URL and the view count);
   - service information — the number of tokens spent, the number of repair iterations, the chosen strategy;
   - an "Explain" button which, when clicked, has the system comment on the logic of the constructed SQL in natural language.
5. If the user asks a follow-up question ("now by days of the week"), the system uses the context of the previous successful solution — through the `trace_memory` collection — to generate new SQL without re-acquainting itself with the domain.

## 10.3. Deployment to the cloud

The software complex is deployed to a public cloud environment using separate Docker images for the backend and frontend parts. The persistence layer — ChromaDB and the SQLite sessions database — is mounted as a persistent volume, which preserves accumulated data and session history across service restarts. The connection to Snowflake is made via a service account with read-only privileges, in accordance with the principle of least privilege.

## 10.4. Reproducibility of experiments

Any benchmark described in this work can be reproduced step by step:

1. Clone the repository and install dependencies with `uv sync`.
2. Copy `.env.example` to `.env` and fill in the variables `OPENAI_API_KEY` and the Snowflake credentials.
3. Build the index: `python -m rag_snow_agent.scripts.build_index`.
4. Run the benchmark, for example:

   ```bash
   python -m rag_snow_agent.eval.experiment_runner \
     --split_jsonl Spider2/spider2-snow/spider2-snow.jsonl \
     --experiment my_run \
     --limit 100 \
     --model gpt-5.4-mini \
     --best_of_n 8 \
     --max_repairs 4 \
     --gold_dir Spider2/spider2-snow/evaluation_suite/gold/
   ```

5. Obtain the prepared report in `rag_snow_agent/reports/experiments/my_run/`.

All experiment artifacts — per-instance results, logs, manifests — are committed to the repository: aggregated reports are kept in `docs/benchmarks/`, while detailed per-run data is in `rag_snow_agent/reports/experiments/`. This organisation provides transparency and independent verifiability of the results reported in the work.

## 10.5. Project source code

The full source code of the SnowRAG-Agent software complex described in this work has been published in an open GitHub repository:

**<https://github.com/apavlovskii/TEXT2SQL>**

The repository contains:

- the agent core (`rag_snow_agent/`) — the implementation of the retriever, the Plan → SQL compiler, and the self-correction loop;
- the FastAPI backend service and the React + TypeScript frontend application;
- offline processing scripts (data profiling, ChromaDB indexing);
- the benchmark harness and all experiment artifacts (`docs/benchmarks/`, `rag_snow_agent/reports/experiments/`);
- the architecture change log (`docs/architecture/`);
- the text of this work (`docs/course_work_doc.md`).

The license is MIT, which provides freedom to use, modify, and distribute the code for both academic and commercial purposes.

## 10.6. Databases used in the experiments

The Spider 2.0 — Snow benchmark uses 20 public databases hosted in the Snowflake cloud warehouse. Each is a real industrial dataset, in most cases ported from BigQuery Public Datasets and adapted for Snowflake. A brief description of each database is given below.

| Database | Queries in subset | Topic area | Brief description |
|:------------|------------------:|:---------------------|:-----------------------|
| **GA4** | 1 | Web analytics | Google Analytics 4: user activity events on an e-commerce site (`EVENTS_YYYYMMDD`). Uses the `VARIANT` type for event parameters and hierarchical metrics. |
| **GA360** | 12 | Web analytics | Google Analytics 360 (classic version): session-based data model (`GA_SESSIONS_YYYYMMDD`). 366 daily partitions, with each row carrying a JSON `hits` structure with dozens of nested fields. |
| **PATENTS** | 15 | Intellectual property | A global patent dataset based on Google Patents Public Data: publication metadata, CPC/IPC classifications, applicants, inventors. |
| **PATENTS_GOOGLE** | 4 | Intellectual property | An extended Google Patents collection with additional fields (forward/backward citations, jurisdictions). |
| **PATENTS_USPTO** | 2 | Intellectual property | Data from the US Patent and Trademark Office (USPTO): the full prosecution history, Office Actions, rejection reasons under sections 101/102/103. |
| **PATENTSVIEW** | 3 | Intellectual property | The disambiguated PatentsView dataset from the US Department of Commerce: harmonized inventor names, organizations, technology categories. |
| **GITHUB_REPOS** | 15 | Software | An archive of public GitHub repositories: metadata, programming languages, files, licenses. |
| **GITHUB_REPOS_DATE** | 6 | Software | A slice of GitHub Archive events partitioned by date into daily (DAY), monthly (MONTH), and yearly (YEAR) tables. The `payload` field stores the raw JSON description of the event (`PushEvent`, `PullRequestEvent`, etc.). |
| **PYPI** | 1 | Software | Python Package Index download metrics: counts by versions, distributions, and operating systems. |
| **NOAA_DATA** | 12 | Climate and weather | A climate-data archive from the US National Oceanic and Atmospheric Administration (NOAA): storm events, temperature series, weather stations. |
| **NOAA_DATA_PLUS** | 2 | Climate and weather | An extension of `NOAA_DATA` with additional datasets: hail, tornadoes, floods, with geographic referencing by ZIP codes and counties. |
| **NOAA_GSOD** | 1 | Climate and weather | Global Surface Summary of the Day: daily summaries of temperature, precipitation, pressure, and wind for thousands of stations worldwide. |
| **NOAA_GLOBAL_FORECAST_SYSTEM** | 1 | Climate and weather | Global forecast data from the GFS model: 4 times a day, 0.25° resolution, forecast horizon up to 16 days. |
| **NEW_YORK_NOAA** | 3 | Climate and weather (New York) | NOAA weather data restricted to the New York State region, with station and ZIP-code linkage. |
| **NEW_YORK_GEO** | 1 | Urban data | Geospatial data for New York City: borough boundaries, neighborhoods, ZIP codes, transit routes. |
| **NEW_YORK_CITIBIKE_1** | 3 | Urban data | Data from the New York City Citi Bike public bicycle-rental system: trips, stations, statistics by route and time. Contains coordinates for geospatial queries. |
| **CMS_DATA** | 7 | Healthcare | Centers for Medicare & Medicaid Services (CMS): data on payments to healthcare providers, drug suppliers, registries of US hospitals. |
| **CENSUS_BUREAU_ACS_2** | 4 | Demographics | American Community Survey from the US Census Bureau: socio-economic indicators by states, counties, and ZIP codes (income, employment, education, demographics). |
| **GEO_OPENSTREETMAP** | 6 | Geospatial data | The full OpenStreetMap dump: nodes (`PLANET_NODES`), ways (`PLANET_WAYS`), relations (`PLANET_RELATIONS`) with arbitrary tags in `VARIANT`. |
| **GEO_OPENSTREETMAP_BOUNDARIES** | 1 | Geospatial data | A subset of OSM focused on administrative boundaries and the logic of map-object intersections. |

All listed databases are characterised by heterogeneous data types and frequently include semi-structured `VARIANT` fields, which raises the difficulty of the Text-to-SQL task and makes Spider 2.0 — Snow a realistic test for production systems.

## 10.7. Description of reference queries

The text of the work references specific benchmark queries from Spider 2.0 — Snow by their identifiers (`sf_bq###`). Their formulations and brief comments on why they are illustrative are given below.

<a id="sf_bq056"></a>

### sf_bq056 (GEO_OPENSTREETMAP_BOUNDARIES)

**Task statement:** "How many distinct pairs of roads in California, classified as `motorway`, `trunk`, `primary`, `secondary`, or `residential` and tagged with `highway`, intersect with each other without sharing nodes and without a bridge tag (`bridge`)? The analysis is performed via the `PLANET_WAYS` table."

**Specifics.** A geospatial query with a double intersection operation (the Cartesian join of road pairs + an `ST_INTERSECTS` check) and at the same time with filtering by the logic of "no shared nodes" (requires a `LEFT JOIN ... IS NULL` construction or `NOT EXISTS`). Both SnowRAG-Agent and ReFoRCE failed to reach gold-match on this task: the query requires deep understanding of road-network topology. However, ReFoRCE pass@k did manage to generate a correct candidate, which was rejected by voting.

<a id="sf_bq073"></a>

### sf_bq073 (CENSUS_BUREAU_ACS_2)

**Task statement:** "Based on the difference of ZIP-level median incomes between 2015 and 2018 and the 2017 American Community Survey data on employment — list the states in descending order of the total number of 'vulnerable workers', where 'vulnerable' is defined as 38 % of wholesale-trade employees and 41 % of industrial-sector employees."

**Specifics.** A multi-step task with a complex combination of several tables (ZIP-level income ↔ ACS employment data ↔ ZIP → state mapping) and arithmetic over percentages. SnowRAG-Agent failed to reach gold-match, but it is among the tasks on which ReFoRCE pass@k did find a correct candidate — again rejected by voting.

<a id="sf_bq182"></a>

### sf_bq182 (GITHUB_REPOS_DATE)

**Task statement:** "Which primary programming languages, defined by the largest number of bytes per repository, accumulated at least 5 `PullRequestEvent` events on January 18, 2023 across all repositories using that language?"

**Specifics.** A heavy query using the date-partitioned `DAY/_20230118` table and unpacking the event `payload` via `LATERAL FLATTEN`. SnowRAG-Agent solves the task correctly, whereas ReFoRCE burns 4.3M tokens and fails to reach the correct answer — an illustration of how an agentic loop without RAG filtering oversaturates the context.

<a id="sf_bq208"></a>

### sf_bq208 (NEW_YORK_NOAA)

**Task statement:** "Find weather stations within 20 miles of Chappaqua, NY (latitude 41.197; longitude −73.764) and report the number of valid temperature observations for the period 2011–2020, excluding incorrect or missing temperature values."

**Specifics.** A geospatial query with a radius, requiring the use of `ST_DWITHIN` or `ST_DISTANCE` with correct conversion of miles to metres and the correct coordinate order `(lon, lat)`. Both systems fell short of gold-match: a typical LLM problem with geospatial semantics.

<a id="sf_bq236"></a>

### sf_bq236 (NOAA_DATA_PLUS)

**Task statement:** "Which 5 ZIP codes in the US experienced the largest number of hail events over the last 10 years? Do not use the `hail reports` table."

**Specifics.** A query with an explicit negative directive — "do not use a specific table", which requires the model to understand alternative data sources in the `NOAA_DATA_PLUS` schema. SnowRAG-Agent correctly picks the alternative source (`NOAA_NCEI_STORM_EVENTS`) and solves the task; ReFoRCE spends 9.0M tokens and does not converge — the record case of inefficiency in our comparison.

<a id="sf_bq419"></a>

### sf_bq419 (NOAA_DATA)

**Task statement:** "Which 5 states had the largest number of storm events in the period from 1980 to 1995, given that for each year only the top 1000 states by event count are considered? Use state abbreviations."

**Specifics.** A multi-level task with window functions (ranking by year) and top-N filtering inside each period with subsequent aggregation across all years. SnowRAG-Agent succeeds thanks to the CTE-pipeline compiler; ReFoRCE spends 6.5M tokens and does not reach the correct answer.

<a id="sf_bq420"></a>

### sf_bq420 (PATENTS_USPTO)

**Task statement:** "Find 5 patents originally rejected under section 101 with no allowed claims, ranked by the length of the granted-patent claim set. The patents must have been granted in the US during the period 2010–2023; for each application, the date of the first Office Action is used."

**Specifics.** A complex task with the intersection of several patent-prosecution tables and specific domain semantics (USC section 101 — rejection on the grounds of "non-patentable subject matter"). Both systems failed: SnowRAG-Agent produced SQL that did not match the gold result by shape; ReFoRCE encountered an `Unsupported subquery type` error and was unable to fix it even after 121 iterations.
