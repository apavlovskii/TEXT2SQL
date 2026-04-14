"""Async wrapper around the synchronous rag_snow_agent pipeline."""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

log = logging.getLogger(__name__)

# Heuristic to detect NON-data questions (inverted: default is data query).
# Only bypass the agent for clearly conversational/general questions.
_GENERAL_PATTERNS = re.compile(
    r"^(hi|hello|hey|thanks|thank you|bye|goodbye|good morning|good evening)\b|"
    r"^(what is your name|who are you|who made you|how are you|tell me a joke|"
    r"what can you do|help me understand|explain the concept|what does .+ mean|"
    r"define |summarize this|translate |write a poem|write a story|"
    r"what is the capital of|what is the population of|"
    r"how do I install|how to configure|what programming language)\b",
    re.IGNORECASE,
)


@dataclass
class AgentResult:
    """Holds both the agent's InstanceResult and the re-execution result."""

    final_sql: str = ""
    success: bool = False
    error_message: str | None = None
    llm_calls: int = 0
    repair_count: int = 0
    candidate_count: int = 1
    model: str = ""
    columns: list[str] | None = None
    rows: list[list[Any]] | None = None
    row_count: int = 0
    elapsed_ms: int | None = None
    execution_log: list[str] = field(default_factory=list)
    is_general_answer: bool = False


class AgentAdapter:
    """Async wrapper that runs the synchronous agent in a thread pool."""

    def __init__(self, chroma_store, retriever, config, executor_factory):
        self._chroma_store = chroma_store
        self._retriever = retriever
        self._config = config
        self._executor_factory = executor_factory
        self._pool = ThreadPoolExecutor(max_workers=4)
        self._call_llm = None
        self._ready = False

        try:
            from rag_snow_agent.agent.llm_client import call_llm
            self._call_llm = call_llm
            self._ready = True
        except ImportError:
            log.warning("rag_snow_agent not importable; agent adapter will return stubs")

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def call_llm_fn(self):
        return self._call_llm

    def is_data_question(self, question: str) -> bool:
        """Heuristic: default True (route to agent). Only False for clearly general questions."""
        return not bool(_GENERAL_PATTERNS.search(question.strip()))

    async def answer_general_question(
        self,
        question: str,
        model: str,
        progress_callback: Callable[[str], None] | None = None,
    ) -> AgentResult:
        """Answer a non-data question directly using the LLM (no agent/RAG)."""
        if not self._call_llm:
            return AgentResult(error_message="LLM not available")

        if progress_callback:
            progress_callback("This doesn't look like a data query — answering directly...")

        execution_log = ["Detected as general question (not data-related)", f"Using model: {model}"]

        messages = [
            {"role": "system", "content": "You are a helpful analytics assistant. Answer the user's question concisely."},
            {"role": "user", "content": question},
        ]

        loop = asyncio.get_event_loop()
        try:
            answer = await loop.run_in_executor(
                self._pool,
                functools.partial(self._call_llm, messages=messages, model=model),
            )
            execution_log.append("LLM response received")
            return AgentResult(
                success=True,
                model=model,
                llm_calls=1,
                execution_log=execution_log,
                is_general_answer=True,
            )
        except Exception as exc:
            return AgentResult(error_message=str(exc), execution_log=execution_log)

    async def run_query(
        self,
        question: str,
        db_id: str,
        model: str = "gpt-4o-mini",
        max_retries: int = 10,
        max_candidates: int = 2,
        datasource: str = "sqlite",
        progress_callback: Callable[[str], None] | None = None,
    ) -> AgentResult:
        """Run the full agent pipeline asynchronously."""
        if not self._ready:
            return AgentResult(error_message="Agent not initialized")

        # Check if this is a general question (Change 9)
        if not self.is_data_question(question):
            result = await self.answer_general_question(question, model, progress_callback)
            if result.is_general_answer:
                return result

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                self._pool,
                functools.partial(
                    self._run_sync, question, db_id, model,
                    max_retries, max_candidates, datasource, progress_callback,
                ),
            )
            return result
        except Exception as exc:
            log.error("Agent pipeline failed: %s", exc, exc_info=True)
            return AgentResult(error_message=str(exc))

    def _run_sync(
        self,
        question: str,
        db_id: str,
        model: str,
        max_retries: int,
        max_candidates: int,
        datasource: str,
        progress_callback: Callable[[str], None] | None,
    ) -> AgentResult:
        """Synchronous agent pipeline (runs in thread pool)."""
        from rag_snow_agent.agent.agent import solve_instance
        from rag_snow_agent.retrieval.debug_retrieve import build_schema_slice

        config = self._config
        execution_log: list[str] = []

        def _log(msg: str):
            execution_log.append(msg)
            if progress_callback:
                progress_callback(msg)

        _log(f"Starting query pipeline (model={model}, candidates={max_candidates}, max_retries={max_retries})")
        _log(f"Database: {db_id}, Datasource: {datasource}")

        # Step 1: Build schema slice
        _log("Retrieving schema from vector database...")
        schema_slice, _, _ = build_schema_slice(
            retriever=self._retriever,
            query=question,
            db_id=db_id,
            top_k_tables=config.TOP_K_TABLES,
            top_k_columns=config.TOP_K_COLUMNS,
            max_schema_tokens=config.MAX_SCHEMA_TOKENS,
        )
        n_tables = len(schema_slice.tables)
        n_cols = sum(len(t.columns) for t in schema_slice.tables)
        _log(f"Schema retrieved: {n_tables} tables, {n_cols} columns (~{schema_slice.token_estimate} tokens)")

        # Step 2: Retrieve semantic context
        _log("Retrieving semantic context...")
        semantic_context = None
        try:
            from rag_snow_agent.retrieval.semantic_retriever import retrieve_semantic_context
            semantic_context = retrieve_semantic_context(
                db_id=db_id, instruction=question,
                chroma_store=self._chroma_store, top_k=8,
            )
            _log(f"Semantic context: {len(semantic_context or '')} chars")
        except Exception:
            _log("Semantic context retrieval failed (continuing without)")

        # Step 3: Retrieve sample records
        _log("Retrieving sample records...")
        sample_context = None
        try:
            from rag_snow_agent.chroma.sample_records import SampleRecordStore, build_sample_context
            sample_store = SampleRecordStore(self._chroma_store)
            table_fqns = [t.qualified_name for t in schema_slice.tables]
            table_docs = sample_store.get_sample_context_for_tables(db_id, table_fqns)
            sample_context = build_sample_context(table_docs, max_tokens=800)
            _log(f"Sample records: {len(sample_context or '')} chars")
        except Exception:
            _log("Sample records retrieval failed (continuing without)")

        # Step 4: Question decomposition
        _log("Decomposing question into subgoals...")

        # Step 5: Generate SQL with retries (Change 3)
        best_result = None
        for attempt in range(1, max_retries + 1):
            _log(f"Attempt {attempt}/{max_retries}: Generating {max_candidates} candidate SQL(s)...")
            _log(f"  Calling LLM ({model}) for plan generation...")

            executor = self._executor_factory(db_id)
            try:
                instance_result = solve_instance(
                    instance_id=str(uuid4()),
                    instruction=question,
                    db_id=db_id,
                    schema_slice=schema_slice,
                    model=model,
                    executor=executor,
                    temperature=config.LLM_TEMPERATURE,
                    max_tokens=4096,
                    max_repairs=2,
                    best_of_n=max_candidates,
                    chroma_store=self._chroma_store,
                    semantic_context=semantic_context,
                    decompose=True,
                    sample_context=sample_context,
                    memory_enabled=False,
                )
            finally:
                executor.close()

            final_sql = instance_result.final_sql
            is_select1 = not final_sql or final_sql.strip().startswith("SELECT 1")

            if not is_select1:
                _log(f"  Generated valid SQL ({len(final_sql)} chars)")
                best_result = instance_result
                break
            else:
                _log(f"  Attempt {attempt} failed: compiler produced SELECT 1 (empty plan)")
                if attempt < max_retries:
                    _log(f"  Retrying with different strategy...")

        if best_result is None:
            _log("All attempts exhausted — unable to generate valid query")
            return AgentResult(
                error_message="Unable to generate valid query",
                model=model,
                execution_log=execution_log,
            )

        # Step 6: Re-execute final SQL
        _log("Executing final SQL against database...")
        columns = None
        rows = None
        row_count = 0
        elapsed_ms = None

        executor2 = self._executor_factory(db_id)
        try:
            exec_result = executor2.execute(best_result.final_sql, sample_rows=config.MAX_RESULT_ROWS)
            if exec_result.success:
                columns = exec_result.column_names
                rows = [list(r) for r in (exec_result.rows_sample or [])]
                row_count = exec_result.row_count or 0
                elapsed_ms = exec_result.elapsed_ms
                _log(f"Query executed: {row_count} rows in {elapsed_ms}ms")
            else:
                _log(f"Execution failed: {exec_result.error_message}")
        finally:
            executor2.close()

        _log("Pipeline complete")

        return AgentResult(
            final_sql=best_result.final_sql,
            success=best_result.success or (columns is not None),
            error_message=best_result.error_message,
            llm_calls=best_result.llm_calls,
            repair_count=len(best_result.repair_trace),
            candidate_count=best_result.candidate_count,
            model=model,
            columns=columns,
            rows=rows,
            row_count=row_count,
            elapsed_ms=elapsed_ms,
            execution_log=execution_log,
        )
