"""High-level agent: solve a single Spider2-Snow instance."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from ..chroma.chroma_store import ChromaStore
from ..chroma.trace_memory import TraceMemoryStore
from ..retrieval.hybrid_retriever import HybridRetriever
from ..retrieval.schema_slice import SchemaSlice
from ..snowflake.executor import SnowflakeExecutor
from .best_of_n import run_best_of_n
from .memory import make_trace_record
from .plan_sql_pipeline import PipelineResult, run_pipeline
from .refiner import RepairTraceItem, refine_sql

log = logging.getLogger(__name__)


@dataclass
class InstanceResult:
    """Full result for one Spider2-Snow instance."""

    instance_id: str
    db_id: str
    instruction: str
    final_sql: str
    success: bool
    pipeline_result: PipelineResult | None = None
    repair_trace: list[RepairTraceItem] = field(default_factory=list)
    llm_calls: int = 0
    error_message: str | None = None
    # Best-of-N metadata
    best_of_n_used: bool = False
    candidate_count: int = 1
    selection_reason: str | None = None
    candidate_summaries: list[dict] = field(default_factory=list)


def _persist_trace(
    instance_id: str,
    db_id: str,
    instruction: str,
    schema_slice: SchemaSlice,
    plan=None,
    final_sql: str = "",
    repair_trace=None,
    candidate_record: dict | None = None,
    chroma_dir: str | None = None,
) -> None:
    """Best-effort persist of a trace record. Never raises."""
    try:
        record = make_trace_record(
            instance_id=instance_id,
            db_id=db_id,
            instruction=instruction,
            schema_slice=schema_slice,
            plan=plan,
            final_sql=final_sql,
            repair_trace=repair_trace,
            candidate_record=candidate_record,
        )
        store = TraceMemoryStore(persist_dir=chroma_dir)
        store.upsert_trace(record.to_dict())
        log.debug("Persisted trace %s for instance %s", record.trace_id, instance_id)
    except Exception:
        log.warning("Failed to persist trace for %s", instance_id, exc_info=True)


def solve_instance(
    instance_id: str,
    instruction: str,
    db_id: str,
    schema_slice: SchemaSlice,
    model: str,
    executor: SnowflakeExecutor,
    temperature: float = 0.2,
    max_tokens: int = 800,
    max_repairs: int = 2,
    explain_first: bool = True,
    stop_on_repeated_error: bool = True,
    best_of_n: int = 1,
    candidate_strategies: list[str] | None = None,
    selector_scoring: dict | None = None,
    chroma_dir: str | None = None,
    memory_enabled: bool = True,
    chroma_store: ChromaStore | None = None,
) -> InstanceResult:
    """Solve one instance.

    If *best_of_n* > 1, generates N candidates, executes+repairs each, and
    selects the best. Otherwise uses the single-candidate M4 flow.
    """
    if best_of_n > 1:
        result = _solve_best_of_n(
            instance_id=instance_id,
            instruction=instruction,
            db_id=db_id,
            schema_slice=schema_slice,
            model=model,
            executor=executor,
            temperature=temperature,
            max_tokens=max_tokens,
            max_repairs=max_repairs,
            explain_first=explain_first,
            stop_on_repeated_error=stop_on_repeated_error,
            n=best_of_n,
            strategies=candidate_strategies,
            scoring=selector_scoring,
            chroma_store=chroma_store,
        )
        if memory_enabled and result.success:
            _persist_trace(
                instance_id=instance_id,
                db_id=db_id,
                instruction=instruction,
                schema_slice=schema_slice,
                final_sql=result.final_sql,
                chroma_dir=chroma_dir,
            )
        return result

    result = _solve_single(
        instance_id=instance_id,
        instruction=instruction,
        db_id=db_id,
        schema_slice=schema_slice,
        model=model,
        executor=executor,
        temperature=temperature,
        max_tokens=max_tokens,
        max_repairs=max_repairs,
        explain_first=explain_first,
        stop_on_repeated_error=stop_on_repeated_error,
        chroma_store=chroma_store,
    )
    if memory_enabled and result.success:
        _persist_trace(
            instance_id=instance_id,
            db_id=db_id,
            instruction=instruction,
            schema_slice=schema_slice,
            plan=result.pipeline_result.plan if result.pipeline_result else None,
            final_sql=result.final_sql,
            repair_trace=result.repair_trace,
            chroma_dir=chroma_dir,
        )
    return result


def _solve_single(
    instance_id: str,
    instruction: str,
    db_id: str,
    schema_slice: SchemaSlice,
    model: str,
    executor: SnowflakeExecutor,
    temperature: float,
    max_tokens: int,
    max_repairs: int,
    explain_first: bool,
    stop_on_repeated_error: bool,
    chroma_store: ChromaStore | None = None,
    retriever: HybridRetriever | None = None,
) -> InstanceResult:
    """Single-candidate flow (Milestone 4)."""
    result = InstanceResult(
        instance_id=instance_id,
        db_id=db_id,
        instruction=instruction,
        final_sql="",
        success=False,
    )

    log.info("Solving instance %s: %s", instance_id, instruction[:80])
    pipeline_result = run_pipeline(
        db_id=db_id,
        instruction=instruction,
        schema_slice=schema_slice,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        retriever=retriever,
    )
    result.pipeline_result = pipeline_result
    result.llm_calls += pipeline_result.llm_calls

    initial_sql = pipeline_result.sql
    if not initial_sql or initial_sql.startswith("SELECT 1"):
        result.final_sql = initial_sql
        result.error_message = "Pipeline failed to generate valid SQL"
        return result

    final_sql, trace, exec_result = refine_sql(
        db_id=db_id,
        instruction=instruction,
        schema_slice=schema_slice,
        sql=initial_sql,
        executor=executor,
        model=model,
        temperature=0.0,
        max_tokens=max_tokens,
        max_repairs=max_repairs,
        explain_first=explain_first,
        stop_on_repeated_error=stop_on_repeated_error,
        chroma_store=chroma_store,
    )

    result.final_sql = final_sql
    result.repair_trace = trace
    result.llm_calls += len(trace)

    if exec_result:
        result.success = exec_result.success
        if not exec_result.success:
            result.error_message = exec_result.error_message
    else:
        result.error_message = "No execution result"

    log.info(
        "Instance %s: success=%s repairs=%d llm_calls=%d",
        instance_id, result.success, len(trace), result.llm_calls,
    )
    return result


def _solve_best_of_n(
    instance_id: str,
    instruction: str,
    db_id: str,
    schema_slice: SchemaSlice,
    model: str,
    executor: SnowflakeExecutor,
    temperature: float,
    max_tokens: int,
    max_repairs: int,
    explain_first: bool,
    stop_on_repeated_error: bool,
    n: int,
    strategies: list[str] | None,
    scoring: dict | None,
    chroma_store: ChromaStore | None = None,
) -> InstanceResult:
    """Best-of-N flow (Milestone 5)."""
    bon_result = run_best_of_n(
        instance_id=instance_id,
        db_id=db_id,
        instruction=instruction,
        schema_slice=schema_slice,
        model=model,
        executor=executor,
        n=n,
        temperature=temperature,
        max_tokens=max_tokens,
        max_repairs=max_repairs,
        explain_first=explain_first,
        stop_on_repeated_error=stop_on_repeated_error,
        strategies=strategies,
        scoring=scoring,
        chroma_store=chroma_store,
    )

    # Summarize candidates (without heavy data like rows_sample)
    summaries = []
    total_llm_calls = 0
    for c in bon_result["candidates"]:
        total_llm_calls += 1 + c["repairs_count"]  # 1 generation + N repairs
        summaries.append({
            "candidate_id": c["candidate_id"],
            "strategy": c["strategy"],
            "success": c["execution_success"],
            "repairs_count": c["repairs_count"],
            "row_count": c["row_count"],
            "score": c["score"],
            "error_type": c["error_type"],
        })

    return InstanceResult(
        instance_id=instance_id,
        db_id=db_id,
        instruction=instruction,
        final_sql=bon_result["best_sql"],
        success=bon_result["best_success"],
        best_of_n_used=True,
        candidate_count=n,
        selection_reason=bon_result["selection_reason"],
        candidate_summaries=summaries,
        llm_calls=total_llm_calls,
    )
