"""Chat routes: synchronous and SSE streaming."""

from __future__ import annotations

import asyncio
import json
import logging
import queue as stdlib_queue
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from ..config import AppSettings
from ..dependencies import get_agent, get_session_store, get_settings
from ..models.requests import ChatRequest
from ..models.responses import ChatResponse, ExecutionMetadata, QueryResult
from ..services.agent_adapter import AgentAdapter
from ..services.answer_generator import generate_answer
from ..services.session_store import SessionStore
from ..services.sql_guardrails import SQLValidationError, validate_read_only

router = APIRouter(tags=["chat"])
log = logging.getLogger(__name__)


async def _process_chat(
    message: str,
    session_id: str | None,
    db_id: str,
    model: str,
    max_retries: int,
    max_candidates: int,
    datasource: str,
    agent: AgentAdapter,
    store: SessionStore,
    settings: AppSettings,
    progress_callback=None,
) -> ChatResponse:
    """Core chat processing shared by sync and streaming routes."""
    execution_log: list[str] = []

    def _log(msg: str):
        execution_log.append(msg)
        if progress_callback:
            progress_callback(msg)

    # Ensure session exists
    if not session_id:
        session = store.create_session(name=None, db_id=db_id)
        session_id = session.id
    elif not store.get_session(session_id):
        session = store.create_session(name=None, db_id=db_id)
        session_id = session.id

    # Persist user message
    store.add_message(session_id, "user", message)

    # Check if this is a general (non-data) question (Change 9)
    if not agent.is_data_question(message):
        _log("Detected as general question — answering without agent pipeline")
        general_result = await agent.answer_general_question(message, model, progress_callback)
        if general_result.is_general_answer:
            # For general answers, the answer text is in the LLM response
            # We need to re-call to get the text (the adapter didn't store it)
            answer_text = await generate_answer(
                question=message, sql="", columns=[], rows=[], row_count=0,
                model=model, call_llm_fn=agent.call_llm_fn,
            ) if agent.call_llm_fn else "I can help with data questions. Try asking about your database."
            # Actually for general questions, just call the LLM directly
            if agent.call_llm_fn:
                import functools
                loop = asyncio.get_event_loop()
                answer_text = await loop.run_in_executor(
                    None,
                    functools.partial(
                        agent.call_llm_fn,
                        messages=[
                            {"role": "system", "content": "You are a helpful analytics assistant. Answer concisely."},
                            {"role": "user", "content": message},
                        ],
                        model=model,
                    ),
                )

            now = datetime.now(timezone.utc)
            msg = store.add_message(session_id, "assistant", answer_text)
            return ChatResponse(
                session_id=session_id,
                message_id=msg.id,
                answer=answer_text,
                execution_log=general_result.execution_log + execution_log,
                timestamp=now,
            )

    # Run agent pipeline
    agent_result = await agent.run_query(
        question=message,
        db_id=db_id,
        model=model,
        max_retries=max_retries,
        max_candidates=max_candidates,
        datasource=datasource,
        progress_callback=_log,
    )
    execution_log = agent_result.execution_log

    # Validate SQL (guardrails)
    sql = agent_result.final_sql
    guardrail_error = None
    if sql and not sql.startswith("SELECT 1"):
        try:
            sql = validate_read_only(sql)
        except SQLValidationError as exc:
            guardrail_error = str(exc)
            sql = None

    # Build results
    results = None
    if agent_result.columns and agent_result.rows is not None:
        results = QueryResult(
            columns=agent_result.columns,
            rows=agent_result.rows,
            row_count=agent_result.row_count,
            truncated=agent_result.row_count > settings.MAX_RESULT_ROWS,
        )

    # Generate NL answer
    _log("Generating natural language answer...")
    error = guardrail_error or agent_result.error_message
    if error:
        answer = f"I encountered an issue: {error}"
    elif results and results.rows:
        answer = await generate_answer(
            question=message, sql=sql or "", columns=results.columns,
            rows=results.rows, row_count=results.row_count,
            model=model, call_llm_fn=agent.call_llm_fn,
        )
    elif sql:
        answer = "The query executed but returned no results."
    else:
        answer = "Unable to generate valid query after all retry attempts."

    # Build metadata
    metadata = ExecutionMetadata(
        elapsed_ms=agent_result.elapsed_ms,
        llm_calls=agent_result.llm_calls,
        repair_count=agent_result.repair_count,
        candidate_count=agent_result.candidate_count,
        model=agent_result.model,
        datasource=datasource,
    )

    now = datetime.now(timezone.utc)
    msg = store.add_message(
        session_id, "assistant", answer,
        sql=sql, results=results, metadata=metadata, error=error,
    )

    return ChatResponse(
        session_id=session_id,
        message_id=msg.id,
        answer=answer,
        sql=sql,
        results=results,
        metadata=metadata,
        error=error,
        execution_log=execution_log,
        timestamp=now,
    )


@router.post("/api/chat", response_model=ChatResponse)
async def chat(
    body: ChatRequest,
    agent: AgentAdapter = Depends(get_agent),
    store: SessionStore = Depends(get_session_store),
    settings: AppSettings = Depends(get_settings),
):
    return await _process_chat(
        message=body.message, session_id=body.session_id,
        db_id=body.db_id, model=body.model,
        max_retries=body.max_retries, max_candidates=body.max_candidates,
        datasource=body.datasource,
        agent=agent, store=store, settings=settings,
    )


@router.get("/api/chat/stream")
async def stream_chat(
    message: str = Query(...),
    session_id: str | None = Query(None),
    db_id: str = Query("GA360"),
    model: str = Query("gpt-4o-mini"),
    max_retries: int = Query(10),
    max_candidates: int = Query(2),
    datasource: str = Query("sqlite"),
    agent: AgentAdapter = Depends(get_agent),
    store: SessionStore = Depends(get_session_store),
    settings: AppSettings = Depends(get_settings),
):
    """SSE endpoint that streams thinking status then the final result."""
    progress_queue: stdlib_queue.Queue[str] = stdlib_queue.Queue()

    def on_progress(status: str):
        progress_queue.put(status)

    async def event_generator():
        task = asyncio.create_task(
            _process_chat(
                message=message, session_id=session_id,
                db_id=db_id, model=model,
                max_retries=max_retries, max_candidates=max_candidates,
                datasource=datasource,
                agent=agent, store=store, settings=settings,
                progress_callback=on_progress,
            )
        )

        while not task.done():
            try:
                status = progress_queue.get_nowait()
                yield f"event: thinking\ndata: {json.dumps({'status': status})}\n\n"
            except stdlib_queue.Empty:
                await asyncio.sleep(0.3)

        while not progress_queue.empty():
            status = progress_queue.get_nowait()
            yield f"event: thinking\ndata: {json.dumps({'status': status})}\n\n"

        try:
            response = task.result()
            yield f"event: result\ndata: {response.model_dump_json()}\n\n"
        except Exception as exc:
            log.error("Chat stream error: %s", exc, exc_info=True)
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"

        yield f"event: done\ndata: {{}}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
