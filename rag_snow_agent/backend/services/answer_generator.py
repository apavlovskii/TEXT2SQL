"""Generate natural language answers from SQL results using LLM."""

from __future__ import annotations

import asyncio
import functools
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

log = logging.getLogger(__name__)

_THREAD_POOL = ThreadPoolExecutor(max_workers=2)

_SYSTEM_PROMPT = """\
You are a data analyst assistant. The user asked a question about their data.
A SQL query was executed and produced the results below.
Provide a clear, concise natural language answer.
Reference specific numbers from the results.
If the results are empty, say no data matched the query.
Keep the answer to 2-3 sentences unless the question asks for a list.\
"""


def _format_results(columns: list[str], rows: list[list[Any]], total: int) -> str:
    """Format results into a compact text table for the LLM prompt."""
    if not columns or not rows:
        return "(no results)"
    header = " | ".join(columns)
    divider = "-" * len(header)
    lines = [header, divider]
    for row in rows[:10]:
        lines.append(" | ".join(str(v) if v is not None else "NULL" for v in row))
    if total > 10:
        lines.append(f"... ({total} total rows)")
    return "\n".join(lines)


async def generate_answer(
    question: str,
    sql: str,
    columns: list[str],
    rows: list[list[Any]],
    row_count: int,
    model: str = "gpt-4o-mini",
    call_llm_fn=None,
) -> str:
    """Generate a natural language answer from SQL results.

    *call_llm_fn* is the synchronous ``call_llm`` function from the agent.
    If not provided, returns a template answer.
    """
    if call_llm_fn is None:
        if not rows:
            return "The query returned no results."
        return f"The query returned {row_count} row(s). Check the results table for details."

    results_text = _format_results(columns, rows, row_count)

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Question: {question}\n\n"
                f"SQL:\n{sql}\n\n"
                f"Results:\n{results_text}"
            ),
        },
    ]

    loop = asyncio.get_event_loop()
    try:
        answer = await loop.run_in_executor(
            _THREAD_POOL,
            functools.partial(call_llm_fn, messages=messages, model=model),
        )
        return answer
    except Exception as exc:
        log.error("Answer generation failed: %s", exc)
        if not rows:
            return "The query returned no results."
        return f"The query returned {row_count} row(s). Check the results table for details."
