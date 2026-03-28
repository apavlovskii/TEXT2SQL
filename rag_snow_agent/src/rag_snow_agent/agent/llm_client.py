"""Thin LLM abstraction over the OpenAI-compatible API."""

from __future__ import annotations

import logging
import os

from openai import OpenAI

log = logging.getLogger(__name__)


def _get_client() -> OpenAI:
    return OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def call_llm(
    messages: list[dict[str, str]],
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_tokens: int = 800,
) -> str:
    """Call the LLM and return plain text content.

    Logs token usage but never prints API keys.
    """
    client = _get_client()
    log.debug(
        "LLM call: model=%s msgs=%d temperature=%.2f max_tokens=%d",
        model,
        len(messages),
        temperature,
        max_tokens,
    )

    response = client.chat.completions.create(
        model=model,
        messages=messages,  # type: ignore[arg-type]
        temperature=temperature,
        max_tokens=max_tokens,
    )

    content = response.choices[0].message.content or ""

    usage = response.usage
    if usage:
        log.info(
            "LLM usage: prompt_tokens=%d completion_tokens=%d total_tokens=%d",
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens,
        )
    else:
        log.info("LLM response length: %d chars", len(content))

    return content.strip()
