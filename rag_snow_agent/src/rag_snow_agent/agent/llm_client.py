"""Thin LLM abstraction over the OpenAI-compatible API."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from openai import OpenAI

log = logging.getLogger(__name__)

# Load .env (primary) or .env.example (fallback) once at import time
try:
    from dotenv import load_dotenv

    for _env_file in [".env", ".env.example"]:
        _p = Path(_env_file)
        if _p.exists():
            load_dotenv(_p, override=False)
            break
except ImportError:
    pass


def _get_client() -> OpenAI:
    return OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def call_llm(
    messages: list[dict[str, str]],
    model: str = "gpt-4o-mini",
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> str:
    """Call the LLM and return plain text content.

    No output token limit is applied by default — the model generates until
    it finishes naturally.  Handles parameter compatibility across model
    families (temperature, etc.).
    Logs token usage but never prints API keys.
    """
    client = _get_client()
    log.debug(
        "LLM call: model=%s msgs=%d temperature=%.2f max_tokens=%s",
        model,
        len(messages),
        temperature,
        max_tokens,
    )

    response = _call_with_fallback(client, messages, model, temperature, max_tokens)

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


def _call_with_fallback(client, messages, model, temperature, max_tokens):
    """Try API call with progressive parameter fallback on unsupported params."""
    kwargs: dict = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    # Only set token limit if explicitly provided
    if max_tokens is not None:
        kwargs["max_completion_tokens"] = max_tokens

    for attempt in range(3):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as exc:
            err = str(exc).lower()
            if "unsupported" not in err and "not supported" not in err:
                raise

            # Drop the problematic parameter and retry
            if "max_completion_tokens" in err and "max_completion_tokens" in kwargs:
                log.debug("Falling back: max_completion_tokens → max_tokens")
                val = kwargs.pop("max_completion_tokens")
                kwargs["max_tokens"] = val
            elif "max_tokens" in err and "max_tokens" in kwargs:
                log.debug("Falling back: dropping max_tokens")
                del kwargs["max_tokens"]
            elif "temperature" in err and "temperature" in kwargs:
                log.debug("Falling back: dropping temperature")
                del kwargs["temperature"]
            else:
                raise

    # Final attempt with minimal params
    return client.chat.completions.create(
        model=model,
        messages=messages,
    )
