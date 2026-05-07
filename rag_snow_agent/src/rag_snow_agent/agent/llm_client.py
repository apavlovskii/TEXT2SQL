"""Thin LLM abstraction supporting Anthropic and OpenAI providers."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

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


# ── Provider detection ──────────────────────────────────────────────────────

_ANTHROPIC_PREFIXES = ("claude-",)


def _is_anthropic_model(model: str) -> bool:
    """Return True if the model name indicates an Anthropic model."""
    return any(model.startswith(p) for p in _ANTHROPIC_PREFIXES)


# ── Usage result (provider-agnostic) ────────────────────────────────────────

@dataclass
class _LLMUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


@dataclass
class _LLMResult:
    content: str
    usage: _LLMUsage | None = None


# ── Anthropic backend ──────────────────────────────────────────────────────

def _call_anthropic(
    messages: list[dict[str, str]],
    model: str,
    temperature: float,
    max_tokens: int | None,
) -> _LLMResult:
    """Call the Anthropic Messages API."""
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # Separate system message from conversation messages
    system_text = ""
    conversation: list[dict[str, str]] = []
    for msg in messages:
        if msg["role"] == "system":
            system_text += msg["content"] + "\n"
        else:
            conversation.append(msg)

    # Anthropic requires at least one user message
    if not conversation:
        conversation = [{"role": "user", "content": "..."}]

    kwargs: dict = {
        "model": model,
        "messages": conversation,
        "max_tokens": max_tokens or 8192,
    }
    if system_text.strip():
        kwargs["system"] = system_text.strip()
    if temperature is not None:
        kwargs["temperature"] = temperature

    for retry in range(3):
        try:
            response = client.messages.create(**kwargs)
            break
        except anthropic.APIStatusError as exc:
            if retry < 2 and exc.status_code in (429, 500, 502, 503, 529):
                delay = 2 ** (retry + 1)
                log.warning(
                    "Anthropic %d error (attempt %d/3), retrying in %ds: %s",
                    exc.status_code, retry + 1, delay, str(exc)[:120],
                )
                time.sleep(delay)
                continue
            raise

    content = ""
    for block in response.content:
        if block.type == "text":
            content += block.text

    usage = None
    if response.usage:
        usage = _LLMUsage(
            prompt_tokens=response.usage.input_tokens,
            completion_tokens=response.usage.output_tokens,
            total_tokens=response.usage.input_tokens + response.usage.output_tokens,
        )

    return _LLMResult(content=content, usage=usage)


# ── OpenAI backend ─────────────────────────────────────────────────────────

def _call_openai(
    messages: list[dict[str, str]],
    model: str,
    temperature: float,
    max_tokens: int | None,
) -> _LLMResult:
    """Call the OpenAI Chat Completions API."""
    from openai import OpenAI

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    response = _openai_call_with_fallback(client, messages, model, temperature, max_tokens)

    content = response.choices[0].message.content or ""
    usage = None
    if response.usage:
        usage = _LLMUsage(
            prompt_tokens=response.usage.prompt_tokens,
            completion_tokens=response.usage.completion_tokens,
            total_tokens=response.usage.total_tokens,
        )

    return _LLMResult(content=content, usage=usage)


def _is_transient_400(exc: Exception) -> bool:
    """Return True if the exception is a transient HTTP 400 JSON parse error."""
    err = str(exc).lower()
    return "400" in err and ("could not parse" in err or "invalid json" in err)


def _openai_call_with_fallback(client, messages, model, temperature, max_tokens):
    """Try API call with progressive parameter fallback on unsupported params."""
    kwargs: dict = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if max_tokens is not None:
        kwargs["max_completion_tokens"] = max_tokens

    for attempt in range(3):
        try:
            return _openai_call_with_transient_retry(client, kwargs)
        except Exception as exc:
            err = str(exc).lower()
            if "unsupported" not in err and "not supported" not in err:
                raise

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

    return client.chat.completions.create(model=model, messages=messages)


def _openai_call_with_transient_retry(client, kwargs, max_retries: int = 2):
    """Call the API, retrying transient 400 JSON-parse errors with backoff."""
    for retry in range(max_retries + 1):
        try:
            return client.chat.completions.create(**kwargs)
        except Exception as exc:
            if retry < max_retries and _is_transient_400(exc):
                delay = 2 ** (retry + 1)
                log.warning(
                    "Transient 400 JSON parse error (attempt %d/%d), retrying in %ds: %s",
                    retry + 1, max_retries + 1, delay, str(exc)[:120],
                )
                time.sleep(delay)
                continue
            raise


# ── Public API ──────────────────────────────────────────────────────────────

def call_llm(
    messages: list[dict[str, str]],
    model: str = "claude-sonnet-4-5",
    temperature: float = 0.2,
    max_tokens: int | None = None,
) -> str:
    """Call the LLM and return plain text content.

    Automatically routes to Anthropic or OpenAI based on the model name:
      - Models starting with "claude-" use the Anthropic Messages API
      - All other models use the OpenAI Chat Completions API

    No output token limit is applied by default — the model generates until
    it finishes naturally.  Logs token usage but never prints API keys.
    """
    log.debug(
        "LLM call: model=%s msgs=%d temperature=%.2f max_tokens=%s",
        model,
        len(messages),
        temperature,
        max_tokens,
    )

    if _is_anthropic_model(model):
        result = _call_anthropic(messages, model, temperature, max_tokens)
    else:
        result = _call_openai(messages, model, temperature, max_tokens)

    if result.usage:
        log.info(
            "LLM usage: prompt_tokens=%d completion_tokens=%d total_tokens=%d",
            result.usage.prompt_tokens,
            result.usage.completion_tokens,
            result.usage.total_tokens,
        )
    else:
        log.info("LLM response length: %d chars", len(result.content))

    return result.content.strip()
