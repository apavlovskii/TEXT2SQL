"""Thin LLM abstraction supporting Anthropic and OpenAI providers."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


class LLMQuotaExhausted(RuntimeError):
    """Raised when the provider rejects a call due to exhausted quota/billing.

    This is a *fatal, resumable* condition (unlike a transient rate limit):
    the caller should stop the run and checkpoint rather than re-attempting
    every remaining instance, which would just fail identically until the user
    tops up their account.
    """


# Substrings that indicate the account is out of quota/credits (not a transient
# burst limit). Matched case-insensitively against the exception text.
_QUOTA_MARKERS = (
    "insufficient_quota",
    "exceeded your current quota",
    "billing_hard_limit_reached",
    "billing hard limit",
    "you have exhausted",
    "credit balance is too low",
    "quota exceeded",
)


def _is_quota_error(exc: Exception) -> bool:
    """Return True if the exception indicates exhausted quota/credits."""
    text = str(exc).lower()
    if any(marker in text for marker in _QUOTA_MARKERS):
        return True
    # OpenAI surfaces a structured `code`; check it when present.
    code = getattr(exc, "code", None)
    if isinstance(code, str) and code.lower() in ("insufficient_quota", "billing_hard_limit_reached"):
        return True
    return False

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

    try:
        if _is_anthropic_model(model):
            result = _call_anthropic(messages, model, temperature, max_tokens)
        else:
            result = _call_openai(messages, model, temperature, max_tokens)
    except LLMQuotaExhausted:
        raise
    except Exception as exc:
        # Convert provider quota/credit-exhaustion into a typed, resumable stop
        # signal so the runner can checkpoint instead of failing every instance.
        if _is_quota_error(exc):
            log.error("Provider quota/credits exhausted: %s", str(exc)[:200])
            raise LLMQuotaExhausted(str(exc)) from exc
        raise

    if result.usage:
        log.info(
            "LLM usage: prompt_tokens=%d completion_tokens=%d total_tokens=%d",
            result.usage.prompt_tokens,
            result.usage.completion_tokens,
            result.usage.total_tokens,
        )
        try:
            from ..observability.instance_telemetry import telemetry
            telemetry.record_tokens(
                prompt=result.usage.prompt_tokens,
                completion=result.usage.completion_tokens,
            )
        except Exception:
            log.debug("Telemetry record_tokens failed", exc_info=True)
    else:
        log.info("LLM response length: %d chars", len(result.content))

    return result.content.strip()


# ── OpenAI Batch API ─────────────────────────────────────────────────────────
#
# For independent, non-interactive requests (e.g. Best-of-N candidate
# generation, where each strategy's prompt doesn't depend on another's
# result), the Batch API gives a flat 50% discount on input/output tokens at
# the cost of unpredictable turnaround (up to 24h, no guarantee). OpenAI-only
# — Anthropic models are not supported here. See sql_correction.py and
# candidate_generator.py's batch-aware candidate generation for the caller
# side of this.

_BATCH_TERMINAL_STATUSES = ("completed", "failed", "expired", "cancelled")


@dataclass
class BatchRequest:
    """One request within a batch job."""

    custom_id: str
    messages: list[dict[str, str]]
    model: str
    temperature: float = 0.2
    max_tokens: int | None = None


def _probe_batch_capable_params(client, model: str) -> dict[str, bool]:
    """Determine which of {temperature, max_completion_tokens} *model*
    actually accepts, via one tiny live call.

    Batch request bodies can't be retried individually after the batch job
    is submitted the way _openai_call_with_fallback retries a synchronous
    call — an unsupported param (e.g. some models, like gpt-5.5, only accept
    the default temperature=1 and 400 on anything else) would otherwise fail
    every request in the batch identically, discovered only after the wait
    for the whole job to complete. This runs once per distinct model in a
    batch, not once per request.
    """
    kwargs: dict = {"model": model, "messages": [{"role": "user", "content": "hi"}], "temperature": 0.2, "max_completion_tokens": 5}
    for _ in range(4):
        try:
            client.chat.completions.create(**kwargs)
            break
        except Exception as exc:
            err = str(exc).lower()
            if "unsupported" not in err and "not supported" not in err:
                break  # some other error — stop probing, use whatever's left
            if "temperature" in err and "temperature" in kwargs:
                del kwargs["temperature"]
            elif "max_completion_tokens" in err and "max_completion_tokens" in kwargs:
                val = kwargs.pop("max_completion_tokens")
                kwargs["max_tokens"] = val
            elif "max_tokens" in err and "max_tokens" in kwargs:
                del kwargs["max_tokens"]
            else:
                break
    supports = {
        "temperature": "temperature" in kwargs,
        "max_completion_tokens": "max_completion_tokens" in kwargs,
        "max_tokens": "max_tokens" in kwargs,
    }
    log.info("Batch capability probe for %s: %s", model, supports)
    return supports


def submit_batch(requests: list["BatchRequest"]) -> str:
    """Upload a batch input file and create the batch job. Returns the batch_id."""
    from openai import OpenAI

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    capability_cache: dict[str, dict[str, bool]] = {}

    lines = []
    for req in requests:
        if req.model not in capability_cache:
            capability_cache[req.model] = _probe_batch_capable_params(client, req.model)
        caps = capability_cache[req.model]

        body: dict = {"model": req.model, "messages": req.messages}
        if caps["temperature"]:
            body["temperature"] = req.temperature
        if req.max_tokens is not None:
            if caps["max_completion_tokens"]:
                body["max_completion_tokens"] = req.max_tokens
            elif caps["max_tokens"]:
                body["max_tokens"] = req.max_tokens
        lines.append(json.dumps({
            "custom_id": req.custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        }))

    jsonl_bytes = ("\n".join(lines) + "\n").encode("utf-8")
    uploaded = client.files.create(file=("batch_input.jsonl", jsonl_bytes), purpose="batch")
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
    )
    log.info("Submitted batch %s with %d requests", batch.id, len(requests))
    return batch.id


def poll_batch(batch_id: str, poll_interval: float = 15.0, timeout: float | None = None):
    """Block until *batch_id* reaches a terminal status. Returns the Batch object."""
    from openai import OpenAI

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    start = time.monotonic()
    while True:
        batch = client.batches.retrieve(batch_id)
        if batch.status in _BATCH_TERMINAL_STATUSES:
            log.info("Batch %s finished with status=%s counts=%s", batch_id, batch.status, batch.request_counts)
            return batch
        if timeout is not None and (time.monotonic() - start) > timeout:
            raise TimeoutError(f"Batch {batch_id} did not reach a terminal status within {timeout}s (status={batch.status})")
        log.info("Batch %s status=%s counts=%s — polling again in %ds", batch_id, batch.status, batch.request_counts, poll_interval)
        time.sleep(poll_interval)


def retrieve_batch_results(batch) -> dict[str, str]:
    """Return {custom_id: content} for successfully completed requests.

    A custom_id missing from the result means that request failed or expired
    — callers must handle gaps (e.g. treat as a failed candidate), not assume
    every submitted custom_id comes back.
    """
    from openai import OpenAI

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    results: dict[str, str] = {}
    if not batch.output_file_id:
        return results

    content = client.files.content(batch.output_file_id).text
    for line in content.strip().split("\n"):
        if not line:
            continue
        rec = json.loads(line)
        custom_id = rec.get("custom_id")
        resp = rec.get("response") or {}
        if resp.get("status_code") == 200:
            choices = (resp.get("body") or {}).get("choices") or []
            if choices:
                results[custom_id] = choices[0]["message"]["content"] or ""
    return results


def call_llm_batch(
    requests: list["BatchRequest"],
    poll_interval: float = 15.0,
    timeout: float | None = None,
) -> dict[str, str]:
    """Submit, wait for, and retrieve results for a batch of independent
    OpenAI chat-completion requests. Returns {custom_id: content}.

    Blocks the caller until the whole batch reaches a terminal status — this
    is meant for offline/eval workloads that can tolerate an unpredictable
    wait (up to 24h), not interactive paths.
    """
    if not requests:
        return {}
    batch_id = submit_batch(requests)
    batch = poll_batch(batch_id, poll_interval=poll_interval, timeout=timeout)
    if batch.status != "completed":
        log.warning("Batch %s ended with status=%s (not 'completed') — some/all requests may be missing", batch_id, batch.status)
    return retrieve_batch_results(batch)
