"""Per-instance telemetry: token counts + component activation flags.

A thread-local accumulator that components write to during solve_instance().
The experiment runner resets it before each instance and snapshots after.

Usage from instrumented call sites::

    from ..observability.instance_telemetry import telemetry
    telemetry.record_tokens(prompt=123, completion=45)
    telemetry.mark("verifier_used")
    telemetry.increment("date_shard_rewrites")
    telemetry.set("join_graph_neighbors_added", 2)

Usage from the runner::

    from ..observability.instance_telemetry import telemetry
    telemetry.reset()
    # ... run instance ...
    snap = telemetry.snapshot()
"""

from __future__ import annotations

import threading
from typing import Any


class _InstanceTelemetry:
    """Thread-local instance-scoped telemetry accumulator."""

    _local = threading.local()

    def _state(self) -> dict[str, Any]:
        st = getattr(self._local, "state", None)
        if st is None:
            st = self._fresh()
            self._local.state = st
        return st

    @staticmethod
    def _fresh() -> dict[str, Any]:
        return {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "llm_calls_observed": 0,
            "flags": {},
            "counters": {},
            "values": {},
        }

    def reset(self) -> None:
        """Reset to a fresh accumulator (call before each instance)."""
        self._local.state = self._fresh()

    def record_tokens(self, prompt: int = 0, completion: int = 0) -> None:
        """Add token counts from one LLM call."""
        st = self._state()
        st["prompt_tokens"] += int(prompt or 0)
        st["completion_tokens"] += int(completion or 0)
        st["total_tokens"] = st["prompt_tokens"] + st["completion_tokens"]
        st["llm_calls_observed"] += 1

    def mark(self, flag: str) -> None:
        """Set a boolean activation flag to True."""
        self._state()["flags"][flag] = True

    def increment(self, counter: str, by: int = 1) -> None:
        """Increment a named counter."""
        c = self._state()["counters"]
        c[counter] = c.get(counter, 0) + int(by)

    def set(self, key: str, value: Any) -> None:
        """Set an arbitrary value (last-write-wins)."""
        self._state()["values"][key] = value

    def snapshot(self) -> dict[str, Any]:
        """Return a copy of the current state — safe to serialize."""
        st = self._state()
        return {
            "prompt_tokens": st["prompt_tokens"],
            "completion_tokens": st["completion_tokens"],
            "total_tokens": st["total_tokens"],
            "llm_calls_observed": st["llm_calls_observed"],
            "flags": dict(st["flags"]),
            "counters": dict(st["counters"]),
            "values": dict(st["values"]),
        }


# Singleton, module-level instance.
telemetry = _InstanceTelemetry()
