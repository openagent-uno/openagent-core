"""Per-turn execution profiles for constrained local-model event runs.

The normal OpenAgent prompt and Team runtime are deliberately broad.  That is
useful for interactive, multi-model work, but wasteful for an unattended event
whose model was explicitly pinned to a self-hosted endpoint.  This module keeps
the narrower behaviour coroutine-local so concurrent cloud/chat turns remain
byte-identical.
"""
from __future__ import annotations

from openagent_core.configuration import runtime_environment
import contextvars
import os
from contextlib import contextmanager
from typing import Any, Iterator


_LEAN_LOCAL_EVENT = contextvars.ContextVar("lean_local_event", default=False)
_STRICT_LOCAL_ONLY = contextvars.ContextVar("strict_local_only", default=False)
_STATELESS_COMPLETION = contextvars.ContextVar(
    "stateless_completion", default=False,
)
_TRUTHY = {"1", "true", "yes", "on"}
_FALSEY = {"0", "false", "no", "off"}


def lean_local_event_active() -> bool:
    return bool(_LEAN_LOCAL_EVENT.get())


_LEAN_LOCAL_TASK = contextvars.ContextVar("lean_local_task", default=False)


def lean_local_task_active() -> bool:
    """True inside a locally-pinned SCHEDULED TASK, as opposed to an event.

    Both lanes share the lean profile, but not the shape of their output: a
    support reply is a few sentences, a scheduled task emits a report and long
    tool calls. Measured: a quality-scorer tool call was cut off mid-JSON at
    the event budget and the whole run died on a parse error.
    """
    return bool(_LEAN_LOCAL_TASK.get())


def strict_local_only_active() -> bool:
    """True when a turn must never use a configured cloud fallback."""
    return bool(_STRICT_LOCAL_ONLY.get())


@contextmanager
def lean_local_task_scope(enabled: bool = True) -> Iterator[None]:
    token = _LEAN_LOCAL_TASK.set(bool(enabled))
    try:
        yield
    finally:
        _LEAN_LOCAL_TASK.reset(token)


@contextmanager
def lean_local_event_scope(enabled: bool = True) -> Iterator[None]:
    token = _LEAN_LOCAL_EVENT.set(bool(enabled))
    try:
        yield
    finally:
        _LEAN_LOCAL_EVENT.reset(token)


def stateless_completion_active() -> bool:
    """True for a one-shot, tool-less generation that must persist nothing.

    The controller's reply composer is a pure function: no tools, no history,
    one turn. Persisting a session row per call bought nothing and cost real
    contention - measured at 8 concurrent deliveries, SQLite answered
    "database is locked" on upsert_session and five composes out of
    twenty-four timed out into the deterministic fallback.
    """
    return bool(_STATELESS_COMPLETION.get())


@contextmanager
def stateless_completion_scope(enabled: bool = True) -> Iterator[None]:
    token = _STATELESS_COMPLETION.set(bool(enabled))
    try:
        yield
    finally:
        _STATELESS_COMPLETION.reset(token)


@contextmanager
def strict_local_only_scope(enabled: bool = True) -> Iterator[None]:
    """Coroutine-local hard boundary for controller-owned local inference."""
    token = _STRICT_LOCAL_ONLY.set(bool(enabled))
    try:
        yield
    finally:
        _STRICT_LOCAL_ONLY.reset(token)



def lean_local_tool_families(prompt: str, available: "Iterable[str]") -> list[str] | None:
    """Tool families a lean local scheduled task should be allowed to use.

    A pinned local task ran against the agent's whole MCP surface: measured on
    Qwen3-30B, a task that only had to send one approved sentence spent ten
    tool calls exploring BillingBear and never reached the send. A small model
    treats a large surface as an invitation.

    So: when the prompt names servers explicitly, restrict the run to those
    (plus ``vault``, which every policy read needs, and ``tool-search``, the
    navigation surface itself). When it names none, return ``None`` — no
    restriction, byte-identical to today, because guessing would be worse than
    the status quo.
    """
    low = str(prompt or "").lower()
    names = [str(name) for name in available or ()]
    named = [
        name for name in names
        if name.lower() in low
        or name.lower().replace("-", " ") in low
        or name.lower().replace("-", "_") in low
    ]
    if not named:
        return None
    always = [
        name for name in names
        if name.lower() in {"vault", "tool-search", "tool_search"}
    ]
    return sorted({*named, *always})
