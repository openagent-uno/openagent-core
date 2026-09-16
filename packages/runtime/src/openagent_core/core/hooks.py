"""Lightweight hook + quick-command registry.

Two unrelated-but-co-located mini-features Hermes shipped that OpenAgent
didn't have until now:

  • **Quick commands**: bridges look up the user text against a static
    map and expand short triggers into the full prompt. Lets a user
    type ``/recap`` and have the agent see "Summarise the last 20 turns
    with bullets, English." Configured under ``quick_commands`` in
    ``openagent.yaml``.

  • **Hooks**: fire-and-forget shell commands triggered by named
    runtime events (``on_turn_end``, ``on_compression``,
    ``on_guardrail_tripped``, ``on_skill_saved``). Each command runs as
    a subprocess with event metadata in env vars (``OPENAGENT_EVENT_*``)
    so the operator can build whatever side-effects they want without
    touching server code. Output is captured (truncated to 4 KB) into
    the event log so failures are visible.

Both are populated by ``core/server.create_agent`` from yaml. Registries are owned by each runtime instance; callbacks never select
another agent by its session ID.

Defaults: empty mapping → both features no-op. Operators opt in by
filling the yaml sections.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import time
from contextvars import ContextVar
from typing import Any

from openagent_core.core.execution_origin import create_server_only_task

from openagent_core.core.logging import elog


# ── Quick commands ──────────────────────────────────────────────────


_registries: ContextVar[dict[str, dict[str, str]] | None] = ContextVar("openagent_hook_registries", default=None)

def _registry(name: str) -> dict[str, str]:
    from openagent_core.runtime import current_runtime
    runtime=current_runtime()
    if runtime is not None:
        registries=getattr(runtime,"hook_registries",None)
        if registries is None:
            registries={}
            runtime.hook_registries=registries
    else:
        registries=_registries.get()
        if registries is None:
            registries={}
            _registries.set(registries)
    return registries.setdefault(name,{})


def set_quick_commands(mapping: dict[str, str] | None) -> None:
    """Replace the live registry. Server calls this once at startup
    after parsing yaml; tests/code can call it directly. Trigger
    matching is case-insensitive on the slash-prefixed form, so
    ``recap`` in yaml matches both ``/recap`` and ``/RECAP``."""
    _registry("quick_commands").clear()
    for k, v in (mapping or {}).items():
        if not k or not isinstance(v, str):
            continue
        key = str(k).strip().lstrip("/").lower()
        if key:
            _registry("quick_commands")[key] = v


def expand_quick_command(text: str) -> str | None:
    """Return the expanded prompt when ``text`` is a quick-command
    invocation, else ``None``. Matching is exact on the first
    whitespace-split token, with the trigger's leading slash stripped —
    anything after the trigger is preserved and appended (so
    ``/recap last week`` becomes ``<expansion>\nlast week``).
    """
    if not _registry("quick_commands") or not text:
        return None
    stripped = text.strip()
    if not stripped.startswith("/"):
        return None
    first, _, rest = stripped[1:].partition(" ")
    expansion = _registry("quick_commands").get(first.lower())
    if expansion is None:
        return None
    rest = rest.strip()
    if rest:
        return f"{expansion}\n{rest}"
    return expansion


# ── Hooks ───────────────────────────────────────────────────────────


# Map of event name → shell command string (POSIX). Commands run via
# ``/bin/sh -c`` with the event payload exposed as env vars prefixed
# ``OPENAGENT_EVENT_``.


# Sane upper bound to keep a misbehaving hook from blocking forever.
_HOOK_TIMEOUT_S = 30.0
_HOOK_OUTPUT_BYTES = 4096


def set_hooks(mapping: dict[str, str] | None) -> None:
    """Replace the live hook registry. Keys are bare event names — the
    server normalises them to lowercase + drops a leading ``on_`` so
    ``on_turn_end`` in yaml matches a ``fire("turn_end", ...)`` call.
    """
    _registry("hooks").clear()
    for k, v in (mapping or {}).items():
        if not k or not isinstance(v, str) or not v.strip():
            continue
        key = str(k).strip().lower()
        if key.startswith("on_"):
            key = key[3:]
        _registry("hooks")[key] = v.strip()


def _payload_to_env(payload: dict[str, Any]) -> dict[str, str]:
    """Flatten a payload dict into ``OPENAGENT_EVENT_<KEY>`` env vars.
    Non-scalar values are JSON-serialised so the shell can pick them up
    via ``jq`` etc. Keys are uppercased + non-alnum stripped."""
    out: dict[str, str] = {}
    for k, v in (payload or {}).items():
        key = "".join(
            ch if ch.isalnum() else "_" for ch in str(k)
        ).upper()
        if not key:
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[f"OPENAGENT_EVENT_{key}"] = "" if v is None else str(v)
        else:
            try:
                out[f"OPENAGENT_EVENT_{key}"] = json.dumps(v, ensure_ascii=False)
            except Exception:
                out[f"OPENAGENT_EVENT_{key}"] = str(v)
    return out


async def _run_hook(event_name: str, command: str, payload: dict[str, Any]) -> None:
    start = time.monotonic()
    from openagent_core.runtime import current_runtime
    runtime=current_runtime()
    env = {**(dict(runtime.settings.environment) if runtime is not None else dict(os.environ)), **_payload_to_env(payload)}
    env["OPENAGENT_EVENT_NAME"] = event_name
    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(), timeout=_HOOK_TIMEOUT_S
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            # ``hook_event``, not ``event``: ``elog(event, level, ...)`` takes
            # the event name as its first positional, so ``event=`` here binds
            # it twice -> ``TypeError: got multiple values for argument
            # 'event'``. All three calls in this function had that bug, which
            # meant hooks ran but never logged — and the failure was invisible
            # *because* the logging call was the thing raising. Worse, the
            # ``except`` below repeated the mistake, so the TypeError from the
            # success-path ``hooks.fired`` re-raised inside the handler and
            # escaped as an unretrieved task exception.
            elog(
                "hooks.timeout",
                hook_event=event_name,
                command=command[:120],
                timeout_s=_HOOK_TIMEOUT_S,
            )
            return
        elapsed = round(time.monotonic() - start, 3)
        out = (stdout or b"").decode("utf-8", errors="replace")[:_HOOK_OUTPUT_BYTES]
        elog(
            "hooks.fired",
            hook_event=event_name,
            command=command[:120],
            exit_code=proc.returncode,
            elapsed_s=elapsed,
            output=out,
        )
    except Exception as e:  # noqa: BLE001
        elog("hooks.error", hook_event=event_name, error=str(e)[:200])


def fire(event_name: str, **payload: Any) -> None:
    """Trigger any hook registered for ``event_name`` as a background
    asyncio task. Returns immediately — never awaited. Safe to call
    from sync code (asyncio.get_running_loop() handles the dispatch).
    """
    if not _registry("hooks"):
        return
    cmd = _registry("hooks").get(event_name)
    if not cmd:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Called from a non-async context — log and skip.
        elog("hooks.skipped", hook_event=event_name, reason="no_event_loop")
        return
    create_server_only_task(
        _run_hook(event_name, cmd, payload), name=f"hook:{event_name}",
    )


# Convenience accessors for tests / debug.
def registered_quick_commands() -> dict[str, str]:
    return dict(_registry("quick_commands"))


def registered_hooks() -> dict[str, str]:
    return dict(_registry("hooks"))
