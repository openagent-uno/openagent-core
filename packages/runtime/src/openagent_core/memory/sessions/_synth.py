"""Synthesize history messages for an interrupted (barge-in / stop) run.

A run assembles its ``messages`` only on normal completion. When the user
stops a turn the runner persists the run with ``messages=None`` even though
the input (the user's question) and any partial reply (in ``content``) are
known. Without recovering them the interrupted turn is invisible to
``add_history_to_context`` and the next turn ("continua", "vai avanti") loses
all prior context. This is the single place that backfills those messages so
every persistence path (every runner, every cancel handler) is covered.
"""

from __future__ import annotations

from typing import Any

from openagent_core.core._run_state.base import RunStatus
from openagent_core.models.providers.message import Message


def completed_history_messages(run: Any) -> list[Message]:
    """Read older user-only fallback envelopes without losing their answer.

    Those completed runs recorded the displayed text in ``content`` while the
    provider appended its messages to a detached list. Only fill the entirely
    missing assistant side of a real user turn. This is historical context,
    never a new final-answer attestation or a reconstruction of tool effects.
    The stored run and its messages remain untouched.
    """
    messages = list(run.messages or [])
    content = getattr(run, "content", None)
    if (getattr(run, "status", None) != RunStatus.completed
            or not isinstance(content, str) or not content.strip()
            or (getattr(run, "metadata", None) or {}).get("compaction")):
        return messages
    direct = [message for message in messages if not message.from_history and not message.temporary]
    if (any(message.role == "user" for message in direct)
            and not any(message.role == "assistant" for message in direct)):
        messages.append(Message(role="assistant", content=content))
    return messages


def synth_interrupted_messages(run: Any) -> list[Message] | None:
    """Build ``[user, assistant?]`` for a run persisted without messages.

    Returns ``None`` (no-op) when there's no user input to recover, so a
    normally-completed run — which already has ``messages`` — is never touched
    by callers (they only call this when ``run.messages`` is empty).

    The assistant message carries whatever was generated before the stop (the
    runner streams the partial onto ``run.content``). We skip the runner's
    "Run … was cancelled" sentinel — written only when nothing was generated
    yet — so we never feed the model a reply it never gave.
    """
    inp = getattr(run, "input", None)
    ic = getattr(inp, "input_content", None) if inp is not None else None
    user_text = ic.strip() if isinstance(ic, str) and ic.strip() else None
    if not user_text:
        return None
    out = [Message(role="user", content=user_text)]
    content = getattr(run, "content", None)
    if isinstance(content, str):
        content = content.strip()
        run_id = getattr(run, "run_id", None)
        # Sentinels the runner writes when nothing was generated before the
        # stop — not a real reply, so don't feed them back to the model.
        sentinels = {"Operation cancelled by user"}
        if run_id:
            sentinels.add(f"Run {run_id} was cancelled")
        if content and content not in sentinels:
            out.append(Message(role="assistant", content=content))
    return out


def promote_interrupted_run(run: Any) -> None:
    """Make a stopped (CANCELLED) run survive history, in place.

    A stop CANCELs the run; ``get_messages`` skips CANCELLED runs, so the
    stopped turn would vanish and the next turn ("continua") would lose
    context. A stopped turn is still a real conversational turn, so promote it
    to COMPLETED — backfilling the user's question + partial reply first if it
    was stopped before its messages were assembled. No-op for non-cancelled
    runs. This is the single promotion path shared by every ``upsert_run``.
    """
    if getattr(run, "status", None) != RunStatus.cancelled:
        return
    if not run.messages:
        run.messages = synth_interrupted_messages(run)
    if run.messages:
        run.status = RunStatus.completed
