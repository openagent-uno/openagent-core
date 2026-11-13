"""History for runs that ended without a final answer.

A run that fails or is cancelled after executing tools has already changed
the world: files were written, commands ran, notes were saved. If the next
turn's history omits that run, the model neither knows what it already did
nor that it may be repeating side effects. These helpers keep the consistent
part of such a run (every assistant tool call paired with its results) and
close it with an explicit note that no final answer was produced.
"""

from __future__ import annotations

from typing import Iterable, List, Sequence

from openagent_core.models.providers.message import Message

INTERRUPTED_NOTE = (
    "[This turn ended with an error or was cancelled before a final answer. "
    "The tool calls and results above did happen; do not assume they were undone.]"
)


def _call_ids(message: Message) -> List[str]:
    ids: List[str] = []
    for call in message.tool_calls or []:
        call_id = call.get("id") if isinstance(call, dict) else getattr(call, "id", None)
        if call_id:
            ids.append(str(call_id))
    return ids


def consistent_prefix(messages: Sequence[Message]) -> List[Message]:
    """Return the longest prefix in which every tool call has its results.

    Providers reject an assistant tool call without its tool results, and a
    tool result without its call. The prefix ends before the first assistant
    tool-call message whose results are incomplete.
    """
    kept: List[Message] = []
    index = 0
    while index < len(messages):
        message = messages[index]
        if message.role == "assistant" and message.tool_calls:
            expected = set(_call_ids(message))
            group = [message]
            cursor = index + 1
            while cursor < len(messages) and messages[cursor].role == "tool":
                group.append(messages[cursor])
                cursor += 1
            answered = {str(m.tool_call_id) for m in group[1:] if m.tool_call_id}
            if not expected or not expected.issubset(answered):
                break
            kept.extend(group)
            index = cursor
            continue
        if message.role == "tool":
            # A result without its call cannot be replayed.
            break
        kept.append(message)
        index += 1
    return kept


def recover_interrupted_messages(messages: Iterable[Message]) -> List[Message]:
    """Messages to persist for a failed or cancelled run.

    History messages carried into the run keep their ``from_history`` tag and
    are kept as-is (readers skip them); the run's own messages are trimmed to
    a consistent prefix.
    """
    selected = [m for m in messages if m.add_to_agent_memory]
    carried = [m for m in selected if m.from_history]
    own = [m for m in selected if not m.from_history]
    return carried + consistent_prefix(own)


def interrupted_history(messages: Sequence[Message]) -> List[Message]:
    """The replayable history of an interrupted run, closed by a note."""
    own = consistent_prefix([m for m in messages if not m.from_history])
    if not any(m.role == "user" for m in own):
        return []
    last = own[-1]
    if not (last.role == "assistant" and not last.tool_calls and last.content):
        own = own + [Message(role="assistant", content=INTERRUPTED_NOTE)]
    return own
