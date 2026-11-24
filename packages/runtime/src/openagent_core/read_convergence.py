"""Bound consecutive model reads of an unchanged, still-authorized resource."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

@dataclass(frozen=True)
class VerifiedToolRead:
    """An executor's content-free description of a successful versioned read."""

    signature: tuple
    is_current: Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class _Published:
    tool_ref: str
    tool_name: str
    read: VerifiedToolRead
    authorize: Callable[[], Awaitable[None]]


@dataclass
class _Reads:
    identity: tuple
    calls: dict[str, _Published | None] = field(default_factory=dict)


_active = ContextVar("openagent_verified_reads", default=None)
_NUDGE = "The last two reads returned the same verified revision. Use that evidence to answer, or choose a different operation if more work is needed."
_FINISH = "The same verified read has succeeded three times. Answer the user now using the authorized results already in this conversation. Only claim work that those results confirm."


@contextmanager
def verified_read_turn(runtime, context, run_id):
    identity = (runtime, context, run_id)
    parent = _active.get()
    if parent is not None and parent.identity == identity:
        yield
        return
    token = _active.set(_Reads(identity) if run_id is not None else None)
    try:
        yield
    finally:
        _active.reset(token)


def published_read(call_id, tool_ref, tool_name, read, authorize):
    state = _active.get()
    if state is None or not call_id:
        return
    # A single provider tool can execute a batch. A repeated ID is ambiguous.
    value = _Published(tool_ref, tool_name, read, authorize) if call_id not in state.calls else None
    state.calls[call_id] = value
    if len(state.calls) > 8:
        del state.calls[next(iter(state.calls))]


async def decision_after_reads(request, *, schema_transport=False):
    state = _active.get()
    if not state or not request.get("tools") or request.get("tool_choice") not in (None, "auto"):
        return request, False
    from .dream_evidence import is_document_dream
    runtime, context, _ = state.identity
    if is_document_dream(context):
        from .memory.vault.document_dream import ground_maintenance_decision
        request = await ground_maintenance_decision(runtime, context, request)
    if len(state.calls) < 2 or request.get("tool_choice") not in (None, "auto"):
        return request, False
    messages = request["messages"]
    index = len(messages)
    latest = None
    count = 0
    used = set()
    # Only inspect an uninterrupted trailing sequence of three single-tool
    # decisions. A user message, parallel batch, other tool or error ends it.
    while index >= 2 and count < 3:
        assistant, result = messages[index - 2:index]
        calls = assistant.tool_calls or []
        if (assistant.role != "assistant" or len(calls) != 1 or result.role != "tool"
                or result.tool_call_error or not result.tool_call_id
                or calls[0].get("id") != result.tool_call_id or result.tool_call_id in used):
            break
        entry = state.calls.get(result.tool_call_id)
        if entry is None:
            break
        # A code execution or composite workflow can read a note while also
        # doing other work under the same provider call ID.
        function = calls[0].get("function") or {}
        if function.get("name") not in (entry.tool_name, "tool_search_call_tool", "tool_search_call_named_tool"):
            break
        if latest is None:
            latest = entry
        elif entry.tool_ref != latest.tool_ref or entry.read.signature != latest.read.signature:
            break
        used.add(result.tool_call_id)
        count += 1
        index -= 2
    if count < 2:
        return request, False
    await latest.authorize()
    if not await latest.read.is_current():
        return request, False
    final = count == 3
    if final:
        from .runtime import guard_current_publication
        guard_current_publication(latest.authorize)
    guidance = _FINISH if final else _NUDGE
    updated = list(messages)
    if updated and updated[0].role == "system" and isinstance(updated[0].content, str):
        updated[0] = updated[0].model_copy(update={"content": updated[0].content + "\n\n" + guidance})
    else:
        from .models.providers.message import Message
        updated.insert(0, Message(role="system", content=guidance, temporary=True))
    options = {**request, "messages": updated}
    if final:
        # The schema adapter uses its usual validated reply-only envelope.
        # A native provider receives no function definitions on this pass.
        options.update(tool_choice="none" if schema_transport else None,
                       tools=request["tools"] if schema_transport else [])
    return options, final


def require_final_text(response, final):
    if final and response.tool_calls:
        from .core.runtime_errors import ModelProviderError
        raise ModelProviderError(message="The model selected a tool on a final answer pass", status_code=422)
