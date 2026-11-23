"""Explicit, bounded conversation context for externally stored model runs."""

from dataclasses import dataclass
from typing import Protocol

from .contracts import ExecutionContext, ResourceRef


@dataclass(frozen=True, slots=True)
class ProviderHistoryMessage:
    role: str
    text: str
    source_run_id: str | None = None

    def __post_init__(self):
        if self.role not in {"user", "assistant"} or not isinstance(self.text, str):
            raise ValueError("Expected a canonical user or assistant text message")


class ProviderHistory(Protocol):
    async def provider_history(
        self, context: ExecutionContext, run_id: str, *, limit: int, max_bytes: int
    ) -> tuple[ProviderHistoryMessage, ...]:
        """Return earlier admitted turns in conversation order in the permitted budget."""
        ...


def external_history_active():
    from .runtime import current_runtime

    runtime = current_runtime()
    if runtime is None:
        return False
    vault = getattr(runtime, "profile", None)
    documents = vault and vault.modules.get("vault")
    return (
        bool(documents and documents.options.get("backend") == "documents")
        or runtime.settings.workspace is None
        or runtime.service(ProviderHistory) is not None
    )


async def previous_model_messages(session_id, run_id):
    from .core.execution_profile import stateless_completion_active
    from .runtime import current_execution_context, current_run_id, current_runtime

    if not external_history_active() or stateless_completion_active():
        return ()
    runtime, context = current_runtime(), current_execution_context()
    if not context or run_id != current_run_id() or session_id != context.session_id:
        # An internal classifier or team member gets only its own input.
        return ()
    history = runtime.service(ProviderHistory, runtime.services.store)
    reader = getattr(history, "provider_history", None)
    if not callable(reader):
        raise RuntimeError(
            "External model execution requires a canonical provider history service"
        )
    resource = ResourceRef("session", context.tenant_id, context.session_id)
    # The history window is authorized one source run at a time. This lets a
    # new turn continue using independent history after the user forgets a note.
    await runtime._authorize_policy(
        context, "session.read", resource, audience=context.audience
    )
    limit, max_bytes = 128, 65536
    messages = await reader(context, run_id, limit=limit, max_bytes=max_bytes)
    if not isinstance(messages, tuple) or len(messages) > limit:
        raise ValueError("Provider history exceeded its message budget")
    if not all(isinstance(message, ProviderHistoryMessage) for message in messages):
        raise TypeError("Expected canonical provider history messages")
    if sum(len(message.text.encode("utf-8")) for message in messages) > max_bytes:
        raise ValueError("Provider history exceeded its byte budget")
    from .dream_evidence import authorize_dream_history, dream_run_id
    from .memory.document_exposure import authorize_document_history

    if dream_run_id(session_id):
        await authorize_dream_history(runtime, context, session_id)
    decisions = {}
    for source in {m.source_run_id for m in messages}:
        try:
            if source is None:
                # A host compactor that doesn't identify the original runs
                # conservatively inherits every source in this conversation.
                await authorize_document_history(
                    runtime, context, session_id=session_id
                )
            else:
                await authorize_document_history(runtime, context, run_id=source)
        except PermissionError:
            decisions[source] = False
        else:
            decisions[source] = True
    messages = tuple(m for m in messages if decisions[m.source_run_id])
    await runtime._authorize_policy(
        context, "session.read", resource, audience=context.audience
    )
    from .runtime import check_current_publication

    await check_current_publication()
    from .models.providers.message import Message

    return tuple(
        Message(role=m.role, content=m.text, from_history=True) for m in messages
    )
