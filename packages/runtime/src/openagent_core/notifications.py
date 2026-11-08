"""Opt-in notification capability for the current user of an admitted run."""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Mapping, Protocol

from .capabilities import CapabilityUnavailable, ToolDefinition
from .contracts import AuthorizationDenied, ExecutionContext, canonical_json
from .runtime import current_execution_context, current_run_id

_TITLE_LIMIT = 120
_BODY_LIMIT = 2000
_SEND = ToolDefinition(
    "send_notification",
    "Send the current user a private in-app notification. Use when they ask "
    "to be notified or need a result they can find outside this conversation.",
    {
        "type": "object",
        "properties": {
            "title": {"type": "string", "minLength": 1, "maxLength": _TITLE_LIMIT},
            "body": {"type": "string", "minLength": 1, "maxLength": _BODY_LIMIT},
        },
        "required": ["title", "body"],
        "additionalProperties": False,
    },
    metadata={"ui": {"visibility": ["model"]}},
)


@dataclass(frozen=True, slots=True)
class NotificationRequest:
    title: str
    body: str
    idempotency_key: str


class NotificationSink(Protocol):
    """Host-owned delivery to context.initiator after fresh domain checks.

    Persist the request once per idempotency key before returning its durable
    ID. A repeat returns the same ID. The sink owns recipient authorization,
    source visibility, transport and any delivery beyond the in-app record.
    """

    async def send(self, request: NotificationRequest, context: ExecutionContext) -> str: ...


def _has_recipient(context: ExecutionContext) -> bool:
    return context.initiator.kind == "user" and context.authority == context.initiator


def _bounded_text(value: Any, field: str, limit: int) -> str:
    text = value.strip() if isinstance(value, str) else ""
    if not 1 <= len(text) <= limit or "\x00" in text:
        raise ValueError(f"Notification {field} must contain 1–{limit} characters")
    try:
        text.encode("utf-8")
    except UnicodeError:
        raise ValueError(f"Notification {field} must be valid UTF-8 text") from None
    return text


class NotificationCapability:
    """A catalog source and executor. Hosts register it only when a sink exists."""

    def __init__(self, sink: NotificationSink) -> None:
        self._sink = sink

    async def inspect(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return (_SEND,)

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return (_SEND,) if _has_recipient(context) else ()

    async def call_tool(
        self, name: str, arguments: Mapping[str, Any], context: ExecutionContext
    ) -> dict[str, str]:
        if name != _SEND.name:
            raise CapabilityUnavailable("Unknown notification tool")
        run_id = current_run_id()
        if not run_id or current_execution_context() != context or not _has_recipient(context):
            raise AuthorizationDenied("A notification requires the current user of an active run")
        if set(arguments) != {"title", "body"}:
            raise ValueError("A notification requires only title and body")
        title = _bounded_text(arguments["title"], "title", _TITLE_LIMIT)
        body = _bounded_text(arguments["body"], "body", _BODY_LIMIT)
        if len(title.splitlines()) != 1:
            raise ValueError("A notification title must fit on one line")

        # A retry or repeated model call with the same content in this run
        # must not enqueue a second delivery, even when its tool call ID changes.
        identity = canonical_json([
            "notification.v1", context.initiator.key, context.agent_id,
            context.session_id, run_id, title, body,
        ])
        key = sha256(identity.encode("utf-8")).hexdigest()
        notification_id = await self._sink.send(NotificationRequest(title, body, key), context)
        if not isinstance(notification_id, str) or not notification_id.strip() or len(notification_id) > 128:
            raise ValueError("Notification sink did not return a durable ID")
        return {"id": notification_id}
