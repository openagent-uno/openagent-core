"""Opt-in capability to post in host-admitted messaging channel conversations.

Unlike a reply, which answers the conversation that started a run, a channel
message is published on the agent's own initiative (for example a report in a
team channel). The host lists the exact destinations this run may use and owns
authorization, file resolution and the provider transport.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Mapping, Protocol, Sequence

from .capabilities import CapabilityUnavailable, ToolDefinition
from .contracts import AuthorizationDenied, ExecutionContext, canonical_json
from .runtime import current_execution_context, current_run_id

TEXT_LIMIT = 12000
FILE_LIMIT = 10
_REFERENCE_LIMIT = 1024
_DESTINATION_LIMIT = 100

_LIST = ToolDefinition(
    "list_channel_destinations",
    "List the messaging channel conversations (for example a team Slack channel) "
    "where you may post on your own initiative.",
    {"type": "object", "properties": {}, "additionalProperties": False},
    metadata={"ui": {"visibility": ["model"]}},
)
_SEND = ToolDefinition(
    "send_channel_message",
    "Post a message, optionally with files, in a conversation returned by "
    "list_channel_destinations. Use it when the user asks you to share something "
    "there or a task requires reporting to that channel. Files are artifact "
    "references returned by your file tools (for example publish_file).",
    {
        "type": "object",
        "properties": {
            "destination": {"type": "string", "minLength": 1, "maxLength": 256},
            "text": {"type": "string", "maxLength": TEXT_LIMIT},
            "files": {"type": "array", "maxItems": FILE_LIMIT,
                      "items": {"type": "string", "minLength": 1, "maxLength": _REFERENCE_LIMIT}},
        },
        "required": ["destination"],
        "additionalProperties": False,
    },
    metadata={"ui": {"visibility": ["model"]}},
)


@dataclass(frozen=True, slots=True)
class ChannelDestination:
    id: str
    label: str
    provider: str


@dataclass(frozen=True, slots=True)
class ChannelMessageRequest:
    destination: str
    text: str
    files: tuple[str, ...]
    idempotency_key: str


class ChannelMessageSink(Protocol):
    """Host-owned publication in admitted channel conversations.

    ``destinations`` returns the conversations this exact run may use after
    fresh domain checks. ``send`` rechecks them, resolves file references in
    the run's own artifact scope, publishes once per idempotency key and
    returns a durable ID; a repeat returns the same ID without sending again.
    """

    async def destinations(self, context: ExecutionContext) -> Sequence[ChannelDestination]: ...

    async def send(self, request: ChannelMessageRequest, context: ExecutionContext) -> str: ...


def _text(value: Any, field: str, limit: int, *, required: bool = True) -> str:
    text = value.strip() if isinstance(value, str) else ""
    if (required and not text) or len(text) > limit or "\x00" in text:
        raise ValueError(f"Channel message {field} must contain at most {limit} characters")
    return text


def _destinations(values: Any) -> tuple[ChannelDestination, ...]:
    if not isinstance(values, (list, tuple)) or len(values) > _DESTINATION_LIMIT:
        raise ValueError("Channel destinations are invalid")
    result = []
    for value in values:
        if not isinstance(value, ChannelDestination):
            raise ValueError("Channel destinations are invalid")
        result.append(ChannelDestination(_text(value.id, "destination", 256), _text(value.label, "label", 256),
                                         _text(value.provider, "provider", 64)))
    return tuple(result)


class ChannelMessageCapability:
    """A catalog source and executor. Hosts register it only when a sink exists."""

    def __init__(self, sink: ChannelMessageSink) -> None:
        self._sink = sink

    async def inspect(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return (_LIST, _SEND)

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return (_LIST, _SEND)

    async def call_tool(self, name: str, arguments: Mapping[str, Any], context: ExecutionContext) -> dict[str, Any]:
        if name not in (_LIST.name, _SEND.name):
            raise CapabilityUnavailable("Unknown channel message tool")
        run_id = current_run_id()
        if not run_id or current_execution_context() != context:
            raise AuthorizationDenied("A channel message requires an active run")
        available = _destinations(await self._sink.destinations(context))
        if name == _LIST.name:
            if arguments:
                raise ValueError("list_channel_destinations takes no arguments")
            return {"destinations": [{"id": item.id, "label": item.label, "provider": item.provider}
                                     for item in available]}
        if not set(arguments) <= {"destination", "text", "files"} or "destination" not in arguments:
            raise ValueError("A channel message requires a destination and text or files")
        destination = _text(arguments["destination"], "destination", 256)
        if destination not in {item.id for item in available}:
            raise AuthorizationDenied("This conversation is not an available channel destination")
        text = _text(arguments.get("text", ""), "text", TEXT_LIMIT, required=False)
        files = arguments.get("files", [])
        if not isinstance(files, list) or len(files) > FILE_LIMIT:
            raise ValueError(f"A channel message can carry at most {FILE_LIMIT} files")
        files = tuple(_text(item, "file", _REFERENCE_LIMIT) for item in files)
        if len(set(files)) != len(files):
            raise ValueError("A channel message cannot repeat a file")
        if not text and not files:
            raise ValueError("A channel message requires text or files")
        # Retries of the same content in this run publish once.
        identity = canonical_json(["channel_message.v1", context.initiator.key, context.agent_id,
                                   context.session_id, run_id, destination, text, list(files)])
        key = sha256(identity.encode("utf-8")).hexdigest()
        message_id = await self._sink.send(ChannelMessageRequest(destination, text, files, key), context)
        if not isinstance(message_id, str) or not message_id.strip() or len(message_id) > 128:
            raise ValueError("Channel message sink did not return a durable ID")
        return {"id": message_id}
