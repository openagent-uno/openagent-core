"""Direct desktop interaction over an explicitly authorized capability catalog."""
from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from typing import Any, Mapping, TypedDict

from .capabilities import CapabilityUnavailable, ToolDescriptor, tool_visible
from .contracts import AuthorizationDenied, ExecutionContext, ResourceRef
from .runtime import Runtime, execution_scope


class ComputerTarget(TypedDict):
    tool_ref: str
    label: str
    live_media: str | None


_POINTER_ACTIONS = {
    "mouse_move", "middle_click", "left_click", "double_click",
    "right_click", "left_click_drag", "scroll",
}
_TEXT_ACTIONS = {"key", "type", "scroll"}
_ACTIONS = _POINTER_ACTIONS | _TEXT_ACTIONS | {"get_screenshot"}
_DESKTOP_ACTIONS = {"get_screenshot", "mouse_move", "left_click", "key", "type"}
_STREAM_AUTHORIZATION_INTERVAL_SECONDS = 2.0
_STREAM_OPTIONS = frozenset({"display_id", "fps", "max_duration_seconds"})
_SCROLL = re.compile(r"(?:up|down|left|right)(?::(?:[1-9][0-9]{0,2}|[12][0-9]{3}|3000))?")


def _is_desktop(tool: ToolDescriptor, source_id: str | None) -> bool:
    if source_id is not None and tool.source_id != source_id:
        return False
    if tool.name not in {"computer", "computer_control_computer"} or not tool_visible(tool.metadata, "app"):
        return False

    # Browser tools can also be named "computer". Match the desktop input
    # schema, never the displayed label or the opaque reference's spelling.
    properties = tool.input_schema.get("properties")
    action = properties.get("action") if isinstance(properties, Mapping) else None
    required = tool.input_schema.get("required", ())
    if (not isinstance(action, Mapping) or not isinstance(required, (list, tuple))
            or any(key not in ("action", "coordinate", "text") for key in required)):
        return False
    actions = action.get("enum")
    return "enum" not in action or (
        isinstance(actions, (list, tuple)) and all(value in actions for value in _DESKTOP_ACTIONS)
    )


def _parse_action(body: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    if not isinstance(body, Mapping) or set(body) - {"tool_ref", "action", "coordinate", "text"}:
        raise ValueError("Invalid computer request")
    reference, action = body.get("tool_ref"), body.get("action")
    if not isinstance(reference, str) or not 1 <= len(reference) <= 512:
        raise ValueError("Exact computer reference required")
    if not isinstance(action, str) or action not in _ACTIONS:
        raise ValueError("Unsupported computer action")

    arguments = {"action": action}
    if action in _POINTER_ACTIONS:
        xy = body.get("coordinate")
        if not isinstance(xy, list) or len(xy) != 2 or any(type(v) is not int or not 0 <= v <= 32768 for v in xy):
            raise ValueError("Screen coordinate required")
        arguments["coordinate"] = xy
    if action in _TEXT_ACTIONS:
        value = body.get("text")
        if not isinstance(value, str) or not 1 <= len(value) <= 16384:
            raise ValueError("Text required")
        if action == "scroll" and _SCROLL.fullmatch(value) is None:
            raise ValueError("Invalid scroll direction or amount")
        arguments["text"] = value
    return reference, arguments


class ComputerInteraction:
    """Optional host service for direct human input to an exact desktop.

    The host authenticates and binds a fresh context for each request. The
    optional source selector is trusted host configuration, never request
    data. Core owns current session and catalog checks around the operation.
    """

    def __init__(self, runtime: Runtime) -> None:
        self._runtime = runtime

    async def _authorize(self, context: ExecutionContext, action: str) -> None:
        if (context.initiator.kind != "user" or context.authority != context.initiator
                or context.deferred or context.delegation_id):
            raise AuthorizationDenied("Direct desktop access requires a current human user")
        session = ResourceRef("session", context.tenant_id, context.session_id)
        await self._runtime.authorize(context, action, session, audience=context.audience)

    async def authorize_access(self, context: ExecutionContext, *, interactive: bool) -> None:
        """Check the current human viewer before resolving an external target assignment."""
        await self._authorize(context, "run.execute" if interactive else "run.read")

    async def targets(self, context: ExecutionContext, *, source_id: str | None = None) -> list[ComputerTarget]:
        """Discover only current, app-visible desktops authorized for this session."""
        await self._authorize(context, "run.read")
        with execution_scope(self._runtime, context, None):
            tools = await self._runtime.capabilities.discover(context, source_id=source_id)
        await self._authorize(context, "run.read")
        targets = []
        for tool in tools:
            if _is_desktop(tool, source_id):
                media = tool.metadata.get("openagent/liveMedia")
                targets.append(ComputerTarget(tool_ref=tool.tool_ref, label=tool.target_label,
                                              live_media=media if isinstance(media, str) else None))
        return targets

    async def interact(self, context: ExecutionContext, body: Mapping[str, Any], *,
                       source_id: str | None = None) -> Any:
        """Invoke one exact desktop action without submitting an agent run."""
        reference, arguments = _parse_action(body)
        await self._authorize(context, "run.execute")
        catalog = self._runtime.capabilities
        with execution_scope(self._runtime, context, None):
            tool = await catalog.describe_tool(reference, context)
            if not _is_desktop(tool, source_id):
                raise CapabilityUnavailable("The exact desktop is unavailable or no longer authorized")
            result = await catalog.call_tool(reference, arguments, context, surface="app")
        await self._authorize(context, "run.execute")
        return result.model_dump(mode="json", by_alias=True) if hasattr(result, "model_dump") else result

    async def stream(
        self, context: ExecutionContext, tool_ref: str, options: Mapping[str, Any],
        *, source_id: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Open exact desktop media within the current human session.

        The host supplies the trusted source selector and owns its producer
        registration. Core revalidates the viewer and the exact capability
        during delivery and while waiting for an idle producer.
        """
        if not isinstance(tool_ref, str) or not 1 <= len(tool_ref) <= 512:
            raise ValueError("Exact computer reference required")
        if not isinstance(options, Mapping) or set(options) - _STREAM_OPTIONS:
            raise ValueError("Invalid desktop media options")
        await self._authorize(context, "run.execute")
        catalog = self._runtime.capabilities
        with execution_scope(self._runtime, context, None):
            tool = await catalog.describe_tool(tool_ref, context)
        if not _is_desktop(tool, source_id) or not tool.metadata.get("openagent/liveMedia"):
            raise CapabilityUnavailable("The exact desktop has no authorized media producer")

        resource = ResourceRef("tool", context.tenant_id, tool.source_id + "/" + tool.name)

        async def authorize(*, check_source: bool = False) -> None:
            await self._authorize(context, "run.execute")
            await self._runtime.authorize(context, "tool.call", resource, audience=context.audience)
            if check_source:
                await self._runtime.authorize(context, "tool.publish", resource, audience=context.audience)
                with execution_scope(self._runtime, context, None):
                    current = await catalog.describe_tool(tool_ref, context)
                if current != tool:
                    raise CapabilityUnavailable("The desktop media producer changed")

        await authorize(check_source=True)
        iterator = catalog.stream_tool(tool_ref, dict(options), context, before_publish=authorize)

        async def read_packet() -> dict[str, Any]:
            with execution_scope(self._runtime, context, None):
                return await anext(iterator)

        pending = None
        try:
            while True:
                await authorize()
                pending = asyncio.create_task(read_packet())
                while True:
                    done, _ = await asyncio.wait((pending,), timeout=_STREAM_AUTHORIZATION_INTERVAL_SECONDS)
                    if done:
                        break
                    await authorize(check_source=True)
                try:
                    packet = pending.result()
                except StopAsyncIteration:
                    return
                finally:
                    pending = None
                yield packet
        finally:
            if pending is not None:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            with execution_scope(self._runtime, context, None):
                await iterator.aclose()
