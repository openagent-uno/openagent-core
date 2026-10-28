"""Opt-in, instance-owned transports for the three management builtins.

The original handlers remain the source of tool semantics. A task-local binding
selects this toolkit's database, never a module-global singleton or another
runtime's environment. No provider keys or host process environment are copied.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from functools import wraps
import inspect
from pathlib import Path
from typing import Any, Mapping, get_type_hints

import jsonschema
from mcp import types as mcp_types
from mcp.shared.exceptions import UrlElicitationRequiredError

from openagent_core.mcp._runtime.function import ToolResult
from openagent_core.mcp._runtime.toolkit import Toolkit
from .management_context import _binding


_TOOLS = {
    "model-manager": (
        "list_providers", "add_provider", "update_provider", "remove_provider",
        "list_models", "get_model", "list_supported_providers", "list_supported_frameworks",
        "add_model", "update_model", "set_classifier_model", "enable_model", "disable_model",
        "remove_model", "pin_session", "unpin_session", "list_available_models", "test_model",
    ),
    "budget-manager": ("list_budgets", "get_budget_usage", "set_budget", "remove_budget"),
    "skill-data": ("list_skills", "search_events", "list_events_by_entity", "list_entities", "recent_events"),
}


class ManagementToolkit(Toolkit):
    """One Runtime/pool owns this object, its connections and active calls."""

    def __init__(self, name: str, module: Any, env: Mapping[str, str]) -> None:
        from openagent_core.runtime import current_runtime

        raw = env.get("OPENAGENT_DB_PATH")
        if not isinstance(raw, str) or not raw.strip() or not Path(raw).is_absolute():
            raise ValueError("In-process management requires an explicit absolute OPENAGENT_DB_PATH")
        self.db_path = str(Path(raw).resolve())
        self._owner = current_runtime()
        self._closed = False
        self._active: set[asyncio.Task] = set()
        self._db = None
        self._db_lock = asyncio.Lock()
        self._module = module
        self._published = {}
        tools = [self._wrap(getattr(module, tool_name), tool_name) for tool_name in _TOOLS[name]]
        super().__init__(name=name, tools=tools)

    async def prepare(self, module: Any) -> ManagementToolkit:
        # Use the SDK's public published schema, including nullable/default
        # arguments; do not regenerate a subtly different provider contract.
        published = {tool.name: tool for tool in await module.mcp.list_tools()}
        if set(published) != set(self.async_functions):
            raise RuntimeError("Management handler and published tool sets disagree")
        self._published = {name: tool.model_copy(deep=True) for name, tool in published.items()}
        for name, function in self.async_functions.items():
            function.parameters = deepcopy(published[name].inputSchema)
            function.description = published[name].description or ""
            # Preserve the SDK's protocol validation/error envelope, rather
            # than raising a different outer pydantic validation exception.
            function.skip_entrypoint_processing = True
        return self

    def _check_owner(self) -> None:
        from openagent_core.runtime import current_runtime

        if self._closed:
            raise RuntimeError("Management toolkit is closed")
        if self._owner is not None and current_runtime() is not self._owner:
            raise PermissionError("Management toolkit belongs to a different runtime")

    async def _database(self):
        from openagent_core.memory.db import MemoryDB

        self._check_owner()
        async with self._db_lock:
            self._check_owner()
            if self._db is None:
                db = MemoryDB(self.db_path)
                try:
                    await db.connect()
                except BaseException:
                    await db.close()
                    raise
                self._db = db
            return self._db

    async def _protocol_result(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        """Keep the public FastMCP conversion and the complete SDK envelope.

        FastMCP produces content blocks and optional structured output; the
        low-level MCP contract packs those into CallToolResult. Do not infer
        text/JSON serialization from one handler's return value.
        """
        tool = self._published[name]
        try:
            # FastMCP deliberately disables the low-level JSON-schema input
            # validator: its own public call_tool validates and converts the
            # arguments. Pre-validating here changes coercion and error text.
            converted = await self._module.mcp.call_tool(name, arguments)
            if isinstance(converted, mcp_types.CallToolResult):
                result = converted
            else:
                if isinstance(converted, tuple) and len(converted) == 2:
                    content, structured = converted
                else:
                    content, structured = converted, None
                if tool.outputSchema is not None:
                    if structured is None:
                        raise ValueError("Output validation error: outputSchema defined but no structured output returned")
                    try:
                        jsonschema.validate(instance=structured, schema=tool.outputSchema)
                    except jsonschema.ValidationError as exc:
                        raise ValueError(f"Output validation error: {exc.message}") from exc
                result = mcp_types.CallToolResult(content=list(content), structuredContent=structured, isError=False)
        except UrlElicitationRequiredError:
            raise  # The MCP server also delegates this protocol error upstream.
        except Exception as exc:
            result = mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=str(exc))], isError=True)

        from openagent_core.core._runner.utils.mcp import (
            _bounded_mcp_text, _MCP_COMBINED_TEXT_MAX_CHARS, _MCP_COMBINED_TEXT_MAX_BYTES,
            _MCP_CONTENT_MAX_ITEMS,
        )
        display = "\n".join(_bounded_mcp_text(block.text, label="MCP text content")[0]
                            for block in result.content[:_MCP_CONTENT_MAX_ITEMS]
                            if isinstance(block, mcp_types.TextContent)).strip()
        if result.isError:
            display = f"Error from MCP tool '{name}': {display or 'MCP tool returned no displayable error content'}"
        display, _ = _bounded_mcp_text(display, label="MCP combined tool output",
                                     max_chars=_MCP_COMBINED_TEXT_MAX_CHARS,
                                     max_bytes=_MCP_COMBINED_TEXT_MAX_BYTES)
        return ToolResult(content=display, mcp_result=result.model_dump(mode="json", by_alias=True, exclude_none=False))

    def _wrap(self, handler, name: str):
        @wraps(handler)
        async def invoke(*args, **kwargs):
            self._check_owner()
            task = asyncio.current_task()
            if task is None or task in self._active:
                raise RuntimeError("Management calls require a distinct running task")
            self._active.add(task)
            token = _binding.set((self, task))
            try:
                arguments = dict(inspect.signature(handler).bind(*args, **kwargs).arguments) if args else kwargs
                return await self._protocol_result(name, arguments)
            finally:
                _binding.reset(token)
                self._active.discard(task)

        # Resolved annotations belong to the original module, not this wrapper.
        # Both the SDK and Toolkit must see the exact original tool signature.
        hints = get_type_hints(handler)
        signature = inspect.signature(handler)
        invoke.__signature__ = signature.replace(
            parameters=[parameter.replace(annotation=hints.get(key, parameter.annotation))
                        for key, parameter in signature.parameters.items()],
            return_annotation=hints.get("return", signature.return_annotation),
        )
        invoke.__annotations__ = hints
        invoke.__name__ = name
        return invoke

    async def aclose(self) -> None:
        self._closed = True  # No new work is admitted while draining this instance.
        current = asyncio.current_task()
        active = [task for task in self._active if task is not current]
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        async with self._db_lock:
            if self._db is not None:
                await self._db.close()
                self._db = None


async def build_model_toolkit(*, env: Mapping[str, str]) -> ManagementToolkit:
    from .servers.model_manager import server
    return await ManagementToolkit("model-manager", server, env).prepare(server)


async def build_budget_toolkit(*, env: Mapping[str, str]) -> ManagementToolkit:
    from .servers.budget_manager import server
    return await ManagementToolkit("budget-manager", server, env).prepare(server)


async def build_skill_data_toolkit(*, env: Mapping[str, str]) -> ManagementToolkit:
    from .servers.skill_data import server
    return await ManagementToolkit("skill-data", server, env).prepare(server)
