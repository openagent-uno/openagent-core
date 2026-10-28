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

from pydantic import validate_call

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
        tools = [self._wrap(getattr(module, tool_name)) for tool_name in _TOOLS[name]]
        super().__init__(name=name, tools=tools)

    async def prepare(self, module: Any) -> ManagementToolkit:
        # Use the SDK's public published schema, including nullable/default
        # arguments; do not regenerate a subtly different provider contract.
        published = {tool.name: tool for tool in await module.mcp.list_tools()}
        if set(published) != set(self.async_functions):
            raise RuntimeError("Management handler and published tool sets disagree")
        for name, function in self.async_functions.items():
            function.parameters = deepcopy(published[name].inputSchema)
            function.description = published[name].description or ""
            # MCP transports also retain the SDK schema verbatim. Validation
            # is already attached to our entrypoint, without regenerating it.
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

    def _wrap(self, handler):
        @wraps(handler)
        async def invoke(*args, **kwargs):
            self._check_owner()
            task = asyncio.current_task()
            if task is None or task in self._active:
                raise RuntimeError("Management calls require a distinct running task")
            self._active.add(task)
            token = _binding.set((self, task))
            try:
                return await handler(*args, **kwargs)
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
        return validate_call(invoke, config={"arbitrary_types_allowed": True})

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
