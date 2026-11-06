"""Optional execution adapters configured by a host; no client or account policy."""
from contextlib import asynccontextmanager
from contextvars import ContextVar
import asyncio
import json


class ToolInventoryContext:
    """Expose a bounded, freshly authorized tool inventory as ephemeral context.

    The host selects the tool and maps domain data. The kernel neither assumes
    device presence nor interprets identities, sharing rules or provider fields.
    """
    def __init__(self, *, source_id, tool_name, key, normalize, instructions="", items_key="items", limit=8, timeout=4):
        if not source_id or not tool_name or not key or not callable(normalize) or not 1 <= limit <= 256:
            raise ValueError("Explicit bounded inventory contract required")
        self.source_id, self.tool_name, self.key = source_id, tool_name, key
        self.normalize, self.instructions, self.items_key, self.limit, self.timeout = normalize, instructions, items_key, limit, timeout
        self.current = ContextVar("openagent_tool_inventory", default=None)

    def prompt_context(self, context):
        return dict(self.current.get() or {})

    async def prepare(self, runtime, context):
        if context.deferred:
            return {}
        try:
            async with asyncio.timeout(self.timeout):
                tools = await runtime.capabilities.discover(context)
                matches = [tool for tool in tools if tool.source_id == self.source_id and tool.name == self.tool_name]
                if len(matches) != 1:
                    raise LookupError("Inventory capability unavailable")
                result = await runtime.capabilities.call_tool(matches[0].tool_ref, {}, context)
            if not isinstance(result, dict) or result.get("isError") or result.get("error"):
                raise ValueError("Invalid inventory result")
            content = result.get("content")
            if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict) or content[0].get("type") != "text":
                raise ValueError("Unexpected inventory envelope")
            raw = content[0]["text"]
            if not isinstance(raw, str) or len(raw.encode()) > 2 * 1024 * 1024:
                raise ValueError("Inventory exceeds its bound")
            items = json.loads(raw)
            if not isinstance(items, list) or len(items) > 4096:
                raise ValueError("Invalid inventory")
            values = [self.normalize(item) for item in items]
            return {self.key: {"status": "available" if values else "none", self.items_key: values[:self.limit],
                              "additional_count": max(0, len(values) - self.limit), "instructions": self.instructions}}
        except PermissionError:
            return {}
        except (TimeoutError, LookupError, ValueError, KeyError, TypeError):
            return {self.key: {"status": "unavailable", "instructions":
                f"Current inventory is unavailable. Discover {self.tool_name} before claiming a destination is reachable. " + self.instructions}}

    @asynccontextmanager
    async def execution(self, runtime, context):
        token = self.current.set(await self.prepare(runtime, context))
        try:
            yield
        finally:
            self.current.reset(token)
