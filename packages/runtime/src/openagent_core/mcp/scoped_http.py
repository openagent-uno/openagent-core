"""A streamable HTTP MCP source with host-issued authority for each operation."""
from __future__ import annotations

import asyncio
import math
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol
from urllib.parse import urlsplit

from openagent_core.capabilities import CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import ExecutionContext

_TOOL = re.compile(r"[A-Za-z0-9_.-]{1,128}")


@dataclass(frozen=True, slots=True)
class McpHttpGrant:
    """The bearer is passed only to the configured service for this exchange."""

    binding: str
    expires_at: float
    bearer: str = field(repr=False)


class McpGrantResolver(Protocol):
    async def issue(self, context: ExecutionContext, tool: str | None) -> McpHttpGrant:
        """Issue discovery authority for None or invocation authority for one remote name."""
        ...

    async def valid(self, context: ExecutionContext, tool: str | None, binding: str) -> bool:
        """Check the same authorization generation, including revocation."""
        ...


class ScopedHttpMcpSource:
    """Use the host's trusted endpoint and resolve authority from the current caller."""

    def __init__(self, endpoint: str, grants: McpGrantResolver, *, name_prefix: str = "", timeout: float = 30):
        remote = urlsplit(endpoint)
        if (remote.scheme not in {"https", "http"} or not remote.hostname or remote.username
                or remote.password or remote.query or remote.fragment):
            raise ValueError("MCP endpoint must be a configured HTTP origin and path")
        if name_prefix and not _TOOL.fullmatch(name_prefix):
            raise ValueError("Invalid MCP tool prefix")
        if not 0 < timeout <= 300:
            raise ValueError("Invalid MCP exchange timeout")
        self.endpoint = endpoint
        self.grants = grants
        self.prefix = name_prefix
        self.timeout = timeout

    async def _authorize(self, context, tool, grant):
        if (not isinstance(grant, McpHttpGrant) or not grant.binding or not grant.bearer
                or not math.isfinite(grant.expires_at) or not time.time() < grant.expires_at
                or not await self.grants.valid(context, tool, grant.binding)):
            raise PermissionError("The scoped MCP grant is unavailable")

    @asynccontextmanager
    async def _session(self, context, tool):
        import httpx
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        async with asyncio.timeout(self.timeout):
            grant = await self.grants.issue(context, tool)
            await self._authorize(context, tool, grant)
            async with httpx.AsyncClient(
                headers={"Authorization": "Bearer " + grant.bearer}, timeout=self.timeout,
                follow_redirects=False, trust_env=False,
            ) as client:
                async with streamable_http_client(self.endpoint, http_client=client) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        await self._authorize(context, tool, grant)
                        yield session
            await self._authorize(context, tool, grant)

    async def discover(self, context):
        from mcp.types import PaginatedRequestParams

        cursor = None
        cursors, definitions, names = set(), [], set()
        async with self._session(context, None) as session:
            for _ in range(32):
                page = await session.list_tools(params=PaginatedRequestParams(cursor=cursor))
                for spec in page.tools:
                    name = self.prefix + spec.name
                    if not _TOOL.fullmatch(name) or name in names or len(names) >= 512:
                        raise CapabilityUnavailable("Invalid scoped MCP tool catalog")
                    names.add(name)
                    definitions.append(ToolDefinition(name, spec.description or "", spec.inputSchema,
                                                      metadata=spec.meta or {}))
                cursor = page.nextCursor
                if not cursor:
                    break
                if cursor in cursors:
                    raise CapabilityUnavailable("Repeated scoped MCP catalog cursor")
                cursors.add(cursor)
            else:
                raise CapabilityUnavailable("Scoped MCP catalog exceeds its page budget")
        return tuple(definitions)

    async def call_tool(self, name, arguments, context):
        if not name.startswith(self.prefix) or not _TOOL.fullmatch(name.removeprefix(self.prefix)):
            raise PermissionError("Invalid scoped MCP tool")
        tool = name.removeprefix(self.prefix)
        async with self._session(context, tool) as session:
            result = await session.call_tool(tool, dict(arguments))
        return result.model_dump(by_alias=True, exclude_none=True, mode="json")
