"""Exact MCP connection lifetime, including unchanged catalogs and cached UI replies."""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from mcp.types import CallToolResult, ListToolsResult, Tool
from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp._runtime.mcp import MCPTools
from openagent_core.mcp.catalog import PoolCapabilitySource
from openagent_core.mcp_apps import McpAppService

URI = "ui://owned/connection-generation"


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return context.author.subject_id == "owner"


def connection(label):
    tools = [Tool(name="show", description="Owned fixture", inputSchema={"type": "object"},
        **{"_meta": {"ui": {"resourceUri": URI}, "openagent/resource-generation": "remote-forgery"}}),
        Tool(name="mutate", description="Owned mutation oracle", inputSchema={"type": "object"})]
    return SimpleNamespace(
        list_tools=AsyncMock(return_value=ListToolsResult(tools=tools)),
        read_resource=AsyncMock(return_value={"contents": [{"uri": URI,
            "mimeType": "text/html;profile=mcp-app", "text": "<p>" + label + "</p>"}]}),
        send_ping=AsyncMock(), call_tool=AsyncMock(return_value=CallToolResult(content=[])))


class ConnectionGenerationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        p = PrincipalRef("test", "tenant", "owner")
        self.context = ExecutionContext(p, p, p, "session", "agent", (p,))
        self.first = connection("first")
        self.toolkit = MCPTools(session=self.first)
        await self.toolkit.build_tools()
        self.pool = SimpleNamespace(toolkit_by_name=lambda _: self.toolkit)
        self.source = PoolCapabilitySource(self.pool, "fixture", self.toolkit)
        self.catalog = CapabilityCatalog(Policy())
        self.catalog.register("fixture", self.source, self.source, target_label="Owned fixture")
        self.service = McpAppService(self.catalog)
        self.ref = next(t.tool_ref for t in await self.catalog.discover(self.context) if t.name == "show")
        self.view = await self.service.open(self.ref, self.context)

    async def asyncTearDown(self):
        await self.service.shutdown()

    async def replace(self):
        self.second = connection("second")
        self.toolkit.session = self.second
        await self.toolkit.build_tools()

    async def test_reconnect_rejects_old_ref_view_and_cached_read_or_mutation(self):
        generation = self.toolkit.capability_generation
        self.assertNotEqual("remote-forgery", generation)
        await self.service.request(self.view["handle"], "resources/read", {"uri": URI},
            self.context, request_id="read")
        await self.service.request(self.view["handle"], "tools/call", {"name": "mutate"},
            self.context, request_id="mutation")
        self.assertEqual(1, self.first.call_tool.await_count)
        await self.replace()
        self.assertNotEqual(generation, self.toolkit.capability_generation)
        with self.assertRaises(CapabilityUnavailable):
            await self.catalog.describe_tool(self.ref, self.context)
        for method, params, request in [("authorize", {}, "authorization"),
                ("resources/read", {"uri": URI}, "read"),
                ("tools/call", {"name": "mutate"}, "mutation")]:
            with self.subTest(method=method), self.assertRaises(CapabilityUnavailable):
                await self.service.request(self.view["handle"], method, params, self.context, request_id=request)
        self.second.call_tool.assert_not_awaited()
        self.second.read_resource.assert_not_awaited()
        new_ref = next(t.tool_ref for t in await self.catalog.discover(self.context) if t.name == "show")
        self.assertNotEqual(self.ref, new_ref)
        new_view = await self.service.open(new_ref, self.context)
        self.assertEqual("<p>second</p>", new_view["resource"]["contents"][0]["text"])

    async def test_same_connection_keeps_reference_and_existing_function_identity(self):
        function = self.toolkit.functions["show"]
        for _ in range(2):
            self.assertEqual(self.ref, next(t.tool_ref for t in await self.catalog.discover(self.context) if t.name == "show"))
            await self.service.request(self.view["handle"], "authorize", {}, self.context)
        self.assertIs(function, self.toolkit.functions["show"])
        generation = self.toolkit.capability_generation
        await self.toolkit.build_tools()
        self.assertEqual(generation, self.toolkit.capability_generation)
        self.assertEqual(self.ref, next(t.tool_ref for t in await self.catalog.discover(self.context) if t.name == "show"))

    async def test_uninitialized_replacement_and_failed_refresh_hide_retained_functions(self):
        self.first.list_tools.side_effect = RuntimeError("owned failed refresh on same connection")
        with self.assertRaises(RuntimeError):
            await self.toolkit.build_tools()
        self.assertEqual((), await self.source.discover(self.context))
        with self.assertRaises(CapabilityUnavailable):
            await self.source.call_tool("mutate", {}, self.context)
        self.first.call_tool.assert_not_awaited()
        self.toolkit.session = connection("not-initialized")
        self.assertIsNone(self.toolkit.capability_generation)
        self.assertEqual((), await self.source.discover(self.context))
        with self.assertRaises(CapabilityUnavailable):
            await self.service.request(self.view["handle"], "tools/call", {"name": "mutate"}, self.context)
        self.toolkit.session.list_tools.side_effect = RuntimeError("owned failed catalog")
        with self.assertRaises(RuntimeError):
            await self.toolkit.build_tools()
        self.assertIn("mutate", self.toolkit.functions)
        self.assertEqual((), await self.source.discover(self.context))
        with self.assertRaises(CapabilityUnavailable):
            await self.source.call_tool("mutate", {}, self.context)
        self.toolkit.session.call_tool.assert_not_awaited()

    async def test_reconnect_during_resource_io_rejects_late_result_without_retry(self):
        async def read(uri):
            await self.replace()
            return {"contents": [{"uri": URI, "mimeType": "text/html;profile=mcp-app", "text": "old"}]}
        self.first.read_resource.side_effect = read
        with self.assertRaises(CapabilityUnavailable):
            await self.service.request(self.view["handle"], "resources/read", {"uri": URI}, self.context)
        self.assertEqual(2, self.first.read_resource.await_count)  # open + one request
        self.second.read_resource.assert_not_awaited()

    async def test_dynamic_credentials_wait_cannot_cross_connection_before_app_mutation(self):
        self.toolkit.header_provider = lambda: {}
        async def credentials(**kwargs):
            await self.replace()
            return self.second
        self.toolkit.get_session_for_run = AsyncMock(side_effect=credentials)
        self.toolkit.cleanup_run_session = AsyncMock()
        with self.assertRaises(CapabilityUnavailable):
            await self.service.request(self.view["handle"], "tools/call", {"name": "mutate"}, self.context)
        self.first.call_tool.assert_not_awaited()
        self.second.call_tool.assert_not_awaited()
        self.toolkit.cleanup_run_session.assert_awaited_once()

    async def test_reconnect_while_ping_or_ready_prevents_model_tool_dispatch(self):
        for boundary in ("ping", "ready"):
            with self.subTest(boundary=boundary):
                original = self.toolkit.session
                async def replace_at_boundary():
                    await self.replace()
                    return True
                if boundary == "ping":
                    original.send_ping.side_effect = replace_at_boundary
                else:
                    self.toolkit._openagent_await_ready = replace_at_boundary
                with self.assertRaises(CapabilityUnavailable):
                    await self.source.call_tool("mutate", {}, self.context)
                original.call_tool.assert_not_awaited()
                self.second.call_tool.assert_not_awaited()
                self.toolkit._openagent_await_ready = None

    async def test_reconnect_during_app_mutation_never_replays_or_publishes_reply(self):
        async def mutate(*args, **kwargs):
            await self.replace()
            return CallToolResult(content=[])
        self.first.call_tool.side_effect = mutate
        with self.assertRaises(CapabilityUnavailable):
            await self.service.request(self.view["handle"], "tools/call", {"name": "mutate"},
                self.context, request_id="once")
        self.assertEqual(1, self.first.call_tool.await_count)
        self.second.call_tool.assert_not_awaited()
        with self.assertRaises(CapabilityUnavailable):
            await self.service.request(self.view["handle"], "tools/call", {"name": "mutate"},
                self.context, request_id="once")
        self.assertEqual(1, self.first.call_tool.await_count)
