from __future__ import annotations

import asyncio
import json
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.runtime import execution_scope
from openagent_core.core.execution_origin import TurnExecutionOrigin, execution_origin_scope
from openagent_core.core.paths import set_agent_dir
from openagent_core.mcp._runtime import Toolkit
from openagent_core.mcp.pool import MCPPool, _ServerSpec
from openagent_core.mcp.catalog import register_interactive_capabilities, revoke_interactive_capabilities
from openagent_core.mcp.servers.tool_search.adapters import (
    _call_scoped_tool_impl, _call_tool_impl, _list_scoped_servers_impl,
    _list_scoped_tools_impl, build_runtime_toolkit, coerce_mcp_result_to_jsonable,
)


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied


class Registry:
    def __init__(self):
        self.revoked = False
        self.calls = []

    def list_servers(self, origin):
        if self.revoked:
            raise PermissionError("revoked")
        return [{"name": "client:files"}]

    def list_tools(self, origin, name):
        self.list_servers(origin)
        return [{"name": "read", "description": "device"}]

    def describe_tool(self, origin, name, tool):
        self.list_servers(origin)
        return {"name": "read", "description": "device", "input_schema": {"type": "object"}}

    async def call_tool(self, origin, server, tool, arguments, *, session_id):
        self.list_servers(origin)
        self.calls.append((origin, session_id, arguments))
        return {"content": [{"type": "text", "text": "device"}], "structuredContent": {"device": origin.device_id},
                "_meta": {"complete": True}, "child_session_id": "child"}


class CatalogAdapters(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name)
        set_agent_dir(self.path)
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        self.runtime = SimpleNamespace(capabilities=self.catalog, settings=SimpleNamespace(workspace=self.path))
        p = PrincipalRef("test", "tenant", "alice")
        self.context = ExecutionContext(p, p, p, "session", "agent", (p,))
        self.calls = []
        self.pool = MCPPool([])
        self.pool._connected = True
        self.pool._toolkit_by_name = {name: self.toolkit(name) for name in ("left", "right")}
        self.pool.bind_capability_catalog(self.catalog)
        self.runtime.services = SimpleNamespace(executor=SimpleNamespace(agent=SimpleNamespace(capability_pool=self.pool)))

    async def asyncTearDown(self):
        await self.pool.close_all()
        self.temp.cleanup()
        set_agent_dir(None)

    def toolkit(self, destination):
        async def read(value: str = "same"):
            """Read a fixture value."""
            self.calls.append((destination, value))
            return {"content": [{"type": "text", "text": destination}],
                    "structuredContent": {"value": value}, "_meta": {"target": destination},
                    "child_session_id": "real-child"}
        return Toolkit(name=destination, tools=[read])

    async def test_real_tool_search_schema_and_lossless_exact_dispatch(self):
        toolkit = build_runtime_toolkit(pool=self.pool)
        functions = {**toolkit.functions, **toolkit.async_functions}
        call = functions["tool_search_call_tool"]
        self.assertIn("tool_ref", call.parameters["properties"])
        self.assertNotIn("server", call.parameters["properties"])
        with execution_scope(self.runtime, self.context, "run"):
            servers = await _list_scoped_servers_impl(self.pool)
            self.assertEqual({item["name"] for item in servers}, {"left", "right"})
            left = (await _list_scoped_tools_impl(self.pool, "left"))[0]
            right = (await _list_scoped_tools_impl(self.pool, "right"))[0]
            self.assertNotEqual(left["tool_ref"], right["tool_ref"])
            self.assertEqual(left["name"], right["name"])
            self.assertIn("value", left["input_schema"]["properties"])
            result = await call.entrypoint(tool_ref=left["tool_ref"], args={"value": "exact"})
            self.assertEqual(result["structuredContent"], {"value": "exact"})
            self.assertEqual(result["_meta"], {"target": "left"})
            self.assertEqual(result["child_session_id"], "real-child")
            with self.assertRaises(Exception):
                await call.entrypoint(tool_ref=left["tool_ref"], args={"server": "right"})
            self.assertEqual(self.calls, [("left", "exact")])

    async def test_no_unauthenticated_fallback_or_old_prefix_route(self):
        with self.assertRaises(PermissionError):
            await _call_tool_impl(self.pool, "left", "read", {})
        with execution_scope(self.runtime, self.context, "run"):
            with self.assertRaises(LookupError):
                await _call_scoped_tool_impl(self.pool, "server:left", {})
            self.policy.denied.add("tool.call")
            reference = (await _list_scoped_tools_impl(self.pool, "left"))[0]["tool_ref"]
            with self.assertRaises(PermissionError):
                await _call_scoped_tool_impl(self.pool, reference, {})
        self.assertEqual(self.calls, [])

    async def test_replacement_revokes_previous_reference_without_fallback(self):
        with execution_scope(self.runtime, self.context, "run"):
            before = (await _list_scoped_tools_impl(self.pool, "left"))[0]["tool_ref"]
            self.pool._toolkit_by_name["left"] = self.toolkit("replacement")
            self.pool._capability_binding.sync()
            after = (await _list_scoped_tools_impl(self.pool, "left"))[0]["tool_ref"]
            self.assertNotEqual(before, after)
            with self.assertRaises(CapabilityUnavailable):
                await _call_scoped_tool_impl(self.pool, before, {})
            await _call_scoped_tool_impl(self.pool, after, {})
        self.assertEqual(self.calls, [("replacement", "same")])

    async def test_device_capabilities_are_turn_scoped_revocable_and_generation_bound(self):
        registry = Registry()
        origin = TurnExecutionOrigin("device", "instance", 3, "Alice Mac", registry, auth_epoch=4)
        leases = register_interactive_capabilities(self.catalog, origin, source_namespace="connection-123")
        interactive = replace(self.context, capabilities=leases)
        with execution_scope(self.runtime, self.context, "channel"):
            self.assertEqual(len(await self.catalog.discover(self.context)), 2)
        with execution_scope(self.runtime, interactive, "app"), execution_origin_scope(origin):
            device = next(tool for tool in await self.catalog.discover(interactive) if tool.source_id == leases[0].source_id)
            result = await _call_scoped_tool_impl(self.pool, device.tool_ref, {})
            self.assertEqual(result["child_session_id"], "child")
            with execution_origin_scope(replace(origin, auth_epoch=5)):
                with self.assertRaises(CapabilityUnavailable):
                    await _call_scoped_tool_impl(self.pool, device.tool_ref, {})
            registry.revoked = True
            self.assertEqual(len(await self.catalog.discover(interactive)), 2)
            with self.assertRaises(CapabilityUnavailable):
                await _call_scoped_tool_impl(self.pool, device.tool_ref, {})
            revoke_interactive_capabilities(self.catalog, leases)
        self.assertEqual(len(registry.calls), 1)

    async def test_ptc_socket_reuses_captured_context_and_checks_current_revocations(self):
        from openagent_core.mcp.servers.ptc.handlers import start_rpc_server
        sock = str(self.path / "ptc.sock")
        with execution_scope(self.runtime, self.context, "run"):
            reference = (await _list_scoped_tools_impl(self.pool, "left"))[0]["tool_ref"]
            server, state = await start_rpc_server(pool=self.pool, sock_path=sock, token="fixture", max_tool_calls=2)
        async def send(payload):
            reader, writer = await asyncio.open_unix_connection(sock)
            writer.write((json.dumps(payload)+"\n").encode()); await writer.drain()
            result = json.loads(await reader.readline())
            writer.close(); await writer.wait_closed()
            return result
        try:
            self.assertFalse((await send({"token": "bad", "tool_ref": reference}))["ok"])
            first = await send({"token": "fixture", "tool_ref": reference, "args": {"value": "PTC"}})
            self.assertTrue(first["ok"])
            self.assertEqual(first["result"]["structuredContent"], {"value": "PTC"})
            self.policy.denied.add("tool.call")
            self.assertFalse((await send({"token": "fixture", "tool_ref": reference}))["ok"])
            self.assertEqual(state["calls"], 2)
        finally:
            server.close(); await server.wait_closed()
        self.assertEqual(self.calls, [("left", "PTC")])

    async def test_workflow_resolves_durable_binding_via_public_catalog(self):
        from openagent_core.workflow.executor import _h_mcp_tool, _RunCtx
        exe = SimpleNamespace(agent=SimpleNamespace(_mcp=self.pool))
        cfg = {"mcp_name": "left", "tool_name": "read", "args": {"value": "workflow"}}
        with execution_scope(self.runtime, self.context, "run"):
            result = await _h_mcp_tool(exe, {}, cfg, _RunCtx("wf-run", "workflow", {}, {}))
            self.assertEqual(result["result"]["_meta"], {"target": "left"})
            self.policy.denied.add("tool.call")
            with self.assertRaises(PermissionError):
                await _h_mcp_tool(exe, {}, cfg, _RunCtx("wf-run", "workflow", {}, {}))
        self.assertEqual(self.calls, [("left", "workflow")])

    async def test_core_default_catalog_has_no_product_or_device_tools(self):
        from openagent_core.mcp.builtins import DEFAULT_MCPS, BUILTIN_MCP_SPECS
        excluded = {"shell", "editor", "filesystem", "computer-control", "agent-in-chrome", "web-search", "ui-manager"}
        self.assertFalse(excluded & BUILTIN_MCP_SPECS.keys())
        self.assertFalse(excluded & {item.get("builtin") for item in DEFAULT_MCPS})
        self.assertIn("vault", BUILTIN_MCP_SPECS)

    async def test_stdio_tool_preserves_full_protocol_envelope(self):
        fixture = self.path / "fixture_mcp.py"
        fixture.write_text('''from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, EmbeddedResource, TextResourceContents
mcp = FastMCP("fixture")
@mcp.tool()
def envelope(value: str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=value),
        EmbeddedResource(type="resource", resource=TextResourceContents(uri="fixture://attachment", text="attached", mimeType="text/plain"))],
        structuredContent={"value": value}, isError=False,
        _meta={"proof": "retained", "child_session_id": "child-exact"})
mcp.run(transport="stdio")
''')
        pool = MCPPool.from_config([{"name": "stdio", "command": [sys.executable, str(fixture)]}], include_defaults=False)
        catalog = CapabilityCatalog(self.policy)
        runtime = SimpleNamespace(capabilities=catalog, settings=self.runtime.settings,
            services=SimpleNamespace(executor=SimpleNamespace(agent=SimpleNamespace(capability_pool=pool))))
        try:
            await pool.connect_all()
            pool.bind_capability_catalog(catalog)
            with execution_scope(runtime, self.context, "stdio-run"):
                definitions = await catalog.discover(self.context)
                self.assertEqual(len(definitions), 1)
                self.assertEqual(definitions[0].input_schema["required"], ["value"])
                result = await _call_scoped_tool_impl(pool, definitions[0].tool_ref, {"value": "roundtrip"})
                self.assertEqual(result["structuredContent"], {"value": "roundtrip"})
                self.assertEqual(result["_meta"], {"proof": "retained", "child_session_id": "child-exact"})
                self.assertEqual(result["content"][1]["resource"]["text"], "attached")
                self.assertFalse(result["isError"])
        finally:
            await pool.close_all()

    async def test_vault_effects_only_from_trusted_module_and_explicit_ownership(self):
        from openagent_core.mcp.catalog import PoolCatalogBinding
        async def vault_write_note(text: str):
            return {"ok": True}
        for trusted in (False, True):
            with self.subTest(trusted=trusted):
                catalog = CapabilityCatalog(self.policy)
                pool = MCPPool([_ServerSpec("vault", trusted_module="vault" if trusted else None)])
                pool._toolkit_by_name["vault"] = Toolkit(name="vault", tools=[vault_write_note])
                binding = PoolCatalogBinding(pool, catalog, trusted_modules=("vault",))
                binding.sync()
                descriptor = (await catalog.discover(self.context))[0]
                self.assertEqual(descriptor.effects, frozenset({"vault.write"}) if trusted else frozenset())
                # Exact tool definition is revalidated at call without stripping
                # the trusted effect and invalidating its own reference.
                self.assertEqual(await catalog.call_tool(descriptor.tool_ref, {"text": "fixture"}, self.context), {"ok": True})
                binding.close()
        catalog = CapabilityCatalog(self.policy, allow_dynamic=True)
        pool = MCPPool([])
        pool._toolkit_by_name = {"fixed": self.toolkit("fixed"), "user": self.toolkit("user")}
        pool.bind_capability_catalog(catalog, user_sources={"user"})
        with self.assertRaises(PermissionError):
            await catalog.remove("fixed", self.context)
        await catalog.remove("user", self.context)
        self.assertEqual({d.source_id for d in await catalog.discover(self.context)}, {"fixed"})
        await pool.close_all()

    async def test_provider_accessors_never_expose_raw_leaf_dispatch(self):
        gateway = build_runtime_toolkit(pool=self.pool)
        self.pool._toolkit_by_name["tool-search"] = gateway
        self.pool._in_process_runtime_toolkits = list(self.pool._toolkit_by_name.values())
        self.assertEqual(self.pool.runtime_toolkits, [gateway])
        self.assertEqual(self.pool.runtime_toolkits_under_budget(-1), [gateway])
        self.assertIsNone(self.pool.toolkit_by_name("LEFT"))
        with self.assertRaises(ValueError):
            MCPPool([_ServerSpec("same"), _ServerSpec("same")])
        with self.assertRaises(ValueError):
            MCPPool.from_config([{"builtin": "not-a-module"}], include_defaults=False)


if __name__ == "__main__":
    unittest.main()
