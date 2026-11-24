"""Real HTTP startup faults and scoped failures on previously issued references."""

import asyncio
import json
from dataclasses import replace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core.capabilities import (
    CapabilityCatalog,
    CapabilitySourceTemporarilyUnavailable,
    CapabilityUnavailable,
    FunctionSource,
    StaleCapabilityReference,
    ToolDefinition,
)
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp.pool import MCPPool, _ServerSpec
from test_mcp_transport_crash_reconnect import _Server


class Policy:
    denied = False

    async def authorize(self, context, action, resource, *, audience=()):
        return (
            not self.denied
            and context.author.subject_id == "owner"
            and audience == (context.author,)
        )


class Boundaries(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        owner = PrincipalRef("test", "tenant", "owner")
        self.context = ExecutionContext(
            owner, owner, owner, "dialog", "agent", (owner,)
        )
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        delay = patch.dict("os.environ", {"OPENAGENT_MCP_STARTUP_RETRY_SECONDS": ".05"})
        delay.start()
        self.addCleanup(delay.stop)

    async def test_transient_http_handshake_is_distinct_from_terminal_401_or_404(self):
        server = _Server()
        await server.start()
        self.addAsyncCleanup(server.stop)
        healthy = server.faults.app

        for status, code in (
            (401, "permission_denied"),
            (404, "initialization_failed"),
            (503, "service_unavailable"),
        ):
            with self.subTest(status=status):

                async def fault(scope, receive, send):
                    if scope.get("method") == "POST":
                        await send(
                            {
                                "type": "http.response.start",
                                "status": status,
                                "headers": [],
                            }
                        )
                        await send({"type": "http.response.body", "body": b"fixture"})
                    else:
                        await healthy(scope, receive, send)

                server.faults.app = fault
                pool = MCPPool([_ServerSpec("remote", url=server.url)])
                pool.bind_capability_catalog(self.catalog)
                try:
                    await pool.connect_all()
                    self.assertEqual(
                        pool.connection_status()[0]["failure"]["code"], code
                    )
                    self.assertEqual(
                        bool(
                            await self.catalog.temporarily_unavailable_sources(
                                self.context
                            )
                        ),
                        status == 503,
                    )
                    server.faults.app = healthy
                    if status == 503:
                        async with asyncio.timeout(5):
                            while not await self.catalog.discover(self.context):
                                await asyncio.sleep(0.02)
                        [tool] = [
                            item
                            for item in await self.catalog.discover(self.context)
                            if item.name.endswith("read_record")
                        ]
                        answer = await self.catalog.call_tool(
                            tool.tool_ref, {"record_id": "synthetic"}, self.context
                        )
                        self.assertIn("synthetic", str(answer))
                    else:
                        self.assertFalse(pool._startup_retries)
                finally:
                    await pool.close_all()

    async def test_refused_connection_recovers_when_the_same_target_starts(self):
        server = _Server()
        pool = MCPPool([_ServerSpec("remote", url=server.url)])
        pool.bind_capability_catalog(self.catalog)
        self.addAsyncCleanup(pool.close_all)
        await pool.connect_all()
        self.assertEqual(
            pool.connection_status()[0]["failure"]["code"], "transport_unreachable"
        )
        await server.start()
        self.addAsyncCleanup(server.stop)
        async with asyncio.timeout(5):
            while not await self.catalog.discover(self.context):
                await asyncio.sleep(0.02)

    async def test_established_disconnect_recovers_exact_registration_and_old_handle_is_scoped(
        self,
    ):
        server = _Server()
        await server.start()
        self.addAsyncCleanup(server.stop)
        pool = MCPPool([_ServerSpec("remote", url=server.url)])
        pool.bind_capability_catalog(self.catalog)
        self.addAsyncCleanup(pool.close_all)
        await pool.connect_all()
        [before] = [
            tool
            for tool in await self.catalog.discover(self.context)
            if tool.name.endswith("read_record")
        ]
        toolkit = pool.toolkit_by_name("remote")
        await server.stop()
        # Lose the physical session on the first ping. A failed replacement
        # handshake has no catalog and must not require invoking a stale leaf.
        await pool.reconnect_toolkit(
            toolkit, toolkit.session, reason="qualified_disconnect"
        )
        self.assertTrue(
            await self.catalog.temporarily_unavailable_sources(self.context)
        )
        with self.assertRaises(CapabilitySourceTemporarilyUnavailable):
            await self.catalog.call_tool(before.tool_ref, {}, self.context)
        self.assertIs(
            type(
                await self.catalog.reference_failure(
                    before.tool_ref, replace(self.context, session_id="other")
                )
            ),
            CapabilityUnavailable,
        )
        await server.start()
        async with asyncio.timeout(5):
            while not await self.catalog.discover(self.context):
                await asyncio.sleep(0.02)
        with self.assertRaises(StaleCapabilityReference):
            await self.catalog.call_tool(before.tool_ref, {}, self.context)
        [after] = [
            tool
            for tool in await self.catalog.discover(self.context)
            if tool.name == before.name
        ]
        self.assertNotEqual(before.tool_ref, after.tool_ref)
        self.assertIn(
            "after-reconnect",
            str(
                await self.catalog.call_tool(
                    after.tool_ref, {"record_id": "after-reconnect"}, self.context
                )
            ),
        )

    async def test_response_lost_after_effect_is_never_replayed_during_recovery(self):
        from mcp.server.fastmcp import FastMCP

        effects = []

        class EffectServer(_Server):
            def _app(self):
                api = FastMCP(
                    "remote",
                    stateless_http=True,
                    json_response=True,
                    streamable_http_path="/",
                )

                @api.tool()
                async def record_effect():
                    effects.append("recorded")
                    return "accepted"

                @api.tool()
                async def read_count():
                    return str(len(effects))

                return api.streamable_http_app()

        server = EffectServer()
        await server.start()
        self.addAsyncCleanup(server.stop)
        pool = MCPPool([_ServerSpec("remote", url=server.url)])
        pool.bind_capability_catalog(self.catalog)
        self.addAsyncCleanup(pool.close_all)
        await pool.connect_all()
        [tool] = [
            item
            for item in await self.catalog.discover(self.context)
            if item.name.endswith("record_effect")
        ]
        healthy_app = server.faults.app
        discarded = []

        async def lose_reply(scope, receive, send):
            if scope.get("method") != "POST":
                return await healthy_app(scope, receive, send)
            first = await receive()
            messages = [first]
            while messages[-1].get("more_body"):
                messages.append(await receive())
            payload = json.loads(b"".join(msg["body"] for msg in messages))

            async def replay_request():
                return messages.pop(0) if messages else await receive()

            if (
                payload.get("method") != "tools/call"
                or payload["params"]["name"] != "record_effect"
            ):
                return await healthy_app(scope, replay_request, send)

            async def discard(message):
                if message["type"] == "http.response.start":
                    discarded.append(message["status"])

            await healthy_app(scope, replay_request, discard)
            await send({"type": "http.response.start", "status": 502, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        server.faults.app = lose_reply
        try:
            result = await self.catalog.call_tool(tool.tool_ref, {}, self.context)
        except (CapabilityUnavailable, RuntimeError) as error:
            self.assertIsNone(getattr(error, "status", None))
        else:
            self.assertTrue(result.get("isError"), result)
        self.assertEqual(discarded, [200])
        async with asyncio.timeout(5):
            while True:
                current = await self.catalog.discover(self.context)
                if any(
                    item.name == tool.name and item.tool_ref != tool.tool_ref
                    for item in current
                ):
                    break
                await asyncio.sleep(0.02)
        [read] = [item for item in current if item.name.endswith("read_count")]
        self.assertIn(
            "1", str(await self.catalog.call_tool(read.tool_ref, {}, self.context))
        )
        self.assertEqual(effects, ["recorded"])

    async def test_reload_and_restart_never_bind_an_old_reference_to_a_replacement(
        self,
    ):
        server = _Server()
        await server.start()
        self.addAsyncCleanup(server.stop)
        spec = _ServerSpec("remote", url=server.url)
        pool = MCPPool([spec])
        pool.bind_capability_catalog(self.catalog)
        self.addAsyncCleanup(pool.close_all)
        await pool.connect_all()
        [old] = [
            item
            for item in await self.catalog.discover(self.context)
            if item.name.endswith("read_record")
        ]
        toolkit = pool.toolkit_by_name("remote")
        await server.stop()
        await pool.reconnect_toolkit(toolkit, toolkit.session, reason="fixture")
        self.assertTrue(
            await self.catalog.temporarily_unavailable_sources(self.context)
        )
        pool._db = object()
        with patch.object(pool, "rebuild_specs", AsyncMock(return_value=[])):
            await pool.reload()
        self.assertFalse(
            await self.catalog.temporarily_unavailable_sources(self.context)
        )
        self.assertFalse(pool._startup_retries)
        await server.start()
        with patch.object(pool, "rebuild_specs", AsyncMock(return_value=[spec])):
            await pool.reload()
        self.assertIs(
            type(await self.catalog.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
        [fresh] = [
            item
            for item in await self.catalog.discover(self.context)
            if item.name == old.name
        ]
        self.assertNotEqual(fresh.tool_ref, old.tool_ref)
        await pool.close_all()
        restarted = MCPPool([spec])
        catalog = CapabilityCatalog(self.policy)
        restarted.bind_capability_catalog(catalog)
        self.addAsyncCleanup(restarted.close_all)
        await restarted.connect_all()
        [current] = [
            item
            for item in await catalog.discover(self.context)
            if item.name == old.name
        ]
        self.assertNotIn(current.tool_ref, {old.tool_ref, fresh.tool_ref})
        with self.assertRaises(CapabilityUnavailable) as unknown:
            await catalog.call_tool(fresh.tool_ref, {}, self.context)
        self.assertIs(type(unknown.exception), CapabilityUnavailable)
        self.assertIn(
            "restart",
            str(
                await catalog.call_tool(
                    current.tool_ref, {"record_id": "restart"}, self.context
                )
            ),
        )

    async def test_model_broker_reports_only_its_own_predispatch_classification(self):
        from pathlib import Path
        from types import SimpleNamespace
        from openagent_core.mcp.servers.tool_search.adapters import (
            build_runtime_toolkit,
        )
        from openagent_core.runtime import execution_scope

        effects = []

        async def action(arguments, context):
            effects.append("recorded")
            return {"content": [{"type": "text", "text": "done"}]}

        source = FunctionSource(
            (ToolDefinition("record", "first", {}),), {"record": action}
        )
        self.catalog.register("sample", source, source, target_label="target")
        pool = MCPPool([])
        pool.bind_capability_catalog(self.catalog)
        runtime = SimpleNamespace(
            capabilities=self.catalog,
            settings=SimpleNamespace(workspace=Path("."), environment=()),
        )
        toolkit = build_runtime_toolkit(pool=pool)
        functions = {**toolkit.functions, **toolkit.async_functions}
        call = functions["tool_search_call_tool"].entrypoint
        describe = functions["tool_search_describe_tool"].entrypoint

        with execution_scope(runtime, self.context, None):
            [old] = await self.catalog.discover(self.context)
            source.tools = (ToolDefinition("record", "second", {}),)
            self.assertEqual(
                await describe(tool_ref=old.tool_ref), {"status": "stale_reference"}
            )
            self.assertEqual(
                await call(tool_ref=old.tool_ref, args={}),
                {"status": "stale_reference"},
            )
            self.assertFalse(effects)

            async def observer(descriptor, arguments, result, context, call_id):
                # A separate lookup after the write returns a valid preflight
                # failure of its own. It cannot characterize the completed write.
                await self.catalog.call_tool(old.tool_ref, {}, context)

            self.catalog.observers = (observer,)
            [new] = await self.catalog.discover(self.context)
            with self.assertRaises(CapabilityUnavailable) as uncertain:
                await call(tool_ref=new.tool_ref, args={})
            self.assertIs(type(uncertain.exception), CapabilityUnavailable)
            self.assertEqual(effects, ["recorded"])

    async def test_old_handle_types_require_its_prior_context_and_current_exact_tool(
        self,
    ):
        calls = []

        async def action(arguments, context):
            calls.append("called")
            raise StaleCapabilityReference(
                "executor cannot declare an expired pre-dispatch binding"
            )

        source = FunctionSource(
            (ToolDefinition("record", "first", {}),), {"record": action}
        )
        self.catalog.register("sample", source, source, target_label="target")
        [old] = await self.catalog.discover(self.context)
        self.assertIs(
            type(await self.catalog.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
        source.tools = (ToolDefinition("record", "second", {}),)
        # The still registered tool changed before rediscovery displaced its ref.
        with self.assertRaises(StaleCapabilityReference):
            await self.catalog.call_tool(old.tool_ref, {}, self.context)
        [new] = await self.catalog.discover(self.context)
        self.assertNotEqual(old.tool_ref, new.tool_ref)
        with self.assertRaises(StaleCapabilityReference):
            await self.catalog.describe_tool(old.tool_ref, self.context)
        cases = [
            replace(self.context, session_id="other-dialog"),
            replace(self.context, scopes=("other",)),
        ]
        for context in cases:
            error = await self.catalog.reference_failure(old.tool_ref, context)
            self.assertIs(type(error), CapabilityUnavailable)
        self.policy.denied = True
        self.assertIs(
            type(await self.catalog.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
        self.policy.denied = False
        source.tools = ()
        self.assertIs(
            type(await self.catalog.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
        source.tools = (ToolDefinition("record", "second", {}),)
        with self.assertRaises(CapabilityUnavailable) as uncertain:
            await self.catalog.call_tool(new.tool_ref, {}, self.context)
        self.assertIs(type(uncertain.exception), CapabilityUnavailable)
        self.assertEqual(calls, ["called"])
        self.catalog.revoke("sample")
        self.catalog.register("sample", source, source, target_label="target")
        self.assertIs(
            type(await self.catalog.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
        restarted = CapabilityCatalog(self.policy)
        restarted.register("sample", source, source, target_label="target")
        self.assertIs(
            type(await restarted.reference_failure(old.tool_ref, self.context)),
            CapabilityUnavailable,
        )
