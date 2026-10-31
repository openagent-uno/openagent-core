"""Partial MCP entry must unwind real AnyIO scopes on their original task.

The actual MCPTools transport/session lifecycle and pool supervisor are used.
Owned synthetic contexts replace network/process I/O, not lifecycle methods.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import anyio
from mcp import StdioServerParameters
from mcp.types import ListToolsResult, Tool

from openagent_core.mcp._runtime.mcp import mcp as toolkit_module
from openagent_core.mcp.pool import MCPPool, _ServerSpec


class EntryFailure(RuntimeError):
    pass


class Contexts:
    def __init__(self, modes, *, fail_session_exit=False):
        self.modes = modes
        self.fail_session_exit = fail_session_exit
        self.primary = EntryFailure("owned partial-entry failure")
        self.session_count = 0
        self.transport_count = 0
        self.events = []
        self.workers = set()
        self.initializing = asyncio.Event()

    async def worker(self, label):
        self.workers.add(label)
        try:
            await asyncio.Event().wait()
        finally:
            self.workers.remove(label)

    @asynccontextmanager
    async def scope(self, label):
        task = asyncio.current_task()
        async with anyio.create_task_group() as group:
            group.start_soon(self.worker, label)
            self.events.append((label, "enter", task))
            try:
                yield
            finally:
                self.events.append((label, "exit", asyncio.current_task()))
                group.cancel_scope.cancel()

    @asynccontextmanager
    async def transport(self, params):
        self.transport_count += 1
        async with self.scope("transport-" + str(self.transport_count)):
            yield object(), object()

    def session(self, *args, **kwargs):
        self.session_count += 1
        index = self.session_count
        fixture = self

        class Session:
            async def __aenter__(self):
                self.context = fixture.scope("session-" + str(index))
                await self.context.__aenter__()
                if fixture.modes[index - 1] == "entry-failure":
                    raise fixture.primary
                return self

            async def __aexit__(self, *exc):
                await self.context.__aexit__(*exc)
                if fixture.fail_session_exit:
                    raise RuntimeError("owned session teardown failure")

            async def initialize(self):
                fixture.initializing.set()
                if fixture.modes[index - 1] == "hang":
                    await asyncio.Event().wait()
                return SimpleNamespace()

            async def list_tools(self, **kwargs):
                return ListToolsResult(tools=[Tool(name="owned_read", inputSchema={"type": "object"})])

        return Session()


class PartialEntryTests(unittest.IsolatedAsyncioTestCase):
    def toolkit(self):
        return toolkit_module.MCPTools(
            server_params=StdioServerParameters(command="owned-never-spawned"),
            transport="stdio", tool_name_prefix="fixture")

    def patches(self, contexts):
        self.enterContext(patch.object(toolkit_module, "stdio_client", contexts.transport))
        self.enterContext(patch.object(toolkit_module, "ClientSession", contexts.session))

    def assert_closed(self, contexts, toolkit, generations=1):
        self.assertEqual(contexts.workers, set())
        self.assertEqual(len(contexts.events), generations * 4)
        for generation in range(1, generations + 1):
            rows = contexts.events[(generation - 1) * 4:generation * 4]
            self.assertEqual([(row[0], row[1]) for row in rows], [
                (f"transport-{generation}", "enter"), (f"session-{generation}", "enter"),
                (f"session-{generation}", "exit"), (f"transport-{generation}", "exit")])
            self.assertTrue(all(row[2] is rows[0][2] for row in rows))
        self.assertIsNone(toolkit._context)
        self.assertIsNone(toolkit._session_context)
        self.assertIsNone(toolkit.session)
        self.assertFalse(toolkit._initialized)
        self.assertEqual(toolkit._active_contexts, [])

    async def test_failed_entry_releases_partial_scopes_before_raising_original(self):
        contexts = Contexts(["entry-failure"])
        self.patches(contexts)
        toolkit = self.toolkit()
        with self.assertRaises(EntryFailure) as raised:
            await toolkit.__aenter__()
        self.assertIs(raised.exception, contexts.primary)
        self.assert_closed(contexts, toolkit)

    async def test_session_exit_failure_still_closes_transport_and_keeps_entry_error(self):
        contexts = Contexts(["entry-failure"], fail_session_exit=True)
        self.patches(contexts)
        toolkit = self.toolkit()
        with self.assertRaises(EntryFailure) as raised:
            await toolkit.__aenter__()
        self.assertIs(raised.exception, contexts.primary)
        self.assert_closed(contexts, toolkit)

    async def test_pool_handshake_timeout_closes_on_supervisor_task(self):
        contexts = Contexts(["hang"])
        self.patches(contexts)
        toolkit, pool = self.toolkit(), MCPPool([])
        with patch("openagent_core.mcp.pool._MCP_CONNECT_TIMEOUT", 0.05):
            self.assertIsNone(await pool._safe_enter(toolkit, _ServerSpec("fixture")))
        self.assert_closed(contexts, toolkit)
        self.assertIsNot(contexts.events[0][2], asyncio.current_task())
        self.assertEqual(pool._toolkit_supervisors, [])

    async def test_cancelled_pool_caller_closes_scopes_and_propagates_cancellation(self):
        contexts = Contexts(["hang"])
        self.patches(contexts)
        toolkit, pool = self.toolkit(), MCPPool([])
        with patch("openagent_core.mcp.pool._MCP_CONNECT_TIMEOUT", 10), \
                patch("openagent_core.mcp.pool._MCP_CLOSE_TIMEOUT", 0.05):
            caller = asyncio.create_task(pool._safe_enter(toolkit, _ServerSpec("fixture")))
            await asyncio.wait_for(contexts.initializing.wait(), 1)
            caller.cancel("owned caller cancellation")
            with self.assertRaises(asyncio.CancelledError) as raised:
                await caller
        self.assertEqual(raised.exception.args, ("owned caller cancellation",))
        self.assert_closed(contexts, toolkit)
        self.assertEqual(pool._toolkit_supervisors, [])

    async def test_reconnect_partial_failure_closes_both_generations_on_same_task(self):
        contexts = Contexts(["ready", "entry-failure"])
        self.patches(contexts)
        toolkit, pool = self.toolkit(), MCPPool([])
        self.assertIs(await pool._safe_enter(toolkit, _ServerSpec("fixture")), toolkit)
        try:
            self.assertFalse(await pool.reconnect_toolkit(toolkit, toolkit.session))
            self.assert_closed(contexts, toolkit, generations=2)
            self.assertTrue(all(row[2] is contexts.events[0][2] for row in contexts.events))
        finally:
            for supervisor in tuple(pool._toolkit_supervisors):
                await pool._shutdown_supervisor(supervisor)

    async def test_normal_exit_closes_transport_even_if_session_exit_fails(self):
        contexts = Contexts(["ready"], fail_session_exit=True)
        self.patches(contexts)
        toolkit = self.toolkit()
        await toolkit.__aenter__()
        with self.assertRaisesRegex(RuntimeError, "owned session teardown failure"):
            await toolkit.__aexit__(None, None, None)
        self.assert_closed(contexts, toolkit)
        # Once released, a normal explicit close is idempotent.
        await toolkit.close()
        self.assertEqual(len(contexts.events), 4)
