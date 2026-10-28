"""A closed MCP stream must be replaced, and a failed call must say it failed.

Incident 2026-10-28 (lyra-agent -> Replio, streamable HTTP): one POST timed
out, the transport's streams closed, and every later call raised
``anyio.ClosedResourceError`` from ``send_nowait`` until the process was
restarted two hours later. ``call_tool`` logged the failed ping and sent into
the dead stream anyway, then returned ``ToolResult(content="Error: ")`` with no
error bit, so product callers recorded patches, tags and handoffs that never
happened as successful.
"""
from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

import anyio

from openagent_core.core._runner.utils import mcp as mcp_utils
from openagent_core.mcp.pool import MCPPool, _ServerSpec


class _Session:
    def __init__(self, label: str, *, dead: bool = False) -> None:
        self.label = label
        self.dead = dead
        self.calls: list[tuple[str, dict]] = []

    async def send_ping(self) -> None:
        if self.dead:
            raise anyio.ClosedResourceError()

    async def call_tool(self, name, arguments, meta=None):
        self.calls.append((name, dict(arguments)))
        if self.dead:
            raise anyio.ClosedResourceError()
        from mcp.types import CallToolResult, TextContent
        return CallToolResult(content=[TextContent(type="text", text=f"ok from {self.label}")])


class _Transport:
    def __init__(self) -> None:
        self.closed = False

    async def __aexit__(self, *exc):
        self.closed = True
        return False


class _ReconnectingToolkit:
    """MCPTools stand-in: ``__aenter__`` opens a fresh session each time."""

    def __init__(self, *, fail_reentries: int = 0, exit_errors: int = 0,
                 uninitialized_reentries: int = 0) -> None:
        self.session: _Session | None = None
        self._initialized = False
        self.functions: dict[str, object] = {}
        self.enters = 0
        self.enter_tasks: list[asyncio.Task] = []
        self.exit_tasks: list[asyncio.Task] = []
        self.fail_reentries = fail_reentries
        self.exit_errors = exit_errors
        self.uninitialized_reentries = uninitialized_reentries
        # When set, re-entries assign the new session (as MCPTools._connect
        # does) and then wait here before finishing the handshake.
        self.reentry_gate: asyncio.Event | None = None
        # Mirrors MCPTools._context (the transport); its exit is what stops
        # the transport workers.
        self._context: _Transport | None = None
        self.transports: list[_Transport] = []
        self.hang_exits = 0
        # When set, __aexit__ swallows cancellation until released, like an
        # anyio task group draining a shielded child.
        self.wedge_release: asyncio.Event | None = None

    async def __aenter__(self):
        self.enter_tasks.append(asyncio.current_task())
        self.enters += 1
        if self.enters > 1 and self.fail_reentries > 0:
            self.fail_reentries -= 1
            raise ConnectionError("server unavailable")
        self._context = _Transport()
        self.transports.append(self._context)
        self.session = _Session(f"session-{self.enters}")
        if self.enters > 1 and self.reentry_gate is not None:
            await self.reentry_gate.wait()
        if self.enters > 1 and self.uninitialized_reentries > 0:
            # MCPTools.initialize() swallows its own failure and returns.
            self.uninitialized_reentries -= 1
            return self
        self._initialized = True
        self.functions = {"replio_thread_brief": object(), "replio_threads_respond": object()}
        return self

    async def __aexit__(self, *exc):
        self.exit_tasks.append(asyncio.current_task())
        if self.hang_exits > 0:
            self.hang_exits -= 1
            await asyncio.Event().wait()
        if self.wedge_release is not None:
            while not self.wedge_release.is_set():
                try:
                    await self.wedge_release.wait()
                except asyncio.CancelledError:
                    continue
        if self.exit_errors > 0:
            # Real MCPTools leaves session/_context set when this raises.
            self.exit_errors -= 1
            raise BaseExceptionGroup("tg", [anyio.ClosedResourceError()])
        self.session = None
        self._initialized = False
        if self._context is not None:
            await self._context.__aexit__(None, None, None)
            self._context = None
        return False

    async def get_session_for_run(self, run_context=None, agent=None, team=None):
        if self.session is None:
            raise ValueError("Session is not initialized")
        return self.session


def _tool(name: str = "threads_patch"):
    return SimpleNamespace(name=name, description="", inputSchema={"type": "object"})


class PoolReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def _entered(self, **kwargs):
        pool = MCPPool([_ServerSpec(name="replio", url="http://replio.invalid/mcp")])
        toolkit = _ReconnectingToolkit(**kwargs)
        self.assertIs(await pool._safe_enter(toolkit, _ServerSpec(name="replio")), toolkit)
        pool._connected = True  # what connect_all records; close_all needs it
        self.addAsyncCleanup(pool.close_all)
        return pool, toolkit

    async def test_dead_session_is_replaced_on_the_supervisor_task(self):
        pool, toolkit = await self._entered()
        dead = toolkit.session
        dead.dead = True

        self.assertTrue(await pool.reconnect_toolkit(toolkit, dead, reason="ClosedResourceError"))

        self.assertIsNot(toolkit.session, dead)
        self.assertEqual(toolkit.session.label, "session-2")
        supervisor = pool._toolkit_supervisors[0].task
        # anyio: the dead context is exited, and the new one entered, on the
        # task that entered the first one.
        self.assertEqual({id(t) for t in toolkit.enter_tasks + toolkit.exit_tasks}, {id(supervisor)})

    async def test_concurrent_callers_share_one_reconnect(self):
        pool, toolkit = await self._entered()
        dead = toolkit.session
        results = await asyncio.gather(*(pool.reconnect_toolkit(toolkit, dead) for _ in range(5)))
        self.assertEqual(results, [True] * 5)
        self.assertEqual(toolkit.enters, 2)

    async def test_already_replaced_session_does_not_reconnect_again(self):
        pool, toolkit = await self._entered()
        dead = toolkit.session
        self.assertTrue(await pool.reconnect_toolkit(toolkit, dead))
        self.assertTrue(await pool.reconnect_toolkit(toolkit, dead))
        self.assertEqual(toolkit.enters, 2)

    async def test_failed_reentry_reports_false_then_recovers(self):
        pool, toolkit = await self._entered(fail_reentries=1)
        dead = toolkit.session
        self.assertFalse(await pool.reconnect_toolkit(toolkit, dead))
        self.assertIsNone(toolkit.session)
        self.assertTrue(await pool.reconnect_toolkit(toolkit, None))
        self.assertEqual(toolkit.session.label, "session-3")

    async def test_close_after_reconnect_unwinds_and_refuses_new_reconnects(self):
        pool, toolkit = await self._entered()
        self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
        await pool.close_all()
        self.assertIsNone(toolkit.session)
        self.assertFalse(await pool.reconnect_toolkit(toolkit, None))

    async def test_caller_joining_a_slow_reconnect_does_not_tear_it_down(self):
        pool, toolkit = await self._entered()
        dead = toolkit.session
        toolkit.reentry_gate = asyncio.Event()
        first = asyncio.create_task(pool.reconnect_toolkit(toolkit, dead))
        for _ in range(20):
            await asyncio.sleep(0)
        # The new session object exists but its handshake has not finished:
        # a second caller must join, not be told "already replaced".
        self.assertEqual(toolkit.session.label, "session-2")
        second = asyncio.create_task(pool.reconnect_toolkit(toolkit, dead))
        for _ in range(5):
            await asyncio.sleep(0)
        self.assertFalse(second.done())
        toolkit.reentry_gate.set()
        self.assertEqual(await asyncio.gather(first, second), [True, True])
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertEqual(toolkit.enters, 2)
        self.assertEqual(toolkit.session.label, "session-2")

    async def test_teardown_error_keeps_the_supervisor_alive(self):
        pool, toolkit = await self._entered(exit_errors=1)
        dead = toolkit.session
        self.assertTrue(await pool.reconnect_toolkit(toolkit, dead))
        self.assertEqual(toolkit.session.label, "session-2")
        self.assertFalse(pool._toolkit_supervisors[0].task.done())
        self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
        self.assertEqual(toolkit.session.label, "session-3")

    async def test_uninitialized_reentry_is_not_reported_as_recovered(self):
        pool, toolkit = await self._entered(uninitialized_reentries=1)
        self.assertFalse(await pool.reconnect_toolkit(toolkit, toolkit.session))
        self.assertIsNone(toolkit.session)
        self.assertTrue(await pool.reconnect_toolkit(toolkit, None))
        self.assertTrue(toolkit._initialized)

    async def test_shutdown_during_reconnect_does_not_reopen(self):
        pool, toolkit = await self._entered()
        dead = toolkit.session
        toolkit.reentry_gate = asyncio.Event()
        pending = asyncio.create_task(pool.reconnect_toolkit(toolkit, dead))
        for _ in range(20):
            await asyncio.sleep(0)
        closing = asyncio.create_task(pool.close_all())
        for _ in range(5):
            await asyncio.sleep(0)
        toolkit.reentry_gate.set()
        await closing
        self.assertFalse(await pending)
        self.assertIsNone(toolkit.session)
        self.assertEqual(toolkit.enters, 2)

    async def test_teardown_error_still_closes_the_old_transport(self):
        pool, toolkit = await self._entered(exit_errors=1)
        old_transport = toolkit._context
        self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
        self.assertTrue(old_transport.closed)
        self.assertFalse(toolkit._context.closed)

    async def test_hung_teardown_is_bounded_and_recovery_continues(self):
        from openagent_core.mcp import pool as pool_module
        saved = pool_module._MCP_CLOSE_TIMEOUT
        pool_module._MCP_CLOSE_TIMEOUT = 0.05
        try:
            pool, toolkit = await self._entered()
            toolkit.hang_exits = 1
            self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
            self.assertEqual(toolkit.session.label, "session-2")
            self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
            self.assertEqual(toolkit.session.label, "session-3")
        finally:
            pool_module._MCP_CLOSE_TIMEOUT = saved

    async def test_wedged_supervisor_is_replaced_by_a_fresh_toolkit(self):
        from openagent_core.mcp import pool as pool_module
        saved = (pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT)
        pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = 0.05, 0.2
        pool, toolkit = await self._entered()
        release = toolkit.wedge_release = asyncio.Event()
        fresh = _ReconnectingToolkit()

        async def build(spec):
            return await pool._safe_enter(fresh, spec)

        pool._build_and_enter_toolkit = build
        try:
            self.assertFalse(await pool.reconnect_toolkit(toolkit, toolkit.session))
            await asyncio.sleep(0.05)
            # Still wedged: the next request replaces the toolkit.
            self.assertFalse(await pool.reconnect_toolkit(toolkit, None))
            self.assertIs(pool._toolkit_by_name["replio"], fresh)
            self.assertEqual(fresh.session.label, "session-1")
            self.assertEqual([s.toolkit for s in pool._toolkit_supervisors], [fresh])
            # The old toolkit is retired: it never reconnects again.
            self.assertFalse(await pool.reconnect_toolkit(toolkit, None))
        finally:
            release.set()
            pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = saved

    async def _wedge(self, pool, toolkit):
        """Leave ``toolkit``'s supervisor stuck in teardown past the budget."""
        toolkit.wedge_release = asyncio.Event()
        self.assertFalse(await pool.reconnect_toolkit(toolkit, toolkit.session))
        await asyncio.sleep(0.05)
        return toolkit.wedge_release

    async def test_replacement_built_across_close_all_is_discarded(self):
        from openagent_core.mcp import pool as pool_module
        saved = (pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT)
        pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = 0.05, 0.2
        pool, toolkit = await self._entered()
        release = await self._wedge(pool, toolkit)
        gate, fresh = asyncio.Event(), _ReconnectingToolkit()

        async def build(spec):
            await gate.wait()
            return await pool._safe_enter(fresh, spec)

        pool._build_and_enter_toolkit = build
        try:
            replacing = asyncio.create_task(pool.reconnect_toolkit(toolkit, None))
            for _ in range(10):
                await asyncio.sleep(0)
            await pool.close_all()
            gate.set()
            self.assertFalse(await replacing)
            self.assertNotIn("replio", pool._toolkit_by_name)
            self.assertEqual(pool._toolkit_supervisors, [])
            self.assertIsNone(fresh.session)  # our build was shut down again
        finally:
            release.set()
            pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = saved

    async def test_cancelled_caller_does_not_strand_the_replacement(self):
        from openagent_core.mcp import pool as pool_module
        saved = (pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT)
        pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = 0.05, 0.2
        pool, toolkit = await self._entered()
        release = await self._wedge(pool, toolkit)
        gate, fresh = asyncio.Event(), _ReconnectingToolkit()
        builds = 0

        async def build(spec):
            nonlocal builds
            builds += 1
            await gate.wait()
            return await pool._safe_enter(fresh, spec)

        pool._build_and_enter_toolkit = build
        old_sup = pool._toolkit_supervisors[0]
        try:
            caller = asyncio.create_task(pool.reconnect_toolkit(toolkit, None))
            for _ in range(10):
                await asyncio.sleep(0)
            caller.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await caller
            # The retired toolkit never starts a second replacement.
            self.assertFalse(await pool.reconnect_toolkit(toolkit, None))
            gate.set()
            await old_sup.control["replacement"]
            self.assertEqual(builds, 1)
            self.assertIs(pool._toolkit_by_name["replio"], fresh)
            self.assertEqual(fresh.session.label, "session-1")
        finally:
            release.set()
            pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = saved

    async def test_reload_before_the_replacement_starts_wins(self):
        from openagent_core.mcp import pool as pool_module
        saved = (pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT)
        pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = 0.05, 0.2
        pool, toolkit = await self._entered()
        release = await self._wedge(pool, toolkit)
        builds = 0

        async def build(spec):
            nonlocal builds
            builds += 1
            return None

        pool._build_and_enter_toolkit = build
        try:
            caller = asyncio.create_task(pool.reconnect_toolkit(toolkit, None))
            await asyncio.sleep(0)  # replacement scheduled, not started
            # A reload that removed the server runs first.
            pool.specs = []
            pool._lifecycle_generation += 1
            self.assertFalse(await caller)
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertEqual(builds, 0)
            self.assertNotIn("replio", pool._toolkit_by_name)
        finally:
            release.set()
            pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = saved

    async def test_uninitialized_replacement_is_recovered_or_marked_dormant(self):
        from openagent_core.mcp import pool as pool_module
        saved = (pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT)
        pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = 0.05, 0.2
        pool, toolkit = await self._entered()
        release = await self._wedge(pool, toolkit)
        old_sup = pool._toolkit_supervisors[0]
        fresh = _ReconnectingToolkit()
        recoveries = []

        async def build(spec):
            entered = await pool._safe_enter(fresh, spec)
            fresh._initialized = False  # initialize() swallowed its failure
            fresh.functions = {}
            return entered

        async def recover(tk, spec):
            recoveries.append(tk)
            return 0

        pool._build_and_enter_toolkit = build
        pool._recover_dormant_toolkit = recover
        try:
            self.assertFalse(await pool.reconnect_toolkit(toolkit, None))
            await old_sup.control["replacement"]
            self.assertEqual(recoveries, [fresh])
            self.assertIs(pool._toolkit_by_name["replio"], fresh)
            self.assertEqual(pool._tool_counts["replio"], 0)
            self.assertIn("replio", pool._last_connect_error)
        finally:
            release.set()
            pool_module._MCP_CLOSE_TIMEOUT, pool_module._MCP_RECONNECT_WAIT = saved

    async def test_unknown_toolkit_is_not_reconnected(self):
        pool = MCPPool([])
        self.assertFalse(await pool.reconnect_toolkit(_ReconnectingToolkit(), None))


class EntrypointTests(unittest.IsolatedAsyncioTestCase):
    async def test_dead_ping_reconnects_and_calls_once_on_the_new_session(self):
        pool = MCPPool([])
        toolkit = _ReconnectingToolkit()
        await pool._safe_enter(toolkit, _ServerSpec(name="replio"))
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        dead = toolkit.session
        dead.dead = True

        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool(), dead, mcp_tools_instance=toolkit)
        result = await entrypoint(thread_id="t1")

        self.assertEqual(dead.calls, [])
        self.assertEqual(toolkit.session.calls, [("threads_patch", {"thread_id": "t1"})])
        self.assertIn("ok from session-2", result.content)
        self.assertFalse(result.mcp_result.get("isError"))

    async def test_dead_session_without_reconnect_fails_visibly_without_sending(self):
        dead = _Session("dead", dead=True)
        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool(), dead)
        result = await entrypoint(thread_id="t1")

        self.assertEqual(dead.calls, [])
        self.assertEqual(result.content, "Error: ClosedResourceError")
        self.assertIs(result.mcp_result["isError"], True)
        marker = result.mcp_result["_meta"]["openagent/transport_error"]
        self.assertEqual(marker["type"], "ClosedResourceError")
        self.assertIs(marker["session_dead"], True)

    async def test_call_that_dies_mid_flight_is_not_retried_but_next_call_works(self):
        pool = MCPPool([])
        toolkit = _ReconnectingToolkit()
        await pool._safe_enter(toolkit, _ServerSpec(name="replio"))
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        first = toolkit.session

        async def _ping_ok():
            return None

        first.dead = True
        first.send_ping = _ping_ok  # stream dies between ping and the request
        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool("threads_respond"), first,
                                                        mcp_tools_instance=toolkit)
        failed = await entrypoint(thread_id="t1", body_text="hi")
        self.assertEqual(len(first.calls), 1)  # never re-sent: it may have landed
        self.assertIs(failed.mcp_result["isError"], True)

        ok = await entrypoint(thread_id="t2", body_text="hi")
        self.assertIn("ok from session-2", ok.content)
        self.assertEqual(len(first.calls), 1)

    async def test_call_after_a_failed_reconnect_tries_again(self):
        pool = MCPPool([])
        toolkit = _ReconnectingToolkit(fail_reentries=1)
        await pool._safe_enter(toolkit, _ServerSpec(name="replio"))
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        dead = toolkit.session
        dead.dead = True
        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool(), dead, mcp_tools_instance=toolkit)

        failed = await entrypoint(thread_id="t1")
        self.assertIs(failed.mcp_result["isError"], True)
        self.assertIsNone(toolkit.session)

        ok = await entrypoint(thread_id="t2")
        self.assertIn("ok from session-3", ok.content)
        self.assertEqual(dead.calls, [])

    async def test_call_arriving_mid_reconnect_waits_for_the_handshake(self):
        pool = MCPPool([])
        toolkit = _ReconnectingToolkit()
        await pool._safe_enter(toolkit, _ServerSpec(name="replio"))
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        toolkit.reentry_gate = asyncio.Event()
        reconnect = asyncio.create_task(pool.reconnect_toolkit(toolkit, toolkit.session))
        for _ in range(20):
            await asyncio.sleep(0)
        half_open = toolkit.session
        self.assertEqual(half_open.label, "session-2")
        self.assertFalse(toolkit._initialized)
        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool(), half_open, mcp_tools_instance=toolkit)
        call = asyncio.create_task(entrypoint(thread_id="t1"))
        for _ in range(10):
            await asyncio.sleep(0)
        self.assertFalse(call.done())
        self.assertEqual(half_open.calls, [])
        toolkit.reentry_gate.set()
        self.assertTrue(await reconnect)
        result = await call
        self.assertIn("ok from session-2", result.content)
        self.assertTrue(toolkit._initialized)

    async def test_other_failures_are_marked_as_errors(self):
        class _Timeout(_Session):
            async def call_tool(self, name, arguments, meta=None):
                raise TimeoutError("Timed out while waiting for response")

        entrypoint = mcp_utils.get_entrypoint_for_tool(_tool(), _Timeout("slow"))
        result = await entrypoint(thread_id="t1")
        self.assertTrue(result.content.startswith("Error: TimeoutError: Timed out"))
        self.assertIs(result.mcp_result["isError"], True)
        self.assertIs(result.mcp_result["_meta"]["openagent/transport_error"]["session_dead"], False)

    def test_session_dead_detection_walks_groups_and_causes(self):
        try:
            try:
                raise anyio.ClosedResourceError()
            except anyio.ClosedResourceError as inner:
                raise RuntimeError("wrapped") from inner
        except RuntimeError as wrapped:
            self.assertTrue(mcp_utils._session_is_dead(wrapped))
        group = BaseExceptionGroup("tg", [ValueError("x"), anyio.BrokenResourceError()])
        self.assertTrue(mcp_utils._session_is_dead(group))
        self.assertFalse(mcp_utils._session_is_dead(TimeoutError("slow")))


if __name__ == "__main__":
    unittest.main()
