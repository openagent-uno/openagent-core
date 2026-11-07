"""A crashed streamable-HTTP transport must be replaced, not silently abandoned.

When an HTTP request of the MCP transport fails, the SDK runs that POST as a child of the
transport's anyio task group, so the failure tore the group down and closed
the session's streams. That task group was entered on the pool's supervisor
task, so anyio also cancelled the supervisor while it waited for work. The
supervisor read that cancellation as a shutdown, marked itself ``closing``
and exited without forgetting the dead session. From then on every call
pinged the dead session (``ClosedResourceError``), asked for a reconnect, and
``reconnect_toolkit`` returned ``False`` at its ``closing`` check without a
single event, requiring a process restart to recover.

These tests drive the real ``MCPPool`` + ``MCPTools`` + MCP SDK against a real
stateless FastMCP streamable-HTTP server, not only fakes.
"""
from __future__ import annotations

import asyncio
import json
import socket
import unittest
from contextlib import suppress

from openagent_core.mcp import pool as pool_module
from openagent_core.mcp.pool import MCPPool, _ServerSpec


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class _FaultInjector:
    """ASGI middleware: answers the next ``fail_posts`` POSTs with a 502.

    A 502 makes ``response.raise_for_status()`` raise inside the SDK's POST
    task, which is the same class of failure as the production one (an httpx
    error in a child task of the transport's task group).
    """

    def __init__(self, app) -> None:
        self.app = app
        self.fail_posts = 0
        self.posts = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] == "POST":
            self.posts += 1
            if self.fail_posts > 0:
                self.fail_posts -= 1
                await send({"type": "http.response.start", "status": 502,
                            "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"bad gateway"})
                return
        await self.app(scope, receive, send)


class _Server:
    """A stateless FastMCP streamable-HTTP server on a fixed
    port. ``stop()``/``start()`` simulate an outage; each start builds a new
    app because the SDK's session manager runs once per instance."""

    def __init__(self) -> None:
        self.port = _free_port()
        self.faults = _FaultInjector(None)
        self._server = None
        self._task: asyncio.Task | None = None

    @staticmethod
    def _app():
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("remote-fake", stateless_http=True, streamable_http_path="/")

        @mcp.tool()
        async def read_record(record_id: str) -> str:
            return json.dumps({"record": record_id, "items": []})

        @mcp.tool()
        async def slow(seconds: float) -> str:
            await asyncio.sleep(seconds)
            return "slow done"

        return mcp.streamable_http_app()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/"

    async def start(self) -> None:
        import uvicorn

        self.faults.app = self._app()
        config = uvicorn.Config(self.faults, host="127.0.0.1", port=self.port,
                                log_level="error", lifespan="on")
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        for _ in range(250):
            if self._server.started:
                return
            if self._task.done():
                break
            await asyncio.sleep(0.02)
        raise RuntimeError("fake MCP server did not start")

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            with suppress(BaseException):
                await asyncio.wait_for(self._task, 10)
        self._server = None
        self._task = None


class RealTransportCrashTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.events: list[tuple[str, dict]] = []
        saved_elog = pool_module.elog

        def _elog(event, **fields):
            self.events.append((event, fields))
            return saved_elog(event, **fields)

        pool_module.elog = _elog
        self.addCleanup(setattr, pool_module, "elog", saved_elog)
        saved_stagger = pool_module._STARTUP_STAGGER_SECONDS
        pool_module._STARTUP_STAGGER_SECONDS = 0
        self.addCleanup(setattr, pool_module, "_STARTUP_STAGGER_SECONDS", saved_stagger)

        self.server = _Server()
        await self.server.start()
        self.addAsyncCleanup(self.server.stop)
        self.pool = MCPPool([_ServerSpec(name="remote", url=self.server.url)])
        await self.pool.connect_all()
        self.addAsyncCleanup(self.pool.close_all)
        self.assertGreater(self.pool.server_summary().get("remote", 0), 0)

    def _toolkit(self):
        toolkit = self.pool.toolkit_by_name("remote")
        self.assertIsNotNone(toolkit)
        return toolkit

    async def _call(self, tool: str, **kwargs):
        # The capability catalog resolves the CURRENT toolkit by name on every
        # call, which is also how a replacement toolkit becomes visible.
        fn = self._toolkit().functions[f"remote_{tool}"]
        return await fn.entrypoint(**kwargs)

    @staticmethod
    def _failed(result) -> bool:
        return bool((result.mcp_result or {}).get("isError"))

    async def _settle(self) -> None:
        for _ in range(50):
            await asyncio.sleep(0.02)

    async def test_healthy_calls_work(self):
        result = await self._call("read_record", record_id="t-1")
        self.assertFalse(self._failed(result), result.content)
        self.assertIn("t-1", result.content)

    async def test_read_timeout_alone_does_not_kill_the_transport(self):
        # A slow tool alone must not make a healthy session unrecoverable.
        toolkit = self._toolkit()
        session = toolkit.session
        saved = session._session_read_timeout_seconds
        from datetime import timedelta
        session._session_read_timeout_seconds = timedelta(seconds=0.5)
        try:
            result = await self._call("slow", seconds=2.0)
        finally:
            session._session_read_timeout_seconds = saved
        self.assertTrue(self._failed(result))
        self.assertIn("Timed out", result.content)
        meta = result.mcp_result["_meta"]["openagent/transport_error"]
        self.assertFalse(meta["session_dead"])
        await asyncio.sleep(2.0)  # the abandoned response arrives later
        result = await self._call("read_record", record_id="t-2")
        self.assertFalse(self._failed(result), result.content)
        self.assertIs(self._toolkit().session, session)

    async def test_crashed_transport_is_replaced_and_later_calls_work(self):
        self.assertFalse(self._failed(await self._call("read_record", record_id="t-0")))
        # The next POST is the pre-call ping: it gets a 502 and the SDK's POST
        # task raises inside the transport task group, which dies.
        self.server.faults.fail_posts = 1
        await self._call("read_record", record_id="t-1")  # may fail: it was in flight
        await self._settle()
        results = [await self._call("read_record", record_id=f"t-{i}") for i in range(2, 5)]
        failures = [r.content for r in results if self._failed(r)]
        self.assertEqual(failures, [], "calls after a crashed transport must recover, "
                         f"events={[e for e, _ in self.events]}")
        names = [e for e, _ in self.events]
        self.assertIn("mcp.session_transport_lost", names)

    async def test_crash_during_a_call_recovers_for_the_next_call(self):
        # The failure lands on the tools/call POST itself (ping succeeds).
        original_posts = self.server.faults.posts
        # ping = POST 1, tools/call = POST 2: fail the call, not the ping.
        app = self.server.faults.app
        faults = self.server.faults
        state = {"n": 0}

        async def _selective(scope, receive, send):
            if scope["type"] == "http" and scope["method"] == "POST":
                state["n"] += 1
                if state["n"] == 2:
                    await send({"type": "http.response.start", "status": 502,
                                "headers": [(b"content-type", b"text/plain")]})
                    await send({"type": "http.response.body", "body": b"bad gateway"})
                    return
            await app(scope, receive, send)

        faults.app = _selective
        try:
            first = await self._call("read_record", record_id="t-1")
        finally:
            faults.app = app
        self.assertTrue(self._failed(first))
        await self._settle()
        second = await self._call("read_record", record_id="t-2")
        self.assertFalse(self._failed(second), second.content)
        self.assertGreater(self.server.faults.posts, original_posts)

    async def test_server_outage_then_recovery(self):
        await self.server.stop()
        down = await self._call("read_record", record_id="t-1")
        self.assertTrue(self._failed(down))
        await self._settle()
        await self.server.start()
        results = []
        for i in range(3):
            results.append(await self._call("read_record", record_id=f"t-{i + 2}"))
            if not self._failed(results[-1]):
                break
            await asyncio.sleep(0.3)
        self.assertFalse(self._failed(results[-1]),
                         f"no recovery after the server came back: {[r.content for r in results]}, "
                         f"events={[e for e, _ in self.events]}")

    async def test_call_coinciding_with_the_crash_is_retried_once_on_a_new_session(self):
        # The pre-call ping can wait its whole read timeout
        # on a transport that had already died, then the call hit the closed
        # stream. The request had not been sent, so it may go out once on the
        # replacement session. A short read timeout keeps the test fast.
        toolkit = self._toolkit()
        from datetime import timedelta
        toolkit.session._session_read_timeout_seconds = timedelta(seconds=1)
        self.server.faults.fail_posts = 1  # the ping's POST
        posts_before = self.server.faults.posts
        result = await self._call("read_record", record_id="t-crash")
        self.assertFalse(self._failed(result), result.content)
        self.assertIn("t-crash", result.content)
        self.assertGreater(self.server.faults.posts, posts_before + 1)

    async def test_transport_lost_during_the_reconnect_handshake_does_not_loop(self):
        # The automatic reconnect's own initialize POST fails as well, so the
        # new transport dies during its handshake. The supervisor must leave
        # that cancelled scope instead of being woken by it at every await
        # (found in review with an injected fault: 130 reconnects in 2 s),
        # and a later call must still get a working session.
        from datetime import timedelta
        self._toolkit().session._session_read_timeout_seconds = timedelta(seconds=1)
        self.server.faults.fail_posts = 2  # the ping's POST, then the reconnect's initialize
        await self._call("read_record", record_id="t-1")  # may fail: it met both faults
        await asyncio.sleep(1.5)
        names = [e for e, _ in self.events]
        self.assertEqual(names.count("mcp.session_transport_lost"), 1, names)
        self.assertLessEqual(names.count("mcp.session_reconnect_failed"), 2, names)
        result = await self._call("read_record", record_id="t-2")
        self.assertFalse(self._failed(result), result.content)

    async def test_reconnect_skips_are_logged(self):
        toolkit = self._toolkit()
        sup = next(s for s in self.pool._toolkit_supervisors if s.toolkit is toolkit)
        self.assertFalse(await self.pool.reconnect_toolkit(object(), None, reason="test"))
        skipped = [f for e, f in self.events if e == "mcp.session_reconnect_skipped"]
        self.assertTrue(skipped, "an early return must leave an event")
        self.assertEqual(skipped[-1].get("why"), "no_supervisor")
        self.assertIsNotNone(sup)


class SupervisorExitedTests(unittest.IsolatedAsyncioTestCase):
    """The c77cf31 failure branch, with fakes: a registered supervisor that
    stopped by itself refused every reconnect without an event."""

    async def test_self_closed_supervisor_is_logged_and_replaced(self):
        from test_mcp_dead_session_reconnect import _ReconnectingToolkit

        events: list[tuple[str, dict]] = []
        saved_elog = pool_module.elog
        pool_module.elog = lambda event, **f: events.append((event, f))
        self.addCleanup(setattr, pool_module, "elog", saved_elog)
        pool = MCPPool([_ServerSpec(name="remote", url="http://remote.invalid/mcp")])
        toolkit = _ReconnectingToolkit()
        self.assertIs(await pool._safe_enter(toolkit, _ServerSpec(name="remote")), toolkit)
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        fresh = _ReconnectingToolkit()

        async def build(spec):
            return await pool._safe_enter(fresh, spec)

        pool._build_and_enter_toolkit = build
        sup = pool._toolkit_supervisors[0]
        # What the old supervisor did on a transport cancel: mark itself
        # closing and exit while still registered.
        sup.control["closing"] = True
        sup.stop_event.set()
        await asyncio.sleep(0.05)
        self.assertTrue(sup.task.done())

        self.assertFalse(await pool.reconnect_toolkit(toolkit, toolkit.session, reason="ClosedResourceError"))
        skipped = [f for e, f in events if e == "mcp.session_reconnect_skipped"]
        self.assertEqual(skipped[0]["why"], "supervisor_exited")
        self.assertTrue(skipped[0]["replace"])
        self.assertIs(pool._toolkit_by_name["remote"], fresh)
        self.assertEqual([s.toolkit for s in pool._toolkit_supervisors], [fresh])
        # The retired toolkit is never reconnected again, and says why.
        self.assertFalse(await pool.reconnect_toolkit(toolkit, None))
        self.assertEqual([f["why"] for e, f in events if e == "mcp.session_reconnect_skipped"][-1],
                         "no_supervisor")


class SupervisorWakeTests(unittest.IsolatedAsyncioTestCase):
    """How the supervisor reads a cancellation, with fakes."""

    async def _entered(self):
        from test_mcp_dead_session_reconnect import _ReconnectingToolkit

        self.events: list[tuple[str, dict]] = []
        saved_elog = pool_module.elog
        pool_module.elog = lambda event, **f: self.events.append((event, f))
        self.addCleanup(setattr, pool_module, "elog", saved_elog)
        pool = MCPPool([_ServerSpec(name="remote", url="http://remote.invalid/mcp")])
        toolkit = _ReconnectingToolkit()
        self.assertIs(await pool._safe_enter(toolkit, _ServerSpec(name="remote")), toolkit)
        pool._connected = True
        self.addAsyncCleanup(pool.close_all)
        return pool, toolkit, pool._toolkit_supervisors[0]

    async def test_transport_cancel_reconnects_and_callers_join_it(self):
        pool, toolkit, sup = await self._entered()
        dead = toolkit.session
        toolkit.reentry_gate = asyncio.Event()
        # What anyio does when a transport worker dies: cancel the host task
        # (the supervisor) with its cancel-scope reason.
        sup.task.cancel("Cancelled via cancel scope 7f00deadbeef")
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertFalse(sup.task.done())
        self.assertFalse(sup.control["closing"])
        self.assertIsNotNone(sup.control["pending"])
        # A caller that finds the dead session meanwhile joins the reconnect
        # in flight; it must not request (and tear down) another one.
        joiner = asyncio.create_task(pool.reconnect_toolkit(toolkit, dead, reason="ClosedResourceError"))
        for _ in range(5):
            await asyncio.sleep(0)
        self.assertFalse(joiner.done())
        toolkit.reentry_gate.set()
        self.assertTrue(await joiner)
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertEqual(toolkit.enters, 2)
        self.assertEqual(toolkit.session.label, "session-2")
        names = [e for e, _ in self.events]
        self.assertIn("mcp.session_transport_lost", names)
        self.assertIn("mcp.session_reconnected", names)
        # Still serviceable afterwards.
        self.assertTrue(await pool.reconnect_toolkit(toolkit, toolkit.session))
        self.assertEqual(toolkit.session.label, "session-3")

    async def test_foreign_cancellation_still_means_shutdown(self):
        pool, toolkit, sup = await self._entered()
        sup.task.cancel()
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertTrue(sup.task.done())
        self.assertTrue(sup.control["closing"])
        self.assertEqual(toolkit.enters, 1)


class ReadTimeoutConfigTests(unittest.TestCase):
    def test_per_server_override_wins_and_rounds_up(self):
        import os
        from unittest import mock
        with mock.patch.dict(os.environ, {"OPENAGENT_MCP_READ_TIMEOUT_SECONDS": "45",
                                          "OPENAGENT_MCP_READ_TIMEOUT_REMOTE": "89.2"}):
            self.assertEqual(pool_module._mcp_read_timeout_seconds("remote"), 90)
            self.assertEqual(pool_module._mcp_read_timeout_seconds("other"), 45)
        with mock.patch.dict(os.environ, {"OPENAGENT_MCP_READ_TIMEOUT_HOST_A": "120"}):
            self.assertEqual(pool_module._mcp_read_timeout_seconds("host-a"), 120)
        with mock.patch.dict(os.environ, {"OPENAGENT_MCP_READ_TIMEOUT_REMOTE": "nope"}, clear=False):
            os.environ.pop("OPENAGENT_MCP_READ_TIMEOUT_SECONDS", None)
            self.assertIsNone(pool_module._mcp_read_timeout_seconds("remote"))

    def test_non_finite_overrides_are_ignored(self):
        import os
        from unittest import mock
        for value in ("inf", "-inf", "nan", "0", "-1"):
            with self.subTest(value=value), mock.patch.dict(os.environ, {
                "OPENAGENT_MCP_READ_TIMEOUT_REMOTE": value,
                "OPENAGENT_MCP_READ_TIMEOUT_SECONDS": "45",
            }):
                self.assertEqual(pool_module._mcp_read_timeout_seconds("remote"), 45)

    def test_connect_phase_stays_bounded(self):
        import httpx
        client = pool_module._bounded_connect_http_client(timeout=httpx.Timeout(90, read=300))
        try:
            self.assertEqual(client.timeout.connect, 30.0)
            self.assertEqual(client.timeout.read, 300)
            self.assertEqual(client.timeout.write, 90)
        finally:
            import asyncio as _asyncio
            _asyncio.run(client.aclose())


class RealReadTimeoutOverrideTests(unittest.IsolatedAsyncioTestCase):
    async def test_override_reaches_the_session(self):
        import os
        from unittest import mock
        server = _Server()
        await server.start()
        self.addAsyncCleanup(server.stop)
        with mock.patch.dict(os.environ, {"OPENAGENT_MCP_READ_TIMEOUT_REMOTE": "1"}):
            pool = MCPPool([_ServerSpec(name="remote", url=server.url)])
            await pool.connect_all()
        self.addAsyncCleanup(pool.close_all)
        toolkit = pool.toolkit_by_name("remote")
        self.assertEqual(toolkit.session._session_read_timeout_seconds.total_seconds(), 1)
        slow = await toolkit.functions["remote_slow"].entrypoint(seconds=2.5)
        self.assertIn("Timed out", slow.content)
        await asyncio.sleep(2.0)
        ok = await toolkit.functions["remote_read_record"].entrypoint(record_id="t")
        self.assertFalse((ok.mcp_result or {}).get("isError"), ok.content)


class DeliveryFailureClassTests(unittest.TestCase):
    def test_transport_failures_are_transient(self):
        from openagent_core.core.event_dispatcher import _classify_delivery_failure
        for text in (
            "remote tool failed in transport (Error: ClosedResourceError)",
            "Error: BrokenResourceError",
            "McpError: Connection closed",
        ):
            self.assertEqual(_classify_delivery_failure(text), "transient", text)
        self.assertEqual(_classify_delivery_failure(ValueError("bad template")), "permanent")


if __name__ == "__main__":
    unittest.main()
