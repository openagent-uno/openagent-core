"""Real projection integration: legacy sessions table → project_legacy_session →
sessions_v2 → SqliteRuntimeStore accept → tool invocation → read_tool_invocation →
saved_invocation_display.

Unlike test_legacy_agent_id_integration (which seeds sessions_v2 directly), this
test exercises the actual projection path that the server uses when a gateway writes
to the compatibility `sessions` table and the operational layer projects into v2.

Covers:
 1. Gateway legacy row with agent_id=NULL → project_legacy_session → sessions_v2 agent_id=NULL
 2. Accept claims agent_id → tool invocation → read_tool_invocation succeeds
 3. saved_invocation_display returns MCP App display data
 4. SDK115 historical: runtime_contract=1 + agent_id=NULL, replay + new run + tool read
 5. Projection of legacy session with explicit agent_id preserves ownership
 6. Negative: cross-agent read_tool_invocation on projected session blocked
 7. Negative: project_legacy_session re-projection with runtime_contract=1 only updates envelope

Observed in isolated QA (beta23 signed App against local fixture server).
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
import time as _time
import unittest
from contextlib import closing
from pathlib import Path

from openagent_core.contracts import (
    ExecutionContext, PrincipalRef, RunRequest, SessionRef,
    require_authorized, ResourceRef,
)
from openagent_core.memory.operational.repository import (
    project_legacy_session, operational_storage_available,
)
from openagent_storage_sqlite import SqliteRuntimeStore


def _now_ms():
    return int(_time.time() * 1000)


_LEGACY_SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    session_type TEXT,
    agent_id TEXT,
    team_id TEXT,
    workflow_id TEXT,
    user_id TEXT,
    session_data TEXT,
    agent_data TEXT,
    team_data TEXT,
    workflow_data TEXT,
    metadata TEXT,
    runs TEXT,
    summary TEXT,
    created_at TEXT,
    updated_at TEXT
);
"""


class _AllowAll:
    """Trivial authorizer that approves every request."""
    async def authorize(self, context, action, resource, *, audience=()):
        return True


class RealProjectionMcpIntegration(unittest.IsolatedAsyncioTestCase):
    """Full integration: legacy `sessions` table → projection → store → MCP."""

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / "projection_integration.sqlite"
        self.owner = PrincipalRef("host", "tenant", "owner")

    def _ctx(self, session_id, agent_id="coordinator"):
        return ExecutionContext(
            self.owner, self.owner, self.owner,
            session_id, agent_id, (self.owner,),
        )

    def _ref(self, session_id, agent_id="coordinator"):
        return SessionRef("host", "tenant", agent_id, session_id)

    async def _boot_store(self):
        store = SqliteRuntimeStore(self.db_path)
        await store.start()
        return store

    def _add_legacy_sessions_table(self, conn):
        conn.executescript(_LEGACY_SESSIONS_DDL)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS network (network_id TEXT);
            INSERT OR IGNORE INTO network(network_id) VALUES('tenant');
        """)

    def _insert_legacy_row(self, conn, session_id, *, agent_id=None,
                           metadata=None, runs=None, session_type="agent"):
        now = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
        meta = metadata or {"client_id": "user:owner"}
        conn.execute(
            """INSERT INTO sessions(session_id, session_type, agent_id, team_id,
               workflow_id, user_id, session_data, agent_data, team_data,
               workflow_data, metadata, runs, summary, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (session_id, session_type, agent_id, None, None, None,
             json.dumps({}), None, None, None,
             json.dumps(meta),
             json.dumps(runs) if runs is not None else None,
             None, now, now),
        )

    def _project(self, conn, session_id, *, source_row=None):
        return project_legacy_session(conn, session_id, source_row=source_row)

    def _read_agent_id(self, session_id):
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT agent_id FROM sessions_v2 WHERE id=?", (session_id,)
            ).fetchone()
            return row["agent_id"] if row else "NO_ROW"

    def _read_runtime_contract(self, session_id):
        with closing(sqlite3.connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT metadata_json FROM sessions_v2 WHERE id=?", (session_id,)
            ).fetchone()
            if not row:
                return None
            meta = json.loads(row[0] or "{}")
            return meta.get("runtime_contract")

    # ── 1. Gateway legacy → project_legacy_session → sessions_v2 agent_id=NULL ──

    async def test_gateway_projection_produces_null_agent_id(self):
        """A gateway legacy session with agent_id=NULL: projection writes NULL
        into sessions_v2.agent_id (the pre-condition of the bug)."""
        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        self._insert_legacy_row(conn, "proj-null")
        conn.execute("BEGIN IMMEDIATE")
        result = self._project(conn, "proj-null")
        conn.execute("COMMIT")

        self.assertTrue(result.changed)
        self.assertIsNone(self._read_agent_id("proj-null"),
                          "Projection of gateway session must produce agent_id=NULL")
        await store.close()

    # ── 2. Projection → accept → tool invocation → read_tool_invocation ──

    async def test_full_mcp_path_after_projection(self):
        """End-to-end: legacy projection (NULL agent_id) → accept claims →
        begin_tool → finish_tool → read_tool_invocation (MCP App query)."""
        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        self._insert_legacy_row(conn, "mcp-proj")
        conn.execute("BEGIN IMMEDIATE")
        self._project(conn, "mcp-proj")
        conn.execute("COMMIT")

        self.assertIsNone(self._read_agent_id("mcp-proj"))

        ctx = self._ctx("mcp-proj")
        ref = self._ref("mcp-proj")

        await store.accept(RunRequest("run-proj", "mcp-proj", "key-proj", "Open MCP view"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("mcp-proj"),
                         "Accept must claim agent_id on projected NULL session")

        await store.transition("run-proj", "running")

        binding = {
            "source_id": "mcp-widget-server",
            "name": "openWidgetView",
            "tool_ref": "opaque-tool-ref-42",
            "metadata": {"ui": {"resourceUri": "ui://widget/dashboard"}},
            "provider_call_id": "pcall-proj",
        }
        await store.begin_tool("run-proj", "call-proj", binding, {"dashboard_id": "d1"})
        await store.finish_tool("run-proj", "call-proj",
                                result={"content": [{"type": "text", "text": "OK"}],
                                        "structuredContent": {"dashboard": "rendered"}})

        invocation = await store.read_tool_invocation(ref, "call-proj", ctx)
        self.assertEqual("run-proj", invocation["run_id"])
        self.assertEqual("mcp-proj", invocation["session_id"])
        self.assertEqual(binding, invocation["binding"])
        self.assertEqual({"dashboard_id": "d1"}, invocation["arguments"])
        self.assertTrue(invocation["result"]["structuredContent"]["dashboard"] == "rendered")
        self.assertEqual("success", invocation["status"])

        by_provider = await store.read_tool_invocation(ref, "pcall-proj", ctx)
        self.assertEqual(invocation["id"], by_provider["id"])

        await store.close()

    # ── 3. saved_invocation_display returns MCP App data ──

    async def test_saved_invocation_display_after_projection(self):
        """saved_invocation_display (the function MCP App open calls) returns
        the correct toolInput/toolResult for a projected+claimed session."""
        from openagent_core.mcp_apps import saved_invocation_display

        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        self._insert_legacy_row(conn, "display-proj")
        conn.execute("BEGIN IMMEDIATE")
        self._project(conn, "display-proj")
        conn.execute("COMMIT")

        ctx = self._ctx("display-proj")
        ref = self._ref("display-proj")
        authorizer = _AllowAll()

        await store.accept(RunRequest("run-disp", "display-proj", "key-disp", "View"), ctx)
        await store.transition("run-disp", "running")

        tool_ref = "opaque-mcp-ref-99"
        binding = {"source_id": "srv", "name": "showPanel", "tool_ref": tool_ref,
                   "metadata": {}, "provider_call_id": "pc-disp"}
        await store.begin_tool("run-disp", "call-disp", binding, {"panel": "metrics"})
        await store.finish_tool("run-disp", "call-disp",
                                result={"content": [], "structuredContent": {"metrics": [1, 2, 3]}})

        display = await saved_invocation_display(store, authorizer, ctx, "call-disp", tool_ref)
        self.assertEqual({"panel": "metrics"}, display["toolInput"])
        self.assertIn("structuredContent", display["toolResult"])
        self.assertEqual([1, 2, 3], display["toolResult"]["structuredContent"]["metrics"])
        self.assertEqual("success", display["toolStatus"])
        self.assertNotIn("toolCancelled", display)

        await store.close()

    # ── 4. SDK115 historical: runtime_contract=1 + agent_id=NULL ──

    async def test_sdk115_projection_then_accept_replay_claims(self):
        """SDK115 scenario: session projected, then accepted (gets runtime_contract=1),
        then agent_id NULLed to simulate pre-fix state. A re-projection only updates
        envelope (runtime_contract=1 path). Then replay claims agent_id."""
        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        run_envelope = [{"run_id": "run-sdk", "id": "run-sdk", "status": "completed",
                         "messages": [{"role": "user", "content": "Hello"}]}]
        self._insert_legacy_row(conn, "sdk-proj", runs=run_envelope)
        conn.execute("BEGIN IMMEDIATE")
        self._project(conn, "sdk-proj")
        conn.execute("COMMIT")

        ctx = self._ctx("sdk-proj")
        await store.accept(
            RunRequest("run-sdk-accept", "sdk-proj", "key-sdk-a", "Start"), ctx
        )
        self.assertEqual("coordinator", self._read_agent_id("sdk-proj"))
        self.assertEqual(1, self._read_runtime_contract("sdk-proj"))

        await store.close()

        with closing(sqlite3.connect(self.db_path)) as raw:
            raw.execute("UPDATE sessions_v2 SET agent_id=NULL WHERE id='sdk-proj'")
            raw.commit()
        self.assertIsNone(self._read_agent_id("sdk-proj"))

        store2 = await self._boot_store()
        conn2 = store2.connection

        conn2.execute("BEGIN IMMEDIATE")
        result = self._project(conn2, "sdk-proj")
        conn2.execute("COMMIT")
        self.assertIsNotNone(result,
                             "Re-projection with runtime_contract=1 takes the envelope-only path")
        self.assertIsNone(self._read_agent_id("sdk-proj"),
                          "Re-projection must NOT claim agent_id (it only updates envelope)")

        ctx2 = self._ctx("sdk-proj")
        await store2.accept(
            RunRequest("run-sdk-new", "sdk-proj", "key-sdk-new", "Continue"), ctx2
        )
        self.assertEqual("coordinator", self._read_agent_id("sdk-proj"),
                         "New run on pre-fix SDK115 session must claim agent_id")

        await store2.transition("run-sdk-new", "running")
        binding = {"source_id": "s", "name": "n", "tool_ref": "tr",
                   "metadata": {}, "provider_call_id": "pc"}
        await store2.begin_tool("run-sdk-new", "tc-sdk", binding, {"x": 1})
        await store2.finish_tool("run-sdk-new", "tc-sdk", result={"content": []})

        ref = self._ref("sdk-proj")
        inv = await store2.read_tool_invocation(ref, "tc-sdk", ctx2)
        self.assertEqual("run-sdk-new", inv["run_id"])
        await store2.close()

    # ── 5. Projection with explicit agent_id preserves ownership ──

    async def test_projection_with_explicit_agent_preserves_ownership(self):
        """A legacy session that has agent_id set: projection preserves it,
        and only the matching agent can accept."""
        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        self._insert_legacy_row(conn, "owned-proj", agent_id="original-agent")
        conn.execute("BEGIN IMMEDIATE")
        self._project(conn, "owned-proj")
        conn.execute("COMMIT")

        # build_session_projection at line 335: agent_id fallback to "openagent" if NULL,
        # but the session dict at line 672 uses legacy.get("agent_id") raw.
        # For a non-NULL agent_id, it passes through.
        # Actually line 672: legacy.get("agent_id") — if agent_id="original-agent", it stays.
        agent = self._read_agent_id("owned-proj")
        self.assertEqual("original-agent", agent,
                         "Projection must preserve explicit agent_id from legacy row")

        ctx_same = self._ctx("owned-proj", agent_id="original-agent")
        await store.accept(
            RunRequest("run-owned", "owned-proj", "key-owned", "Ok"), ctx_same
        )
        self.assertEqual("original-agent", self._read_agent_id("owned-proj"))

        ctx_other = self._ctx("owned-proj", agent_id="intruder")
        with self.assertRaises(PermissionError):
            await store.accept(
                RunRequest("run-steal", "owned-proj", "key-steal", "No"), ctx_other
            )
        self.assertEqual("original-agent", self._read_agent_id("owned-proj"))
        await store.close()

    # ── 6. Cross-agent read_tool_invocation on projected session ──

    async def test_cross_agent_read_tool_invocation_blocked_after_projection(self):
        """After projection + claim, read_tool_invocation with wrong agent fails."""
        store = await self._boot_store()
        conn = store.connection

        self._add_legacy_sessions_table(conn)
        self._insert_legacy_row(conn, "xagent-proj")
        conn.execute("BEGIN IMMEDIATE")
        self._project(conn, "xagent-proj")
        conn.execute("COMMIT")

        ctx = self._ctx("xagent-proj")
        await store.accept(RunRequest("run-xa", "xagent-proj", "key-xa", "Go"), ctx)
        await store.transition("run-xa", "running")

        binding = {"source_id": "s", "name": "n", "tool_ref": "t",
                   "metadata": {}, "provider_call_id": "p"}
        await store.begin_tool("run-xa", "call-xa", binding, {})
        await store.finish_tool("run-xa", "call-xa", result={"content": []})

        wrong_ref = self._ref("xagent-proj", agent_id="intruder")
        with self.assertRaises((PermissionError, LookupError)):
            await store.read_tool_invocation(wrong_ref, "call-xa", ctx)
        await store.close()

    # ── 7. project_legacy_session with source_row bypasses _fetch_legacy ──

    async def test_projection_with_source_row_bypasses_fetch(self):
        """project_legacy_session accepts source_row directly, avoiding the
        legacy sessions table. This is the path used by reviewed runtime writers."""
        store = await self._boot_store()
        conn = store.connection
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS network (network_id TEXT);
            INSERT OR IGNORE INTO network(network_id) VALUES('tenant');
        """)

        source = {
            "session_id": "source-row-test",
            "session_type": "agent",
            "agent_id": None,
            "team_id": None,
            "workflow_id": None,
            "user_id": None,
            "session_data": json.dumps({}),
            "agent_data": None,
            "team_data": None,
            "workflow_data": None,
            "metadata": json.dumps({"client_id": "user:owner"}),
            "runs": None,
            "summary": None,
            "created_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
            "updated_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        }

        conn.execute("BEGIN IMMEDIATE")
        result = self._project(conn, "source-row-test", source_row=source)
        conn.execute("COMMIT")

        self.assertTrue(result.changed)
        self.assertIsNone(self._read_agent_id("source-row-test"))

        ctx = self._ctx("source-row-test")
        await store.accept(RunRequest("run-sr", "source-row-test", "key-sr", "Hi"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("source-row-test"))
        await store.close()


if __name__ == "__main__":
    unittest.main()
