"""Integration: legacy projection → accept → tool invocation → read_tool_invocation.

Covers the end-to-end path from a gateway-projected legacy session (agent_id=NULL)
through runtime acceptance, tool invocation persistence, session metadata updates,
and the read_tool_invocation query that backs MCP App interactive views.

Also covers SDK115 pre-fix sessions: rows that already have runtime_contract=1
in metadata_json but agent_id=NULL (accepted before the COALESCE fix). Both
idempotent replay of an existing run and submission of a new run must claim
agent_id. Negative proofs for cross-agent and cross-tenant boundaries.

Observed in isolated QA (beta23 signed App against local fixture server), not in
production.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import time as _time
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest, SessionRef
from openagent_storage_sqlite import SqliteRuntimeStore


def _now_ms():
    return int(_time.time() * 1000)


class LegacyAgentIdIntegration(unittest.IsolatedAsyncioTestCase):
    """Full integration: legacy session → accept → tool invocation → read."""

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / "integration.sqlite"
        self.owner = PrincipalRef("host", "tenant", "owner")

    def _ctx(self, session_id, agent_id="coordinator"):
        return ExecutionContext(
            self.owner, self.owner, self.owner,
            session_id, agent_id, (self.owner,),
        )

    def _ref(self, session_id, agent_id="coordinator"):
        return SessionRef("host", "tenant", agent_id, session_id)

    async def _new_store(self):
        store = SqliteRuntimeStore(self.db_path)
        await store.start()
        return store

    async def _ensure_schema(self):
        """Create the schema if this is the first access to the DB file."""
        if not self.db_path.exists():
            store = SqliteRuntimeStore(self.db_path)
            await store.start()
            await store.close()

    async def _seed_legacy_session(self, session_id, *, metadata=None):
        """Insert a legacy-projected session with agent_id=NULL directly."""
        await self._ensure_schema()
        now = _now_ms()
        meta = json.dumps(metadata or {})
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """INSERT INTO sessions_v2(id, tenant_id, owner_principal_id,
                   visibility, session_type, kind, status,
                   created_at_ms, updated_at_ms, last_activity_at_ms,
                   agent_id, metadata_json)
                VALUES(?,?,?, ?,?,?,?, ?,?,?, ?,?)""",
                (session_id, "tenant", "owner",
                 "private", "agent", "chat", "active",
                 now, now, now,
                 None, meta),
            )
            conn.commit()

    async def _seed_sdk115_session(self, session_id, run_id, idempotency_key, input_text="Hello"):
        """Seed a pre-fix SDK115 state: runtime_contract=1 but agent_id=NULL,
        with an already-accepted run.

        Creates the run through the real store API (correct digest), then NULLs
        out agent_id on sessions_v2 to simulate the pre-fix DB state."""
        store = await self._new_store()
        ctx = self._ctx(session_id)
        await store.accept(RunRequest(run_id, session_id, idempotency_key, input_text), ctx)
        await store.close()
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute("UPDATE sessions_v2 SET agent_id=NULL WHERE id=?", (session_id,))
            conn.commit()
        self.assertIsNone(self._read_agent_id(session_id))

    def _read_agent_id(self, session_id):
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT agent_id FROM sessions_v2 WHERE id=?", (session_id,)
            ).fetchone()
            return row["agent_id"] if row else "NO_ROW"

    # ── 1. Legacy projection → accept → tool invocation → read_tool_invocation ──

    async def test_legacy_accept_tool_invocation_and_read(self):
        """Full path: legacy NULL session → accept → begin/finish tool →
        read_tool_invocation (the query behind MCP App interactive views)."""
        await self._seed_legacy_session("mcp-session")
        self.assertIsNone(self._read_agent_id("mcp-session"))

        store = await self._new_store()
        ctx = self._ctx("mcp-session")
        ref = self._ref("mcp-session")

        await store.accept(RunRequest("run-mcp", "mcp-session", "key-mcp", "Open view"), ctx)
        await store.transition("run-mcp", "running")
        self.assertEqual("coordinator", self._read_agent_id("mcp-session"))

        binding = {"source_id": "fixture", "name": "widget",
                   "tool_ref": "opaque-ref-1",
                   "metadata": {"ui": {"resourceUri": "ui://widget"}},
                   "provider_call_id": "prov-call-1"}
        await store.begin_tool("run-mcp", "call-1", binding, {"arg": "value"})
        await store.finish_tool("run-mcp", "call-1",
                                result={"content": [], "structuredContent": {"ok": True}})

        invocation = await store.read_tool_invocation(ref, "call-1", ctx)
        self.assertEqual("run-mcp", invocation["run_id"])
        self.assertEqual(binding, invocation["binding"])
        self.assertEqual({"arg": "value"}, invocation["arguments"])
        self.assertTrue(invocation["result"]["structuredContent"]["ok"])

        by_provider = await store.read_tool_invocation(ref, "prov-call-1", ctx)
        self.assertEqual(invocation["id"], by_provider["id"])

        by_id = await store.read_tool_invocation(ref, invocation["id"], ctx)
        self.assertEqual(invocation, by_id)

        await store.close()

    async def test_legacy_accept_then_rename_and_metadata_update(self):
        """After claiming a legacy session, rename and metadata operations
        that call _require_local_session must succeed."""
        await self._seed_legacy_session("rename-session")
        store = await self._new_store()
        ctx = self._ctx("rename-session")
        ref = self._ref("rename-session")

        await store.accept(RunRequest("run-rn", "rename-session", "key-rn", "Hi"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("rename-session"))

        result = store._rename_session(ref, ctx, title="New Title")
        self.assertEqual("New Title", result["title"])

        archived = store._archive_session(ref, ctx)
        self.assertEqual("archived", archived["status"])

        restored = store._restore_session(ref, ctx)
        self.assertEqual("active", restored["status"])

        await store.close()

    # ── 2. SDK115 pre-fix: idempotent replay claims agent_id ──

    async def test_sdk115_idempotent_replay_claims_agent_id(self):
        """A pre-fix session (runtime_contract=1, agent_id=NULL) with an
        existing run: replaying the same run_id+key must still claim agent_id."""
        await self._seed_sdk115_session("sdk-idem", "run-sdk", "key-sdk")
        self.assertIsNone(self._read_agent_id("sdk-idem"))

        store = await self._new_store()
        ctx = self._ctx("sdk-idem")
        record, is_new = await store.accept(
            RunRequest("run-sdk", "sdk-idem", "key-sdk", "Hello"), ctx
        )
        self.assertFalse(is_new, "Idempotent replay must return False")
        self.assertEqual("coordinator", self._read_agent_id("sdk-idem"),
                         "Idempotent replay must claim agent_id on pre-fix session")

        ref = self._ref("sdk-idem")
        row = store._require_local_session(ref, ctx)
        self.assertEqual("coordinator", row["agent_id"])
        await store.close()

    async def test_sdk115_new_run_claims_agent_id(self):
        """A pre-fix session with an existing run: a NEW run (different key)
        must also claim agent_id through the COALESCE path."""
        await self._seed_sdk115_session("sdk-new", "run-old", "key-old")
        self.assertIsNone(self._read_agent_id("sdk-new"))

        store = await self._new_store()
        ctx = self._ctx("sdk-new")
        record, is_new = await store.accept(
            RunRequest("run-new", "sdk-new", "key-new", "Continue"), ctx
        )
        self.assertTrue(is_new, "New run must return True")
        self.assertEqual("coordinator", self._read_agent_id("sdk-new"),
                         "New run on pre-fix session must claim agent_id")
        await store.close()

    async def test_sdk115_read_tool_invocation_after_replay_claim(self):
        """After idempotent replay claims agent_id, read_tool_invocation works."""
        await self._seed_sdk115_session("sdk-tool", "run-tool", "key-tool")
        store = await self._new_store()
        ctx = self._ctx("sdk-tool")

        await store.accept(RunRequest("run-tool", "sdk-tool", "key-tool", "Hello"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("sdk-tool"))

        await store.transition("run-tool", "running")
        binding = {"source_id": "fx", "name": "op", "tool_ref": "ref-2",
                   "metadata": {"ui": {"resourceUri": "ui://op"}},
                   "provider_call_id": "pcall-2"}
        await store.begin_tool("run-tool", "tcall", binding, {})
        await store.finish_tool("run-tool", "tcall", result={"content": []})

        ref = self._ref("sdk-tool")
        inv = await store.read_tool_invocation(ref, "tcall", ctx)
        self.assertEqual("run-tool", inv["run_id"])
        self.assertEqual(binding, inv["binding"])
        await store.close()

    # ── 3. Boundary proofs ──

    async def test_cross_agent_new_run_on_sdk115_blocked(self):
        """A pre-fix session claimed by coordinator via replay: a NEW run from
        a different agent must be blocked by the agent boundary guard."""
        await self._seed_sdk115_session("sdk-guard", "run-g", "key-g")

        store = await self._new_store()
        ctx_a = self._ctx("sdk-guard", agent_id="coordinator")
        await store.accept(RunRequest("run-g", "sdk-guard", "key-g", "Hello"), ctx_a)
        self.assertEqual("coordinator", self._read_agent_id("sdk-guard"))

        ctx_b = self._ctx("sdk-guard", agent_id="intruder")
        with self.assertRaises(PermissionError) as cm:
            await store.accept(RunRequest("run-g2", "sdk-guard", "key-g2", "Steal"), ctx_b)
        self.assertIn("another agent", str(cm.exception))
        self.assertEqual("coordinator", self._read_agent_id("sdk-guard"),
                         "agent_id must not mutate after cross-agent block")
        await store.close()

    async def test_cross_agent_new_run_blocked(self):
        """A new run from a different agent on an already-claimed session."""
        await self._seed_legacy_session("guard-new")
        store = await self._new_store()

        ctx_a = self._ctx("guard-new", agent_id="coordinator")
        await store.accept(RunRequest("run-a", "guard-new", "key-a", "Claim"), ctx_a)
        self.assertEqual("coordinator", self._read_agent_id("guard-new"))

        ctx_b = self._ctx("guard-new", agent_id="intruder")
        with self.assertRaises(PermissionError):
            await store.accept(RunRequest("run-b", "guard-new", "key-b", "Steal"), ctx_b)
        self.assertEqual("coordinator", self._read_agent_id("guard-new"))
        await store.close()

    async def test_cross_tenant_blocked(self):
        """Cross-tenant acceptance must fail even on legacy NULL sessions."""
        await self._seed_legacy_session("xt-session")
        store = await self._new_store()

        other_tenant = PrincipalRef("host", "other-tenant", "owner")
        ctx = ExecutionContext(other_tenant, other_tenant, other_tenant,
                              "xt-session", "coordinator", (other_tenant,))
        with self.assertRaises(PermissionError) as cm:
            await store.accept(RunRequest("run-xt", "xt-session", "key-xt", "Hi"), ctx)
        self.assertIn("another tenant", str(cm.exception))
        self.assertIsNone(self._read_agent_id("xt-session"),
                          "agent_id must stay NULL after cross-tenant block")
        await store.close()

    async def test_legacy_session_with_explicit_agent_stays_owned(self):
        """A legacy session that already has an explicit agent_id (non-NULL) from
        a non-default projection must not be re-claimed by a different agent."""
        await self._ensure_schema()
        now = _now_ms()
        with closing(sqlite3.connect(self.db_path)) as conn:
            conn.execute(
                """INSERT INTO sessions_v2(id, tenant_id, owner_principal_id,
                   visibility, session_type, kind, status,
                   created_at_ms, updated_at_ms, last_activity_at_ms,
                   agent_id, metadata_json)
                VALUES(?,?,?, ?,?,?,?, ?,?,?, ?,?)""",
                ("owned-session", "tenant", "owner",
                 "private", "agent", "chat", "active",
                 now, now, now,
                 "original-agent", json.dumps({})),
            )
            conn.commit()

        store = await self._new_store()
        ctx = self._ctx("owned-session", agent_id="original-agent")
        await store.accept(RunRequest("run-own", "owned-session", "key-own", "Ok"), ctx)
        self.assertEqual("original-agent", self._read_agent_id("owned-session"))

        ctx_b = self._ctx("owned-session", agent_id="coordinator")
        with self.assertRaises(PermissionError):
            await store.accept(RunRequest("run-steal", "owned-session", "key-steal", "Nope"), ctx_b)
        self.assertEqual("original-agent", self._read_agent_id("owned-session"))
        await store.close()

    async def test_same_agent_idempotent_new_run_no_mutation(self):
        """Same agent re-accepting a claimed session: agent_id must stay the same."""
        await self._seed_legacy_session("idem2")
        store = await self._new_store()
        ctx = self._ctx("idem2")

        await store.accept(RunRequest("r1", "idem2", "k1", "First"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("idem2"))

        await store.accept(RunRequest("r2", "idem2", "k2", "Second"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("idem2"))

        await store.accept(RunRequest("r3", "idem2", "k3", "Third"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("idem2"))
        await store.close()

    # ── 4. read_tool_invocation boundary failures ──

    async def test_read_tool_invocation_wrong_agent_fails(self):
        """read_tool_invocation with wrong agent_id in SessionRef must fail."""
        await self._seed_legacy_session("inv-boundary")
        store = await self._new_store()
        ctx = self._ctx("inv-boundary")
        await store.accept(RunRequest("run-ib", "inv-boundary", "key-ib", "Go"), ctx)
        await store.transition("run-ib", "running")
        binding = {"source_id": "f", "name": "t", "tool_ref": "r",
                   "metadata": {}, "provider_call_id": "p"}
        await store.begin_tool("run-ib", "c1", binding, {})
        await store.finish_tool("run-ib", "c1", result={"content": []})

        wrong_ref = self._ref("inv-boundary", agent_id="wrong-agent")
        with self.assertRaises(PermissionError):
            await store.read_tool_invocation(wrong_ref, "c1", ctx)
        await store.close()

    async def test_read_tool_invocation_wrong_session_fails(self):
        """read_tool_invocation referencing a different session must fail."""
        await self._seed_legacy_session("inv-s1")
        store = await self._new_store()
        ctx = self._ctx("inv-s1")
        await store.accept(RunRequest("run-is", "inv-s1", "key-is", "Go"), ctx)

        wrong_ref = self._ref("inv-nonexistent")
        with self.assertRaises((PermissionError, LookupError)):
            await store.read_tool_invocation(wrong_ref, "anything", ctx)
        await store.close()


if __name__ == "__main__":
    unittest.main()
