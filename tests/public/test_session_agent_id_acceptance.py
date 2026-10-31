"""Regression: runtime acceptance must claim agent_id for legacy-projected sessions.

A fresh chat session created by the gateway (legacy path) has agent_id=NULL in
sessions_v2 because the legacy ``sessions`` table does not carry agent_id.  When
the runtime accepts such a session, the ``_accept`` method must set agent_id to
``context.agent_id`` (e.g. "coordinator") so that subsequent tool-invocation
lookups via ``_require_local_session`` succeed.

This test covers the exact failure path observed in isolated QA (beta23 signed
App against local fixture server): MCP App opening an interactive view returned
"mcp_app_unavailable" because ``_require_local_session`` searched
``WHERE agent_id='coordinator'`` but found only ``agent_id=NULL``.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest, SessionRef
from openagent_storage_sqlite import SqliteRuntimeStore


class SessionAgentIdAcceptance(unittest.IsolatedAsyncioTestCase):
    """Ensure _accept claims agent_id on legacy sessions and enforces boundaries."""

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fixture.sqlite"

        self.owner = PrincipalRef("host", "tenant-one", "owner")

    def _coordinator_context(self, session_id):
        return ExecutionContext(
            self.owner, self.owner, self.owner,
            session_id, "coordinator", (self.owner,),
        )

    def _other_agent_context(self, session_id):
        return ExecutionContext(
            self.owner, self.owner, self.owner,
            session_id, "other-agent", (self.owner,),
        )

    async def _seed_legacy_session(self, session_id):
        """Simulate a legacy-projected session in sessions_v2 with agent_id=NULL.

        This mirrors the QA fixture state: the gateway creates a legacy
        ``sessions`` row without agent_id, the projection writes it to
        sessions_v2 with agent_id=NULL (projection.py:672 reads raw
        legacy.get("agent_id") which is None).
        """
        store = SqliteRuntimeStore(self.path)
        await store.start()
        import time as _t
        now = int(_t.time() * 1000)
        store.connection.execute(
            """INSERT INTO sessions_v2(id, tenant_id, owner_principal_id,
               visibility, session_type, kind, status,
               created_at_ms, updated_at_ms, last_activity_at_ms,
               agent_id, metadata_json)
            VALUES(?,?,?, ?,?,?,?, ?,?,?, ?,?)""",
            (session_id, "tenant-one", "owner",
             "private", "agent", "chat", "active",
             now, now, now,
             None, json.dumps({})),
        )
        store.connection.commit()
        await store.close()

    def _read_agent_id(self, session_id):
        with closing(sqlite3.connect(self.path)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT agent_id FROM sessions_v2 WHERE id=?", (session_id,)
            ).fetchone()
            return row["agent_id"] if row else "NO_ROW"

    async def test_fresh_session_gets_agent_id(self):
        """A brand-new session (not legacy) receives context.agent_id directly."""
        store = SqliteRuntimeStore(self.path)
        await store.start()
        ctx = self._coordinator_context("fresh-session")
        await store.accept(RunRequest("run-1", "fresh-session", "key-1", "Hello"), ctx)
        agent_id = self._read_agent_id("fresh-session")
        self.assertEqual("coordinator", agent_id)
        await store.close()

    async def test_legacy_session_claims_agent_id_on_first_acceptance(self):
        """Legacy-projected session (agent_id=NULL) is claimed by coordinator."""
        await self._seed_legacy_session("legacy-session")
        before = self._read_agent_id("legacy-session")
        self.assertIsNone(before, "Legacy projection should leave agent_id NULL")

        store = SqliteRuntimeStore(self.path)
        await store.start()
        ctx = self._coordinator_context("legacy-session")
        await store.accept(RunRequest("run-2", "legacy-session", "key-2", "Hi"), ctx)
        after = self._read_agent_id("legacy-session")
        self.assertEqual("coordinator", after,
                         "Runtime acceptance must claim agent_id from context")
        await store.close()

    async def test_tool_invocation_succeeds_after_legacy_acceptance(self):
        """After legacy acceptance, _require_local_session finds the session."""
        await self._seed_legacy_session("tool-session")
        store = SqliteRuntimeStore(self.path)
        await store.start()
        ctx = self._coordinator_context("tool-session")
        await store.accept(RunRequest("run-3", "tool-session", "key-3", "Test"), ctx)

        ref = SessionRef(
            authority=ctx.authority.authority,
            tenant_id=ctx.tenant_id,
            agent_id=ctx.agent_id,
            session_id="tool-session",
        )
        row = store._require_local_session(ref, ctx)
        self.assertIsNotNone(row)
        self.assertEqual("coordinator", row["agent_id"])
        await store.close()

    async def test_idempotent_re_acceptance_preserves_agent_id(self):
        """Re-accepting same session with same agent is idempotent."""
        await self._seed_legacy_session("idem-session")
        store = SqliteRuntimeStore(self.path)
        await store.start()
        ctx = self._coordinator_context("idem-session")
        await store.accept(RunRequest("run-4", "idem-session", "key-4", "First"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("idem-session"))

        await store.accept(RunRequest("run-5", "idem-session", "key-5", "Second"), ctx)
        self.assertEqual("coordinator", self._read_agent_id("idem-session"),
                         "Re-acceptance must not change agent_id")
        await store.close()

    async def test_cross_agent_acceptance_blocked(self):
        """A different agent cannot claim a session already owned by another."""
        await self._seed_legacy_session("guarded-session")
        store = SqliteRuntimeStore(self.path)
        await store.start()

        ctx_a = self._coordinator_context("guarded-session")
        await store.accept(RunRequest("run-6", "guarded-session", "key-6", "Claim"), ctx_a)
        self.assertEqual("coordinator", self._read_agent_id("guarded-session"))

        ctx_b = self._other_agent_context("guarded-session")
        with self.assertRaises(PermissionError) as cm:
            await store.accept(
                RunRequest("run-7", "guarded-session", "key-7", "Steal"), ctx_b
            )
        self.assertIn("another agent", str(cm.exception))
        self.assertEqual("coordinator", self._read_agent_id("guarded-session"),
                         "agent_id must not change after blocked cross-agent attempt")
        await store.close()


if __name__ == "__main__":
    unittest.main()
