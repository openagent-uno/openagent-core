"""An already populated standalone index must not bypass later document revocation."""
import unittest

import test_document_run_lineage as lineage
from openagent_core.contracts import ResourceRef
from openagent_core.memory.db import MemoryDB
from openagent_core.memory.legacy_document_history import SessionProjectionGuard
from openagent_core.memory.operational.search import (
    literal_fts_query, operational_search_path, read_search_rows, sync_operational_search,
)
from openagent_core.memory.operational.service import search_rows_visible
from openagent_core.memory.transcript_index import TranscriptIndex
from openagent_core.memory_access import HistoryAccess
from openagent_core.workspace_administration import SessionInspection


class LegacyDocumentProjections(unittest.IsolatedAsyncioTestCase):
    backend = "sqlite"
    asyncSetUp = lineage.DocumentRunLineageTests.asyncSetUp
    new_runtime = lineage.DocumentRunLineageTests.new_runtime
    turn = lineage.DocumentRunLineageTests.turn

    async def test_existing_index_and_plain_fork_are_fenced_at_first_note_use(self):
        await self.turn(self.user, "violet-marker")
        store = self.rt.services.store
        db = MemoryDB(str(store.path))
        await db.connect()
        self.addAsyncCleanup(db.close)
        conn = await db._ensure_connected()
        await sync_operational_search(db)
        acl = HistoryAccess.from_principal(self.owner)
        def rows():
            return read_search_rows(operational_search_path(store.path),
                fts_query=literal_fts_query("violet"), scopes=("chats",), sort="relevance",
                tenant_id=self.user.tenant_id, principal_ids=acl.principal_ids)
        old = rows()
        self.assertTrue(old)
        self.assertTrue(all(await search_rows_visible(conn, old, acl)))
        cache = TranscriptIndex(store.path)
        self.addCleanup(cache.close)
        cache.sync()
        self.assertTrue(cache.search("violet"))

        await self.turn(self.user, "read")
        inspection = SessionInspection(db, self.policy, runtime=self.rt)
        self.assertIsNotNone(await inspection.authorize(self.user, self.user.session_id))
        with self.assertRaises(PermissionError):
            await SessionInspection(db, self.policy).authorize(self.user, self.user.session_id)
        with self.assertRaises(PermissionError):
            await inspection.fork_text(self.user, self.user.session_id, old[0]["message_id"])
        self.assertFalse(any(await search_rows_visible(conn, old, acl)))
        self.assertEqual(cache.search("violet"), [])
        await sync_operational_search(db)
        self.assertEqual(rows(), [])
        cache.sync()
        self.assertEqual(cache.stats()["sessions"], 0)
        # A viewer who read the page before the forget gets rechecked at publication.
        async def authorize(sid):
            await self.rt.authorize(self.user, "session.read", ResourceRef("session", self.user.tenant_id, sid))
        page = SessionProjectionGuard(authorize)
        ids = {self.user.session_id}
        self.assertEqual(await page.visible(conn, ids), ids)
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await page.recheck()
        with self.assertRaises(PermissionError):
            await inspection.authorize(self.user, self.user.session_id)


    async def test_management_viewer_binds_the_verified_actor_and_session(self):
        from contextlib import asynccontextmanager
        from dataclasses import replace
        from openagent_core.administration import ManagementContext

        await self.turn(self.user, "read")
        db = MemoryDB(str(self.rt.services.store.path))
        await db.connect()
        self.addAsyncCleanup(db.close)
        management = ManagementContext(self.owner, self.user.agent_id)
        resolved = self.user

        @asynccontextmanager
        async def resolve(context, session_id):
            self.assertEqual(context, management)
            self.assertEqual(session_id, self.user.session_id)
            yield resolved

        inspection = SessionInspection(db, self.policy, runtime=self.rt, document_viewer=resolve)
        self.assertIsNotNone(await inspection.authorize(management, self.user.session_id))
        for resolved in (replace(self.user, session_id="different"), replace(self.user, agent_id="different"),
                         replace(self.user, initiator=replace(self.owner, subject_id="different"))):
            with self.assertRaises(PermissionError):
                await inspection.authorize(management, self.user.session_id)
        resolved = self.user
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await inspection.authorize(management, self.user.session_id)
