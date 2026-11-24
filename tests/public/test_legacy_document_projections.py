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

    async def test_host_scoped_transcript_fences_existing_index_and_fork(self):
        from sqlalchemy.dialects.sqlite import insert
        from sqlalchemy.ext.asyncio import create_async_engine
        from openagent_core.memory.host_scoped_sessions import host_scoped_sessions, initialize_host_scoped_sessions

        from openagent_core.administration import ManagementContext

        await self.turn(self.user, "orchid-marker")
        db = MemoryDB(str(self.rt.services.store.path))
        await db.connect()
        self.addAsyncCleanup(db.close)
        await sync_operational_search(db)
        acl = HistoryAccess.from_principal(self.owner)
        def indexed():
            return read_search_rows(operational_search_path(self.rt.services.store.path),
                fts_query=literal_fts_query("orchid"), scopes=("chats",), sort="relevance",
                tenant_id=self.user.tenant_id, principal_ids=acl.principal_ids)
        stale = indexed()
        self.assertTrue(stale)
        cache = TranscriptIndex(self.rt.services.store.path)
        self.addCleanup(cache.close)
        cache.sync()
        self.assertTrue(cache.search("orchid"))
        engine = create_async_engine("sqlite+aiosqlite:///" + str(db.db_path))
        self.addAsyncCleanup(engine.dispose)
        async with engine.begin() as db_writer:
            await initialize_host_scoped_sessions(db_writer)
            await db_writer.execute(insert(host_scoped_sessions).values(session_id=self.user.session_id))
        self.assertFalse(cache.search("orchid"))
        self.assertFalse(any(await search_rows_visible(await db._ensure_connected(), stale, acl)))
        await sync_operational_search(db)
        self.assertEqual(indexed(), [])
        inspection = SessionInspection(db, self.policy, runtime=self.rt)
        self.assertIsNotNone(await inspection.authorize(self.user, self.user.session_id))
        with self.assertRaises(PermissionError):
            await inspection.authorize(ManagementContext(self.owner, self.user.agent_id), self.user.session_id)
        with self.assertRaises(PermissionError):
            await inspection.fork_text(self.user, self.user.session_id, stale[0]["message_id"])

        # Generated bytes keep the originating run even if the delivery link is removed.
        import json
        from openagent_core.memory.artifact_document_history import record_origin, visible
        from openagent_core.runtime import execution_scope
        conn = await db._ensure_connected()
        row = await (await conn.execute("SELECT id FROM session_runs WHERE session_id=?", (self.user.session_id,))).fetchone()
        artifact = {"id": "generated", "tenant_id": self.user.tenant_id, "metadata_json": "{}"}
        await conn.execute("CREATE TEMP TABLE artifacts(id TEXT,tenant_id TEXT,metadata_json TEXT)")
        await conn.execute("INSERT INTO temp.artifacts VALUES(?,?,?)", tuple(artifact.values()))
        with execution_scope(self.rt, self.user, row[0]):
            await record_origin(conn, artifact)
        artifact["metadata_json"] = (await (await conn.execute("SELECT metadata_json FROM temp.artifacts")).fetchone())[0]
        self.assertEqual(json.loads(artifact["metadata_json"])["openagent_host_session_origins"], [[self.user.session_id, row[0]]])
        self.assertFalse(await visible(conn, artifact, None))
        async def own(sid):
            if sid != self.user.session_id:
                raise PermissionError
        self.assertTrue(await visible(conn, artifact, SessionProjectionGuard(own)))
