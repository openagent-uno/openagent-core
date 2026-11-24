"""SQLite admission, publication and passive indexes share the host's live scope boundary."""
from dataclasses import replace
import sqlite3
import tempfile
import unittest
from pathlib import Path

import aiosqlite
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core.memory.host_scoped_sessions import host_scoped_sessions, initialize_host_scoped_sessions
from openagent_core.memory.legacy_document_history import SessionProjectionGuard, visible_cache_hits, withheld_sessions
from openagent_core.memory.operational.access import resource_is_visible, resources_are_visible
from tests.public.test_operational_acl_batch import access, row


class HostScopedSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "sessions.db"
        self.engine = create_async_engine("sqlite+aiosqlite:///" + str(self.path))
        self.addAsyncCleanup(self.engine.dispose)
        self.reader = await aiosqlite.connect(self.path)
        self.addAsyncCleanup(self.reader.close)

    async def mark(self, *ids):
        async with self.engine.begin() as db:
            await initialize_host_scoped_sessions(db)
            if ids:
                await db.execute(insert(host_scoped_sessions).on_conflict_do_nothing(),
                                 [{"session_id": sid} for sid in ids])

    async def test_existing_cached_results_disappear_as_soon_as_authority_is_required(self):
        hits = [{"session_id": "private"}, {"session_id": "ordinary"}]
        open_source = lambda: sqlite3.connect(self.path)
        self.assertEqual(visible_cache_hits(open_source, hits), hits)
        await self.mark("private")
        await self.mark("private")
        self.assertEqual(visible_cache_hits(open_source, hits), hits[1:])
        source = open_source()
        self.addCleanup(source.close)
        self.assertEqual(withheld_sessions(source), {"private"})
        self.assertEqual(withheld_sessions(source, ["ordinary"]), set())
        # Registering a policy and a new session can fail together.
        with self.assertRaises(RuntimeError):
            async with self.engine.begin() as db:
                await db.execute(insert(host_scoped_sessions).values(session_id="uncommitted"))
                raise RuntimeError("Abort admission")
        self.assertEqual(withheld_sessions(source), {"private"})

    async def test_scalar_batch_admin_recheck_and_absent_viewer(self):
        await self.mark("private")
        allowed = {"private"}
        async def authorize(sid):
            if sid not in allowed:
                raise PermissionError
        guard = SessionProjectionGuard(authorize)
        actor = replace(access(), projection=guard)
        viewless = replace(actor, projection=None)
        rows = [row("private", resource_type="session", owner_principal_id="user:reader"),
                row("ordinary", resource_type="session", owner_principal_id="user:reader")]
        for permission in ("view", "search", "admin"):
            self.assertTrue(await resource_is_visible(self.reader, rows[0], actor, permission=permission))
            self.assertFalse(await resource_is_visible(self.reader, rows[0], viewless, permission=permission))
            self.assertEqual(await resources_are_visible(self.reader, rows, actor, permission=permission), [True, True])
            self.assertEqual(await resources_are_visible(self.reader, rows, viewless, permission=permission), [False, True])
        allowed.clear()
        with self.assertRaises(PermissionError):
            await guard.recheck()
        self.assertEqual(await resources_are_visible(self.reader, rows, actor), [False, True])
