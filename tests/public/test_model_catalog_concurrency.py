"""Catalog management must coexist with the runtime's independent WAL writer."""
import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from openagent_core.memory.db import MemoryDB


class ModelCatalogConcurrency(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'agent.db'
        self.db = MemoryDB(str(self.path))
        await self.db.connect()
        self.addCleanup(self.tmp.cleanup)
        self.addAsyncCleanup(self.db.close)
        self.pid = await self.db.upsert_provider(name='proxy', framework='api-based')
        self.mid = await self.db.upsert_model(provider_id=self.pid, model='existing')

    async def stale_reader(self):
        conn = await self.db._ensure_connected()
        cursor = await conn.execute('SELECT name FROM sqlite_master')
        await cursor.fetchone()
        self.addAsyncCleanup(cursor.close)
        with sqlite3.connect(self.path) as writer:
            writer.execute('UPDATE providers SET updated_at=updated_at+1')

    async def test_add_model_after_runtime_commit_with_live_catalog_reader(self):
        await self.stale_reader()
        model_id = await self.db.upsert_model(provider_id=self.pid, model='new')
        with sqlite3.connect(self.path) as reader:
            self.assertEqual('new', reader.execute('SELECT model FROM models WHERE id=?', (model_id,)).fetchone()[0])

    async def test_catalog_mutations_do_not_promote_stale_shared_snapshot(self):
        other = await self.db.upsert_provider(name='temporary', framework='api-based')
        await self.stale_reader()
        await self.db.set_model_enabled(self.mid, False)
        await self.db.set_model_is_classifier(self.mid, True)
        await self.db.set_provider_enabled(self.pid, False)
        await self.db.upsert_provider(name='added', framework='api-based')
        await self.db.delete_provider(other)
        await self.db.delete_model(self.mid)
        with sqlite3.connect(self.path) as reader:
            self.assertEqual(0, reader.execute('SELECT enabled FROM providers WHERE id=?', (self.pid,)).fetchone()[0])
            self.assertEqual(0, reader.execute('SELECT count(*) FROM models WHERE id=?', (self.mid,)).fetchone()[0])

    async def test_parallel_mcp_additions_and_failed_write_leave_catalog_usable(self):
        from openagent_core.mcp.servers.model_manager import server
        with patch.object(server, '_db', self.db):
            results = await asyncio.gather(*(server.add_model(provider_id=self.pid, model=f'model-{n}') for n in range(8)))
            self.assertEqual(8, len({r['id'] for r in results}))
            with self.assertRaises(ValueError):
                await self.db.upsert_model(provider_id=99999, model='orphan')
            await server.update_model(model_id=self.mid, display_name='Updated')
        with sqlite3.connect(self.path) as reader:
            self.assertEqual('Updated', reader.execute('SELECT display_name FROM models WHERE id=?', (self.mid,)).fetchone()[0])

    async def test_parallel_first_mcp_calls_wait_for_complete_initialization(self):
        from openagent_core.mcp.servers.model_manager import server
        entered = asyncio.Event()
        release = asyncio.Event()
        class Candidate:
            ready = False
            async def connect(self):
                entered.set()
                await release.wait()
                self.ready = True
            async def close(self):
                pass
        with patch.object(server, '_db', None), patch.object(server, '_db_lock', asyncio.Lock()):
            # MemoryDB's constructor accepts a path.
            with patch.object(server, 'MemoryDB', lambda path: Candidate()):
                first = asyncio.create_task(server._get_db())
                await entered.wait()
                second = asyncio.create_task(server._get_db())
                await asyncio.sleep(0)
                self.assertFalse(second.done())
                release.set()
                one, two = await asyncio.gather(first, second)
                self.assertIs(one, two)
                self.assertTrue(one.ready)
