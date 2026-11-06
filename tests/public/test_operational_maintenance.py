"""Idle maintenance remains bounded while durable writes and repairs progress."""
import asyncio
from pathlib import Path
import sqlite3
import threading
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from openagent_core import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.memory.db import MemoryDB
from openagent_core.memory.operational import maintenance, repository, search
from openagent_storage_sqlite import SqliteRuntimeStore


class OperationalMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = SqliteRuntimeStore(Path(self.directory.name) / 'state.sqlite3')
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        self.db = MemoryDB(str(self.store.path))
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.updates = []
        self.task = None
        self.addAsyncCleanup(self.stop_worker)

    async def stop_worker(self):
        if self.task is not None:
            self.task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await self.task
            self.task = None

    async def wait_for(self, predicate):
        async def wait():
            while not predicate():
                await asyncio.sleep(.005)
        await asyncio.wait_for(wait(), 10)

    async def start_worker(self, *, audit_interval=3600):
        self.task = asyncio.create_task(maintenance.maintain_operational_storage(
            self.db, self.updates.append, poll_interval=.005, audit_interval=audit_interval))
        await self.wait_for(lambda: self.updates and self.updates[-1].history_ready and self.updates[-1].search.get('ready'))

    async def write(self):
        actor = PrincipalRef('host', 'tenant', 'alice')
        context = ExecutionContext(actor, actor, actor, 'session', 'agent', (actor,))
        await self.store.accept(RunRequest('run', 'session', 'key', 'orchid'), context)
        await self.store.transition('run', 'success', output='orchid complete')
        return self.store.connection.execute('SELECT MAX(seq) FROM search_outbox').fetchone()[0]

    async def test_unchanged_polls_do_not_rescan_history_or_fts(self):
        with patch.object(repository, 'projection_coverage_async', wraps=repository.projection_coverage_async) as coverage, \
                patch.object(search, '_index_has_rowid_mismatch', wraps=search._index_has_rowid_mismatch) as integrity:
            await self.start_worker()
            checks = (coverage.call_count, integrity.call_count)
            count = len(self.updates)
            await self.wait_for(lambda: len(self.updates) >= count + 8)
            self.assertEqual((coverage.call_count, integrity.call_count), checks)

    async def test_new_durable_writes_are_indexed_without_an_on_demand_search(self):
        await self.start_worker()
        seq = await self.write()
        await self.wait_for(lambda: self.updates[-1].search.get('seq', 0) >= seq)
        with sqlite3.connect(search.operational_search_path(self.store.path)) as index:
            self.assertGreater(index.execute("SELECT COUNT(*) FROM search_fts WHERE search_fts MATCH 'orchid'").fetchone()[0], 0)
        self.assertEqual((await self.store.get('run')).status, 'success')
        self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM session_messages').fetchone()[0], 2)

    async def test_lost_index_is_rebuilt_while_canonical_history_is_preserved(self):
        await self.start_worker()
        seq = await self.write()
        await self.wait_for(lambda: self.updates[-1].search.get('seq', 0) >= seq)
        generation = self.updates[-1].search['generation']
        path = search.operational_search_path(self.store.path)
        for suffix in ('', '-wal', '-shm'):
            Path(str(path) + suffix).unlink(missing_ok=True)
        await self.wait_for(lambda: self.updates[-1].search.get('generation') != generation and self.updates[-1].search.get('ready'))
        self.assertEqual((await self.store.get('run')).status, 'success')
        self.assertEqual(self.store.connection.execute('SELECT COUNT(*) FROM session_messages').fetchone()[0], 2)

    async def test_periodic_audit_remains_enabled_without_new_writes(self):
        with patch.object(search, '_index_has_rowid_mismatch', wraps=search._index_has_rowid_mismatch) as integrity:
            await self.start_worker(audit_interval=.05)
            count = integrity.call_count
            await self.wait_for(lambda: integrity.call_count > count)

    async def test_incomplete_unchanged_history_stays_unready_without_a_busy_loop(self):
        async def incomplete(conn):
            return {'pending_sessions': 0, 'complete': False}
        with patch.object(repository, 'projection_coverage_async', side_effect=incomplete) as coverage:
            self.task = asyncio.create_task(maintenance.maintain_operational_storage(
                self.db, self.updates.append, poll_interval=.005, audit_interval=3600))
            await self.wait_for(lambda: self.updates and self.updates[-1].search.get('ready'))
            count = coverage.call_count
            updates = len(self.updates)
            await self.wait_for(lambda: len(self.updates) >= updates + 5)
            self.assertEqual(coverage.call_count, count)
            self.assertTrue(all(not status.history_ready for status in self.updates))

    async def test_recoverable_error_is_redacted_and_retried(self):
        original = repository.reconcile_pending_async
        calls = 0
        async def transient(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError('PRIVATE_SENTINEL')
            return await original(*args, **kwargs)
        with patch.object(repository, 'reconcile_pending_async', side_effect=transient), \
                self.assertLogs(maintenance.logger, level='ERROR') as logs:
            await self.start_worker()
        self.assertTrue(any(update.failed for update in self.updates))
        self.assertFalse(self.updates[-1].failed)
        self.assertNotIn('PRIVATE_SENTINEL', '\n'.join(logs.output))

    async def test_cancellation_during_connection_open_drains_and_closes_its_worker(self):
        import aiosqlite
        original = aiosqlite.connect
        started = threading.Event()
        release = threading.Event()
        connections = []
        def delayed(*args, **kwargs):
            conn = original(*args, **kwargs)
            connector = conn._connector
            def open_connection():
                started.set()
                assert release.wait(5)
                return connector()
            conn._connector = open_connection
            connections.append(conn)
            return conn
        with patch.object(aiosqlite, 'connect', side_effect=delayed):
            task = asyncio.create_task(maintenance.maintain_operational_storage(self.db, self.updates.append))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 5))
                task.cancel()
                await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        conn = connections[0]
        worker = getattr(conn, '_thread', conn)
        await asyncio.to_thread(worker.join, 1)
        self.assertFalse(worker.is_alive())
        self.assertIsNone(conn._connection)

    async def test_initial_connection_failure_is_redacted_and_recovered(self):
        import aiosqlite
        original = aiosqlite.connect
        calls = 0
        def transient(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError('PRIVATE_CONNECT_SENTINEL')
            return original(*args, **kwargs)
        with patch.object(aiosqlite, 'connect', side_effect=transient), \
                self.assertLogs(maintenance.logger, level='ERROR') as logs:
            await self.start_worker()
        self.assertTrue(any(update.failed for update in self.updates))
        self.assertFalse(self.updates[-1].failed)
        self.assertNotIn('PRIVATE_CONNECT_SENTINEL', '\n'.join(logs.output))


if __name__ == '__main__':
    unittest.main()
