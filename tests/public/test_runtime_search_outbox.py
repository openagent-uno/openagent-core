"""Canonical runtime writes feed the existing rebuildable search corpus."""
from pathlib import Path
from tempfile import TemporaryDirectory
import asyncio
import unittest
import sqlite3
import threading
from unittest.mock import patch

from openagent_core import PrincipalRef, ExecutionContext, RunRequest
from openagent_core.memory_access import CanonicalHistorySearch, HistoryAccess
from openagent_storage_sqlite import SqliteRuntimeStore


class RuntimeSearchOutbox(unittest.IsolatedAsyncioTestCase):
    async def test_full_index_integrity_checks_leave_event_loop_responsive(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.memory.operational import search as search_module

        with TemporaryDirectory() as directory:
            store = SqliteRuntimeStore(Path(directory) / 'state.sqlite3')
            await store.start()
            db = MemoryDB(str(store.path))
            await db.connect()
            try:
                await search_module.sync_operational_search(db, limit=100)
                event_loop_thread = threading.get_ident()
                observed_threads: list[int] = []
                original = search_module._index_has_rowid_mismatch

                def observed_check(connection):
                    observed_threads.append(threading.get_ident())
                    return original(connection)

                async def heartbeat():
                    await asyncio.sleep(0)
                    return threading.get_ident()

                with patch.object(
                    search_module,
                    '_index_has_rowid_mismatch',
                    side_effect=observed_check,
                ):
                    status, heartbeat_thread = await asyncio.gather(
                        search_module.operational_search_status(db),
                        heartbeat(),
                    )
                    self.assertIn(status['state'], {'ready', 'warming'})
                    self.assertEqual(heartbeat_thread, event_loop_thread)
                    await search_module.sync_operational_search(db, limit=100)

                self.assertGreaterEqual(len(observed_threads), 3)
                self.assertTrue(
                    all(thread_id != event_loop_thread for thread_id in observed_threads)
                )
            finally:
                await db.close()
                await store.close()

    async def test_atomic_write_retry_restart_backfill_and_index_loss(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.memory.operational.search import (
            operational_search_path,
            operational_search_status,
            warm_operational_search,
        )
        with TemporaryDirectory() as directory:
            store = SqliteRuntimeStore(Path(directory)/'state.sqlite3')
            principal = PrincipalRef('host', 'tenant', 'alice')
            context = ExecutionContext(principal, principal, principal, 'session', 'agent', (principal,))
            await store.start()
            db = MemoryDB(str(store.path))
            await db.connect()
            try:
                request = RunRequest('run', 'session', 'run', 'orchid question')
                # A failing outbox write must roll back the accepted message and run.
                store.connection.execute("CREATE TRIGGER fail_search BEFORE INSERT ON search_outbox BEGIN SELECT RAISE(ABORT, 'fixture'); END")
                with self.assertRaises(sqlite3.IntegrityError):
                    await store.accept(request, context)
                self.assertIsNone(await store.get('run'))
                self.assertEqual(store.connection.execute('SELECT count(*) FROM session_messages').fetchone()[0], 0)
                store.connection.execute('DROP TRIGGER fail_search')
                await store.accept(request, context)
                count = store.connection.execute('SELECT count(*) FROM search_outbox').fetchone()[0]
                await store.accept(request, context)
                self.assertEqual(store.connection.execute('SELECT count(*) FROM search_outbox').fetchone()[0], count)
                await store.transition('run', 'running')
                await store.begin_tool('run', 'call', {'source_id':'fixed','name':'orchid_tool'}, {'secret':'NEVER_INDEX_ARGUMENT'})
                await store.finish_tool('run', 'call', result={'content':[{'type':'text','text':'NEVER_INDEX_RESULT'}]})
                await store.transition('run', 'success', output='orchid answer')
                async def access(principal, context):
                    return HistoryAccess.from_principal(principal)
                search = CanonicalHistorySearch(db, access)
                async def query(word, scopes):
                    return await search.search_history(context, query=word, scopes=scopes, limit=25, offset=0, session_id=None)
                result = await query('orchid', ['chats','tools'])
                self.assertEqual({hit['target']['kind'] for hit in result['hits']}, {'chat_message','chat_tool'})
                self.assertEqual(len([hit for hit in result['hits'] if hit['target']['kind']=='chat_message']), 2)
                for sentinel in ('NEVER_INDEX_ARGUMENT','NEVER_INDEX_RESULT'):
                    self.assertEqual((await query(sentinel, ['tools']))['hits'], [])
                # Emulate an early-beta database whose canonical rows had no intents.
                store.connection.execute('DELETE FROM search_outbox')
                store.connection.execute('DROP TABLE runtime_search_intent_repair')
                await store.close()
                store = SqliteRuntimeStore(Path(directory)/'state.sqlite3')
                await store.start()
                self.assertEqual(store.connection.execute('SELECT count(*) FROM search_outbox').fetchone()[0], 0)
                self.assertEqual((await operational_search_status(db))['state'], 'warming')
                during_repair = await query('orchid', ['chats','tools'])
                self.assertFalse(during_repair['index']['complete'])
                self.assertEqual(during_repair['hits'], [])
                await warm_operational_search(db)
                restored = store.connection.execute('SELECT count(*) FROM search_outbox').fetchone()[0]
                # The durable tool card is also a canonical message. Its outbox
                # intent must survive rebuild alongside the separate tool record.
                self.assertEqual(restored, 5)  # Session, three messages, one exact tool.
                await store.close(); await store.start()
                self.assertEqual(store.connection.execute('SELECT count(*) FROM search_outbox').fetchone()[0], restored)
                # Only derived files are removed; canonical history remains intact.
                index = operational_search_path(str(store.path))
                for suffix in ('','-wal','-shm'):
                    Path(str(index)+suffix).unlink(missing_ok=True)
                self.assertEqual(len((await query('orchid', ['chats','tools']))['hits']), 3)
                self.assertEqual(store.connection.execute('SELECT count(*) FROM session_messages').fetchone()[0], 3)
                anchor = store.connection.execute(
                    "SELECT tool_call_id, status FROM session_messages WHERE role='tool'"
                ).fetchall()
                self.assertEqual([tuple(row) for row in anchor], [('call', 'complete')])
            finally:
                await db.close()
                await store.close()

    async def test_repair_resumes_between_pages_and_never_blocks_on_a_live_writer(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.memory.operational import search as search_module, search_repair
        from openagent_core.memory.operational.search import sync_operational_search, warm_operational_search

        with TemporaryDirectory() as directory:
            store = SqliteRuntimeStore(Path(directory) / 'history.db')
            principal = PrincipalRef('host', 'tenant', 'alice')
            context = ExecutionContext(principal, principal, principal, 'earlier', 'agent', (principal,))
            await store.start()
            db = MemoryDB(str(store.path))
            await db.connect()
            try:
                for number in range(3):
                    run = f'past-{number}'
                    await store.accept(RunRequest(run, 'earlier', run, 'historic receipt'), context)
                    await store.transition(run, 'running')
                    await store.transition(run, 'success', output='historic response')
                old_message = store.connection.execute(
                    "SELECT id FROM session_messages ORDER BY id LIMIT 1"
                ).fetchone()[0]
                store.connection.execute('DELETE FROM search_outbox')
                store.connection.execute("""INSERT INTO search_outbox
                  (tenant_id,source_kind,source_id,operation,source_version,acl_version,committed_at_ms)
                  VALUES('tenant','message',?,'delete',99,99,1)""", (old_message,))
                store.connection.execute('DROP TABLE runtime_search_intent_repair')
                await store.close()
                await store.start()
                def cursor():
                    return store.connection.execute(
                        "SELECT after_id FROM runtime_search_intent_repair WHERE source_kind='message'"
                    ).fetchone()[0]
                with patch.object(search_repair, '_BATCH_SIZE', 2):
                    held_writer = sqlite3.connect(store.path, isolation_level=None)
                    try:
                        held_writer.execute('BEGIN IMMEDIATE')
                        status = await asyncio.wait_for(sync_operational_search(db), timeout=1)
                        self.assertEqual(status.state, 'warming')
                        self.assertIsNone(cursor())
                    finally:
                        held_writer.close()
                    self.assertEqual((await sync_operational_search(db)).state, 'warming')
                    committed_cursor = cursor()
                    self.assertIsNotNone(committed_cursor)
                    await store.close()
                    await store.start()
                    self.assertEqual(cursor(), committed_cursor)
                    current = RunRequest('current', 'earlier', 'current', 'interactive input')
                    accepted, new = await asyncio.wait_for(store.accept(current, context), timeout=1)
                    self.assertTrue(new)
                    self.assertEqual(accepted.status, 'queued')
                    self.assertTrue((await warm_operational_search(db)).ready)
                self.assertFalse(await search_repair.runtime_search_repair_pending(await db._ensure_connected()))
                current_rows = store.connection.execute(
                    "SELECT count(*) FROM session_messages WHERE run_id='current'"
                ).fetchone()[0]
                self.assertEqual(current_rows, 1)
                self.assertIsNone(store.connection.execute(
                    "SELECT 1 FROM search_outbox WHERE source_id=? AND operation='upsert'", (old_message,)
                ).fetchone())
                with patch.object(search_module, 'advance_runtime_search_repair', side_effect=AssertionError('repeated repair')):
                    await store.close()
                    await store.start()
                    self.assertTrue((await sync_operational_search(db)).ready)
            finally:
                await db.close()
                await store.close()


if __name__ == '__main__':
    unittest.main()
