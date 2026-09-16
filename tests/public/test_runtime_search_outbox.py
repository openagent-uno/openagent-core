"""Canonical runtime writes feed the existing rebuildable search corpus."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import sqlite3

from openagent_core import PrincipalRef, ExecutionContext, RunRequest
from openagent_core.memory_access import CanonicalHistorySearch, HistoryAccess
from openagent_storage_sqlite import SqliteRuntimeStore


class RuntimeSearchOutbox(unittest.IsolatedAsyncioTestCase):
    async def test_atomic_write_retry_restart_backfill_and_index_loss(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.memory.operational.search import operational_search_path
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
                await store.close()
                store = SqliteRuntimeStore(Path(directory)/'state.sqlite3')
                await store.start()
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


if __name__ == '__main__':
    unittest.main()
