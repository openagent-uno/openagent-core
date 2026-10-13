import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from openagent_core import PrincipalRef, ExecutionContext, RunRequest
from openagent_core.content_privacy import ContentOrigin, PrivacyRequest, PrivacyConflict
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink


class RuntimePrivacy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = SqliteRuntimeStore(self.root/'state.sqlite')
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        self.ledger = SqliteContentPrivacyStore(self.store.path, required_surfaces={'terminal-public-run'})
        self.exports = []
        async def export(request, revision, content):
            self.exports.append(content)
        self.sink = SqliteRuntimeContentPrivacySink(self.store, self.ledger, export=export)
        p = PrincipalRef('host','tenant','alice')
        self.context = ExecutionContext(p,p,p,'session','agent',(p,))
        self.origin = ContentOrigin('tenant','shopify','group',10,('customer-1',))
        self.request = PrivacyRequest('event','tenant','shopify','group',10,'redact')

    async def populate(self):
        await self.store.accept(RunRequest('run','session','key','UNRELATED HUMAN INPUT'),self.context)
        await self.store.transition('run','running')
        for call, source, data in (('other','gmail','UNRELATED GMAIL DATA'),('shop','shopify','PRIVATE CUSTOMER DATA')):
            await self.store.begin_tool('run',call,{'source_id':source,'name':'read'}, {})
            await self.store.finish_tool('run',call,result={'data':data})
        await self.store.append_event('run','run.stream',{'kind':'delta','text':'PRIVATE STREAM DELTA'})
        await self.store.append_event('run','run.stream',{'kind':'done','text':'PRIVATE STREAM FINAL'})
        await self.store.append_event('run','run.status',{'text':'PRIVATE GENERATED STATUS'})
        await self.store.transition('run','success',output='DERIVED PRIVATE SUMMARY')

    async def test_actual_runtime_read_and_replay_redacted_without_session_purge(self):
        await self.populate()
        revisions = await self.sink.capture('run',('shop',),(self.origin,))
        self.assertEqual(len(revisions),9)  # tool, tool message, assistant, tool event, terminal event, output
        await self.ledger.enqueue(self.request)
        result = await self.ledger.process('event',(self.sink,),covered_surfaces=frozenset({'terminal-public-run'}))
        self.assertEqual(result.status,'complete')
        self.assertIsNone((await self.store.get('run')).output)
        wire = str(await self.store.events('run'))
        self.assertNotIn('PRIVATE CUSTOMER DATA', wire)
        self.assertNotIn('DERIVED PRIVATE SUMMARY',wire)
        self.assertNotIn('PRIVATE STREAM',wire)
        self.assertNotIn('PRIVATE GENERATED STATUS',wire)
        messages = self.store.connection.execute('SELECT text FROM session_messages').fetchall()
        self.assertIn('UNRELATED HUMAN INPUT',[r[0] for r in messages])
        self.assertIn('{"data":"UNRELATED GMAIL DATA"}',[r[0] for r in messages])
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM sessions_v2').fetchone()[0],1)
        with self.assertRaisesRegex(Exception,'domain_events rows are immutable'):
            self.store.connection.execute("UPDATE domain_events SET metadata_json='{}'")
        self.assertNotIn(b'PRIVATE CUSTOMER DATA',self.store.path.read_bytes())
        wal = Path(str(self.store.path)+'-wal')
        self.assertTrue(not wal.exists() or b'PRIVATE CUSTOMER DATA' not in wal.read_bytes())
        await self.store.close()
        restored = SqliteRuntimeStore(self.store.path)
        await restored.start()
        self.addAsyncCleanup(restored.close)
        self.assertIsNone((await restored.get('run')).output)
        self.assertNotIn('PRIVATE CUSTOMER DATA',str(await restored.events('run')))

    async def test_capture_requires_terminal_exact_run_and_provider_invocation(self):
        await self.store.accept(RunRequest('run','session','key','human'),self.context)
        await self.store.transition('run','running')
        with self.assertRaises(PrivacyConflict):
            await self.sink.capture('run',('shop',),(self.origin,))
        await self.store.transition('run','success',output='done')
        with self.assertRaises(PrivacyConflict):
            await self.sink.capture('run',('missing',),(self.origin,))

    async def test_actual_replay_export_and_search_coverage_gate(self):
        await self.populate()
        await self.sink.capture('run',('shop',),(self.origin,))
        request = PrivacyRequest('export','tenant','shopify','group',10,'export')
        await self.ledger.enqueue(request)
        result = await self.ledger.process('export',(self.sink,),covered_surfaces=frozenset())
        self.assertEqual(result.reason,'coverage_incomplete')
        self.assertEqual(self.exports,[])
        result = await self.ledger.process('export',(self.sink,),covered_surfaces=frozenset({'terminal-public-run'}))
        self.assertEqual(result.status,'complete')
        self.assertIn('PRIVATE CUSTOMER DATA',str(self.exports))
        self.assertNotIn('UNRELATED HUMAN INPUT',str(self.exports))
        self.assertNotIn('UNRELATED GMAIL DATA',str(self.exports))
        self.assertEqual((await self.store.get('run')).output,'DERIVED PRIVATE SUMMARY')
