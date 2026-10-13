import json
from pathlib import Path
import tempfile
import unittest
from dataclasses import replace

from openagent_core import Runtime,RuntimeSettings,RuntimeServices,PrincipalRef,ExecutionContext,RunRequest
from openagent_core.content_privacy import ContentOrigin,ContentFragment,ContentRevision,ToolResultProvenance,PrivacyRequest,PrivacyFence,SessionJournalPrivacyWriter,ToolResultPrivacyPublisher
from openagent_core.runtime import execution_scope
from openagent_core.memory.db import MemoryDB
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
from openagent_storage_sqlite.runtime_privacy import SqliteToolResultPrivacyPublisher
from openagent_storage_sqlite.journal_privacy import SqliteSessionJournalContentPrivacy


class JournalPrivacy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        p=PrincipalRef('host','tenant','alice');self.context=ExecutionContext(p,p,p,'session','agent',(p,))
        class Allow:
            async def authorize(self,*args,**kwargs): return True
        self.store=SqliteRuntimeStore(self.root/'state.sqlite')
        self.runtime=Runtime(RuntimeSettings('agent',self.root),RuntimeServices(self.store,None,Allow()))
        await self.runtime.start();self.addAsyncCleanup(self.runtime.close)
        self.memory=MemoryDB(str(self.store.path));await self.memory.connect();self.addAsyncCleanup(self.memory.close)
        self.ledger=SqliteContentPrivacyStore(self.store.path)
        self.publisher=SqliteToolResultPrivacyPublisher(self.store,self.ledger)
        self.runtime.services.registry.bind(ToolResultPrivacyPublisher,self.publisher)
        self.origin=ContentOrigin('tenant','shopify/app','shop',1,('customer:23',))
        await self.store.accept(RunRequest('run','session','key','ORIGINAL HUMAN'),self.context)
        await self.store.transition('run','running')
        await self.store.begin_tool('run','source',{'source_id':'shopify','name':'read'}, {})
        await self.publisher.publish_tool_result(self.context,'run','source',{'private':'SOURCE'},(self.origin,))
        self.exports=[]
        async def export(request,revision,data):self.exports.append(data)
        self.export=export

    async def test_actual_memorydb_writer_is_fenced_without_adapter_then_tracks_generated_journal(self):
        await self.memory.append_session_event('session','user/message',{'text':'ORIGINAL HUMAN'})
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):
                await self.memory.append_session_event('session','assistant/message',{'text':'NEVER PERSIST'})
        writer=SqliteSessionJournalContentPrivacy(self.store,self.ledger,self.publisher,export=self.export)
        self.runtime.services.registry.bind(SessionJournalPrivacyWriter,writer)
        with execution_scope(self.runtime,self.context,'run'):
            seq=await self.memory.append_session_event('session','assistant/message',{'text':'PRIVATE GENERATED'})
            with self.assertRaises(PrivacyFence):
                await self.memory.append_session_event('session','tool/status',{'result':'RAW NEEDS PARTITION'})
        self.assertEqual(seq,2)
        self.assertNotIn('RAW NEEDS PARTITION',str(await self.memory.list_session_events('session')))
        request=PrivacyRequest('event','tenant','shopify/app','shop',1,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        await self._apply_targets(request,writer)
        events=str(await self.memory.list_session_events('session'))
        self.assertIn('ORIGINAL HUMAN',events)
        self.assertNotIn('PRIVATE GENERATED',events)
        self.assertNotIn('NEVER PERSIST',events)
        self.assertNotIn(b'PRIVATE GENERATED',self.store.path.read_bytes())
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):
                await self.memory.append_session_event('session','assistant/message',{'text':'LATE'})

    async def _apply_targets(self,request,writer):
        rows=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision
            WHERE t.request_id=? AND r.sink='session-journal' ''',(request.request_id,)).fetchall()
        self.assertTrue(rows)
        for row in rows:
            revision=ContentRevision(row['tenant'],row['artifact'],row['revision'],row['sink'],row['digest'])
            self.assertEqual(await writer.apply(request,revision),'complete')

    async def test_trusted_raw_partition_exports_only_customer_and_retains_sibling(self):
        second=replace(self.origin,subjects=('customer:24',))
        async def provenance(event_type,payload,context,run_id):
            return ToolResultProvenance((ContentFragment(('results',0),(self.origin,)),
                                         ContentFragment(('results',1),(second,)),ContentFragment(('model_origin',),())))
        writer=SqliteSessionJournalContentPrivacy(self.store,self.ledger,self.publisher,export=self.export,provenance=provenance)
        self.runtime.services.registry.bind(SessionJournalPrivacyWriter,writer)
        with execution_scope(self.runtime,self.context,'run'):
            await self.memory.append_session_event('session','tool/status',{'results':['FIRST PRIVATE','SECOND RETAIN'],
                                                                         'model_origin':{'tenant':'forged'}})
        request=PrivacyRequest('export','tenant','shopify/app','shop',1,'export',('customer:23',))
        await self.ledger.enqueue(request);await self._apply_targets(request,writer)
        self.assertIn('FIRST PRIVATE',str(self.exports));self.assertNotIn('SECOND RETAIN',str(self.exports))
        request=replace(request,request_id='redact',operation='redact')
        await self.ledger.enqueue(request);await self._apply_targets(request,writer)
        events=str(await self.memory.list_session_events('session'))
        self.assertNotIn('FIRST PRIVATE',events);self.assertIn('SECOND RETAIN',events)
        self.assertIn('forged',events)  # Payload labels grant no tenant/origin privilege.

    async def test_revocation_during_schema_resolution_rolls_back_journal_publication(self):
        request=PrivacyRequest('revoked','tenant','shopify/app','shop',1,'redact')
        async def provenance(*args):
            await self.ledger.enqueue(request)
            return ToolResultProvenance((ContentFragment((),(self.origin,)),))
        writer=SqliteSessionJournalContentPrivacy(self.store,self.ledger,self.publisher,export=self.export,provenance=provenance)
        self.runtime.services.registry.bind(SessionJournalPrivacyWriter,writer)
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):
                await self.memory.append_session_event('session','assistant/message',{'text':'REVOKED NEVER PERSIST'})
        self.assertEqual(await self.memory.list_session_events('session'),[])
