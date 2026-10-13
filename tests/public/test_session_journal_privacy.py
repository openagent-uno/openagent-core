import json
import asyncio
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

    async def test_actual_stream_status_tracks_generated_text_and_fences_future_frames(self):
        from types import SimpleNamespace
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.events import OutToolStatus, OutTextDelta
        writer = SqliteSessionJournalContentPrivacy(self.store, self.ledger, self.publisher, export=self.export)
        self.runtime.services.registry.bind(SessionJournalPrivacyWriter, writer)
        stream = StreamSession(SimpleNamespace(memory_db=self.memory, config={}),
                               client_id='client', session_id='session', speak_enabled=False)
        observed = []
        stream.on_event = observed.append
        with execution_scope(self.runtime, self.context, 'run'):
            await stream._publish(OutToolStatus(session_id='session', text='PRIVATE GENERATED STATUS'))
        self.assertEqual((await stream.outbound.get()).text, 'PRIVATE GENERATED STATUS')
        self.assertIn('PRIVATE GENERATED STATUS', str(await self.memory.list_session_events('session')))
        request = PrivacyRequest('stream-redact', 'tenant', 'shopify/app', 'shop', 1, 'redact', ('customer:23',))
        await self.ledger.enqueue(request)
        await self._apply_targets(request, writer)
        self.assertNotIn('PRIVATE GENERATED STATUS', str(await self.memory.list_session_events('session')))
        with execution_scope(self.runtime, self.context, 'run'):
            for event in (OutToolStatus(session_id='session', text='LATE STATUS'), OutTextDelta(session_id='session', text='LATE DELTA')):
                with self.assertRaises(PrivacyFence):
                    await stream._publish(event)
        self.assertTrue(stream.outbound.empty())
        self.assertEqual(observed, [])  # Unattested observer copies are fenced.

    async def test_pending_and_inflight_cleanup_preserves_human_and_other_customer(self):
        from types import SimpleNamespace
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.events import OutTextDelta
        agent = SimpleNamespace(memory_db=self.memory, config={})
        first = StreamSession(agent, client_id='first', session_id='session', speak_enabled=False)
        second = StreamSession(agent, client_id='second', session_id='other-session', speak_enabled=False)
        other_context = replace(self.context, session_id='other-session')
        await self.store.accept(RunRequest('other-run', 'other-session', 'other-key', 'OTHER HUMAN'), other_context)
        await self.store.transition('other-run', 'running')
        await self.store.begin_tool('other-run', 'source', {'source_id':'shopify','name':'read'}, {})
        other_origin = replace(self.origin, subjects=('customer:24',))
        await self.publisher.publish_tool_result(other_context, 'other-run', 'source', {'private':'OTHER'}, (other_origin,))
        inflight = OutTextDelta(session_id='session', text='INFLIGHT CUSTOMER A')
        pending = OutTextDelta(session_id='session', text='PENDING CUSTOMER A')
        human = OutTextDelta(session_id='session', text='UNRELATED HUMAN')
        with execution_scope(self.runtime, self.context, 'run'):
            await first._publish(inflight)
            await first._publish(pending)
        await first._publish(human)
        with execution_scope(self.runtime, other_context, 'other-run'):
            await second._publish(OutTextDelta(session_id='other-session', text='CUSTOMER B'))
        self.assertIs(await first.outbound.get(), inflight)
        request = PrivacyRequest('queued', 'tenant', 'shopify/app', 'shop', 1, 'redact', ('customer:23',))
        await self.ledger.enqueue(request)
        with self.assertRaises(PrivacyFence):
            first.purge_outbound(request)
        with self.assertRaises(PrivacyFence):
            await first.authorize_outbound(inflight)
        first.release_outbound(inflight)
        self.assertEqual(first.purge_outbound(request), 1)
        self.assertIs(await first.outbound.get(), human)
        self.assertEqual(second.purge_outbound(request), 0)
        other_frame = await second.outbound.get()
        await second.authorize_outbound(other_frame)
        self.assertEqual(other_frame.text, 'CUSTOMER B')
        second.release_outbound(other_frame)

    async def test_mixed_pending_frame_blocks_customer_cleanup_without_deleting_sibling(self):
        from types import SimpleNamespace
        from openagent_core.content_privacy import PrivacyConflict
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.events import OutTextDelta
        await self.store.begin_tool('run', 'other', {'source_id':'shopify','name':'read'}, {})
        await self.publisher.publish_tool_result(self.context, 'run', 'other', {'private':'B'},
                                                (replace(self.origin, subjects=('customer:24',)),))
        stream = StreamSession(SimpleNamespace(memory_db=self.memory, config={}),
                               client_id='client', session_id='session', speak_enabled=False)
        frame = OutTextDelta(session_id='session', text='MIXED A AND B')
        with execution_scope(self.runtime, self.context, 'run'):
            await stream._publish(frame)
        request = PrivacyRequest('mixed', 'tenant', 'shopify/app', 'shop', 1, 'redact', ('customer:23',))
        await self.ledger.enqueue(request)
        with self.assertRaises(PrivacyConflict):
            stream.purge_outbound(request)
        self.assertEqual(stream.outbound.qsize(), 1)
        whole_shop = replace(request, request_id='whole-shop', subjects=())
        await self.ledger.enqueue(whole_shop)
        self.assertEqual(stream.purge_outbound(whole_shop), 1)
        self.assertTrue(stream.outbound.empty())

    async def test_realtime_owner_receives_protected_reply_but_unattested_cache_does_not(self):
        from types import SimpleNamespace
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.channel import RealtimeChannel
        from openagent_core.stream.events import OutTextDelta
        stream = StreamSession(SimpleNamespace(memory_db=self.memory, config={}),
                               client_id='client', session_id='session', speak_enabled=False)
        delivered, cache = [], []
        done = asyncio.Event()
        async def send(payload):
            delivered.append(payload)
            done.set()
            return True
        channel = RealtimeChannel(stream, send, encoder=lambda event, **kwargs: {'text':event.text},
                                  decoder=lambda frame: None, on_outbound=cache.append)
        with execution_scope(self.runtime, self.context, 'run'):
            await stream._publish(OutTextDelta(session_id='session', text='VISIBLE OWNER REPLY'))
        await channel.start()
        try:
            await asyncio.wait_for(done.wait(), 2)
            self.assertEqual(delivered, [{'text':'VISIBLE OWNER REPLY'}])
            self.assertEqual(cache, [])
            self.assertEqual(stream._privacy_frames, {})
        finally:
            await channel.close()

    async def test_stream_journal_does_not_swallow_a_privacy_failure(self):
        from types import SimpleNamespace
        from openagent_core.stream.session import StreamSession
        stream = StreamSession(SimpleNamespace(memory_db=self.memory, config={}),
                               client_id='client', session_id='session', speak_enabled=False)
        with execution_scope(self.runtime, self.context, 'run'):
            with self.assertRaises(PrivacyFence):
                await stream._journal('tool/status', {'result':'RAW PROVIDER DATA'})

    async def test_status_text_schema_does_not_accept_raw_result_or_unbounded_text(self):
        writer = SqliteSessionJournalContentPrivacy(self.store, self.ledger, self.publisher, export=self.export)
        self.runtime.services.registry.bind(SessionJournalPrivacyWriter, writer)
        with execution_scope(self.runtime, self.context, 'run'):
            for payload in ({'text':'status', 'result':'PRIVATE RAW'}, {'text':['RAW']}, {'text':'x'*4001}):
                with self.assertRaises(PrivacyFence):
                    await self.memory.append_session_event('session', 'tool/status', payload)
        self.assertEqual(await self.memory.list_session_events('session'), [])

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
