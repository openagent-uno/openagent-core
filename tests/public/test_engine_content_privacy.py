import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from openagent_core import Runtime, RuntimeSettings, RuntimeServices, PrincipalRef, ExecutionContext, RunRequest
from openagent_core.content_privacy import ContentOrigin, PrivacyRequest, PrivacyFence, EngineContentPrivacyWriter
from openagent_core.runtime import execution_scope
from openagent_core.memory.store.sqlite.sqlite import SqliteDb
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.response import ToolExecution
from openagent_core.core._run_state.agent import RunOutput
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
from openagent_storage_sqlite.runtime_privacy import SqliteToolResultPrivacyPublisher
from openagent_storage_sqlite.engine_privacy import SqliteEngineContentPrivacy


class EnginePrivacy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        principal=PrincipalRef('host','tenant','alice')
        self.context=ExecutionContext(principal,principal,principal,'session','agent',(principal,))
        class Allow:
            async def authorize(self,*args,**kwargs): return True
        self.store=SqliteRuntimeStore(self.root/'state.sqlite')
        self.runtime=Runtime(RuntimeSettings('agent',self.root),RuntimeServices(self.store,None,Allow()))
        await self.runtime.start()
        self.addAsyncCleanup(self.runtime.close)
        self.ledger=SqliteContentPrivacyStore(self.store.path)
        self.publisher=SqliteToolResultPrivacyPublisher(self.store,self.ledger)
        async def export(*args): pass
        self.sink=SqliteEngineContentPrivacy(self.store,self.ledger,self.publisher,export=export)
        self.runtime.services.registry.bind(EngineContentPrivacyWriter,self.sink)
        self.db=SqliteDb(db_file=str(self.store.path))
        self.origin=ContentOrigin('tenant','shopify/app','group',10,('customer:23',))
        await self.store.accept(RunRequest('run','session','key','HUMAN INPUT'),self.context)
        await self.store.transition('run','running')
        await self.store.begin_tool('run','catalog-call',{'source_id':'shopify','name':'read','provider_call_id':'model-shop-call'}, {})
        await self.publisher.publish_tool_result(self.context,'run','catalog-call',{'private':'PRIVATE SHOP RESULT'},(self.origin,))
        run=RunOutput(run_id='run',agent_id='agent',session_id='session',content='PRIVATE DERIVED FINAL',
            messages=[Message(role='user',content='HUMAN INPUT'),
                      Message(role='tool',tool_call_id='model-shop-call',content='PRIVATE SHOP RESULT'),
                      Message(role='tool',tool_call_id='model-mail-call',content='UNRELATED MAIL RESULT'),
                      Message(role='assistant',content='PRIVATE DERIVED FINAL')],
            tools=[ToolExecution(tool_call_id='model-shop-call',tool_name='read',tool_args={'id':23},result='PRIVATE SHOP RESULT'),
                   ToolExecution(tool_call_id='model-mail-call',tool_name='mail',tool_args={},result='UNRELATED MAIL RESULT')])
        self.session=AgentSession(session_id='session',agent_id='agent',user_id=principal.key,runs=[run],created_at=1,updated_at=1)

    async def test_actual_engine_upsert_read_and_selective_redaction_preserve_human_and_other_provider(self):
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        source=self.db.get_session('session',deserialize=False)
        self.assertIn('PRIVATE SHOP RESULT',str(source))
        nodes=self.store.connection.execute("SELECT * FROM privacy_revisions WHERE sink='engine-history'").fetchall()
        self.assertEqual(len(nodes),5)
        await self.store.transition('run','success',output='PRIVATE DERIVED FINAL')
        request=PrivacyRequest('event','tenant','shopify/app','group',10,'redact')
        await self.ledger.enqueue(request)
        from openagent_core.content_privacy import ContentRevision
        for node in nodes:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await self.sink.apply(request,revision),'complete')
        source=self.db.get_session('session',deserialize=False)
        self.assertNotIn('PRIVATE SHOP RESULT',str(source))
        self.assertNotIn('PRIVATE DERIVED FINAL',str(source))
        self.assertIn('HUMAN INPUT',str(source))
        self.assertIn('UNRELATED MAIL RESULT',str(source))
        compatibility=self.store.connection.execute('SELECT raw_envelope_json FROM session_runs WHERE id=?',('run',)).fetchone()[0]
        self.assertNotIn('PRIVATE SHOP RESULT',compatibility)
        self.assertIn('UNRELATED MAIL RESULT',compatibility)
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):
                self.db.upsert_session(self.session)
        self.assertNotIn('PRIVATE SHOP RESULT',str(self.db.get_session('session',deserialize=False)))

    async def test_different_customer_raw_tool_results_keep_exact_origins(self):
        from dataclasses import replace
        await self.store.begin_tool('run','second-catalog',{'source_id':'shopify','name':'read','provider_call_id':'model-second-shop'}, {'query':'PRIVATE SHOP RESULT'})
        second=replace(self.origin,subjects=('customer:24',))
        await self.publisher.publish_tool_result(self.context,'run','second-catalog',{'private':'CUSTOMER TWO RESULT'},(second,))
        run=self.session.runs[0]
        run.messages.insert(2,Message(role='tool',tool_call_id='model-second-shop',content='CUSTOMER TWO RESULT'))
        run.tools.append(ToolExecution(tool_call_id='model-second-shop',tool_name='read',tool_args={'id':24,'query':'PRIVATE SHOP RESULT'},result='CUSTOMER TWO RESULT'))
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        request=PrivacyRequest('customer-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        from openagent_core.content_privacy import ContentRevision
        targets=self.store.connection.execute("SELECT r.* FROM privacy_targets t JOIN privacy_revisions r ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=? AND r.sink='engine-history'",('customer-event',)).fetchall()
        self.assertEqual(len(targets),6)  # Later model query arguments inherit earlier protected context too.
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await self.sink.apply(request,revision),'complete')
        source=self.db.get_session('session',deserialize=False)
        self.assertNotIn('PRIVATE SHOP RESULT',str(source))
        self.assertIn('CUSTOMER TWO RESULT',str(source))
        self.assertIn('UNRELATED MAIL RESULT',str(source))
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        async def export(*args):self.fail('unexpected export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        public_targets=self.store.connection.execute("SELECT r.* FROM privacy_targets t JOIN privacy_revisions r ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=? AND r.sink='runtime-ledger'",('customer-event',)).fetchall()
        for node in public_targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await public.apply(request,revision),'complete')
        raw=self.store.connection.execute('SELECT args_json,result_json,raw_envelope_json FROM tool_invocations WHERE tool_call_id=?',('second-catalog',)).fetchone()
        self.assertNotIn('PRIVATE SHOP RESULT',str(tuple(raw)))
        self.assertIn('CUSTOMER TWO RESULT',str(tuple(raw)))

    async def test_completed_redaction_allows_new_turn_but_rejects_stale_cached_history(self):
        from openagent_core.content_privacy import PrivacyConflict
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        await self.store.transition('run','success',output='PRIVATE DERIVED FINAL')
        request=PrivacyRequest('continue-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        async def export(*args): self.fail('unexpected export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        progress=await self.ledger.process(request.request_id,(public,self.sink),
                                          covered_surfaces=self.ledger.required_surfaces)
        self.assertEqual(progress.status,'complete')
        await self.store.accept(RunRequest('new-run','session','new-key','NEW HUMAN QUESTION'),self.context)
        await self.store.transition('new-run','running')
        self.assertEqual(await self.publisher.origins_for_execution(self.context,'new-run'),())
        fresh=self.db.get_session('session')
        fresh.runs.append(RunOutput(run_id='new-run',agent_id='agent',session_id='session',
                                   content='NEW UNRELATED ANSWER',messages=[Message(role='user',content='NEW HUMAN QUESTION')]))
        with execution_scope(self.runtime,self.context,'new-run'):
            self.db.upsert_session(fresh)
        stored=str(self.db.get_session('session',deserialize=False))
        self.assertIn('NEW UNRELATED ANSWER',stored)
        self.assertIn('UNRELATED MAIL RESULT',stored)
        self.assertNotIn('PRIVATE SHOP RESULT',stored)
        # The old in-memory session is not allowed to overwrite the clean
        # historical run, even when the new run itself is not tainted.
        self.session.runs.append(fresh.runs[-1])
        with execution_scope(self.runtime,self.context,'new-run'):
            with self.assertRaises(PrivacyConflict):
                self.db.upsert_session(self.session)
        self.assertNotIn('PRIVATE SHOP RESULT',str(self.db.get_session('session',deserialize=False)))

    async def test_summary_and_changed_state_leaves_are_selective_across_canonical_copies(self):
        from openagent_core.memory.sessions.summary import SessionSummary
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        baseline=AgentSession(session_id='session',agent_id='agent',user_id=self.context.authority.key,
            runs=[],session_data={'session_state':{'human_preference':'HUMAN KEEP'},'session_name':'HUMAN TITLE'},
            created_at=1,updated_at=1)
        self.db.upsert_session(baseline)
        self.session.session_data={'session_state':{'human_preference':'HUMAN KEEP','derived':'PRIVATE STATE A'},
                                   'session_name':'PRIVATE GENERATED TITLE',
                                   'session_metrics':{'input_tokens':12,'details':{'model':[{'id':'fixture','provider':'fixture','input_tokens':12}]}}}
        self.session.summary=SessionSummary(summary='PRIVATE ROLLING SUMMARY')
        self.session.runs[0].session_state=dict(self.session.session_data['session_state'])
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        await self.store.transition('run','success',output='PRIVATE DERIVED FINAL')
        request=PrivacyRequest('state-erasure','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        async def export(*args):self.fail('unexpected export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        progress=await self.ledger.process(request.request_id,(public,self.sink),covered_surfaces=self.ledger.required_surfaces)
        self.assertEqual(progress.status,'complete')
        stored=str(self.db.get_session('session',deserialize=False))
        self.assertIn('HUMAN KEEP',stored)
        for value in ('PRIVATE STATE A','PRIVATE ROLLING SUMMARY','PRIVATE GENERATED TITLE'):
            self.assertNotIn(value,stored)
            for table,column in (('sessions_v2','metadata_json'),('sessions_v2','title'),('session_runs','raw_envelope_json')):
                self.assertNotIn(value,str([tuple(row) for row in self.store.connection.execute(f'SELECT {column} FROM {table}')]))
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):self.db.upsert_session(self.session)

    async def test_unattested_changed_session_field_fails_before_commit(self):
        self.session.session_data={'unknown_extension_copy':'UNTRACKED PRIVATE COPY'}
        with execution_scope(self.runtime,self.context,'run'):
            with self.assertRaises(PrivacyFence):self.db.upsert_session(self.session)
        self.assertIsNone(self.db.get_session('session',deserialize=False))

    async def test_revoked_queued_run_performs_no_executor_io_and_active_run_becomes_terminal(self):
        import asyncio
        from openagent_core.content_privacy import ToolResultPrivacyPublisher
        self.runtime.services.registry.bind(ToolResultPrivacyPublisher,self.publisher)
        await self.store.transition('run','success',output='OLDER SOURCE ANSWER')
        entered=asyncio.Event();release=asyncio.Event();calls=[]
        async def execute(request,context,runtime):
            calls.append(request.run_id)
            entered.set();await release.wait()
            return 'PRIVATE LATE OUTPUT'
        self.runtime.services.executor=SimpleNamespace(execute=execute)
        await self.runtime.submit(RunRequest('active','session','active-key','Active question'),self.context)
        await entered.wait()
        await self.runtime.submit(RunRequest('queued','session','queued-key','Queued question'),self.context)
        await self.ledger.enqueue(PrivacyRequest('concurrent-erasure','tenant','shopify/app','group',10,'redact',('customer:23',)))
        release.set()
        active=await self.runtime.wait('active',self.context)
        queued=await self.runtime.wait('queued',self.context)
        self.assertEqual(calls,['active'])
        self.assertEqual((active.status,queued.status),('failed','failed'))
        self.assertIsNone(active.output);self.assertIsNone(queued.output)
        self.assertNotIn('PRIVATE LATE OUTPUT',str([tuple(row) for row in self.store.connection.execute('SELECT output_json,raw_envelope_json FROM session_runs')]))



    async def test_mixed_customer_one_call_preserves_sibling_across_real_public_and_engine_copies(self):
        from dataclasses import replace
        from openagent_core.content_privacy import ContentFragment,ToolResultProvenance,ContentRevision,JsonStringBoundary
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        second=replace(self.origin,subjects=('customer:24',))
        inventory=replace(self.origin,subjects=(),nonpersonal=True)
        payload={'data':{'customers':[{'id':23,'name':'FIRST CUSTOMER PRIVATE'},
                                     {'id':24,'name':'SECOND CUSTOMER RETAIN'}],'inventory':'NONPERSONAL INVENTORY'},
                 '_host':{'count':2}}
        result={'content':[{'type':'text','text':json.dumps(payload)}],'structuredContent':payload}
        boundary=JsonStringBoundary()
        fragments=[]
        for prefix in (('structuredContent',),('content',0,'text',boundary)):
            fragments.extend((ContentFragment(prefix+('data','customers',0),(self.origin,)),
                              ContentFragment(prefix+('data','customers',1),(second,)),
                              ContentFragment(prefix+('data','inventory'),(inventory,)),ContentFragment(prefix+('_host',),())))
        fragments.append(ContentFragment(('content',0,'type'),()))
        provenance=ToolResultProvenance(tuple(fragments))
        provenance.validate_result(result,'tenant')
        await self.store.begin_tool('run','mixed-catalog',{'source_id':'shopify','name':'read','provider_call_id':'mixed-model'}, {})
        await self.publisher.publish_tool_result(self.context,'run','mixed-catalog',result,provenance)
        encoded=json.dumps(result)
        run=self.session.runs[0]
        run.messages.insert(2,Message(role='tool',tool_call_id='mixed-model',content=encoded))
        run.tools.append(ToolExecution(tool_call_id='mixed-model',tool_name='read',tool_args={},result=encoded))
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        await self.store.transition('run','success',output='PRIVATE GENERATED BOTH')
        request=PrivacyRequest('mixed-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        async def export(*args): self.fail('redaction must not export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        targets=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=?''',('mixed-event',)).fetchall()
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            sink=public if node['sink']=='runtime-ledger' else self.sink
            self.assertEqual(await sink.apply(request,revision),'complete')
        source=str(self.db.get_session('session',deserialize=False))
        self.assertNotIn('FIRST CUSTOMER PRIVATE',source)
        self.assertIn('SECOND CUSTOMER RETAIN',source)
        self.assertIn('NONPERSONAL INVENTORY',source)
        self.assertIn('HUMAN INPUT',source)
        for table,column in (('tool_invocations','result_json'),('tool_invocations','raw_envelope_json'),
                             ('session_messages','text'),('domain_events','metadata_json'),('session_runs','raw_envelope_json')):
            copies=' '.join(str(row[0]) for row in self.store.connection.execute(f'SELECT {column} FROM {table}'))
            self.assertNotIn('FIRST CUSTOMER PRIVATE',copies)
            self.assertIn('SECOND CUSTOMER RETAIN',copies)
        # Exact sibling selectors continue to work after the first request.
        second_request=replace(request,request_id='second-mixed-event',subjects=('customer:24',))
        await self.ledger.enqueue(second_request)
        targets=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=?''',('second-mixed-event',)).fetchall()
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await (public if node['sink']=='runtime-ledger' else self.sink).apply(second_request,revision),'complete')
        self.assertNotIn('SECOND CUSTOMER RETAIN',str(self.db.get_session('session',deserialize=False)))
        self.assertIn('NONPERSONAL INVENTORY',str(self.db.get_session('session',deserialize=False)))
        shop_request=replace(request,request_id='whole-shop',subjects=())
        await self.ledger.enqueue(shop_request)
        targets=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=?''',('whole-shop',)).fetchall()
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await (public if node['sink']=='runtime-ledger' else self.sink).apply(shop_request,revision),'complete')
        self.assertNotIn('NONPERSONAL INVENTORY',str(self.db.get_session('session',deserialize=False)))
        for table,column in (('tool_invocations','result_json'),('tool_invocations','raw_envelope_json'),
                             ('session_messages','text'),('domain_events','metadata_json'),('session_runs','raw_envelope_json')):
            copies=' '.join(str(row[0]) for row in self.store.connection.execute(f'SELECT {column} FROM {table}'))
            self.assertNotIn('NONPERSONAL INVENTORY',copies)

    async def test_actual_spawn_tracks_child_mission_and_engine_output_without_new_source_tool(self):
        from openagent_core.core._run_state.agent import RunInput
        from openagent_core.content_privacy import ContentRevision
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        async def execute(request,context,runtime):
            run=RunOutput(run_id=request.run_id,agent_id='agent',session_id=request.session_id,
                          input=RunInput(request.input),content='PRIVATE CHILD ANSWER',
                          messages=[Message(role='user',content='UNRELATED OLDER HUMAN'),
                                    Message(role='user',content=request.input),Message(role='assistant',content='PRIVATE CHILD ANSWER')])
            session=AgentSession(session_id=request.session_id,agent_id='agent',user_id=context.authority.key,runs=[run],created_at=1,updated_at=1)
            self.db.upsert_session(session)
            return 'PRIVATE CHILD ANSWER'
        self.runtime.services.executor=SimpleNamespace(execute=execute)
        with execution_scope(self.runtime,self.context,'run'):
            child=await self.runtime.spawn(RunRequest('child','child-session','child-key','PRIVATE CHILD MISSION'),self.context)
        self.assertEqual(child.status,'success')
        self.assertIn('PRIVATE CHILD MISSION',str(self.db.get_session('child-session',deserialize=False)))
        request=PrivacyRequest('child-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        async def export(*args):self.fail('unexpected export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        targets=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=?''',('child-event',)).fetchall()
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await (public if node['sink']=='runtime-ledger' else self.sink).apply(request,revision),'complete')
        source=str(self.db.get_session('child-session',deserialize=False))
        self.assertNotIn('PRIVATE CHILD MISSION',source)
        self.assertNotIn('PRIVATE CHILD ANSWER',source)
        self.assertIn('UNRELATED OLDER HUMAN',source)
        self.assertIsNone(self.store.connection.execute('SELECT input_json FROM session_runs WHERE id=?',('child',)).fetchone()[0])
        self.assertEqual(json.loads(self.store.connection.execute('SELECT input_json FROM session_runs WHERE id=?',('run',)).fetchone()[0]),'HUMAN INPUT')
        with self.assertRaises(PrivacyFence):
            await self.store.accepted_request('child')
        metadata=json.loads(self.store.connection.execute('SELECT metadata_json FROM session_runs WHERE id=?',('child',)).fetchone()[0])
        self.assertEqual(metadata['execution_context']['authority']['subject_id'],'alice')

    async def test_protected_child_attachments_are_not_admitted_without_managed_copy_bridge(self):
        from openagent_core import PrincipalRef
        context=self.context.child(session_id='child-session',run_id='run',agent=PrincipalRef('host','tenant','agent','agent'))
        with self.assertRaises(PrivacyFence):
            await self.store.accept(RunRequest('child','child-session','child-key','mission',attachments=({'path':'unattested'},)),context)
        self.assertIsNone(await self.store.get('child'))

    async def test_inflight_query_and_failed_tool_copies_are_recorded_before_results(self):
        from openagent_core.content_privacy import ContentRevision
        from openagent_storage_sqlite.runtime_privacy import SqliteRuntimeContentPrivacySink
        await self.store.begin_tool('run','failed',{'source_id':'shopify','name':'read','provider_call_id':'failed-model'}, {'query':'DERIVED PRIVATE QUERY'})
        # No successful result is needed for the invocation intent to be owned.
        self.assertGreater(self.store.connection.execute("SELECT count(*) FROM runtime_privacy_fragments WHERE field='args_json'").fetchone()[0],0)
        await self.store.finish_tool('run','failed',error={'detail':'DERIVED PRIVATE ERROR'})
        run=self.session.runs[0]
        run.messages.insert(2,Message(role='tool',tool_call_id='failed-model',content='DERIVED PRIVATE ERROR'))
        run.tools.append(ToolExecution(tool_call_id='failed-model',tool_name='read',tool_args={'query':'DERIVED PRIVATE QUERY'},result='DERIVED PRIVATE ERROR'))
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        request=PrivacyRequest('failed-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        async def export(*args):self.fail('unexpected export')
        public=SqliteRuntimeContentPrivacySink(self.store,self.ledger,export=export)
        targets=self.store.connection.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
            ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=?''',('failed-event',)).fetchall()
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await (public if node['sink']=='runtime-ledger' else self.sink).apply(request,revision),'complete')
        self.assertNotIn('DERIVED PRIVATE',str(self.db.get_session('session',deserialize=False)))
        for table,column in (('tool_invocations','args_json'),('tool_invocations','raw_envelope_json'),
                             ('session_messages','text'),('domain_events','metadata_json')):
            self.assertNotIn('DERIVED PRIVATE',' '.join(str(row[0]) for row in self.store.connection.execute(f'SELECT {column} FROM {table}')))

    async def test_real_compaction_records_recap_and_redacts_folded_compatibility_copy(self):
        from unittest.mock import patch
        from openagent_core.core.compaction import compact
        from openagent_core.runtime import runtime_scope
        from openagent_core.content_privacy import ContentRevision
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        await self.store.transition('run','success',output='PRIVATE DERIVED FINAL')
        async def summarize(*args): return 'PRIVATE COMPACTED RECAP'
        agent=SimpleNamespace(_db=SimpleNamespace(db_path=str(self.store.path)))
        model=SimpleNamespace(model='fixture')
        with runtime_scope(self.runtime),patch('openagent_core.core.compaction._summarize_runs',summarize):
            result=await compact('session',model,agent,keep=0)
        self.assertIsNotNone(result)
        self.assertIn('PRIVATE COMPACTED RECAP',str(self.db.get_session('session',deserialize=False)))
        request=PrivacyRequest('compaction-event','tenant','shopify/app','group',10,'redact')
        await self.ledger.enqueue(request)
        nodes=self.store.connection.execute("SELECT r.* FROM privacy_targets t JOIN privacy_revisions r ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=? AND r.sink='engine-history' ORDER BY r.rowid DESC",('compaction-event',)).fetchall()
        self.assertEqual(len(nodes),7)
        for node in nodes:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await self.sink.apply(request,revision),'complete')
        source=self.db.get_session('session',deserialize=False)
        self.assertNotIn('PRIVATE COMPACTED RECAP',str(source))
        compatibility=self.store.connection.execute('SELECT raw_envelope_json FROM session_runs WHERE id=?',('run',)).fetchone()[0]
        self.assertNotIn('PRIVATE SHOP RESULT',compatibility)
        self.assertIn('HUMAN INPUT',compatibility)
        self.assertIn('UNRELATED MAIL RESULT',compatibility)

    async def test_revocation_during_compaction_does_not_persist_generated_recap(self):
        from unittest.mock import patch
        from openagent_core.core.compaction import compact
        from openagent_core.runtime import runtime_scope
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        request=PrivacyRequest('during-summary','tenant','shopify/app','group',10,'redact')
        async def summarize(*args):
            await self.ledger.enqueue(request)
            return 'LATE REVOKED RECAP'
        agent=SimpleNamespace(_db=SimpleNamespace(db_path=str(self.store.path)))
        with runtime_scope(self.runtime),patch('openagent_core.core.compaction._summarize_runs',summarize):
            with self.assertRaises(PrivacyFence):
                await compact('session',SimpleNamespace(model='fixture'),agent,keep=0)
        self.assertNotIn('LATE REVOKED RECAP',str(self.db.get_session('session',deserialize=False)))
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM engine_privacy_recaps').fetchone()[0],0)

    async def test_compaction_does_not_overwrite_human_change_during_summary(self):
        from unittest.mock import patch
        from openagent_core.core.compaction import compact
        from openagent_core.runtime import runtime_scope
        from openagent_core.content_privacy import PrivacyConflict
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        async def summarize(*args):
            current=self.store.connection.execute('SELECT runs FROM sessions WHERE session_id=?',('session',)).fetchone()[0]
            values=json.loads(json.loads(current))
            values[0]['messages'][0]['content']='NEW HUMAN CONTENT'
            self.store.connection.execute('UPDATE sessions SET runs=? WHERE session_id=?',(json.dumps(values),'session'))
            return 'STALE RECAP'
        agent=SimpleNamespace(_db=SimpleNamespace(db_path=str(self.store.path)))
        with runtime_scope(self.runtime),patch('openagent_core.core.compaction._summarize_runs',summarize):
            with self.assertRaises(PrivacyConflict):
                await compact('session',SimpleNamespace(model='fixture'),agent,keep=0)
        self.assertIn('NEW HUMAN CONTENT',str(self.db.get_session('session',deserialize=False)))
        self.assertNotIn('STALE RECAP',str(self.db.get_session('session',deserialize=False)))
