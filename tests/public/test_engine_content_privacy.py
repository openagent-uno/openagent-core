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
        await self.store.begin_tool('run','second-catalog',{'source_id':'shopify','name':'read','provider_call_id':'model-second-shop'}, {})
        second=replace(self.origin,subjects=('customer:24',))
        await self.publisher.publish_tool_result(self.context,'run','second-catalog',{'private':'CUSTOMER TWO RESULT'},(second,))
        run=self.session.runs[0]
        run.messages.insert(2,Message(role='tool',tool_call_id='model-second-shop',content='CUSTOMER TWO RESULT'))
        run.tools.append(ToolExecution(tool_call_id='model-second-shop',tool_name='read',tool_args={'id':24},result='CUSTOMER TWO RESULT'))
        with execution_scope(self.runtime,self.context,'run'):
            self.db.upsert_session(self.session)
        request=PrivacyRequest('customer-event','tenant','shopify/app','group',10,'redact',('customer:23',))
        await self.ledger.enqueue(request)
        from openagent_core.content_privacy import ContentRevision
        targets=self.store.connection.execute("SELECT r.* FROM privacy_targets t JOIN privacy_revisions r ON t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision WHERE t.request_id=? AND r.sink='engine-history'",('customer-event',)).fetchall()
        self.assertEqual(len(targets),5)
        for node in targets:
            revision=ContentRevision(node['tenant'],node['artifact'],node['revision'],node['sink'],node['digest'])
            self.assertEqual(await self.sink.apply(request,revision),'complete')
        source=self.db.get_session('session',deserialize=False)
        self.assertNotIn('PRIVATE SHOP RESULT',str(source))
        self.assertIn('CUSTOMER TWO RESULT',str(source))
        self.assertIn('UNRELATED MAIL RESULT',str(source))
