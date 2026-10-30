"""Saved MCP App display data uses a public exact-session store query."""
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from openagent_core import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.contracts import SessionRef
from openagent_storage_sqlite import SqliteRuntimeStore


class InvocationDisplayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.store = SqliteRuntimeStore(Path(self.temp.name)/'store.sqlite')
        await self.store.start()
        principal = PrincipalRef('host','tenant','alice')
        self.context = ExecutionContext(principal,principal,principal,'session','agent',(principal,))
        self.reference = SessionRef('host','tenant','agent','session')
        self.binding = {'source_id':'fixture','name':'counter','tool_ref':'exact-opaque-reference',
            'metadata':{'ui':{'resourceUri':'ui://counter'}},'provider_call_id':'provider-call'}
        await self.begin('run-1')
        await self.store.begin_tool('run-1','call',self.binding,{'selection':'one'})
        await self.store.finish_tool('run-1','call',result={'content':[],'structuredContent':{'count':2},'_meta':{'opaque':True}})

    async def asyncTearDown(self):
        await self.store.close();self.temp.cleanup()

    async def begin(self,run):
        await self.store.accept(RunRequest(run,'session',run,'display'),self.context)
        await self.store.transition(run,'running')

    async def test_public_read_preserves_saved_binding_args_result_without_new_writes(self):
        before=await self.store.events('run-1')
        value=await self.store.read_tool_invocation(self.reference,'call',self.context)
        self.assertEqual(self.binding,value['binding'])
        self.assertEqual({'selection':'one'},value['arguments'])
        self.assertEqual({'opaque':True},value['result']['_meta'])
        self.assertEqual(value,await self.store.read_tool_invocation(self.reference,'provider-call',self.context))
        self.assertEqual(value,await self.store.read_tool_invocation(self.reference,value['id'],self.context))
        value['binding']['metadata']['ui']['resourceUri']='ui://forged'
        self.assertEqual('ui://counter',(await self.store.read_tool_invocation(self.reference,'call',self.context))['binding']['metadata']['ui']['resourceUri'])
        self.assertEqual(before,await self.store.events('run-1'))

    async def test_wrong_session_agent_tenant_and_unknown_selectors_fail_closed(self):
        for reference in [replace(self.reference,session_id='other'),replace(self.reference,agent_id='other'),replace(self.reference,tenant_id='other')]:
            with self.assertRaises(PermissionError):await self.store.read_tool_invocation(reference,'call',self.context)
        with self.assertRaises(LookupError):await self.store.read_tool_invocation(self.reference,'unknown',self.context)

    async def test_repeated_call_ids_across_runs_require_exact_invocation_id(self):
        original=await self.store.read_tool_invocation(self.reference,'call',self.context)
        await self.begin('run-2')
        await self.store.begin_tool('run-2','call',self.binding,{})
        with self.assertRaises(ValueError):await self.store.read_tool_invocation(self.reference,'call',self.context)
        self.assertEqual('run-1',(await self.store.read_tool_invocation(self.reference,original['id'],self.context))['run_id'])

    async def test_journal_rehydration_retains_exact_ui_binding(self):
        import json
        from openagent_core.session_transcript import runtime_journal_run, expand_run_messages
        row={'id':'run-1','status':'running','created_at_ms':1,'input_json':'"display"',
            'metadata_json':json.dumps({'execution_context':{'author':{'kind':'user','subject_id':'alice'}}})}
        async def events():
            yield {'event_type':'tool.completed','metadata_json':json.dumps({'call_id':'call',
                'binding':self.binding,'arguments':{'selection':'one'},'result':{'content':[]}})}
        run=await runtime_journal_run(row,events(),[])
        messages=expand_run_messages(run,timestamp=1,msg_counter=[0])
        tool=next(message['toolInfo'] for message in messages if message['role']=='tool')
        self.assertEqual(self.binding['tool_ref'],tool['tool_ref'])
        self.assertEqual(self.binding['metadata'],tool['metadata'])

    def test_provider_overlay_uses_exact_call_ids_and_refuses_ambiguous_or_name_matches(self):
        from openagent_core.session_transcript import overlay_tool_presentation
        run={'tools':[{'tool_name':'counter','tool_call_id':'provider-call'},
                      {'tool_name':'counter','tool_call_id':'different-call'}]}
        envelope={'call_id':'call','binding':self.binding}
        merged=overlay_tool_presentation(run,[envelope])
        self.assertEqual(self.binding['tool_ref'],merged['tools'][0]['tool_ref'])
        self.assertNotIn('tool_ref',merged['tools'][1]);self.assertNotIn('tool_ref',run['tools'][0])
        self.assertNotIn('tool_ref',overlay_tool_presentation(run,[envelope,envelope])['tools'][0])
