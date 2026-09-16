from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from openagent_core import *
from openagent_core.runtime import execution_scope, runtime_scope
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    denied=set()
    async def authorize(self,context,action,resource,*,audience=()):
        return action not in self.denied


class Effects(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=TemporaryDirectory();self.path=Path(self.tmp.name)
        self.p=PrincipalRef('host','tenant','alice')
        self.ctx=ExecutionContext(self.p,self.p,self.p,'session','agent',(self.p,))
        self.policy=Policy();self.policy.denied=set()
        self.store=SqliteRuntimeStore(self.path/'state.sqlite3')
        self.catalog=CapabilityCatalog(self.policy)
        self.runtime=Runtime(RuntimeSettings('agent',self.path),RuntimeServices(self.store,None,self.policy,self.catalog))
    async def asyncTearDown(self):
        await self.runtime.close();self.tmp.cleanup()
    def request(self,name='run'):
        return RunRequest(name,'session',name,'input')
    async def run_executor(self,execute,name='run'):
        self.runtime.services=replace(self.runtime.services,executor=SimpleNamespace(execute=execute))
        await self.runtime.start();await self.runtime.submit(self.request(name),self.ctx)
        return await self.runtime.wait(name,self.ctx)

    async def test_intent_precedes_effect_and_complete_mcp_envelope_is_durable(self):
        result={'content':[{'type':'image','data':'aGVsbG8=','mimeType':'image/png'}],
                'structuredContent':{'id':1},'isError':False,'_meta':{'receipt':'r'}}
        async def tool(args,context):
            row=self.store.connection.execute('SELECT status,args_json FROM tool_invocations').fetchone()
            self.assertEqual(row[0],'running');self.assertEqual(json.loads(row[1]),args)
            anchor=self.store.connection.execute("SELECT status,tool_call_id FROM session_messages WHERE role='tool'").fetchone()
            self.assertEqual(tuple(anchor),('streaming','provider-call'))
            return result
        source=FunctionSource((ToolDefinition('write','Write',{}),),{'write':tool})
        self.catalog.register('source',source,source,target_label='Exact destination')
        async def execute(request,ctx,runtime):
            ref=(await self.catalog.discover(ctx))[0].tool_ref
            self.assertEqual(await self.catalog.call_tool(ref,{'x':1},ctx,call_id='provider-call'),result)
            with self.assertRaises(IdempotencyConflict):
                await self.catalog.call_tool(ref,{'x':1},ctx,call_id='provider-call')
            return 'done'
        self.assertEqual((await self.run_executor(execute)).status,'success')
        row=self.store.connection.execute('SELECT status,result_json,result_complete FROM tool_invocations').fetchone()
        self.assertEqual(tuple(row),('success',json.dumps(result,sort_keys=True,separators=(',',':')),1))
        events=await self.runtime.events('run',0,self.ctx)
        self.assertLess(next(e.cursor for e in events if e.kind=='tool.invoking'),next(e.cursor for e in events if e.kind=='tool.completed'))
        completed=next(e for e in events if e.kind=='tool.completed')
        self.assertEqual(completed.payload['binding']['execution_host']['device_label'],'Exact destination')
        self.assertEqual(completed.payload['binding']['execution_host']['kind'],'capability')
        messages=self.store.connection.execute('SELECT role,status,tool_call_id,text FROM session_messages ORDER BY sequence').fetchall()
        self.assertEqual([m['role'] for m in messages],['user','tool','assistant'])
        self.assertEqual(messages[1]['status'],'complete')
        self.assertEqual(json.loads(messages[1]['text']),result)

    async def test_private_tool_result_never_enters_shared_replay(self):
        async def tool(args,ctx):
            self.policy.denied.add('tool.publish');return {'private':'SECRET'}
        source=FunctionSource((ToolDefinition('read','Read',{}),),{'read':tool});self.catalog.register('data',source,source,target_label='Data')
        async def execute(request,ctx,runtime):
            ref=(await self.catalog.discover(ctx))[0].tool_ref
            return await self.catalog.call_tool(ref,{},ctx)
        self.assertEqual((await self.run_executor(execute)).status,'failed')
        self.assertNotIn('SECRET',str(await self.runtime.events('run',0,self.ctx)))
        row=self.store.connection.execute('SELECT result_json,status_raw FROM tool_invocations').fetchone()
        self.assertIsNone(row[0]);self.assertEqual(row[1],'effects_unknown')

    async def test_crash_recovery_records_unknown_tool_effect_without_reexecution(self):
        await self.runtime.start();await self.store.accept(self.request(),self.ctx);await self.store.transition('run','running')
        await self.store.begin_tool('run','call',{'source_id':'source','name':'write'}, {})
        await self.runtime.close()
        store=SqliteRuntimeStore(self.store.path)
        self.runtime=Runtime(self.runtime.settings,replace(self.runtime.services,store=store))
        await self.runtime.start()
        self.assertEqual((await self.runtime.get_run('run',self.ctx)).status,'interrupted')
        self.assertEqual(store.connection.execute('SELECT status_raw FROM tool_invocations').fetchone()[0],'effects_unknown')

    async def test_children_keep_authority_and_have_agent_authorship_and_ancestry(self):
        seen=[]
        async def execute(request,ctx,runtime):
            seen.append(ctx)
            if request.run_id=='parent':
                child=await runtime.spawn(RunRequest('child','child-session','child','mission'),ctx)
                self.assertEqual(child.status,'success')
            return 'done'
        await self.run_executor(execute,'parent')
        self.assertEqual(seen[1].author.kind,'agent');self.assertEqual(seen[1].initiator,self.p)
        self.assertEqual(seen[1].authority,self.p);self.assertEqual(seen[1].parent_run_id,'parent')
        self.assertEqual([r.run_id for r in await self.runtime.children('parent',self.ctx)],['child'])
        row=self.store.connection.execute("SELECT parent_session_id,root_session_id FROM sessions_v2 WHERE id='child-session'").fetchone()
        self.assertEqual(tuple(row),('session','session'))
        self.assertEqual((await self.runtime.get_run('child',self.ctx)).parent_run_id,'parent')
        self.assertEqual(self.store.connection.execute("SELECT delegated_parent_run_id FROM session_runs WHERE id='child'").fetchone()[0],'parent')
        self.assertIsNone(self.store.connection.execute("SELECT parent_run_id FROM session_runs WHERE id='child'").fetchone()[0])

    async def test_automatic_operation_uses_refreshed_delegation_and_same_run_authority(self):
        token=ContextVar('test_ephemeral_token',default=None);active={'allowed':True}
        class Delegations:
            @asynccontextmanager
            async def execution_scope(self,ctx):
                t=token.set('fresh');
                try: yield
                finally: token.reset(t)
            async def validate(self,ctx): return active['allowed']
        class TokenPolicy:
            async def authorize(self,ctx,action,resource,*,audience=()): return token.get()=='fresh'
        ctx=replace(self.ctx,deferred=True,delegation_id='durable-definition-v1')
        self.runtime.services=replace(self.runtime.services,delegations=Delegations(),authorizer=TokenPolicy())
        calls=[]
        async def execute(request,ctx,runtime): calls.append(token.get());return 'automatic'
        await self.runtime.start()
        await self.runtime.execute_operation(self.request(),ctx,SimpleNamespace(execute=execute))
        # The observer resolves its own transient credentials, just like the host.
        async with self.runtime.services.delegations.execution_scope(ctx):
            self.assertEqual((await self.runtime.wait('run',ctx)).status,'success')
        await self.runtime.execute_operation(self.request(),ctx,SimpleNamespace(execute=execute))
        self.assertEqual(calls,['fresh']);self.assertIsNone(token.get())
        active['allowed']=False
        with self.assertRaises(PermissionError):
            await self.runtime.execute_operation(self.request('next'),ctx,SimpleNamespace(execute=execute))
        metadata=self.store.connection.execute('SELECT metadata_json FROM session_runs').fetchone()[0]
        self.assertNotIn('fresh',metadata)

    async def test_instance_registries_and_credentials_do_not_cross_agent_boundaries(self):
        from openagent_core.core.compaction import session_lock
        from openagent_core.core.hooks import set_quick_commands,expand_quick_command
        from openagent_core.models.credential_pool import get_or_build_pool
        from openagent_core.models.native_provider import NativeProvider
        before=dict(os.environ)
        NativeProvider('openai:example',api_key='explicit-1',base_url='http://127.0.0.1:1')
        NativeProvider('openai:example',api_key='explicit-2',base_url='http://127.0.0.1:2')
        self.assertEqual(dict(os.environ),before)
        second=Runtime(RuntimeSettings('agent',self.path/'other'),self.runtime.services)
        config={'metadata':{'accounts':[{'api_key':'one'},{'api_key':'two'}]}}
        with runtime_scope(self.runtime):
            lock=session_lock('same');pool=get_or_build_pool('provider',config);set_quick_commands({'hello':'one'})
        with runtime_scope(second):
            self.assertIsNot(session_lock('same'),lock);self.assertIsNot(get_or_build_pool('provider',config),pool)
            self.assertIsNone(expand_quick_command('/hello'));set_quick_commands({'hello':'two'})
        with runtime_scope(self.runtime): self.assertEqual(expand_quick_command('/hello'),'one')

    async def test_background_job_events_do_not_cross_users_devices_or_generations(self):
        from openagent_core.jobs import BackgroundJobs,JobEvent
        jobs=BackgroundJobs();ctx=replace(self.ctx,capabilities=(CapabilityLease('app','i','1'),))
        other=replace(ctx,capabilities=(CapabilityLease('app','i','2'),))
        jobs.register('job','session',ctx.coalescing_key)
        jobs.complete('session',ctx.coalescing_key,JobEvent('job','completed','app','output',{'job_id':'job'},'Finished'))
        self.assertEqual(jobs.drain('session',context_key=other.coalescing_key),[])
        self.assertEqual(len(jobs.drain('session',context_key=ctx.coalescing_key)),1)

    async def test_configuration_provider_defaults_breaker_and_logs_are_instance_scoped(self):
        from unittest.mock import patch
        from openagent_core.configuration import runtime_environment
        from openagent_core.models.providers.openai.chat import OpenAIChat
        from openagent_core.models.providers.fallback import _breaker_record_failure,breaker_snapshot
        from openagent_core.core.logging import elog,events_path,close_runtime_logging
        self.runtime.settings=replace(self.runtime.settings,environment=(('OPENAI_API_KEY','runtime-a'),))
        second=Runtime(RuntimeSettings('agent',self.path/'other'),self.runtime.services)
        with patch.dict(os.environ,{'OPENAI_API_KEY':'ambient-secret'}):
            with runtime_scope(self.runtime):
                self.assertEqual(runtime_environment().get('OPENAI_API_KEY'),'runtime-a')
                client=OpenAIChat(id='fixture').get_client()
                self.assertEqual(client.api_key,'runtime-a');client.close()
                _breaker_record_failure('same-provider-model')
                elog('runtime-a.event',marker='only-a');first_log=events_path()
            with runtime_scope(second):
                self.assertNotIn('OPENAI_API_KEY',runtime_environment())
                self.assertEqual(breaker_snapshot(),{})
                with self.assertRaises(Exception):OpenAIChat(id='fixture').get_client()
                elog('runtime-b.event',marker='only-b');second_log=events_path()
                close_runtime_logging()
            with runtime_scope(self.runtime):
                self.assertIn('same-provider-model',breaker_snapshot())
                elog('runtime-a.second',marker='only-a-again');close_runtime_logging()
        self.assertNotIn('only-b',first_log.read_text())
        self.assertNotIn('only-a',second_log.read_text())


if __name__=='__main__': unittest.main()
