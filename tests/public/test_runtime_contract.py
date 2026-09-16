from __future__ import annotations
import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from openagent_core import *
from openagent_core.contracts import AuthorizationDenied
from openagent_core.runtime import current_execution_context
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    def __init__(self):
        self.denied = set()
    async def authorize(self,context,action,resource,*,audience=()):
        return action not in self.denied and resource.tenant_id==context.tenant_id


class Echo:
    def __init__(self):self.calls=[];self.hold=None;self.started=asyncio.Queue()
    async def execute(self,request,context,runtime):
        self.calls.append((request.run_id,context.author.subject_id,current_execution_context()))
        self.started.put_nowait(request.run_id)
        if self.hold:await self.hold.wait()
        return request.input


class Contracts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=TemporaryDirectory();self.path=Path(self.tmp.name)
        self.policy=Policy();self.executor=Echo()
        self.p=PrincipalRef('host','tenant','alice')
        self.ctx=ExecutionContext(self.p,self.p,self.p,'s','agent',(self.p,))
        self.store=SqliteRuntimeStore(self.path/'one'/'state.sqlite3')
        self.r=Runtime(RuntimeSettings('agent',self.path/'one'),RuntimeServices(self.store,self.executor,self.policy))
    async def asyncTearDown(self):
        await self.r.close();self.tmp.cleanup()
    def request(self,id='run',**kwargs):
        return RunRequest(id,'s',id,'hello',**kwargs)

    async def test_construction_has_no_files_or_execution(self):
        self.assertFalse((self.path/'one').exists());self.assertEqual(self.executor.calls,[])

    async def test_durable_idempotency_and_replay_after_restart(self):
        await self.r.start();req=self.request()
        await asyncio.gather(self.r.submit(req,self.ctx),self.r.submit(req,self.ctx))
        self.assertEqual((await self.r.wait('run',self.ctx)).status,'success')
        self.assertEqual(len(self.executor.calls),1)
        before=await self.r.events('run',0,self.ctx)
        await self.r.close()
        self.r=Runtime(self.r.settings,RuntimeServices(SqliteRuntimeStore(self.store.path),self.executor,self.policy))
        await self.r.start()
        self.assertEqual((await self.r.submit(req,self.ctx)).status,'success')
        self.assertEqual(len(self.executor.calls),1)
        self.assertEqual(await self.r.events('run',0,self.ctx),before)
        self.assertEqual(await self.r.events('run',before[-1].cursor,self.ctx),())

    async def test_reconnect_reads_original_admission_without_rebinding(self):
        original=replace(self.ctx,ingress_id='old-device')
        request=self.request(deadline_seconds=4,attachments=({'artifact_id':'verified'},),model_ref=None)
        await self.r.start();await self.r.submit(request,original)
        await self.r.wait('run',original)
        fresh=replace(self.ctx,ingress_id='new-device')
        accepted=await self.r.accepted_request('run',fresh)
        self.assertEqual(accepted.request,request);self.assertEqual(accepted.author,original.author)
        self.assertFalse(hasattr(accepted,'capabilities'))
        with self.assertRaises(IdempotencyConflict):await self.r.submit(request,fresh)
        self.assertEqual(len(self.executor.calls),1)
        self.policy.denied.add('run.replay')
        with self.assertRaises(PermissionError):await self.r.accepted_request('run',fresh)
        self.policy.denied.clear()
        await self.r.cancel('reservation',fresh,reserve=True)
        with self.assertRaises(LookupError):await self.r.accepted_request('reservation',fresh)

    async def test_changed_input_or_author_conflicts(self):
        await self.r.start();req=self.request();await self.r.submit(req,self.ctx)
        for request,ctx in [(replace(req,input='changed'),self.ctx),(req,replace(self.ctx,author=PrincipalRef('host','tenant','bob')))]:
            with self.assertRaises(IdempotencyConflict):await self.r.submit(request,ctx)
        await self.r.wait('run',self.ctx)

    async def test_observer_disconnect_does_not_cancel_run(self):
        self.executor.hold=asyncio.Event();await self.r.start();await self.r.submit(self.request(),self.ctx)
        waiter=asyncio.create_task(self.r.wait('run',self.ctx));await asyncio.sleep(0);waiter.cancel()
        await asyncio.gather(waiter,return_exceptions=True)
        self.executor.hold.set();self.assertEqual((await self.r.wait('run',self.ctx)).status,'success')

    async def test_exact_cancel_before_execution(self):
        self.executor.hold=asyncio.Event()
        await self.r.start();await self.r.submit(self.request('blocker'),self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(),2),'blocker')
        await self.r.submit(self.request(),self.ctx)
        self.assertEqual((await self.r.cancel('run',self.ctx)).status,'cancelled')
        self.assertEqual([row[0] for row in self.executor.calls],['blocker'])
        self.executor.hold.set();await self.r.wait('blocker',self.ctx)

    async def test_steering_cancels_exact_target_and_uses_new_authority(self):
        self.executor.hold=asyncio.Event()
        await self.r.start();await self.r.submit(self.request('old'),self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(),2),'old')
        bob=PrincipalRef('host','tenant','bob')
        context=replace(self.ctx,author=bob,initiator=bob,authority=bob,audience=(bob,),ingress_id='bob-device')
        request=self.request('steer',steer_run_id='old')
        await self.r.submit(request,context)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(),2),'steer')
        self.assertEqual((await self.r.get_run('old',self.ctx)).status,'cancelled')
        self.assertEqual(self.executor.calls[-1][2],context)
        self.executor.hold.set()
        self.assertEqual((await self.r.wait('steer',context)).status,'success')
        # A late duplicate must never cancel the current run for this session.
        self.executor.hold=asyncio.Event()
        await self.r.submit(self.request('later'),context)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(),2),'later')
        await self.r.submit(request,context)
        self.assertEqual((await self.r.get_run('later',context)).status,'running')
        self.executor.hold.set();await self.r.wait('later',context)
        self.assertEqual(len(self.executor.calls),3)

    async def test_steering_requires_target_authorization_before_admission(self):
        await self.r.start();await self.r.submit(self.request('old'),self.ctx)
        self.policy.denied.add('run.cancel')
        with self.assertRaises(PermissionError):
            await self.r.submit(self.request('steer',steer_run_id='old'),self.ctx)
        self.assertIsNone(await self.store.get('steer'))
        self.policy.denied.clear()
        other=replace(self.ctx,session_id='other')
        with self.assertRaises(PermissionError):
            await self.r.submit(RunRequest('other','other','other','hello',steer_run_id='old'),other)

    async def test_stop_before_lost_acceptance_creates_no_fake_author_and_never_executes(self):
        await self.r.start()
        cancelled=await self.r.cancel('run',self.ctx,reserve=True)
        self.assertEqual(cancelled.status,'cancelled')
        self.assertEqual((await self.r.cancel('run',self.ctx,reserve=True)).status,'cancelled')
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM sessions_v2').fetchone()[0],0)
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM session_messages').fetchone()[0],0)
        # A separately authenticated actor supplies the delayed original input;
        # the cancellation never assigns the stopping actor as its author.
        bob=PrincipalRef('host','tenant','bob')
        context=replace(self.ctx,author=bob,initiator=bob,authority=bob,audience=(bob,))
        self.assertEqual((await self.r.submit(self.request(),context)).status,'cancelled')
        self.assertEqual(self.executor.calls,[])
        author=self.store.connection.execute('SELECT author_principal_id FROM session_messages').fetchone()[0]
        self.assertEqual(author,bob.key)
        self.assertEqual((await self.r.submit(self.request(),context)).status,'cancelled')
        with self.assertRaises(IdempotencyConflict):
            await self.r.submit(replace(self.request(),input='different'),context)

    async def test_runtime_deadline_is_terminal(self):
        self.executor.hold=asyncio.Event();await self.r.start()
        await self.r.submit(self.request(deadline_seconds=.01),self.ctx)
        self.assertEqual((await self.r.wait('run',self.ctx)).status,'timed_out')

    async def test_multiuser_is_separate_authorized_turn(self):
        await self.r.start();b=PrincipalRef('host','tenant','bob');ctx=replace(self.ctx,author=b,initiator=b,authority=b)
        await self.r.submit(self.request('a'),self.ctx);await self.r.submit(self.request('b'),ctx)
        await self.r.wait('a',self.ctx);await self.r.wait('b',ctx)
        self.assertEqual([c[1] for c in self.executor.calls],['alice','bob'])
        self.assertNotEqual(self.ctx.coalescing_key,ctx.coalescing_key)
        authors=[row[0] for row in self.store.connection.execute("SELECT author_principal_id FROM session_messages WHERE role='user' ORDER BY sequence")]
        self.assertEqual(authors,[self.p.key,b.key])

    async def test_two_instances_overlapping_ids_and_close_isolation(self):
        await self.r.start();other_executor=Echo()
        other=Runtime(RuntimeSettings('agent',self.path/'two'),RuntimeServices(SqliteRuntimeStore(self.path/'two'/'state.sqlite3'),other_executor,Policy()))
        await other.start()
        try:
            await asyncio.gather(self.r.submit(self.request(),self.ctx),other.submit(self.request(),self.ctx))
            await self.r.wait('run',self.ctx);await self.r.close()
            self.assertEqual((await other.wait('run',self.ctx)).status,'success')
            await other.submit(self.request('next'),self.ctx);await other.wait('next',self.ctx)
            self.assertEqual(len(other_executor.calls),2)
        finally:await other.close()

    async def test_revocation_blocks_replay_and_publication(self):
        self.executor.hold=asyncio.Event();await self.r.start();await self.r.submit(self.request(),self.ctx)
        await asyncio.sleep(0);self.policy.denied.add('run.publish');self.executor.hold.set()
        self.assertEqual((await self.r.wait('run',self.ctx)).status,'failed')
        self.assertFalse(self.store.connection.execute("SELECT 1 FROM session_messages WHERE role='assistant'").fetchone())
        self.policy.denied.add('run.replay')
        with self.assertRaises(AuthorizationDenied):await self.r.events('run',0,self.ctx)

    async def test_single_writer_lock_and_uncertain_restart(self):
        await self.r.start();second=SqliteRuntimeStore(self.store.path)
        with self.assertRaises(RuntimeError):await second.start()
        # Persisted intent with no executor completion models process death.
        await self.store.accept(self.request(),self.ctx)
        await self.store.transition('run','running');await self.r.close()
        self.r=Runtime(self.r.settings,RuntimeServices(SqliteRuntimeStore(self.store.path),self.executor,self.policy))
        await self.r.start();self.assertEqual((await self.r.get_run('run',self.ctx)).status,'interrupted')
        self.assertEqual(self.executor.calls,[])

    async def test_temporary_capability_context_and_managed_ownership(self):
        catalog=CapabilityCatalog(self.policy,allow_dynamic=True);calls=[]
        async def f(args,ctx):calls.append(ctx.author);return {'content':[{'type':'text','text':'ok'}],'structuredContent':{'value':1},'_meta':{'a':1}}
        source=FunctionSource((ToolDefinition('shell','Shell',{},frozenset({'vault.write'})),),{'shell':f})
        lease=CapabilityLease('mac','instance','generation')
        catalog.register('mac',source,source,target_label='Mac di Alice',lease=lease)
        self.assertEqual(await catalog.discover(self.ctx),())
        ctx=replace(self.ctx,capabilities=(lease,));tool=(await catalog.discover(ctx))[0]
        self.assertEqual(tool.effects,frozenset())
        result=await catalog.call_tool(tool.tool_ref,{'source_id':'other'},ctx)
        self.assertEqual(result['structuredContent'],{'value':1});self.assertEqual(calls,[self.p])
        with self.assertRaises(PermissionError):await catalog.remove('mac',ctx)
        catalog.revoke('mac')
        with self.assertRaises(CapabilityUnavailable):await catalog.call_tool(tool.tool_ref,{},ctx)
        catalog.register('mac',source,source,target_label='Mac',lease=replace(lease,generation='new'))
        self.assertEqual(await catalog.discover(ctx),())

    async def test_fixed_catalog_cannot_install_and_deferred_leases_rejected(self):
        catalog=CapabilityCatalog(self.policy)
        with self.assertRaises(PermissionError):await catalog.install('x',None,None,self.ctx,target_label='x')
        with self.assertRaises(ValueError):replace(self.ctx,deferred=True,capabilities=(CapabilityLease('x','i','g'),))
        with self.assertRaises(ValueError):replace(self.ctx,deferred=True)

    async def test_audience_authorization_applies_to_tool_results(self):
        catalog=CapabilityCatalog(self.policy)
        async def f(args,ctx):return {'private':'value'}
        source=FunctionSource((ToolDefinition('read','Read',{}),),{'read':f});catalog.register('data',source,source,target_label='Data')
        ref=(await catalog.discover(self.ctx))[0].tool_ref;self.policy.denied.add('tool.publish')
        with self.assertRaises(AuthorizationDenied):await catalog.call_tool(ref,{},self.ctx)


if __name__=='__main__':unittest.main()
