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

    async def test_workspace_free_kernel_never_resolves_a_local_agent_directory(self):
        from openagent_core.core.paths import config_dir, data_dir, get_agent_dir
        from openagent_core.runtime import runtime_scope

        runtime = Runtime(RuntimeSettings('agent'), self.r.services)
        self.addAsyncCleanup(runtime.close)
        with runtime_scope(runtime):
            self.assertIsNone(get_agent_dir())
            for resolve in (data_dir, config_dir):
                with self.assertRaisesRegex(RuntimeError, 'requires a local runtime workspace'):
                    resolve()
        await runtime.start()
        await runtime.submit(self.request(), self.ctx)
        result = await runtime.wait('run', self.ctx)
        self.assertEqual((result.status, result.output), ('success', 'hello'))

    async def test_full_worker_returns_identical_receipt_without_creating_another_task(self):
        from openagent_core.contracts import AdmissionCapacityExceeded, IdempotencyConflict
        self.r = Runtime(replace(self.r.settings, max_active_runs=1), self.r.services)
        self.executor.hold = asyncio.Event()
        await self.r.start()
        await self.r.submit(self.request(), self.ctx)
        await self.executor.started.get()
        try:
            retry = await self.r.submit(self.request(), self.ctx)
            self.assertEqual(retry.run_id, 'run')
            with self.assertRaises(IdempotencyConflict):
                await self.r.submit(replace(self.request(), input='changed'), self.ctx)
            with self.assertRaises(AdmissionCapacityExceeded):
                await self.r.submit(self.request('other'), self.ctx)
            self.assertIsNone(await self.store.get('other'))
            self.assertEqual(len(self.r._tasks), 1)
        finally:
            self.executor.hold.set()
        await self.r.wait('run', self.ctx)
        await self.r.submit(self.request('other'), self.ctx)
        self.assertEqual((await self.r.wait('other', self.ctx)).status, 'success')
        self.assertFalse(self.r._sessions)

    async def test_concurrent_acceptance_reserves_capacity_and_deduplicates_pending_retry(self):
        from openagent_core.contracts import AdmissionCapacityExceeded
        self.r = Runtime(replace(self.r.settings, max_active_runs=2), self.r.services)
        self.executor.hold = asyncio.Event()
        await self.r.start()
        entered = asyncio.Queue()
        release = asyncio.Event()
        accept = self.store.accept

        async def blocked(request, context):
            entered.put_nowait(request.run_id)
            await release.wait()
            return await accept(request, context)

        self.store.accept = blocked
        session_b = replace(self.ctx, session_id='s-b')
        pending = [
            asyncio.create_task(self.r.submit(self.request(), self.ctx)),
            asyncio.create_task(self.r.submit(replace(self.request('b'), session_id='s-b'), session_b)),
        ]
        try:
            async with asyncio.timeout(2):
                self.assertEqual({await entered.get(), await entered.get()}, {'run', 'b'})
            pending.append(asyncio.create_task(self.r.submit(self.request(), self.ctx)))
            with self.assertRaises(AdmissionCapacityExceeded):
                await self.r.submit(self.request('c'), self.ctx)
            self.assertIsNone(await self.store.get('c'))
            release.set()
            records = await asyncio.gather(*pending)
            self.assertEqual([r.run_id for r in records], ['run', 'b', 'run'])
            self.assertEqual(len(self.r._tasks), 2)
            self.assertFalse(self.r._accepting)
            self.policy.denied.add('run.read')
            with self.assertRaises(PermissionError):
                await self.r.submit(self.request(), self.ctx)
        finally:
            release.set()
            self.executor.hold.set()
            await asyncio.gather(*pending, return_exceptions=True)

    async def test_failed_registration_interrupts_accepted_receipt_and_releases_reservation(self):
        await self.r.start()
        append = self.store.append_event

        async def fail_profile(ident, kind, payload):
            if kind == 'run.profile':
                raise OSError('unavailable')
            return await append(ident, kind, payload)

        self.store.append_event = fail_profile
        with self.assertRaises(OSError):
            await self.r.submit(self.request(), self.ctx)
        self.assertEqual((await self.r.get_run('run', self.ctx)).status, 'interrupted')
        self.assertFalse(self.r._accepting)
        self.assertFalse(self.r._tasks)
        self.policy.denied.add('run.read')
        with self.assertRaises(PermissionError):
            await self.r.submit(self.request(), self.ctx)

    async def assert_writer_barrier(self, change):
        await self.r.start()
        entered, release = asyncio.Event(), asyncio.Event()
        accept = self.store.accept

        async def blocked(request, context):
            entered.set()
            await release.wait()
            return await accept(request, context)

        self.store.accept = blocked
        admission = asyncio.create_task(self.r.submit(self.request(), self.ctx))
        await entered.wait()
        writer = asyncio.create_task(change())
        try:
            async with asyncio.timeout(2):
                while not self.r._lifecycle.locked():
                    await asyncio.sleep(0)
            self.assertFalse(writer.done())
        finally:
            release.set()
        await asyncio.gather(admission, writer)
        self.assertFalse(self.r._accepting)
        self.assertEqual(self.r._admissions, 0)

    async def test_close_drains_admissions_in_progress_before_releasing_store(self):
        await self.assert_writer_barrier(self.r.close)
        with self.assertRaises(RuntimeError):
            await self.r.submit(self.request('late'), self.ctx)

    async def test_profile_swap_drains_inflight_admission_on_original_generation(self):
        from openagent_core.modules import RuntimeProfile
        await self.assert_writer_barrier(
            lambda: self.r.reconfigure(RuntimeProfile(1), expected_generation=0))
        events = await self.r.events('run', 0, self.ctx)
        profiles = [event.payload['generation'] for event in events if event.kind == 'run.profile']
        self.assertEqual(profiles, [0])
        self.assertEqual(self.r.profile.generation, 1)

    async def test_construction_has_no_files_or_execution(self):
        self.assertFalse((self.path/'one').exists());self.assertEqual(self.executor.calls,[])

    async def test_idle_sessions_release_locks_and_retain_durable_history(self):
        await self.r.start()

        async def run_turn(number):
            session_id = f'session-{number}'
            context = replace(self.ctx, session_id=session_id)
            request = RunRequest(f'run-{number}', session_id, 'one', 'hello')
            await self.r.submit(request, context)
            self.assertEqual((await self.r.wait(request.run_id, context)).status, 'success')

        for start in range(0, 64, 8):
            await asyncio.gather(*(run_turn(number) for number in range(start, start + 8)))
            self.assertFalse(self.r._sessions)
        first = await self.r.get_run('run-0', replace(self.ctx, session_id='session-0'))
        self.assertEqual(first.output, 'hello')

    async def test_cancelling_queued_turn_keeps_waiters_on_same_lock(self):
        await self.r.start()
        self.executor.hold = asyncio.Event()
        await self.r.submit(self.request('active'), self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(), 2), 'active')
        key = (self.ctx.tenant_id, self.ctx.session_id)
        for count, run_id in enumerate(('cancel-me', 'queued'), start=2):
            await self.r.submit(self.request(run_id), self.ctx)
            async with asyncio.timeout(2):
                while self.r._sessions[key].users != count:
                    await asyncio.sleep(0)
        await self.r.cancel('cancel-me', self.ctx)
        self.assertEqual(self.r._sessions[key].users, 2)
        self.assertEqual((await self.r.get_run('queued', self.ctx)).status, 'queued')
        self.executor.hold.set()
        for run_id in ('active', 'queued'):
            self.assertEqual((await self.r.wait(run_id, self.ctx)).status, 'success')
        self.assertEqual([call[0] for call in self.executor.calls], ['active', 'queued'])
        self.assertFalse(self.r._sessions)

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
        self.assertEqual(accepted.audience,original.audience)
        self.assertEqual(accepted.scopes,original.scopes)
        self.assertEqual(accepted.ingress_id,'old-device')
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

    async def test_steering_retains_only_authorized_unfinished_inputs(self):
        self.executor.hold = asyncio.Event()
        await self.r.start()
        old = replace(self.request("old"), input="Start the work")
        await self.r.submit(old, self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(), 2), "old")
        update = replace(self.request("update", steer_run_id="old"), input="Also check the result")
        await self.r.submit(update, self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(), 2), "update")
        final = replace(self.request("final", steer_run_id="update"), input="Report the outcome")
        await self.r.submit(final, self.ctx)
        self.assertEqual(await asyncio.wait_for(self.executor.started.get(), 2), "final")
        self.assertEqual(await self.r.steering_inputs(final, self.ctx),
                         ("Start the work", "Also check the result"))
        self.assertEqual((await self.r.accepted_request("final", self.ctx)).request, final)
        bob = PrincipalRef("host", "tenant", "bob")
        self.assertEqual(await self.r.steering_inputs(final, replace(self.ctx, audience=(self.p, bob))), ())
        self.policy.denied.add("run.replay")
        self.assertEqual(await self.r.steering_inputs(final, self.ctx), ())
        self.policy.denied.clear()
        self.executor.hold.set()
        await self.r.wait("final", self.ctx)
        self.assertEqual(await self.r.steering_inputs(self.request("later", steer_run_id="final"), self.ctx), ())

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
