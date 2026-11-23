import asyncio
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
from contextlib import asynccontextmanager
from aiohttp import web, ClientSession

from openagent_core import CapabilityCatalog, ExecutionContext, FunctionSource, PrincipalRef
from openagent_core import RunRequest, Runtime, RuntimeServices, RuntimeSettings, ToolDefinition
from openagent_core.durable_operations import DurableOperationPolicy, DurableOperationRunner, OperationGrant, OperationHandoff
from openagent_core.engine import AgentExecutor
from openagent_core.operations import CompleteOperation, CompletionStep, OperationInvocation, OperationPending, StagedOperation, ToolStep, WaitStep
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return context.authority.subject_id == 'owner-a' and (resource.kind != 'session' or resource.resource_id == context.session_id)


class Grants:
    def __init__(self):
        self.calls = 0
        self.revoked = False
        self.captured = {}

    async def capture(self, request, context, **binding):
        self.calls += 1
        assert binding['operation_name'] == 'process-record'
        assert binding['operation_version'] == 1
        if request.run_id not in self.captured:
            self.captured[request.run_id] = OperationGrant('grant-' + request.run_id, binding['expires_at'])
        self.last_binding = binding
        return self.captured[request.run_id]

    async def validate(self, context):
        return (not self.revoked and context.deferred and context.delegation_id in {grant.id for grant in self.captured.values()}
                and context.authority.subject_id == 'owner-a')


class DurableOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name)
        principal = PrincipalRef('host-a', 'tenant-a', 'owner-a')
        self.context = ExecutionContext(principal, principal, principal, 'session-a', 'agent-a', (principal,))
        self.grants = Grants()

    def make_store(self):
        return SqliteRuntimeStore(self.path / 'state.sqlite')

    async def runtime(self, read, *, policy=None, now=None, submit_behavior=None, submit_idempotent=True, infer=None):
        self.submissions = getattr(self, 'submissions', [])

        async def submit(args, context):
            self.assertFalse(context.capabilities)
            self.submissions.append(args['key'])
            if submit_behavior is not None:
                await submit_behavior()
            return {'id': 'receipt-a'}

        def next_step(state):
            self.assertEqual(state.origin_run_id, 'run-a')
            if 'submit' not in state.results:
                return ToolStep('submit', 'records', 'submit', {'key': state.idempotency_key('submit')}, idempotent=submit_idempotent)
            if 'wait' not in state.results:
                return WaitStep('wait', 'records', 'read', state.results['submit'],
                                until=lambda result: result['ready'], interval_seconds=.1)
            if infer is not None and 'decide' not in state.results:
                return CompletionStep('decide', 'Assess the admitted record', {'type': 'object'})
            return CompleteOperation('Record ready')

        def select(request, context):
            data = json.loads(request.input)
            if data.get('handoff'):
                return OperationHandoff('process-record', inputs=data.get('inputs'),
                                        resume_run_id=data['handoff'])
            if data.get('resume'):
                return OperationInvocation('process-record', resume_run_id=data['resume'])
            return OperationInvocation('process-record', inputs=data) if data.get('produce') else None

        authorizer = Policy()
        catalog = CapabilityCatalog(authorizer)
        functions = {'submit': submit, 'read': read}
        source = FunctionSource(tuple(ToolDefinition(name, name, {}) for name in functions), functions)
        catalog.register('records', source, source, target_label='Records')
        agent = SimpleNamespace(config={}, capability_pool=None, initialize=AsyncMock(), shutdown=AsyncMock())
        definitions = (StagedOperation('process-record', 1, next_step),)
        inference = SimpleNamespace(complete=infer) if infer else None
        executor = AgentExecutor(agent, operations=definitions, resolve_operation=select, operation_inference=inference)
        runner = DurableOperationRunner(operations=definitions,
                                        resolve_operation=select, delegation=self.grants,
                                        operation_inference=inference,
                                        policy=policy, poll_seconds=.01, **({'now': now} if now else {}))
        store = self.make_store()
        runtime = Runtime(RuntimeSettings('agent-a', self.path),
                          RuntimeServices(store, executor, authorizer, catalog, delegations=self.grants),
                          modules=(executor, runner))
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        return runtime, runner

    async def finished(self, runner, run_id='run-a'):
        async with asyncio.timeout(4):
            while True:
                record = await runner.get_run(run_id, self.context)
                if record.terminal:
                    return record
                await asyncio.sleep(.01)

    def request(self):
        return RunRequest('run-a', self.context.session_id, 'operation-a', json.dumps({'produce': True}))

    async def test_timeout_continues_without_user_turn_or_repeating_submission(self):
        reads = []

        async def read(args, context):
            self.assertEqual(args, {'id': 'receipt-a'})
            reads.append(args)
            if len(reads) == 1:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read, policy=DurableOperationPolicy(segment_seconds=.15))
        accepted = await runner.submit(self.request(), self.context)
        self.assertEqual(accepted.run_id, 'run-a')
        result = await self.finished(runner)
        self.assertEqual((result.run_id, result.status, result.output, result.segment),
                         ('run-a', 'success', 'Record ready', 1))
        self.assertNotEqual(result.segment_run_id, result.run_id)
        self.assertEqual(len(self.submissions), 1)
        raw = await runtime.get_run(result.segment_run_id, self.context)
        self.assertEqual(raw.origin_run_id, 'run-a')
        self.assertEqual((await runner.submit(self.request(), self.context)).output, 'Record ready')
        self.assertEqual(self.grants.calls, 1)
        events = await runner.events('run-a', 0, self.context)
        self.assertTrue(any(event.kind == 'run.operation.segment.timed_out' for event in events))
        self.assertTrue(any(event.kind == 'run.success' for event in events))
        self.assertEqual(len({event.cursor for event in events}), len(events))

    async def test_restart_reauthenticates_and_resumes_the_existing_receipt(self):
        entered = asyncio.Event()
        ready = False

        async def read(args, context):
            self.assertEqual(args, {'id': 'receipt-a'})
            entered.set()
            if not ready:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        await runtime.close()
        ready = True
        recovered, next_runner = await self.runtime(read)
        result = await self.finished(next_runner)
        self.assertEqual((result.status, result.output), ('success', 'Record ready'))
        self.assertEqual(len(self.submissions), 1)
        self.assertEqual(self.grants.calls, 1)

    async def test_cancel_origin_stops_the_whole_operation(self):
        entered = asyncio.Event()

        async def read(args, context):
            entered.set()
            await asyncio.Future()

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        result = await runner.cancel('run-a', self.context)
        self.assertEqual(result.status, 'cancelled')
        await runtime.close()
        recovered, next_runner = await self.runtime(read)
        await next_runner.tick()
        self.assertEqual((await next_runner.get_run('run-a', self.context)).status, 'cancelled')
        self.assertEqual(len(self.submissions), 1)

    async def test_revoked_grant_prevents_another_segment(self):
        entered = asyncio.Event()

        async def read(args, context):
            entered.set()
            await asyncio.Future()

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        await runtime.close()
        self.grants.revoked = True
        recovered, next_runner = await self.runtime(read)
        result = await self.finished(next_runner)
        self.assertEqual(result.status, 'failed')
        self.assertEqual(len(self.submissions), 1)

    async def test_cancel_origin_after_rollover_stops_current_segment(self):
        reads = 0
        second_segment = asyncio.Event()

        async def read(args, context):
            nonlocal reads
            reads += 1
            if reads > 1:
                second_segment.set()
            await asyncio.Future()

        runtime, runner = await self.runtime(read, policy=DurableOperationPolicy(segment_seconds=.15))
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(second_segment.wait(), 3)
        current = await runner.get_run('run-a', self.context)
        self.assertEqual(current.segment, 1)
        await runner.cancel('run-a', self.context)
        self.assertEqual((await runtime.get_run(current.segment_run_id, self.context)).status, 'cancelled')
        await runtime.close()
        recovered, next_runner = await self.runtime(read)
        await next_runner.tick()
        self.assertEqual((await next_runner.get_run('run-a', self.context)).status, 'cancelled')
        self.assertEqual(len(self.submissions), 1)

    async def test_cancelling_a_finished_operation_preserves_result(self):
        async def read(args, context):
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await self.finished(runner)
        result = await runner.cancel('run-a', self.context)
        self.assertEqual((result.status, result.output), ('success', 'Record ready'))

    async def test_interactive_authority_cannot_be_replayed_by_another_principal(self):
        async def read(args, context):
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await self.finished(runner)
        foreign = replace(self.context, author=replace(self.context.author, subject_id='other-owner'))
        with self.assertRaises((PermissionError, ValueError)):
            await runner.submit(self.request(), foreign)
        with self.assertRaises(PermissionError):
            await runner.submit(replace(self.request(), run_id='run-b'), replace(self.context, deferred=True, delegation_id='grant-run-a'))
        self.assertEqual(self.grants.calls, 1)

    async def test_restart_before_first_checkpoint_preserves_origin(self):
        async def read(args, context):
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        entered = asyncio.Event()

        async def admitted_before_checkpoint(request, context, current_runtime):
            entered.set()
            await asyncio.Future()

        runner._executor.execute = admitted_before_checkpoint
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        self.assertEqual(await runtime.services.store.operation_events('run-a'), ())
        await runtime.close()
        recovered, next_runner = await self.runtime(read)
        result = await self.finished(next_runner)
        self.assertEqual((result.status, result.output), ('success', 'Record ready'))
        self.assertEqual(len(self.submissions), 1)

    async def test_expired_lifetime_stops_continuation(self):
        async def read(args, context):
            await asyncio.Future()

        runtime, runner = await self.runtime(read, policy=DurableOperationPolicy(lifetime_seconds=.15, segment_seconds=.15))
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual(result.status, 'failed')
        self.assertEqual((await runtime.services.store.durable_operation('run-a'))['state'], 'expired')
        self.assertTrue(any(event.kind == 'run.failed' for event in await runner.events('run-a', 0, self.context)))

    async def test_segment_limit_stops_continuation(self):
        async def read(args, context):
            await asyncio.Future()

        runtime, runner = await self.runtime(read, policy=DurableOperationPolicy(segment_seconds=.15, max_segments=1))
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual(result.status, 'failed')
        self.assertEqual((await runtime.services.store.durable_operation('run-a'))['state'], 'exhausted')

    async def test_failed_tool_receipt_does_not_trigger_automatic_retry(self):
        async def read(args, context):
            return {'isError': True, 'content': [{'type': 'text', 'text': 'Uncertain output'}]}

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual((result.status, result.segment), ('failed', 0))
        self.assertEqual(len(self.submissions), 1)

    async def test_uncertain_effect_is_not_repeated_by_a_continuation(self):
        async def read(args, context):
            self.fail('A failed submission has no readable receipt')

        async def uncertain_effect():
            raise TimeoutError

        runtime, runner = await self.runtime(read, submit_behavior=uncertain_effect, submit_idempotent=False)
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual((result.status, result.segment), ('failed', 1))
        self.assertEqual(len(self.submissions), 1)

    async def test_lost_admission_keeps_the_grants_original_expiry(self):
        async def read(args, context):
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        with patch.object(runtime, 'execute_operation', new=AsyncMock(side_effect=OSError('Admission unavailable'))):
            with self.assertRaises(OSError):
                await runner.submit(self.request(), self.context)
        captured = self.grants.captured['run-a']
        await runner.submit(self.request(), self.context)
        await self.finished(runner)
        accepted = await runtime.accepted_request('run-a', self.context)
        self.assertEqual(accepted.request.continuation.expires_at, captured.expires_at)
        self.assertEqual(self.grants.calls, 2)
        self.assertEqual(len(self.submissions), 1)

    async def test_explicit_handoff_reads_existing_receipt_without_new_submission(self):
        ready = False

        async def read(args, context):
            self.assertEqual(args, {'id': 'receipt-a'})
            if not ready:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runtime.submit(replace(self.request(), deadline_seconds=.15), self.context)
        self.assertEqual((await runtime.wait('run-a', self.context)).status, 'timed_out')
        self.assertEqual(self.grants.calls, 0)
        ready = True
        request = RunRequest('run-b', 'session-a', 'handoff-a', json.dumps({'handoff': 'run-a'}))
        await runner.submit(request, self.context)
        result = await self.finished(runner, 'run-b')
        self.assertEqual((result.status, result.output, result.origin_run_id), ('success', 'Record ready', 'run-b'))
        self.assertEqual(len(self.submissions), 1)
        accepted = await runtime.accepted_request('run-b', self.context)
        self.assertEqual(accepted.request.continuation.checkpoint_origin_run_id, 'run-a')
        with self.assertRaises(ValueError):
            await runner.submit(replace(request, run_id='run-c', idempotency_key='handoff-b'), self.context)
        self.assertEqual(len(self.submissions), 1)

    async def test_handoff_cannot_change_input_or_identity(self):
        async def read(args, context):
            await asyncio.Future()

        runtime, runner = await self.runtime(read)
        await runtime.submit(replace(self.request(), deadline_seconds=.15), self.context)
        await runtime.wait('run-a', self.context)
        request = RunRequest('run-b', 'session-a', 'handoff-a', json.dumps({'handoff': 'run-a', 'inputs': {'produce': 'other'}}))
        with self.assertRaises(PermissionError):
            await runner.submit(request, self.context)
        request = replace(request, input=json.dumps({'handoff': 'run-a'}))
        with self.assertRaises(PermissionError):
            await runner.submit(request, replace(self.context, author=replace(self.context.author, subject_id='other-owner')))
        self.assertEqual(self.grants.calls, 0)
        self.assertEqual(len(self.submissions), 1)

    async def test_source_only_handoff_resolves_canonical_inputs_through_legacy_chain(self):
        from openagent_core.operations import _digest
        ready = False

        async def read(args, context):
            if not ready:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runtime.submit(replace(self.request(), deadline_seconds=.15), self.context)
        await runtime.wait('run-a', self.context)
        await runtime.submit(RunRequest('run-m', 'session-a', 'legacy-resume', json.dumps({'resume': 'run-a'}), .15), self.context)
        self.assertEqual((await runtime.wait('run-m', self.context)).status, 'timed_out')
        ready = True
        await runner.submit(RunRequest('run-b', 'session-a', 'handoff-a', json.dumps({'handoff': 'run-m'})), self.context)
        result = await self.finished(runner, 'run-b')
        self.assertEqual((result.status, result.origin_run_id), ('success', 'run-b'))
        self.assertEqual(self.grants.last_binding['handoff_run_id'], 'run-m')
        self.assertEqual(self.grants.last_binding['input_digest'], _digest({'produce': True}))
        self.assertEqual(len(self.submissions), 1)
        accepted = await runtime.accepted_request('run-b', self.context)
        self.assertEqual(accepted.request.continuation.checkpoint_origin_run_id, 'run-a')

    async def test_handoff_survives_a_further_restart_without_repeating_effect(self):
        reads = 0
        handed_off = asyncio.Event()
        ready = False

        async def read(args, context):
            nonlocal reads
            reads += 1
            if reads > 1:
                handed_off.set()
            if not ready:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runtime.submit(replace(self.request(), deadline_seconds=.15), self.context)
        await runtime.wait('run-a', self.context)
        await runner.submit(RunRequest('run-b', 'session-a', 'handoff-a', json.dumps({'handoff': 'run-a'})), self.context)
        await asyncio.wait_for(handed_off.wait(), 3)
        await runtime.close()
        ready = True
        recovered, next_runner = await self.runtime(read)
        result = await self.finished(next_runner, 'run-b')
        self.assertEqual((result.status, result.segment), ('success', 1))
        self.assertEqual(len(self.submissions), 1)

    async def test_handoff_before_checkpoint_cannot_fork_into_legacy_resume(self):
        ready = False

        async def read(args, context):
            if not ready:
                await asyncio.Future()
            return {'ready': True}

        runtime, runner = await self.runtime(read)
        await runtime.submit(replace(self.request(), deadline_seconds=.15), self.context)
        await runtime.wait('run-a', self.context)
        admitted = asyncio.Event()

        async def before_checkpoint(request, context, current_runtime):
            admitted.set()
            await asyncio.Future()

        runner._executor.execute = before_checkpoint
        await runner.submit(RunRequest('run-b', 'session-a', 'handoff-a', json.dumps({'handoff': 'run-a'})), self.context)
        await asyncio.wait_for(admitted.wait(), 3)
        await runtime.close()
        ready = True
        recovered, next_runner = await self.runtime(read)
        self.assertEqual((await self.finished(next_runner, 'run-b')).status, 'success')
        await recovered.submit(RunRequest('run-c', 'session-a', 'legacy-fork', json.dumps({'resume': 'run-a'})), self.context)
        result = await recovered.wait('run-c', self.context)
        self.assertEqual(result.status, 'failed')
        self.assertEqual(result.output['error_type'], 'IdempotencyConflict')
        self.assertEqual(len(self.submissions), 1)

    async def test_readiness_pending_completion_continues_after_bounded_delay(self):
        calls = 0

        async def read(args, context):
            return {'ready': True}

        async def infer(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OperationPending(retry_after_seconds=.15)
            return SimpleNamespace(content='{"ready":true}')

        runtime, runner = await self.runtime(read, infer=infer)
        await runner.submit(self.request(), self.context)
        await runtime.wait('run-a', self.context)
        await runner.tick()
        self.assertEqual((await runtime.services.store.durable_operation('run-a'))['segment'], 0)
        result = await self.finished(runner)
        self.assertEqual((result.status, result.segment, calls), ('success', 1, 2))
        self.assertEqual(len(self.submissions), 1)
        raw = await runtime.get_run('run-a', self.context)
        self.assertEqual(raw.output['reason'], 'operation_pending')

    async def test_generic_completion_failure_is_not_readiness_pending(self):
        calls = 0

        async def read(args, context):
            return {'ready': True}

        async def infer(request):
            nonlocal calls
            calls += 1
            raise RuntimeError('Provider error with unknown outcome')

        runtime, runner = await self.runtime(read, infer=infer)
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual((result.status, result.segment, calls), ('failed', 0, 1))
        self.assertEqual(len(self.submissions), 1)

    async def test_readiness_signal_cannot_retry_a_non_idempotent_submission(self):
        async def read(args, context):
            self.fail('There is no receipt for this effect')

        async def pending():
            raise OperationPending()

        runtime, runner = await self.runtime(read, submit_behavior=pending, submit_idempotent=False)
        await runner.submit(self.request(), self.context)
        result = await self.finished(runner)
        self.assertEqual((result.status, result.segment), ('failed', 0))
        self.assertEqual(len(self.submissions), 1)

    async def test_cancel_before_admission_reserves_the_origin(self):
        async def read(args, context):
            self.fail('A reserved operation must not execute')

        runtime, runner = await self.runtime(read)
        self.assertEqual((await runner.cancel('run-a', self.context, reserve=True)).status, 'cancelled')
        await runner.submit(self.request(), self.context)
        self.assertEqual((await runner.get_run('run-a', self.context)).status, 'cancelled')
        self.assertFalse(self.submissions)

    async def test_read_access_in_another_session_does_not_grant_cancellation(self):
        entered = asyncio.Event()

        async def read(args, context):
            entered.set()
            await asyncio.Future()

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        original = runtime.services.authorizer.authorize

        async def cross_session_read(context, action, resource, *, audience=()):
            if action in {'run.read', 'run.replay'}:
                return True
            return await original(context, action, resource, audience=audience)

        with patch.object(runtime.services.authorizer, 'authorize', new=cross_session_read):
            with self.assertRaises(PermissionError):
                await runner.cancel('run-a', replace(self.context, session_id='session-b'))
        self.assertEqual((await runtime.services.store.durable_operation('run-a'))['state'], 'active')

    async def test_caller_disconnect_drains_logical_and_physical_cancellation(self):
        entered = asyncio.Event()

        async def read(args, context):
            entered.set()
            await asyncio.Future()

        runtime, runner = await self.runtime(read)
        await runner.submit(self.request(), self.context)
        await asyncio.wait_for(entered.wait(), 3)
        stopped = asyncio.Event()
        original_stop = runtime.services.store.stop_durable_operation

        async def pause_after_stop(*args):
            await original_stop(*args)
            self.assertTrue((await runtime.get_run('run-a', self.context)).cancel_requested)
            stopped.set()
            await asyncio.sleep(.05)

        with patch.object(runtime.services.store, 'stop_durable_operation', new=pause_after_stop):
            observer = asyncio.create_task(runner.cancel('run-a', self.context))
            await asyncio.wait_for(stopped.wait(), 3)
            observer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await observer
        self.assertEqual((await runner.get_run('run-a', self.context)).status, 'cancelled')
        self.assertEqual((await runtime.get_run('run-a', self.context)).status, 'cancelled')

    async def test_gateway_sdk_follows_origin_and_cannot_supply_delegation(self):
        from openagent_gateway import create_app
        from openagent_sdk import RunClient

        from openagent_core.runtime import current_run_id
        reads = 0
        second_running, complete_second = asyncio.Event(), asyncio.Event()
        answer = {'text':'ready','attachments':[{'artifact_id':'qualified-fixture'}]}

        async def read(args, context):
            nonlocal reads
            reads += 1
            if reads == 1:
                await runtime.services.store.append_event(current_run_id(), "run.stream",
                    {"kind": "done", "final_answer": {"text": "old"}})
                await asyncio.Future()
            second_running.set()
            await complete_second.wait()
            await runtime.services.store.append_event(current_run_id(), "run.stream",
                {"kind": "done", "final_answer": answer})
            return {'ready': True}

        runtime, runner = await self.runtime(read, policy=DurableOperationPolicy(segment_seconds=.15))

        @asynccontextmanager
        async def identity(request, action, run_id, session_id):
            if request.headers.get('Authorization') != 'Bearer fixture-owner':
                raise PermissionError('Missing identity')
            yield self.context

        app = create_app(runtime, identity, manage_lifecycle=False, durable_operations=runner)
        server = web.AppRunner(app)
        await server.setup()
        self.addAsyncCleanup(server.cleanup)
        site = web.TCPSite(server, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f'http://127.0.0.1:{port}'
        client = RunClient(url, lambda: {'Authorization': 'Bearer fixture-owner'})
        await client.submit(self.request())
        await asyncio.wait_for(second_running.wait(), 4)
        try:
            self.assertIsNone((await client.final_answer('run-a')).final_answer)
        finally:
            complete_second.set()
        async with asyncio.timeout(4):
            result = await client.wait('run-a', interval=.01)
        self.assertEqual((result.run_id, result.origin_run_id, result.output), ('run-a', 'run-a', 'Record ready'))
        segment = await client.get(result.segment_run_id)
        self.assertEqual((segment.run_id, segment.origin_run_id), (result.segment_run_id, 'run-a'))
        events = await client.events('run-a')
        self.assertTrue(any(event.kind == 'run.success' for event in events))
        store = runtime.services.store
        window = store.durable_operation_event_window
        sizes = []
        async def measured(*args):
            rows = await window(*args)
            sizes.append(len(rows))
            return rows
        store.durable_operation_event_window = measured
        async def unbounded(*args):
            self.fail('An origin request must not materialize individual or aggregate transcripts')
        store.events = store.durable_operation_events = unbounded
        seen, cursor = [], 0
        while True:
            page = await client.events_page('run-a', cursor, limit=3)
            seen.extend(page.events)
            cursor = page.cursor
            if not page.has_more:
                break
        self.assertEqual(tuple(seen), events)
        self.assertLessEqual(max(sizes), 4)
        self.assertEqual({row.payload['segment_run_id'] for row in seen}, {'run-a',segment.run_id})
        final = await client.final_answer('run-a')
        self.assertEqual((final.run_id, final.segment_run_id, final.final_answer), ('run-a',segment.run_id,answer))
        async with ClientSession() as http:
            async with http.post(url + '/api/v1/runs', json={
                'run_id': 'forged', 'session_id': 'session-a', 'idempotency_key': 'forged', 'input': '{}',
                'continuation': {'delegation_id': 'grant-run-a'},
            }, headers={'Authorization': 'Bearer fixture-owner'}) as response:
                self.assertEqual(response.status, 400)
