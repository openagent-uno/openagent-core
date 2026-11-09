import asyncio
import base64
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from openagent_core import CapabilityCatalog, ExecutionContext, FunctionSource, PrincipalRef
from openagent_core import RunRequest, Runtime, RuntimeServices, RuntimeSettings, ToolDefinition
from openagent_core.engine import AgentExecutor
from openagent_core.inference import StatelessInference
from openagent_core.models.video_frames import VideoFrame
from openagent_core.operations import CompleteOperation, CompletionStep, OperationInvocation
from openagent_core.operations import StagedOperation, ToolStep, WaitStep, STEP_COMPLETED
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return resource.kind != 'session' or resource.resource_id == context.session_id


class OperationsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        principal = PrincipalRef('host-a', 'tenant-a', 'operator-a')
        self.context = ExecutionContext(principal, principal, principal, 'session-a', 'agent-a', (principal,))

    async def runtime(self, next_step, tools, *, infer=None, extractor=None, operation_extractor=None):
        policy = Policy()
        catalog = CapabilityCatalog(policy)
        source = FunctionSource(tuple(ToolDefinition(name, name, {}) for name in tools), tools)
        catalog.register('records', source, source, target_label='Records')
        agent = SimpleNamespace(config={}, capability_pool=None,
                                initialize=AsyncMock(), shutdown=AsyncMock())
        executor = AgentExecutor(
            agent, operations=(StagedOperation('processing', 1, next_step),),
            resolve_operation=lambda request, context: OperationInvocation('processing', **json.loads(request.input)),
            operation_inference=StatelessInference(SimpleNamespace(infer=infer)) if infer else None,
        )
        store = SqliteRuntimeStore(self.path / 'runtime.db')
        services = RuntimeServices(store, executor, policy, catalog)
        if extractor:
            services.registry.bind('media.tool_video_frame_extractor', extractor)
        if operation_extractor:
            services.registry.bind('media.operation_video_frame_extractor', operation_extractor)
        runtime = Runtime(RuntimeSettings('agent-a', self.path), services, modules=(executor,))
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        return runtime

    async def submit(self, runtime, run_id, *, context=None, **invocation):
        context = context or self.context
        return await runtime.submit(RunRequest(run_id, context.session_id, run_id, json.dumps(invocation)), context)

    async def test_plan_wait_and_visual_completion_use_current_authority_and_bounded_inputs(self):
        await self.exercise_visual_operation(dedicated_extractor=False)

    async def test_visual_completion_prefers_the_operation_extractor(self):
        await self.exercise_visual_operation(dedicated_extractor=True)

    async def exercise_visual_operation(self, *, dedicated_extractor):
        calls = []
        observations = []
        video_blob = base64.b64encode(b'approved encoded video').decode()
        preview = {'content': [{'type': 'text', 'text': 'source offset 2s'}, {'type': 'resource', 'resource': {
            'mimeType': 'video/mp4', 'uri': 'resource://sample', 'blob': video_blob,
        }}]}

        async def tool(args, context):
            self.assertEqual(context, self.context)
            calls.append(dict(args))
            if args.get('kind') == 'video':
                return preview
            if args.get('kind') == 'wait':
                return {'structuredContent': {'ready': calls.count(args) == 2}}
            return {'structuredContent': {'accepted': True}, 'isError': False}

        async def extract(content, mime_type):
            self.assertEqual((content, mime_type), (b'approved encoded video', 'video/mp4'))
            return (VideoFrame(b'approved JPEG sample', 1.2),)

        async def infer(request):
            observations.append(request)
            self.assertEqual(len(request.messages), 1)
            self.assertIn('JSON Schema', request.instructions)
            if request.images:
                self.assertIn('source offset 2s', request.messages[0]['content'])
                self.assertIn('1.200s', request.messages[0]['content'])
                self.assertEqual(request.images[0].content, b'approved JPEG sample')
                return SimpleNamespace(content='{"accepted":true}')
            self.assertEqual(request.messages[0]['content'], 'Short brief')
            return SimpleNamespace(content='{"accepted":true}')

        schema = {'type': 'object', 'properties': {'accepted': {'type': 'boolean'}},
                  'required': ['accepted'], 'additionalProperties': False}

        def next_step(state):
            if 'plan' not in state.results:
                return CompletionStep('plan', state.inputs['brief'], schema)
            if 'submit' not in state.results:
                return ToolStep('submit', 'records', 'execute', {'kind': 'submit', 'key': state.idempotency_key('submit')})
            if 'wait' not in state.results:
                return WaitStep('wait', 'records', 'execute', {'kind': 'wait'},
                                until=lambda result: result['structuredContent']['ready'], interval_seconds=.1)
            if 'review' not in state.results:
                return CompletionStep('review', 'Inspect the evidence', schema,
                                      video_tool=ToolStep('preview', 'records', 'execute', {'kind': 'video'}))
            if state.results['review']['accepted'] and 'publish' not in state.results:
                return ToolStep('publish', 'records', 'execute', {'kind': 'publish'})
            return CompleteOperation('Publication complete')

        preview_extractor = SimpleNamespace(extract=extract)
        fallback = SimpleNamespace(extract=AsyncMock(side_effect=AssertionError('ordinary chat extractor used')))
        runtime = await self.runtime(next_step, {'execute': tool}, infer=infer,
                                     extractor=fallback if dedicated_extractor else preview_extractor,
                                     operation_extractor=preview_extractor if dedicated_extractor else None)
        await self.submit(runtime, 'run-a', inputs={'brief': 'Short brief'})
        record = await runtime.wait('run-a', self.context)
        self.assertEqual((record.status, record.output), ('success', 'Publication complete'))
        self.assertEqual([args['kind'] for args in calls], ['submit', 'wait', 'wait', 'video', 'publish'])
        self.assertEqual(len(observations), 2)
        events = await runtime.services.store.operation_events('run-a')
        finished = [event.payload for event in events if event.kind == STEP_COMPLETED]
        self.assertEqual([row['key'] for row in finished], ['plan', 'submit', 'wait', 'review', 'publish'])
        self.assertNotIn(video_blob, json.dumps(finished))

    async def test_interrupt_resume_reuses_checkpoints_and_single_use_ancestry(self):
        calls, planned = [], []
        waiting = asyncio.Event()
        ready = False

        async def submit(args, _context):
            calls.append(args['key'])
            return {'id': 'job-a'}

        async def read(args, _context):
            self.assertEqual(args['id'], 'job-a')
            waiting.set()
            return {'ready': ready}

        def next_step(state):
            self.assertEqual(state.inputs['brief'], 'Accepted brief')
            key = state.idempotency_key('submit')
            planned.append((state.origin_run_id, key))
            if 'submit' not in state.results:
                return ToolStep('submit', 'records', 'submit', {'key': key}, idempotent=True)
            self.assertEqual(state.result_run_ids['submit'], 'run-a')
            if 'wait' not in state.results:
                return WaitStep('wait', 'records', 'read', state.results['submit'], until=lambda result: result['ready'])
            return CompleteOperation('Ready')

        tools = {'submit': submit, 'read': read}
        runtime = await self.runtime(next_step, tools)
        await self.submit(runtime, 'run-a', inputs={'brief': 'Accepted brief'})
        await asyncio.wait_for(waiting.wait(), 3)
        await runtime.close()

        ready = True
        recovered = await self.runtime(next_step, tools)
        self.assertEqual((await recovered.get_run('run-a', self.context)).status, 'interrupted')
        alternate = replace(self.context.author, subject_id='operator-b')
        foreign = replace(self.context, author=alternate, initiator=alternate, authority=alternate, audience=(alternate,))
        await self.submit(recovered, 'foreign', context=foreign, resume_run_id='run-a')
        self.assertEqual((await recovered.wait('foreign', foreign)).status, 'failed')
        await self.submit(recovered, 'run-b', resume_run_id='run-a')
        outcome = await recovered.wait('run-b', self.context)
        self.assertEqual((outcome.status, outcome.output), ('success', 'Ready'))
        await self.submit(recovered, 'run-c', resume_run_id='run-a')
        self.assertEqual((await recovered.wait('run-c', self.context)).status, 'failed')
        self.assertEqual(len(calls), 1)
        self.assertEqual(set(planned), {('run-a', calls[0])})

    async def test_mcp_error_and_uncertain_effect_block_following_stage_and_resume(self):
        calls = []

        async def tool(args, _context):
            calls.append(args)
            return {'isError': True, 'content': [{'type': 'text', 'text': 'unavailable'}]}

        def next_step(state):
            if 'effect' not in state.results:
                return ToolStep('effect', 'records', 'write', {'value': 1})
            return CompleteOperation('Finished')

        runtime = await self.runtime(next_step, {'write': tool})
        await self.submit(runtime, 'run-a', inputs={})
        self.assertEqual((await runtime.wait('run-a', self.context)).status, 'failed')
        await self.submit(runtime, 'run-b', resume_run_id='run-a')
        self.assertEqual((await runtime.wait('run-b', self.context)).status, 'failed')
        self.assertEqual(calls, [{'value': 1}])

    async def test_invalid_structured_completion_cannot_publish(self):
        async def infer(request):
            return SimpleNamespace(content='{"accepted":false,"accepted":true}')

        def next_step(state):
            if 'review' not in state.results:
                return CompletionStep('review', 'Inspect the record', {'type': 'object'})
            return CompleteOperation('Should never be published')

        runtime = await self.runtime(next_step, {}, infer=infer)
        await self.submit(runtime, 'run-a', inputs={})
        result = await runtime.wait('run-a', self.context)
        self.assertEqual(result.status, 'failed')
        self.assertNotIn('published', str(result.output))

    async def test_cancellation_stops_pending_stage_and_prevents_resume(self):
        entered = asyncio.Event()
        calls = 0

        async def tool(_args, _context):
            nonlocal calls
            calls += 1
            entered.set()
            await asyncio.Future()

        def next_step(_state):
            return WaitStep('wait', 'records', 'read', {}, until=lambda result: True)

        runtime = await self.runtime(next_step, {'read': tool})
        await self.submit(runtime, 'run-a', inputs={})
        await asyncio.wait_for(entered.wait(), 3)
        await runtime.cancel('run-a', self.context)
        self.assertEqual((await runtime.wait('run-a', self.context)).status, 'cancelled')
        await self.submit(runtime, 'run-b', resume_run_id='run-a')
        self.assertEqual((await runtime.wait('run-b', self.context)).status, 'failed')
        self.assertEqual(calls, 1)
