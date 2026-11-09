import asyncio
import base64
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core import CapabilityCatalog, ExecutionContext, FunctionSource, PrincipalRef
from openagent_core import RunRequest, Runtime, RuntimeServices, RuntimeSettings, ToolDefinition
from openagent_core.engine import AgentExecutor
from openagent_core.inference import StatelessInference
from openagent_core.models.video_frames import VideoFrame
from openagent_core.operations import CompleteOperation, CompletionStep, OperationError, OperationInvocation
from openagent_core.operations import StagedOperation, ToolStep, WaitStep, STEP_COMPLETED, _image_evidence, _step_digest
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    allow_tool = True

    async def authorize(self, context, action, resource, *, audience=()):
        if action == 'tool.call':
            return self.allow_tool
        return resource.kind != 'session' or resource.resource_id == context.session_id


PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ipxkAAAAASUVORK5CYII='
IMAGE = {'type': 'image', 'mimeType': 'image/png', 'data': PNG}


class OperationsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        principal = PrincipalRef('host-a', 'tenant-a', 'operator-a')
        self.context = ExecutionContext(principal, principal, principal, 'session-a', 'agent-a', (principal,))

    async def runtime(self, next_step, tools, *, infer=None, extractor=None, operation_extractor=None, policy=None):
        policy = policy or Policy()
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
            self.assertEqual(request.response_schema, schema)
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

    async def test_image_completion_uses_exact_catalog_and_checkpoints_only_the_decision(self):
        calls, requests = [], []
        policy = Policy()

        async def read(args, context):
            self.assertEqual(context, self.context)
            self.assertEqual(args, {'id': 'record-a'})
            calls.append(args)
            return {'content': [{'type': 'text', 'text': 'first authorized still'}, IMAGE,
                                {'type': 'text', 'text': 'second authorized still'}, IMAGE]}

        async def infer(request):
            requests.append(request)
            self.assertEqual([(image.content, image.mime_type) for image in request.images],
                             [(base64.b64decode(PNG), 'image/png')] * 2)
            prompt = request.messages[0]['content']
            self.assertIn('first authorized still\n[Image 1]\nsecond authorized still\n[Image 2]', prompt)
            self.assertNotIn(PNG, prompt)
            return SimpleNamespace(content='{"accepted":true}')

        def next_step(state):
            if 'review' not in state.results:
                return CompletionStep('review', 'Inspect the still', {'type': 'object'},
                                      image_tool=ToolStep('preview', 'records', 'read', {'id': 'record-a'}, idempotent=True))
            return CompleteOperation('Reviewed')

        runtime = await self.runtime(next_step, {'read': read}, infer=infer, policy=policy)
        await self.submit(runtime, 'run-a', inputs={})
        self.assertEqual((await runtime.wait('run-a', self.context)).output, 'Reviewed')
        checkpoints = await runtime.services.store.operation_events('run-a')
        self.assertEqual(checkpoints[-1].payload['output'], {'accepted': True})
        self.assertNotIn(PNG, str(checkpoints))
        self.assertIn(PNG, str(await runtime.events('run-a', 0, self.context)))

        policy.allow_tool = False
        await self.submit(runtime, 'run-b', inputs={})
        self.assertEqual((await runtime.wait('run-b', self.context)).status, 'failed')
        self.assertEqual((len(calls), len(requests)), (1, 1))

    def test_image_evidence_rejects_incomplete_or_out_of_bounds_results(self):
        tool = ToolStep('preview', 'records', 'read')
        with self.assertRaises(ValueError):
            CompletionStep('review', 'Inspect', {}, video_tool=tool, image_tool=tool)
        self.assertEqual(CompletionStep('review', 'Inspect', {}, tool, 25).timeout_seconds, 25)
        for blocks in ([], [{'type': 'text', 'text': 'no image'}], [IMAGE] * 25,
                       [IMAGE, {'type': 'resource_link', 'uri': 'https://example.invalid/'}],
                       [IMAGE, {**IMAGE, 'data': 'invalid!'}], [{**IMAGE, 'mimeType': 'image/jpeg'}]):
            with self.subTest(blocks=len(blocks)), self.assertRaises(OperationError):
                _image_evidence({'content': blocks})
        with patch('openagent_core.inference.MAX_INFERENCE_IMAGE_BYTES', len(base64.b64decode(PNG))):
            with self.assertRaises(OperationError):
                _image_evidence({'content': [IMAGE, IMAGE]})

    async def test_completion_retries_invalid_output_without_rereading_or_replaying_content(self):
        evidence_calls, requests = [], []
        invalid = ('private malformed response', '{"accepted":"private schema mismatch"}')
        responses = iter((*invalid, '{"accepted":true}'))

        async def read(args, context):
            self.assertEqual(context, self.context)
            evidence_calls.append(args)
            return {'content': [IMAGE]}

        async def infer(request):
            requests.append(request)
            return SimpleNamespace(content=next(responses))

        def next_step(state):
            if 'review' in state.results:
                return CompleteOperation('Reviewed')
            schema = {'type': 'object', 'properties': {'accepted': {'type': 'boolean'}},
                      'required': ['accepted']}
            return CompletionStep('review', 'Inspect the still', schema, max_attempts=3,
                                  image_tool=ToolStep('preview', 'records', 'read', {'id': 'record-a'}))

        runtime = await self.runtime(next_step, {'read': read}, infer=infer)
        await self.submit(runtime, 'run-a', inputs={})
        self.assertEqual((await runtime.wait('run-a', self.context)).output, 'Reviewed')
        self.assertEqual(evidence_calls, [{'id': 'record-a'}])
        self.assertEqual(len(requests), 3)
        for request in requests[1:]:
            self.assertEqual((request.messages, request.images), (requests[0].messages, requests[0].images))
            self.assertIn('meets every schema constraint', request.instructions)
        checkpoints = await runtime.services.store.operation_events('run-a')
        self.assertEqual(checkpoints[-1].payload['output'], {'accepted': True})
        for content in invalid:
            self.assertNotIn(content, str(requests))
            self.assertNotIn(content, str(checkpoints))

    async def test_completion_retry_limit_default_and_provider_error(self):
        calls = []
        schema = {'type': 'object'}
        default = CompletionStep('plan', 'Plan', schema)
        tool = ToolStep('preview', 'records', 'read', {'id': 'record-a'})
        previous = (
            (default, '3f890a3d8aa170186c703e8f0d76a5bc046bfce67176da5e6fe561b926acfcb7'),
            (replace(default, image_tool=tool), 'fcb8234296194e615b780456fad79deba3219e841d66fe6b9a157915ab12cccb'),
            (replace(default, video_tool=tool), 'cafe41c69e50ab83046267925137a190541f3ab583f3547ae25bc93f8fef58df'),
        )
        for step, digest in previous:
            self.assertEqual(_step_digest(step), digest)
            self.assertEqual(_step_digest(replace(step, max_attempts=1)), digest)
        self.assertNotEqual(_step_digest(default), _step_digest(replace(default, max_attempts=2)))
        for count in (0, 4, True, 2.5):
            with self.subTest(count=count), self.assertRaises(ValueError):
                replace(default, max_attempts=count)

        async def infer(request):
            prompt = request.messages[0]['content']
            calls.append(prompt)
            if prompt == 'provider error':
                raise OperationError('Provider failure')
            return SimpleNamespace(content='invalid')

        def next_step(state):
            return replace(default, **state.inputs)

        runtime = await self.runtime(next_step, {}, infer=infer)
        cases = ({}, {'prompt': 'invalid twice', 'max_attempts': 2},
                 {'prompt': 'provider error', 'max_attempts': 3})
        for index, inputs in enumerate(cases):
            run_id = f'run-{index}'
            await self.submit(runtime, run_id, inputs=inputs)
            self.assertEqual((await runtime.wait(run_id, self.context)).status, 'failed')
        self.assertEqual(calls, ['Plan', 'invalid twice', 'invalid twice', 'provider error'])

    async def test_completion_retry_cancels_within_the_stage_deadline(self):
        calls = []

        async def infer(request):
            calls.append(request)
            if len(calls) == 1:
                return SimpleNamespace(content='invalid')
            await asyncio.Event().wait()

        runtime = await self.runtime(
            lambda state: CompletionStep('plan', 'Plan', {'type': 'object'}, timeout_seconds=0.2, max_attempts=3),
            {}, infer=infer,
        )
        await self.submit(runtime, 'run-a', inputs={})
        self.assertEqual((await runtime.wait('run-a', self.context)).status, 'timed_out')
        self.assertEqual(len(calls), 2)

    async def test_interrupted_image_completion_replays_only_declared_idempotent_read(self):
        calls, inference_count = [], 0

        async def read(args, _context):
            calls.append(args['key'])
            return {'content': [IMAGE]}

        async def infer(_request):
            nonlocal inference_count
            inference_count += 1
            if inference_count <= 2:
                raise TimeoutError
            return SimpleNamespace(content='{}')

        def next_step(state):
            if 'review' in state.results:
                return CompleteOperation('Reviewed')
            return CompletionStep('review', 'Inspect', {'type': 'object'}, image_tool=ToolStep(
                'preview', 'records', 'read', {'key': state.idempotency_key('preview')},
                idempotent=state.inputs['idempotent'],
            ))

        runtime = await self.runtime(next_step, {'read': read}, infer=infer)
        for run_id, idempotent in (('run-a', False), ('run-b', True)):
            await self.submit(runtime, run_id, inputs={'idempotent': idempotent})
            self.assertEqual((await runtime.wait(run_id, self.context)).status, 'timed_out')
        await self.submit(runtime, 'run-c', resume_run_id='run-a')
        self.assertEqual((await runtime.wait('run-c', self.context)).status, 'failed')
        await self.submit(runtime, 'run-d', resume_run_id='run-b')
        self.assertEqual((await runtime.wait('run-d', self.context)).output, 'Reviewed')
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[1], calls[2])
        self.assertNotEqual(calls[0], calls[1])

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
