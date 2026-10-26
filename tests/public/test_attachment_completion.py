"""A send-file call cannot silently disappear from an attested final answer."""
import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core import ExecutionContext, PrincipalRef
from openagent_core.core import output_delivery
from openagent_core.runtime import execution_scope


class DeliveryIsolation(unittest.IsolatedAsyncioTestCase):
    def context(self, session='session'):
        p = PrincipalRef('host', 'tenant', 'alice')
        return ExecutionContext(p, p, p, session, 'agent', (p,))

    async def test_concurrent_run_and_child_intents_never_enter_the_parent_scope(self):
        runtime = object()
        async def one(session, run):
            with execution_scope(runtime, self.context(session), run):
                scope, token = output_delivery.open_scope()
                try:
                    output_delivery.record_delivery('[FILE:/'+run+']')
                    await asyncio.sleep(0)
                    with execution_scope(runtime, self.context('child'), 'child-run'):
                        output_delivery.record_delivery('[FILE:/child]')
                    with execution_scope(runtime, self.context(session), 'another-run'):
                        output_delivery.record_delivery('[FILE:/another]')
                    return scope.markers
                finally: output_delivery.close_scope(scope, token)
        a, b = await asyncio.gather(one('a', 'first'), one('b', 'second'))
        self.assertEqual(['[FILE:/first]'], a)
        self.assertEqual(['[FILE:/second]'], b)

    async def test_nested_scopes_and_closed_inherited_tasks_cannot_leak(self):
        with execution_scope(object(), self.context(), 'run'):
            parent, pt = output_delivery.open_scope()
            child, ct = output_delivery.open_scope()
            output_delivery.record_delivery('[FILE:/child]')
            output_delivery.close_scope(child, ct)
            output_delivery.record_delivery('[FILE:/parent]')
            self.assertEqual(['[FILE:/parent]'], parent.markers)
            self.assertEqual(['[FILE:/child]'], child.markers)
            output_delivery.close_scope(parent, pt)
            output_delivery.record_delivery('[FILE:/late]')
            self.assertEqual(['[FILE:/parent]'], parent.markers)

    async def test_unscoped_legacy_calls_do_not_gain_an_attachment_attestation(self):
        scope, token = output_delivery.open_scope()
        try:
            output_delivery.record_delivery('[FILE:/legacy]')
            self.assertEqual([], scope.markers)
        finally: output_delivery.close_scope(scope, token)

    async def test_no_path_name_or_prose_inference_and_repair_is_bounded(self):
        scope = output_delivery.DeliveryScope(('fixture',), ['[FILE:/CAS/exact]'])
        for _ in range(2):
            self.assertIn('[FILE:/CAS/exact]', scope.repair('Sent exact', iteration=1, cap=25))
        with self.assertRaises(output_delivery.AttachmentDeliveryIncomplete):
            scope.repair('Sent exact', iteration=1, cap=25)
        self.assertIsNone(scope.repair('Done [FILE:/CAS/exact]', iteration=1, cap=25))
        other = output_delivery.DeliveryScope(('fixture',), ['[FILE:/CAS/exact]'])
        with self.assertRaises(output_delivery.AttachmentDeliveryIncomplete):
            other.repair('Done', iteration=2, cap=2)


class ActualAgentToolCompletion(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, mode='stream', *, persistent_omission=False, cancelled=False, cap=25):
        from openagent_core import Runtime, RuntimeServices, RuntimeSettings, RunRequest
        from openagent_storage_sqlite import SqliteRuntimeStore
        from openagent_core.engine import MemoryDB
        from openagent_core.artifacts import ArtifactRepository
        from openagent_core.core.agent import Agent
        from openagent_core.mcp.servers.attachments.adapters import build_runtime_toolkit
        from openagent_core.models.base import ModelResponse
        with TemporaryDirectory() as temp:
            root=Path(temp); db=MemoryDB(str(root/'state.sqlite')); await db.connect()
            store=SqliteRuntimeStore(root/'state.sqlite'); await store.start()
            async def allow(*args, **kwargs): return True
            policy=SimpleNamespace(authorize=allow)
            context=DeliveryIsolation().context()
            repository=ArtifactRepository(db, policy)
            await store.accept(RunRequest('actual-run','session','idempotency','Send report'),context)
            source=root/'report.txt'; source.write_text('result 41')
            class Publisher:
                async def publish(self, current, path):
                    self_context=current
                    assert self_context == context and Path(path) == source
                    return await repository.upload(current, source.read_bytes(), filename='report.txt', session_id=current.session_id)
            runtime=Runtime(RuntimeSettings('agent',root),RuntimeServices(store,SimpleNamespace(agent=SimpleNamespace(memory_db=db)),policy))
            runtime.services.registry.bind('attachments.publisher', Publisher())
            send=build_runtime_toolkit().async_functions['send_file_to_user'].entrypoint
            class Model:
                history_mode='caller'; model='fixture'
                def __init__(self): self.inputs=[]; self.marker=None; self.calls=0
                def effective_model_id(self, _sid=None): return self.model
                async def close_session(self, _sid): pass
                async def response(self, messages):
                    self.inputs.append(messages)
                    if len(self.inputs)==1:
                        self.calls+=1
                        result=await send(str(source)); self.marker=result['marker']
                        return 'Both files attached.'
                    if cancelled: raise asyncio.CancelledError()
                    return 'Both files attached.' if persistent_omission else 'Here is the file. '+self.marker
                async def stream(self,messages,system=None,session_id=None,on_status=None,on_final_answer=None):
                    text=await self.response(messages)
                    if on_final_answer is not None: await on_final_answer({'text':text})
                    yield text
                async def generate(self,messages,**kwargs):
                    text=await self.response(messages)
                    return ModelResponse(content=text,final_answer={'text':text})
            model=Model(); agent=Agent(name='fixture',model=model,memory=None,system_prompt='fixture',config={'shell':{'autoloop_cap':cap}})
            try:
                with execution_scope(runtime,context,'actual-run'):
                    if mode=='stream':
                        events=[e async for e in agent.run_stream('Send report',session_id='session')]
                        if persistent_omission or cap==1:
                            self.assertFalse(any(e.get('final_answer') for e in events if e.get('kind')=='done'))
                            self.assertTrue(events[-1].get('errored'))
                        else:
                            self.assertEqual('Here is the file. '+model.marker, events[-1]['final_answer']['text'])
                            self.assertEqual(1,sum(e.get('kind')=='iteration_break' for e in events))
                    else:
                        if persistent_omission or cap==1:
                            text=await agent.run('Send report',session_id='session')
                            self.assertNotIn('Both files attached.',text)
                            self.assertIn('attachment delivery',text)
                        else:
                            text=await agent.run('Send report',session_id='session')
                            self.assertEqual('Here is the file. '+model.marker,text)
                self.assertEqual(1,model.calls)
                if not persistent_omission and cap>1:
                    self.assertEqual(2,len(model.inputs))
                    self.assertIn('not yet delivered',model.inputs[1][0]['content'])
                return model
            finally:
                await agent.shutdown(); await store.close(); await db.close()

    async def test_stream_repairs_real_authorized_send_tool_without_fabricating_final(self): await self.exercise()
    async def test_generate_repairs_the_same_real_tool(self): await self.exercise('generate')
    async def test_persistent_omission_never_becomes_success(self): await self.exercise(persistent_omission=True)
    async def test_generate_persistent_omission_fails_closed(self): await self.exercise('generate',persistent_omission=True)
    async def test_configured_iteration_cap_is_not_bypassed(self): await self.exercise(cap=1)
    async def test_cancelled_repair_cannot_deliver_or_attest(self):
        with self.assertRaises(asyncio.CancelledError): await self.exercise(cancelled=True)
