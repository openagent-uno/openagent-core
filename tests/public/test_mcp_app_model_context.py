from dataclasses import replace
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import tempfile
import unittest

from openagent_core import Runtime,RuntimeServices,RuntimeSettings,RunRequest
from openagent_core.engine import Agent,AgentExecutor
from openagent_core.mcp.pool import MCPPool
from openagent_core.mcp_apps import McpAppService,current_application_context,saved_invocation_display
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.runtime import execution_scope
from openagent_core.models.base import BaseModel
from openagent_storage_sqlite import SqliteRuntimeStore
from test_mcp_apps_resources import McpAppResourceTests

class AppModelContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await McpAppResourceTests.asyncSetUp(self)
        self.apps=McpAppService(self.catalog)
        self.opened=await self.apps.open(self.tool.tool_ref,self.context)
    async def asyncTearDown(self):
        await self.apps.shutdown()
    async def update(self,value,request_id='state'):
        return await self.apps.request(self.opened['handle'],'ui/update-model-context',value,self.context,request_id=request_id)
    async def test_app_state_reaches_real_engine_prompt_without_rewriting_user_input(self):
        await self.update({'structuredContent':{'selected_product':'fixture-only-product'}})
        observed=[]; inputs=[]
        class CaptureModel(BaseModel):
            model='fixture'
            async def stream(self,messages,system=None,**kwargs):
                observed.append(system); inputs.append(str(messages))
                yield 'Okay'
            async def generate(self,*args,**kwargs):return 'Okay'
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);store=SqliteRuntimeStore(directory/'state.sqlite3')
            agent=Agent(model=CaptureModel(),mcp_pool=MCPPool.from_config([],include_defaults=False),system_prompt='Host rules',config={'_enabled_prompt_modules':[]})
            executor=AgentExecutor(agent)
            runtime=Runtime(RuntimeSettings('agent',directory),RuntimeServices(store,executor,self.policy,self.catalog),modules=(executor,))
            runtime.services.registry.bind('mcp_apps',self.apps)
            await runtime.start()
            try:
                await runtime.submit(RunRequest('run','session','one','hello'),self.context)
                result=await runtime.wait('run',self.context)
                self.assertEqual(result.status,'success')
                self.assertTrue(any('fixture-only-product' in str(system) and 'untrusted_app_context' in str(system) for system in observed))
                self.assertTrue(any('hello' in value for value in inputs))
                self.assertFalse(any('fixture-only-product' in value for value in inputs))
                self.assertEqual(current_application_context(),{})
                await self.apps.close(self.opened['handle'],self.context)
                observed.clear()
                await runtime.submit(RunRequest('run-two','session','two','continue'),self.context)
                self.assertEqual((await runtime.wait('run-two',self.context)).status,'success')
                self.assertTrue(observed)
                self.assertFalse(any('untrusted_app_context' in str(system) for system in observed))
            finally:await runtime.close()
    async def test_context_requires_same_viewer_session_and_current_source(self):
        await self.update({'content':[{'type':'text','text':'selected fixture row'}]})
        value=await self.apps.prompt_context(self.context)
        self.assertTrue(value['untrusted']);self.assertEqual(value['applications'][0]['source_id'],'left')
        self.assertEqual(await self.apps.prompt_context(replace(self.context,session_id='other')), {})
        self.assertEqual(await self.apps.prompt_context(replace(self.context,deferred=True,delegation_id='owned-test-delegation')), {})
        self.catalog.revoke('left')
        value=await self.apps.prompt_context(self.context)
        self.assertEqual(value['applications'],[]);self.assertEqual(value['unavailable_count'],1)
    async def test_bounded_state_cannot_replace_prior_context_on_invalid_update(self):
        await self.update({'structuredContent':{'selection':'kept'}})
        for value in [{'structuredContent':{'text':'x'*(64*1024)}},{'authority':'forged'}]:
            with self.assertRaises(ValueError):await self.update(value,request_id=str(len(str(value))))
        self.assertEqual((await self.apps.prompt_context(self.context))['applications'][0]['data']['structuredContent']['selection'],'kept')
        await self.update({},request_id='clear')
        self.assertEqual(await self.apps.prompt_context(self.context),{})

    async def test_legacy_uri_spelling_is_normalized_without_changing_the_tool_binding(self):
        self.left.tools=(replace(self.left.tools[0],metadata={'ui/resourceUri':'ui://fixture/counter'}),self.left.tools[1])
        tool=next(value for value in await self.catalog.discover(self.context) if value.source_id=='left' and value.name=='show')
        opened=await self.apps.open(tool.tool_ref,self.context)
        self.assertEqual(opened['tool']['_meta']['ui']['resourceUri'],'ui://fixture/counter')
        self.assertEqual(opened['tool']['_meta']['ui/resourceUri'],'ui://fixture/counter')
        described=await self.catalog.describe_tool(tool.tool_ref,self.context)
        self.assertEqual(described.metadata,{'ui/resourceUri':'ui://fixture/counter'})

    async def test_original_result_arrives_after_open_and_never_crosses_revocation(self):
        state={'toolInput':{'fixture':'pending'},'toolStatus':'running'}
        async def read(context):
            self.assertEqual(context,self.context)
            return dict(state)
        opened=await self.apps.open(self.tool.tool_ref,self.context,display_reader=read)
        self.assertNotIn('toolResult',opened)
        pending=await self.apps.request(opened['handle'],'authorize',{},self.context)
        self.assertEqual(pending['display']['toolStatus'],'running')
        state.update(toolStatus='success',toolResult={'content':[{'type':'text','text':'completed later'}]})
        done=await self.apps.request(opened['handle'],'authorize',{},self.context)
        self.assertEqual(done['display']['toolResult']['content'][0]['text'],'completed later')
        self.assertNotIn('display',await self.apps.request(opened['handle'],'authorize',{'display_received':True},self.context))
        self.catalog.revoke('left')
        with self.assertRaises((PermissionError,LookupError,RuntimeError)):
            await self.apps.request(opened['handle'],'authorize',{},self.context)

    async def test_original_display_reader_is_bounded_and_source_rechecked(self):
        async def oversized(_context):return {'toolResult':{'data':'x'*(8*1024*1024)}}
        before=len(self.apps.sessions)
        with self.assertRaises(ValueError):await self.apps.open(self.tool.tool_ref,self.context,display_reader=oversized)
        self.assertEqual(len(self.apps.sessions),before)
        async def revoked(_context):
            self.catalog.revoke('left')
            return {'toolResult':{'content':[]}}
        with self.assertRaises((PermissionError,LookupError,RuntimeError)):
            await self.apps.open(self.tool.tool_ref,self.context,display_reader=revoked)


    async def test_close_or_shutdown_during_authorization_never_starts_a_mutation(self):
        # The app operation has not entered its request ledger while _resolve
        # waits for policy. Closing at that point must fence admission itself.
        for action in ('close', 'shutdown'):
            with self.subTest(action=action):
                service = McpAppService(self.catalog)
                opened = await service.open(self.tool.tool_ref, self.context)
                entered, release = asyncio.Event(), asyncio.Event()
                original = self.policy.authorize
                async def waiting_policy(context, operation, resource, *, audience=()):
                    if operation == 'tool.call':
                        entered.set()
                        await release.wait()
                    return await original(context, operation, resource, audience=audience)
                self.policy.authorize = waiting_policy
                task = asyncio.create_task(service.request(opened['handle'], 'tools/call',
                    {'name':'increment'}, self.context, request_id='never-dispatch'))
                try:
                    await asyncio.wait_for(entered.wait(), 5)
                    if action == 'close':
                        await service.close(opened['handle'], self.context)
                    else:
                        await service.shutdown()
                    release.set()
                    with self.assertRaises(CapabilityUnavailable):
                        await task
                    self.assertEqual(self.left.calls, [])
                finally:
                    release.set()
                    if not task.done():task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    self.policy.authorize = original
                    await service.shutdown()

    @asynccontextmanager
    async def invocation_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = SqliteRuntimeStore(directory/'invocations.sqlite3')
            await store.start()
            runtime = Runtime(RuntimeSettings('agent', directory),
                RuntimeServices(store, object(), self.policy, self.catalog))
            try:
                await store.accept(RunRequest('display-run','session','display-run','show'), self.context)
                await store.transition('display-run','running')
                yield runtime, store
            finally:
                await store.close()

    async def test_actual_durable_cancel_is_a_cancellation_notification(self):
        entered = asyncio.Event()
        async def waiting_tool(name, arguments, context):
            entered.set()
            await asyncio.Future()
        self.left.call_tool = waiting_tool
        async with self.invocation_runtime() as (runtime, store):
            with execution_scope(runtime, self.context, 'display-run'):
                task = asyncio.create_task(self.catalog.call_tool(self.tool.tool_ref, {},
                    self.context, call_id='cancelled-original'))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
                display = await saved_invocation_display(store, self.policy, self.context,
                    'cancelled-original', self.tool.tool_ref)
                # Core durably stores cancelled tool execution as error with
                # unknown effects; presentation must not turn that into success.
                self.assertEqual(display['toolStatus'], 'error')
                self.assertTrue(display['toolCancelled'])
                self.assertNotIn('toolResult', display)
            finally:
                if not task.done():task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_actual_durable_null_and_error_have_valid_distinct_displays(self):
        async def null_tool(name, arguments, context):return None
        async def failing_tool(name, arguments, context):raise ValueError('private-provider-detail')
        async with self.invocation_runtime() as (runtime, store):
            self.left.call_tool = null_tool
            with execution_scope(runtime, self.context, 'display-run'):
                self.assertIsNone(await self.catalog.call_tool(self.tool.tool_ref, {},
                    self.context, call_id='null-original'))
            display = await saved_invocation_display(store, self.policy, self.context,
                'null-original', self.tool.tool_ref)
            self.assertEqual(display['toolStatus'], 'success')
            self.assertEqual(display['toolResult'], {'content':[]})
            self.assertNotIn('toolCancelled', display)
            self.left.call_tool = failing_tool
            with execution_scope(runtime, self.context, 'display-run'):
                with self.assertRaises(ValueError):
                    await self.catalog.call_tool(self.tool.tool_ref, {}, self.context,
                        call_id='failed-original')
            failed = await saved_invocation_display(store, self.policy, self.context,
                'failed-original', self.tool.tool_ref)
            self.assertTrue(failed['toolResult']['isError'])
            self.assertNotIn('toolCancelled', failed)
            self.assertNotIn('private-provider-detail', str(failed))
