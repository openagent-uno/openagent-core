import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from openagent_core import CapabilityCatalog, FunctionSource, ToolDefinition, Runtime, RuntimeSettings, RuntimeServices, PrincipalRef, ExecutionContext, RunRequest
from openagent_core.mcp._runtime.function import Function, FunctionCall
from openagent_core.runtime import current_provider_tool_call_id
from openagent_storage_sqlite import SqliteRuntimeStore


class ToolProvenance(unittest.IsolatedAsyncioTestCase):
    async def test_actual_function_dispatch_retains_engine_id_in_public_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            principal=PrincipalRef('host','tenant','user')
            context=ExecutionContext(principal,principal,principal,'session','agent',(principal,))
            class Allow:
                async def authorize(self,*args,**kwargs): return True
            policy=Allow()
            store=SqliteRuntimeStore(Path(directory)/'state.sqlite')
            catalog=CapabilityCatalog(policy)
            async def read(args,context):
                self.assertEqual(current_provider_tool_call_id(),'model-call-123')
                return {'private':'customer'}
            source=FunctionSource((ToolDefinition('read','Read',{}),),{'read':read})
            catalog.register('shopify',source,source,target_label='Shop')
            async def execute(request,context,runtime):
                ref=(await catalog.discover(context))[0].tool_ref
                async def dispatch():
                    return await catalog.call_tool(ref,{},context)
                function=Function.from_callable(dispatch)
                call=FunctionCall(function=function,call_id='model-call-123',arguments={})
                result=await call.aexecute()
                self.assertEqual(result.status,'success')
                self.assertIsNone(current_provider_tool_call_id())
                return 'done'
            runtime=Runtime(RuntimeSettings('agent',Path(directory)),RuntimeServices(store,SimpleNamespace(execute=execute),policy,catalog))
            await runtime.start()
            try:
                await runtime.submit(RunRequest('run','session','key','human'),context)
                self.assertEqual((await runtime.wait('run',context)).status,'success')
                row=store.connection.execute('SELECT raw_envelope_json FROM tool_invocations').fetchone()
                self.assertEqual(json.loads(row[0])['binding']['provider_call_id'],'model-call-123')
            finally:
                await runtime.close()

    async def test_parallel_and_nested_functions_do_not_share_call_identity(self):
        seen=[]
        async def nested():
            seen.append(current_provider_tool_call_id())
            await asyncio.sleep(.001)
            seen.append(current_provider_tool_call_id())
            return 'ok'
        async def outer():
            seen.append(current_provider_tool_call_id())
            function=Function.from_callable(nested)
            await FunctionCall(function=function,call_id='inner',arguments={}).aexecute()
            seen.append(current_provider_tool_call_id())
            return 'ok'
        await asyncio.gather(
            FunctionCall(function=Function.from_callable(outer),call_id='outer',arguments={}).aexecute(),
            FunctionCall(function=Function.from_callable(nested),call_id='parallel',arguments={}).aexecute())
        self.assertEqual(seen.count('outer'),2)
        self.assertEqual(seen.count('inner'),2)
        self.assertEqual(seen.count('parallel'),2)
        self.assertIsNone(current_provider_tool_call_id())

    async def test_sync_and_failed_calls_restore_scope(self):
        def sync():
            self.assertEqual(current_provider_tool_call_id(),'sync')
            raise ValueError('failed')
        function=Function.from_callable(sync)
        call=FunctionCall(function=function,call_id='sync',arguments={})
        self.assertEqual(call.execute().status,'failure')
        self.assertIsNone(current_provider_tool_call_id())

    async def test_provenance_source_cannot_execute_without_canonical_publisher(self):
        class Allow:
            async def authorize(self,*args,**kwargs): return True
        calls=[]
        async def read(args,context):
            calls.append('executed')
            return 'private'
        async def origins(*args): return ()
        p=PrincipalRef('host','tenant','user')
        context=ExecutionContext(p,p,p,'session','agent',(p,))
        source=FunctionSource((ToolDefinition('read','Read',{}),),{'read':read})
        catalog=CapabilityCatalog(Allow())
        catalog.register('shop',source,source,target_label='Shop',result_provenance=origins)
        ref=(await catalog.discover(context))[0].tool_ref
        with self.assertRaisesRegex(Exception,'canonical provenance publisher'):
            await catalog.call_tool(ref,{},context)
        self.assertEqual(calls,[])

    async def test_live_publication_fences_revocation_and_generated_stream_atomically(self):
        from openagent_core.content_privacy import ContentOrigin, PrivacyRequest, PrivacyFence, ToolResultPrivacyPublisher
        from openagent_core.modules import ServiceRegistry
        from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
        from openagent_storage_sqlite.runtime_privacy import SqliteToolResultPrivacyPublisher
        with tempfile.TemporaryDirectory() as directory:
            principal=PrincipalRef('host','tenant','user')
            context=ExecutionContext(principal,principal,principal,'session','agent',(principal,))
            class Allow:
                async def authorize(self,*args,**kwargs): return True
            policy=Allow()
            store=SqliteRuntimeStore(Path(directory)/'state.sqlite')
            catalog=CapabilityCatalog(policy)
            origin=ContentOrigin('tenant','shopify/app','group',10,('customer:23',))
            request=PrivacyRequest('redaction','tenant','shopify/app','group',10,'redact',('customer:23',))
            async def provenance(descriptor,result,context,call_id):
                return (origin,)
            ledger=None
            async def read(args,context):
                if args.get('revoke'):
                    await ledger.enqueue(request)
                return {'private':'NEVER PUBLISH AFTER REVOCATION'}
            source=FunctionSource((ToolDefinition('read','Read',{}),),{'read':read})
            catalog.register('shop',source,source,target_label='Shop',result_provenance=provenance,privacy_fenced=True)
            calls=[]
            async def execute(request,context,runtime):
                ref=(await catalog.discover(context))[0].tool_ref
                calls.append(await catalog.call_tool(ref,{},context,call_id='first'))
                self.assertEqual(await publisher.origins_for_execution(context,'run'),(origin,))
                from dataclasses import replace
                wrong=replace(context,initiator=PrincipalRef('host','tenant','another-user'))
                with self.assertRaisesRegex(Exception,'admitted authority'):
                    await publisher.origins_for_execution(wrong,'run')
                await store.append_event('run','run.stream',{'kind':'delta','text':'DERIVED PRIVATE LIVE TEXT'})
                try:
                    await catalog.call_tool(ref,{'revoke':True},context,call_id='revoked')
                except PrivacyFence:
                    pass
                else:
                    self.fail('revoked source result was published')
                with self.assertRaises(PrivacyFence):
                    await store.append_event('run','run.stream',{'kind':'delta','text':'LATE PRIVATE TEXT'})
                with self.assertRaises(PrivacyFence):
                    await catalog.call_tool(ref,{},context,call_id='not-admitted')
                return None
            runtime=Runtime(RuntimeSettings('agent',Path(directory)),RuntimeServices(store,SimpleNamespace(execute=execute),policy,catalog))
            await runtime.start()
            try:
                ledger=SqliteContentPrivacyStore(store.path)
                publisher=SqliteToolResultPrivacyPublisher(store,ledger)
                runtime.services.registry.bind(ToolResultPrivacyPublisher,publisher)
                await runtime.submit(RunRequest('run','session','key','human'),context)
                await runtime.wait('run',context)
                rows=store.connection.execute('SELECT tool_call_id,result_json FROM tool_invocations').fetchall()
                self.assertEqual(len(rows),2)
                self.assertIsNotNone(rows[0]['result_json'])
                self.assertIsNone(rows[1]['result_json'])
                self.assertNotIn('LATE PRIVATE TEXT',str(await store.events('run')))
                self.assertGreater((await ledger.progress('redaction')).remaining_revisions,0)
            finally:
                await runtime.close()


    async def test_new_run_scope_does_not_inherit_parent_model_call_identity(self):
        from openagent_core.runtime import provider_tool_call_scope, execution_scope
        p=PrincipalRef('host','tenant','user')
        context=ExecutionContext(p,p,p,'session','agent',(p,))
        runtime=SimpleNamespace()
        with provider_tool_call_scope('parent-model-call'):
            with execution_scope(runtime,context,'child'):
                self.assertIsNone(current_provider_tool_call_id())
            self.assertEqual(current_provider_tool_call_id(),'parent-model-call')
        self.assertIsNone(current_provider_tool_call_id())

    async def test_unattested_tools_stay_visible_but_are_fenced_before_io_after_protected_read(self):
        from openagent_core.content_privacy import ContentOrigin,PrivacyFence,ToolResultPrivacyPublisher
        from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
        from openagent_storage_sqlite.runtime_privacy import SqliteToolResultPrivacyPublisher
        with tempfile.TemporaryDirectory() as directory:
            p=PrincipalRef('host','tenant','user')
            context=ExecutionContext(p,p,p,'session','agent',(p,))
            class Allow:
                async def authorize(self,*args,**kwargs): return True
            policy=Allow()
            store=SqliteRuntimeStore(Path(directory)/'state.sqlite')
            catalog=CapabilityCatalog(policy)
            calls=[]
            async def effect(args,context):
                calls.append(args['tag'])
                return 'ok'
            async def provenance(*args):
                return (ContentOrigin('tenant','shopify/app','shop',1,('customer:23',)),)
            read=FunctionSource((ToolDefinition('read','Read',{}),),{'read':effect})
            writer=FunctionSource((ToolDefinition('safe','Safe',{}),ToolDefinition('unsafe','Unsafe',{})),{'safe':effect,'unsafe':effect})
            catalog.register('shop',read,read,target_label='Shop',result_provenance=provenance,privacy_fenced={'read':True})
            catalog.register('writer',writer,writer,target_label='Writer',privacy_fenced={'safe':True})
            async def execute(request,context,runtime):
                descriptors=await catalog.discover(context)
                refs={d.name:d.tool_ref for d in descriptors}
                await catalog.call_tool(refs['unsafe'],{'tag':'ordinary'},context)
                await catalog.call_tool(refs['read'],{'tag':'read'},context)
                # Discovery is unchanged; only a host-attested writer is admitted.
                self.assertEqual(len(await catalog.discover(context)),3)
                with self.assertRaises(PrivacyFence):
                    await catalog.call_tool(refs['unsafe'],{'tag':'must-not-execute'},context)
                await catalog.call_tool(refs['safe'],{'tag':'safe'},context)
                return 'done'
            runtime=Runtime(RuntimeSettings('agent',Path(directory)),RuntimeServices(store,SimpleNamespace(execute=execute),policy,catalog))
            await runtime.start()
            try:
                ledger=SqliteContentPrivacyStore(store.path)
                runtime.services.registry.bind(ToolResultPrivacyPublisher,SqliteToolResultPrivacyPublisher(store,ledger))
                await runtime.submit(RunRequest('run','session','key','human'),context)
                self.assertEqual((await runtime.wait('run',context)).status,'success')
                self.assertEqual(calls,['ordinary','read','safe'])
            finally:
                await runtime.close()
