import json
import unittest
from dataclasses import replace
from types import SimpleNamespace
from openagent_core.capabilities import CapabilityCatalog,FunctionSource,ToolDefinition
from openagent_core.contracts import ExecutionContext,PrincipalRef
from openagent_core.execution_adapters import ToolInventoryContext
from test_mcp_catalog import Policy

class InventoryAdapter(unittest.IsolatedAsyncioTestCase):
    async def test_optional_bounded_scoped_inventory_and_deferred_work(self):
        policy=Policy();catalog=CapabilityCatalog(policy);p=PrincipalRef('host','tenant','person');ctx=ExecutionContext(p,p,p,'session','agent',(p,))
        async def inventory(args,context):return {'content':[{'type':'text','text':json.dumps([{'id':'a'},{'id':'b'}])}]}
        source=FunctionSource((ToolDefinition('inventory','',{}),),{'inventory':inventory})
        catalog.register('source',source,source,target_label='Source');runtime=SimpleNamespace(capabilities=catalog)
        adapter=ToolInventoryContext(source_id='source',tool_name='inventory',key='optional',normalize=lambda item:item['id'],limit=1)
        async with adapter.execution(runtime,ctx):self.assertEqual({'status':'available','items':['a'],'additional_count':1,'instructions':''},adapter.prompt_context(ctx)['optional'])
        self.assertEqual({},adapter.prompt_context(ctx))
        deferred=replace(ctx,deferred=True,delegation_id='task',ingress_id=None)
        self.assertEqual({},await adapter.prepare(runtime,deferred))
        policy.denied.add('tool.call');self.assertEqual({},await adapter.prepare(runtime,ctx))
