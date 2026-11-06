import copy
import unittest
from dataclasses import replace
from openagent_core.capabilities import CapabilityCatalog,CapabilityUnavailable
from openagent_core.contracts import ExecutionContext,PrincipalRef
from openagent_core.remote_mcp import RemoteMcpRegistry
from test_mcp_catalog import Policy

class Transport:
    def __init__(self):
        self.rows=[{'host_id':'host-a','generation':1,'server':'files','version':'1','catalog_digest':'a'*64,
            'resource_protocol':'mcp-app-resources/1','tools':[{'name':'read','input_schema':{},'_meta':{'ui':{'resourceUri':'ui://view'}}}]}]
        self.calls=[];self.truncated=False;self.after=None
    async def catalog(self,context,selectors):return {'servers':self.rows,'truncated':self.truncated}
    async def call(self,context,scope,name,args):
        self.calls.append((scope,name,args))
        if self.after:self.after()
        return {'content':[]}
    async def read(self,context,scope,uri):return {'uri':uri}

class RemoteFederation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        p=PrincipalRef('host','tenant','person');self.context=ExecutionContext(p,p,p,'session','agent',(p,))
        self.catalog=CapabilityCatalog(Policy());self.transport=Transport();self.registry=RemoteMcpRegistry(self.catalog,self.transport)
        await self.registry.synchronize(self.context);self.tool=(await self.catalog.discover(self.context))[0]
    async def test_exact_host_binding_and_resource_declaration(self):
        await self.catalog.call_tool(self.tool.tool_ref,{},self.context)
        self.assertEqual('host-a',self.transport.calls[0][0]['host_id'])
        source=next(iter(self.registry.sources.values()))
        self.assertEqual({'uri':'ui://view'},await source.read_resource('ui://view',self.context))
        with self.assertRaises(PermissionError):await source.read_resource('ui://other',self.context)
        with self.assertRaises(PermissionError):await source.call_tool('read',{},replace(self.context,session_id='other'))
    async def test_in_place_catalog_mutation_cannot_retarget_old_reference(self):
        self.transport.rows[0]['tools'][0]['input_schema']={'changed':True}
        with self.assertRaises(CapabilityUnavailable):await self.catalog.call_tool(self.tool.tool_ref,{},self.context)
        await self.registry.synchronize(self.context)
        tools=await self.catalog.discover(self.context);self.assertEqual({'changed':True},tools[0].input_schema)
        with self.assertRaises(CapabilityUnavailable):await self.catalog.call_tool(self.tool.tool_ref,{},self.context)
    async def test_revoke_during_call_suppresses_output_and_partial_catalog_retains_sources(self):
        self.transport.after=lambda:self.transport.rows.clear()
        with self.assertRaises(CapabilityUnavailable):await self.catalog.call_tool(self.tool.tool_ref,{},self.context)
        self.transport.truncated=True;await self.registry.synchronize(self.context);self.assertEqual(1,len(self.registry.sources))
        self.transport.truncated=False;await self.registry.synchronize(self.context);self.assertFalse(self.registry.sources)
