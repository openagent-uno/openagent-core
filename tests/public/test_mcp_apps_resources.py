from __future__ import annotations

import asyncio
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp.catalog import PoolCapabilitySource
from openagent_core.mcp._runtime import Toolkit
from openagent_core.mcp._runtime.mcp import MCPTools
from mcp.types import Tool, ListToolsResult

URI='ui://fixture/counter'

class Policy:
    def __init__(self): self.denied=set()
    async def authorize(self,context,action,resource,*,audience=()):
        return action not in self.denied and context.author.subject_id=='owner'

class Source:
    def __init__(self,label='left'):
        self.tools=(ToolDefinition('show','Open counter',{},metadata={'ui':{'resourceUri':URI}}),ToolDefinition('increment','Increment',{}))
        self.calls=[];self.label=label;self.hook=None
    async def discover(self,context):return self.tools
    async def call_tool(self,name,args,context):self.calls.append((name,args));return{'content':[{'type':'text','text':self.label}]}
    async def read_resource(self,uri,context):
        if self.hook:await self.hook()
        return {'contents':[{'uri':uri,'mimeType':'text/html;profile=mcp-app','text':'<button>Increment</button>','_meta':{'ui':{'csp':{'connectDomains':[]}}}}]}

class McpAppResourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.policy=Policy();self.catalog=CapabilityCatalog(self.policy);self.left=Source();self.right=Source('right')
        p=PrincipalRef('test','tenant','owner');self.context=ExecutionContext(p,p,p,'session','agent',(p,))
        self.catalog.register('left',self.left,self.left,target_label='left')
        self.catalog.register('right',self.right,self.right,target_label='right')
        self.tool=next(d for d in await self.catalog.discover(self.context) if d.source_id=='left' and d.name=='show')
    async def test_metadata_and_exact_ui_resource(self):
        described=await self.catalog.describe_tool(self.tool.tool_ref,self.context)
        self.assertEqual(described.metadata,{'ui':{'resourceUri':URI}})
        described.metadata['ui']['resourceUri']='ui://forged'
        resource=await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
        self.assertEqual(resource['contents'][0]['text'],'<button>Increment</button>')
        for uri in ['ui://other','file:///etc/passwd','https://example.com/app',URI+'#other']:
            with self.assertRaises(PermissionError):await self.catalog.read_tool_resource(self.tool.tool_ref,uri,self.context)
    async def test_sibling_calls_cannot_change_source(self):
        result=await self.catalog.call_source_tool(self.tool.tool_ref,'increment',{'amount':1},self.context)
        self.assertEqual(result['content'][0]['text'],'left');self.assertEqual(self.right.calls,[])
        with self.assertRaises(CapabilityUnavailable):await self.catalog.call_source_tool(self.tool.tool_ref,'right/increment',{},self.context)
    async def test_source_removal_and_schema_change_revoke_resource(self):
        self.left.tools=(replace(self.left.tools[0],metadata={'ui':{'resourceUri':'ui://new'}}),self.left.tools[1])
        with self.assertRaises(CapabilityUnavailable):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
        self.catalog.revoke('left')
        with self.assertRaises(CapabilityUnavailable):await self.catalog.call_source_tool(self.tool.tool_ref,'increment',{},self.context)
    async def test_permissions_checked_before_and_after_resource_io(self):
        self.policy.denied.add('tool.call')
        with self.assertRaises(PermissionError):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
        self.policy.denied.clear()
        async def revoke():self.policy.denied.add('tool.publish')
        self.left.hook=revoke
        with self.assertRaises(PermissionError):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
    async def test_revoke_during_read_never_publishes_html(self):
        async def revoke():self.catalog.revoke('left')
        self.left.hook=revoke
        with self.assertRaises(CapabilityUnavailable):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
    async def test_wrong_principal_cannot_open_app(self):
        p=PrincipalRef('test','tenant','other');context=ExecutionContext(p,p,p,'session','agent',(p,))
        with self.assertRaises(PermissionError):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,context)
    async def test_malformed_and_oversize_resources_fail_closed(self):
        for content in [{'uri':'ui://other','mimeType':'text/html;profile=mcp-app','text':'x'},
                        {'uri':URI,'mimeType':'text/html','text':'x'},
                        {'uri':URI,'mimeType':'text/html;profile=mcp-app','text':'x'*(2*1024*1024)}]:
            self.left.read_resource=AsyncMock(return_value={'contents':[content]})
            with self.assertRaises(ValueError):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)
    async def test_real_mcp_tool_metadata_survives_catalog_adaptation(self):
        remote=Tool(name='show',description='counter',inputSchema={'type':'object'},**{'_meta':{'ui':{'resourceUri':URI}}})
        session=SimpleNamespace(list_tools=AsyncMock(return_value=ListToolsResult(tools=[remote])),read_resource=AsyncMock(return_value={'contents':[]}))
        toolkit=MCPTools(session=session)
        await toolkit.build_tools()
        pool=SimpleNamespace(toolkit_by_name=lambda _:toolkit)
        source=PoolCapabilitySource(pool,'counter',toolkit)
        definition=(await source.discover(self.context))[0]
        self.assertEqual(definition.metadata,{'ui':{'resourceUri':URI}})

class McpAppSessionTests(McpAppResourceTests):
    async def test_service_same_source_dedup_and_revocation(self):
        from openagent_core.mcp_apps import McpAppService
        service=McpAppService(self.catalog)
        view=await service.open(self.tool.tool_ref,self.context,tool_input={'by':1})
        self.assertIsInstance(view['binding']['capabilityEpoch'],str)
        params={'name':'increment','arguments':{'by':1}}
        first=await service.request(view['handle'],'tools/call',params,self.context,request_id='one')
        second=await service.request(view['handle'],'tools/call',params,self.context,request_id='one')
        self.assertEqual(first,second);self.assertEqual(len(self.left.calls),1)
        with self.assertRaises(ValueError):await service.request(view['handle'],'tools/call',{'name':'increment','arguments':{'by':2}},self.context,request_id='one')
        with self.assertRaises(PermissionError):await service.request(view['handle'],'authorize',{},replace(self.context,session_id='other'))
        self.catalog.revoke('left')
        with self.assertRaises(CapabilityUnavailable):await service.request(view['handle'],'authorize',{},self.context)
        await service.shutdown()
    async def test_service_cancellation_and_concurrent_open_bound(self):
        from openagent_core.mcp_apps import McpAppService
        service=McpAppService(self.catalog,max_sessions=1)
        entered=asyncio.Event();release=asyncio.Event()
        async def block():entered.set();await release.wait()
        self.left.hook=block
        opening=asyncio.create_task(service.open(self.tool.tool_ref,self.context))
        await entered.wait()
        with self.assertRaises(RuntimeError):await service.open(self.tool.tool_ref,self.context)
        release.set();view=await opening
        self.left.hook=None
        entered.clear();release.clear()
        async def slow(name,args,context):entered.set();await release.wait();self.left.calls.append((name,args));return {}
        self.left.call_tool=slow
        call=asyncio.create_task(service.request(view['handle'],'tools/call',{'name':'increment'},self.context,request_id='slow'))
        await entered.wait();self.assertTrue(await service.cancel(view['handle'],'slow',self.context))
        with self.assertRaises(asyncio.CancelledError):await call
        self.assertEqual(self.left.calls,[])
        await service.close(view['handle'],self.context)
        with self.assertRaises(CapabilityUnavailable):await service.request(view['handle'],'authorize',{},self.context)
    async def test_standard_visibility_and_blob_resources(self):
        import base64
        self.left.tools=(self.left.tools[0],replace(self.left.tools[1],metadata={'ui':{'visibility':['app']}}))
        names=[tool.name for tool in await self.catalog.discover(self.context) if tool.source_id=='left']
        self.assertNotIn('increment',names)
        self.assertIsNotNone(await self.catalog.call_source_tool(self.tool.tool_ref,'increment',{},self.context))
        # Calling a sibling app tool never invalidates the origin model ref.
        await self.catalog.describe_tool(self.tool.tool_ref,self.context)
        self.left.read_resource=AsyncMock(return_value={'contents':[{'uri':URI,'mimeType':'text/html;profile=mcp-app','blob':base64.b64encode(b'<p>counter</p>').decode()}]})
        self.assertIn('blob',(await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context))['contents'][0])
        self.left.read_resource=AsyncMock(return_value={'contents':[{'uri':URI,'mimeType':'text/html;profile=mcp-app','blob':'invalid%'}]})
        with self.assertRaises(ValueError):await self.catalog.read_tool_resource(self.tool.tool_ref,URI,self.context)

    async def test_open_aggregate_limit_rejects_before_copy_and_session_allocation(self):
        from openagent_core.mcp_apps import McpAppService
        service=McpAppService(self.catalog,max_sessions=1)
        with self.assertRaisesRegex(ValueError,'8 MiB'):
            await service.open(self.tool.tool_ref,self.context,tool_input={'value':'x'*(4*1024*1024)},tool_result={'value':'y'*(4*1024*1024)})
        self.assertEqual({},service.sessions)
        self.assertEqual(0,service._opening)
        result=await service.open(self.tool.tool_ref,self.context,tool_input={'selection':'original'})
        self.assertEqual({'selection':'original'},result['toolInput'])
        await service.shutdown()

    async def test_shutdown_fences_late_open_and_rejects_future_sessions(self):
        from openagent_core.mcp_apps import McpAppService
        service=McpAppService(self.catalog)
        entered=asyncio.Event();release=asyncio.Event()
        async def block():entered.set();await release.wait()
        self.left.hook=block
        opening=asyncio.create_task(service.open(self.tool.tool_ref,self.context))
        await entered.wait();await service.shutdown();release.set()
        with self.assertRaises(CapabilityUnavailable):await opening
        self.assertEqual({},service.sessions)
        with self.assertRaises(CapabilityUnavailable):await service.open(self.tool.tool_ref,self.context)
