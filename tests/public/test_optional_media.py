"""Media is optional, exact, freshly authorized and bounded."""
import unittest
from dataclasses import replace
from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp.pool import MCPPool
from openagent_core.mcp._runtime import Toolkit
from test_mcp_catalog import Policy


class Source:
    def __init__(self): self.closed=False; self.hints={"media":"test"}; self.after=None
    async def discover(self,context): return (ToolDefinition('view','view',{},metadata=dict(self.hints)),)
    async def call_tool(self,*args): raise AssertionError('stream must not call ordinary tool')
    async def stream_tool(self,name,options,context):
        try:
            yield {'number':1}
            if self.after: self.after()
            yield {'number':2}
        finally: self.closed=True


class OptionalMedia(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.policy=Policy();self.catalog=CapabilityCatalog(self.policy)
        p=PrincipalRef('host','tenant','person');self.context=ExecutionContext(p,p,p,'session','agent',(p,))
        self.source=Source();self.catalog.register('source',self.source,self.source,target_label='Source')
        self.tool=(await self.catalog.discover(self.context))[0]
    async def test_no_client_required_and_iterator_close_releases_executor(self):
        iterator=self.catalog.stream_tool(self.tool.tool_ref,{},self.context)
        self.assertEqual({'number':1},await anext(iterator));await iterator.aclose();self.assertTrue(self.source.closed)
    async def test_permission_revocation_suppresses_late_frame(self):
        self.source.after=lambda:self.policy.denied.add('tool.publish')
        iterator=self.catalog.stream_tool(self.tool.tool_ref,{},self.context)
        await anext(iterator)
        with self.assertRaises(PermissionError):await anext(iterator)
        self.assertTrue(self.source.closed)
    async def test_definition_mutation_suppresses_late_frame(self):
        self.source.after=lambda:self.source.hints.update(changed=True)
        iterator=self.catalog.stream_tool(self.tool.tool_ref,{},self.context);await anext(iterator)
        with self.assertRaises(CapabilityUnavailable):await anext(iterator)
        self.assertTrue(self.source.closed)
    async def test_wrong_principal_unknown_ref_and_unbounded_options(self):
        with self.assertRaises(CapabilityUnavailable):await anext(self.catalog.stream_tool('unknown',{},self.context))
        with self.assertRaises(ValueError):await anext(self.catalog.stream_tool(self.tool.tool_ref,{'huge':'x'*65536},self.context))
        self.policy.denied.add('tool.call')
        with self.assertRaises(PermissionError):await anext(self.catalog.stream_tool(self.tool.tool_ref,{},self.context))
        self.assertFalse(self.source.closed)
    async def test_protected_sources_require_a_streaming_privacy_publisher(self):
        self.catalog.register('protected',self.source,self.source,target_label='Protected',result_provenance=lambda *_:None)
        tool=next(t for t in await self.catalog.discover(self.context) if t.source_id=='protected')
        with self.assertRaises(CapabilityUnavailable):await anext(self.catalog.stream_tool(tool.tool_ref,{},self.context))
        self.assertFalse(self.source.closed)

    async def test_pool_replacement_does_not_redirect_live_stream(self):
        pool=MCPPool([]);toolkit=Toolkit(name='camera')
        def view():return 'unused'
        toolkit.register(view);pool._toolkit_by_name={'camera':toolkit}
        class Media(Source):
            def metadata(self,name):return {'media':'test'} if name=='view' else {}
        first=Media();second=Media();pool.register_media_executor('camera',first)
        pool.bind_capability_catalog(self.catalog)
        tool=next(t for t in await self.catalog.discover(self.context) if t.source_id=='camera')
        iterator=self.catalog.stream_tool(tool.tool_ref,{},self.context);await anext(iterator)
        pool.register_media_executor('camera',second)
        with self.assertRaises(CapabilityUnavailable):await anext(iterator)
        self.assertTrue(first.closed);self.assertFalse(second.closed)
