"""Real Agent/NativeProvider/SQLite and authenticated HTTP, with a model fixture."""
import ast
import asyncio
import json
from pathlib import Path
import tempfile
import unittest

from aiohttp import web, ClientSession
from aiohttp.test_utils import TestServer
from openagent_core.capabilities import FunctionSource
from openagent_core.contracts import PrincipalRef
from replio_agent_example import Document, ReplioHost, create_app


class ExternalIdentities:
    def __init__(self):
        self.alice=PrincipalRef('replio','tenant-a','alice')
        self.bob=PrincipalRef('replio','tenant-a','bob')
        self.other=PrincipalRef('replio','tenant-b','alice')
        self.tokens={'alice-login':self.alice,'bob-login':self.bob,'other-tenant-login':self.other}
        self.revoked=set()
    async def authenticate(self,bearer):
        principal=self.tokens.get(bearer)
        if principal is None or not await self.active(principal): raise PermissionError('Invalid external identity')
        return principal
    async def active(self,principal): return principal in self.tokens.values() and principal not in self.revoked


class ReferenceConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.identities=ExternalIdentities()
        self.model_requests=[]
        self.http=ClientSession()
        model_app=web.Application()
        model_app.router.add_post('/v1/chat/completions',self.model_reply)
        self.provider=TestServer(model_app)
        await self.provider.start_server()
        await self.open_host()

    async def open_host(self):
        self.host=ReplioHost(data_dir=Path(self.directory.name),tenant_id='tenant-a',project_id='docs',
            identity=self.identities,sessions={'shared':(self.identities.alice,self.identities.bob),
                'alice-private':(self.identities.alice,)},
            documents=(Document('policy','Trial policy','Trial projects last 14 days.'),),
            provider_model='fixture',provider_base_url=str(self.provider.make_url('/v1')),
            provider_key='model-fixture-secret',system_prompt='Use the Replio project tools and cite the policy title.')
        self.server=TestServer(create_app(self.host))
        await self.server.start_server()

    async def asyncTearDown(self):
        await self.server.close()
        await self.provider.close()
        await self.http.close()
        self.directory.cleanup()

    async def request(self,method,path,*,token='alice-login',body=None):
        async with self.http.request(method,self.server.make_url(path),
                headers={'Authorization':'Bearer '+token},json=body) as response:
            raw=await response.text()
            try: value=json.loads(raw)
            except ValueError: value=raw
            return response.status,value

    async def finish(self,run_id,principal,session='shared'):
        context=await self.host.policy.context(principal,session)
        return await asyncio.wait_for(self.host.runtime.wait(run_id,context),20)

    @staticmethod
    def tool_data(text):
        try: return json.loads(text)
        except ValueError: return ast.literal_eval(text)

    async def model_reply(self,request):
        if request.headers.get('Authorization')!='Bearer model-fixture-secret': raise web.HTTPUnauthorized()
        body=await request.json()
        self.model_requests.append(body)
        messages=body.get('messages',[])
        # Shared-session history is retained. Only count this user's current
        # turn, so the second author performs their own authorized discovery.
        start=max((i for i,m in enumerate(messages) if m.get('role')=='user'),default=0)
        results=[m for m in messages[start:] if m.get('role')=='tool']
        call=None
        final='Trial policy: Trial projects last 14 days.'
        if body.get('tools'):
            names={t['function']['name'] for t in body['tools']}
            self.assertEqual(names,{'tool_search_list_servers','tool_search_list_tools','tool_search_describe_tool','tool_search_call_tool'})
            if len(results)==0: call=('tool_search_list_servers',{})
            elif len(results)==1:
                sources=self.tool_data(results[0]['content'])
                self.assertEqual({s['source_ref'] for s in sources},{'replio-project'})
                call=('tool_search_list_tools',{'source_ref':'replio-project'})
            elif len(results)==2:
                tools=self.tool_data(results[1]['content'])
                ref=next(t['tool_ref'] for t in tools if t['name']=='list_documents')
                call=('tool_search_call_tool',{'tool_ref':ref,'args':{}})
            elif len(results)==3:
                tools=self.tool_data(results[1]['content'])
                ref=next(t['tool_ref'] for t in tools if t['name']=='read_document')
                documents=self.tool_data(results[2]['content'])
                call=('tool_search_call_tool',{'tool_ref':ref,'args':{'document_id':documents['documents'][0]['id']}})
            else:
                document=self.tool_data(results[-1]['content'])
                self.assertEqual(document['structuredContent']['title'],'Trial policy')
                final+=' Requested by '+document['_meta']['requested_by']+'.'
        elif 'response_format' in body:
            final=json.dumps({'summary':'The Replio trial policy was read','topics':['project policy']})
        delta={'role':'assistant'}
        if call:
            delta['tool_calls']=[{'index':0,'id':'fixture-'+str(len(results)),'type':'function',
                'function':{'name':call[0],'arguments':json.dumps(call[1])}}]
            reason='tool_calls'
        else:
            delta['content']=final
            reason='stop'
        if body.get('stream'):
            response=web.StreamResponse(headers={'Content-Type':'text/event-stream'})
            await response.prepare(request)
            for content,finish in ((delta,None),({},reason)):
                chunk={'id':'fixture','object':'chat.completion.chunk','created':1,'model':'fixture',
                    'choices':[{'index':0,'delta':content,'finish_reason':finish}]}
                await response.write(('data: '+json.dumps(chunk)+'\n\n').encode())
            await response.write(b'data: [DONE]\n\n')
            await response.write_eof()
            return response
        for item in delta.get('tool_calls',[]): item.pop('index',None)
        return web.json_response({'id':'fixture','object':'chat.completion','created':1,'model':'fixture',
            'choices':[{'index':0,'message':delta,'finish_reason':reason}],
            'usage':{'prompt_tokens':1,'completion_tokens':1,'total_tokens':2}})

    async def test_two_authors_real_tools_persistence_and_idempotent_restart(self):
        for name,principal in [('alice',self.identities.alice),('bob',self.identities.bob)]:
            payload={'run_id':name+'-run','idempotency_key':name+'-key','input':'Read the current trial policy.'}
            status,_=await self.request('POST','/sessions/shared/runs',token=name+'-login',body=payload)
            self.assertEqual(status,202)
        # Bob's message arrives while Alice's accepted run is still active.
        # The runtime must queue a new authorized turn, preserving both authors.
        for name,principal in [('alice',self.identities.alice),('bob',self.identities.bob)]:
            result=await self.finish(name+'-run',principal)
            self.assertEqual(result.status,'success',result.output)
            self.assertEqual(result.output,'Trial policy: Trial projects last 14 days. Requested by '+name+'.')
        self.assertEqual(self.host.tool_audit,[('list_documents',self.identities.alice),('read_document',self.identities.alice),
            ('list_documents',self.identities.bob),('read_document',self.identities.bob)])
        status,events=await self.request('GET','/sessions/shared/runs/alice-run/events',token='bob-login')
        self.assertEqual(status,200)
        self.assertTrue(any(e['kind']=='run.prompt' for e in events))
        self.assertTrue(any(e['kind']=='run.stream' for e in events))
        initial=self.model_requests[0]
        system='\n'.join(m['content'] for m in initial['messages'] if m.get('role')=='system')
        self.assertIn('tool_search_list_tools(source_ref)',system)
        self.assertIn('Use the Replio project tools',system)
        self.assertNotIn('model-fixture-secret',system)
        self.assertIsNone(self.host.runtime.services.code_executor)
        self.assertFalse(self.host.catalog.allow_dynamic)
        request_count=len(self.model_requests)
        await self.server.close()
        await self.open_host()
        status,result=await self.request('POST','/sessions/shared/runs',body={
            'run_id':'alice-run','idempotency_key':'alice-key','input':'Read the current trial policy.'})
        self.assertEqual(status,202,result)
        self.assertEqual(result['status'],'success')
        self.assertEqual(len(self.model_requests),request_count)
        status,_=await self.request('POST','/sessions/shared/runs',token='bob-login',body={
            'run_id':'alice-run','idempotency_key':'alice-key','input':'Read the current trial policy.'})
        self.assertEqual(status,409)

    async def test_external_identity_scope_catalog_and_live_revocation(self):
        for token in ['unknown','other-tenant-login']:
            status,_=await self.request('POST','/sessions/shared/runs',token=token,
                body={'run_id':'no','idempotency_key':'no','input':'No'})
            self.assertEqual(status,403)
        status,_=await self.request('GET','/sessions/alice-private/tools',token='bob-login')
        self.assertEqual(status,403)
        status,_=await self.request('POST','/sessions/shared/runs',body={
            'run_id':'spoof','idempotency_key':'spoof','input':'No','principal':'bob'})
        self.assertEqual(status,400)
        status,tools=await self.request('GET','/sessions/shared/tools')
        self.assertEqual(status,200)
        self.assertEqual({t['name'] for t in tools},{'list_documents','read_document'})
        for method,path in [('POST','/sessions/shared/tools'),('DELETE','/sessions/shared/tools/replio-project')]:
            status,_=await self.request(method,path,body={'command':'forbidden'})
            self.assertEqual(status,403)
        context=await self.host.policy.context(self.identities.alice,'shared')
        empty=FunctionSource((),{})
        with self.assertRaises(PermissionError):
            await self.host.catalog.install('shell',empty,empty,context,target_label='forbidden')
        with self.assertRaises(PermissionError): await self.host.catalog.remove('replio-project',context)
        ref=next(t['tool_ref'] for t in tools if t['name']=='read_document')
        with self.assertRaises(ValueError):
            await self.host.catalog.call_tool(ref,{'document_id':'policy','project_id':'other'},context)
        self.identities.revoked.add(self.identities.alice)
        with self.assertRaises(PermissionError):
            await self.host.catalog.call_tool(ref,{'document_id':'policy'},context)
        status,_=await self.request('GET','/sessions/shared/tools')
        self.assertEqual(status,403)
        self.assertEqual(self.model_requests,[])


if __name__=='__main__': unittest.main()
