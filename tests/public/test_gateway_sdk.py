from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest
from aiohttp import web, ClientSession

from openagent_core import *
from openagent_gateway import create_app
from openagent_sdk import RunClient,RunHTTPError,AcceptanceUncertain
from openagent_storage_sqlite import SqliteRuntimeStore


class GatewaySDK(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=TemporaryDirectory();self.path=Path(self.tmp.name)
        self.alice=PrincipalRef('external-idp','tenant','alice');self.bob=PrincipalRef('external-idp','tenant','bob')
        self.context=ExecutionContext(self.alice,self.alice,self.alice,'session','agent',(self.alice,))
        self.calls=[];self.allowed={'alice','bob'}
        parent=self
        class Policy:
            async def authorize(self,ctx,action,resource,*,audience=()):
                return ctx.initiator.subject_id in parent.allowed and ctx.initiator in ctx.audience
        class Executor:
            async def execute(self,request,ctx,runtime):
                parent.calls.append(request.run_id)
                if request.input=='slow': await asyncio.sleep(60)
                return ctx.author.subject_id+': '+request.input
        self.runtime=Runtime(RuntimeSettings('agent',self.path),RuntimeServices(SqliteRuntimeStore(self.path/'state.sqlite3'),Executor(),Policy()))
        @asynccontextmanager
        async def identity(request,action,run_id,session_id):
            token=request.headers.get('Authorization')
            if token!='Bearer test-alice': raise PermissionError('Unverified')
            if session_id and session_id!='session': raise PermissionError('Wrong session')
            yield self.context
        self.app=create_app(self.runtime,identity)
        self.runner=web.AppRunner(self.app);await self.runner.setup()
        site=web.TCPSite(self.runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1];self.url=f'http://127.0.0.1:{port}'
        self.client=RunClient(self.url,lambda:{'Authorization':'Bearer test-alice'})
    async def asyncTearDown(self):
        await self.runner.cleanup();self.tmp.cleanup()

    async def test_python_sdk_authenticated_submit_replay_and_exact_cancel(self):
        request=RunRequest('run','session','request','hello')
        await self.client.submit(request)
        self.assertEqual((await self.client.wait('run',interval=.01)).output,'alice: hello')
        await self.client.submit(request);self.assertEqual(self.calls,['run'])
        events=await self.client.events('run');self.assertEqual(await self.client.events('run',events[-1].cursor),())
        with self.assertRaises(RunHTTPError) as conflict: await self.client.submit(replace(request,input='different'))
        self.assertEqual(conflict.exception.status,409)
        await self.client.submit(RunRequest('slow','session','slow','slow'))
        self.assertEqual((await self.client.cancel('slow')).status,'cancelled')
        self.assertEqual((await self.client.get('run')).status,'success')
        self.assertEqual(await self.client.children('run'),())

    async def test_wire_cannot_supply_principal_or_unverified_attachment(self):
        async with ClientSession() as http:
            for extra in ({'principal':{'subject_id':'bob'}},{'attachments':[{'path':'/etc/passwd'}]}):
                async with http.post(self.url+'/api/v1/runs',headers={'Authorization':'Bearer test-alice'},json={
                    'run_id':'bad','session_id':'session','idempotency_key':'bad','input':'x',**extra}) as response:
                    self.assertEqual(response.status,400)
        bad=RunClient(self.url,lambda:{'Authorization':'Bearer unverified'})
        with self.assertRaises(RunHTTPError) as error: await bad.submit(RunRequest('bad','session','bad','x'))
        self.assertEqual(error.exception.status,403);self.assertEqual(self.calls,[])

    async def test_bounded_event_pages_and_authorized_final_answer(self):
        await self.client.submit(RunRequest('run','session','key','hello'))
        await self.client.wait('run',interval=.01)
        store = self.runtime.services.store
        start = after = (await self.client.events('run'))[-1].cursor
        answer = {'text':'ready','parts':[{'kind':'attachment','attachment':{'artifact_id':'art-1'}}]}
        with store._transaction() as db:
            for n in range(2499):
                store._event(db, 'run', 'run.progress', {'n':n})
            store._event(db, 'run', 'run.stream', {'kind':'done','text':'other progress','final_answer':answer})

        sizes = []
        window = store.event_window
        async def measured(*args):
            rows = await window(*args)
            sizes.append(len(rows))
            return rows
        store.event_window = measured
        async def unbounded(*args):
            self.fail('Indexed replay must not read the complete transcript')
        store.events = unbounded
        cursors = []
        while True:
            page = await self.client.events_page('run', after)
            cursors.extend(e.cursor for e in page.events)
            after = page.cursor
            if not page.has_more:
                break
        self.assertEqual(sizes, [1001,1001,500])
        self.assertEqual(len(set(cursors)), 2500)
        sizes.clear()
        streamed = [event.cursor async for event in self.client.iter_events('run', start, limit=200)]
        self.assertEqual(streamed, cursors)
        self.assertEqual(len(sizes), 13)
        self.assertLessEqual(max(sizes), 201)
        empty = await self.client.events_page('run', after, limit=1)
        self.assertEqual((empty.events,empty.cursor,empty.has_more), ((),after,False))
        final = await self.client.final_answer('run')
        self.assertEqual((final.segment_run_id,final.final_answer), ('run',answer))
        await store.append_event('run','run.stream',{'privacy_redacted':True})
        self.assertIsNone((await self.client.final_answer('run')).final_answer)
        await store.append_event('run','run.stream',{'kind':'done','text':'unattested'})
        self.assertIsNone((await self.client.final_answer('run')).final_answer)
        self.allowed.clear()
        for read in (self.client.events_page, self.client.final_answer):
            with self.assertRaises(RunHTTPError) as denied:
                await read('run')
            self.assertEqual(denied.exception.status,403)

    async def test_invalid_event_windows_rejected_at_the_gateway(self):
        async with ClientSession(headers={'Authorization':'Bearer test-alice'}) as http:
            for query in ('limit=0','limit=1001','limit=x','after=-1','after=9223372036854775808','drop=1'):
                async with http.get(self.url + '/api/v1/runs/run/events?' + query) as response:
                    self.assertEqual(response.status,400,query)

    async def test_existing_store_without_optional_replay_readers(self):
        store = self.runtime.services.store
        store.event_window = None
        store.final_answer_for_run = None
        await self.client.submit(RunRequest('legacy','session','key','hello'))
        await self.client.wait('legacy',interval=.01)
        events = await store.events('legacy')
        page = await self.client.events_page('legacy', limit=1)
        self.assertEqual((page.events, page.has_more), (events[:1], True))
        self.assertEqual(tuple([event async for event in self.client.iter_events('legacy', limit=1)]), events)
        await store.append_event('legacy','run.stream',{'kind':'done','final_answer':{'text':'done'}})
        self.assertEqual((await self.client.final_answer('legacy')).final_answer, {'text':'done'})
        await store.append_event('legacy','run.stream',{'privacy_redacted':True})
        self.assertIsNone((await self.client.final_answer('legacy')).final_answer)

    async def test_typescript_sdk_against_same_live_gateway(self):
        root=Path(__file__).resolve().parents[2]
        node=shutil.which('node')
        if not node: self.skipTest('Node is not installed on this platform')
        compiler=root/'sdk/typescript/node_modules/typescript/bin/tsc'
        self.assertTrue(compiler.is_file(), 'Install the locked SDK compiler: npm --prefix sdk/typescript ci')
        process=await asyncio.create_subprocess_exec(node,str(compiler),'-p',str(root/'sdk/typescript/tsconfig.json'),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
        output=await process.communicate();self.assertEqual(process.returncode,0,output)
        import os
        # Wheel qualification installs the npm archive in an empty application
        # and copies the same live HTTP fixture next to its node_modules.
        script=Path(os.environ.get('OPENAGENT_TEST_INSTALLED_JS_FIXTURE',root/'sdk/typescript/test/live-gateway.mjs'))
        process=await asyncio.create_subprocess_exec(node,str(script),self.url,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
        stdout,_=await asyncio.wait_for(process.communicate(),20)
        self.assertEqual(process.returncode,0,stdout.decode())
        self.assertEqual(self.calls,['typescript'])


if __name__=='__main__': unittest.main()
