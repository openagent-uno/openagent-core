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

    async def test_typescript_sdk_against_same_live_gateway(self):
        root=Path(__file__).resolve().parents[2]
        node=shutil.which('node');tsc=shutil.which('tsc')
        if not node or not tsc: self.skipTest('Node/TypeScript are not installed on this platform')
        process=await asyncio.create_subprocess_exec(tsc,'-p',str(root/'sdk/typescript/tsconfig.json'),stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
        output=await process.communicate();self.assertEqual(process.returncode,0,output)
        script=root/'sdk/typescript/test/live-gateway.mjs'
        process=await asyncio.create_subprocess_exec(node,str(script),self.url,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT)
        stdout,_=await asyncio.wait_for(process.communicate(),20)
        self.assertEqual(process.returncode,0,stdout.decode())
        self.assertEqual(self.calls,['typescript'])


if __name__=='__main__': unittest.main()
