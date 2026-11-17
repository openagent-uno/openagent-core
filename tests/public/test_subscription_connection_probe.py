"""A connection probe uses native account dispatch without conversation writes."""
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
import httpx
from openagent_core.engine import MemoryDB
from openagent_core.models.runtime import run_provider_smoke_test
from openagent_core.core.execution_profile import stateless_completion_active


class SubscriptionProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_probe_has_bound_store_exact_account_and_no_history(self):
        with tempfile.TemporaryDirectory() as temp:
            db=MemoryDB(str(Path(temp)/'host.db'));await db.connect()
            calls=[]
            def respond(request):
                calls.append(json.loads(request.content))
                return httpx.Response(200,json={'id':'fixture','object':'chat.completion','created':1,'model':'fixture-model',
                    'choices':[{'index':0,'message':{'role':'assistant','content':'ok'},'finish_reason':'stop'}],
                    'usage':{'prompt_tokens':1,'completion_tokens':1,'total_tokens':2}})
            client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
            account_ids=[]
            service=SimpleNamespace(settings=SimpleNamespace(api_key='synthetic'),
                inference_client=lambda identifier: (account_ids.append(identifier) or client))
            try:
                pid=await db.upsert_provider(name='named-account',framework='api-based',metadata={
                    'vendor':'openai','access':'subscription','account_id':'fixture-account','label':'Work'})
                await db.upsert_model(provider_id=pid,model='fixture-model')
                with patch('openagent_core.subscriptions.service.subscription_for',return_value=service) as resolve:
                    runtime,response=await run_provider_smoke_test('named-account',await db.materialise_providers_config(),
                        model_id='named-account:fixture-model',db=db)
                self.assertEqual(response.content,'ok')
                self.assertEqual(runtime,'named-account:fixture-model')
                self.assertEqual(resolve.call_args.args[1],db.db_path)
                self.assertEqual(account_ids,['fixture-account'])
                self.assertEqual(calls[0]['model'],'fixture-model')
                self.assertFalse(client.is_closed,'The service owns the borrowed inference client')
                self.assertEqual((await (await db._conn.execute('SELECT COUNT(*) FROM sessions')).fetchone())[0],0)
                self.assertEqual((await (await db._conn.execute('SELECT COUNT(*) FROM session_runs')).fetchone())[0],0)
                self.assertFalse(stateless_completion_active())
            finally:
                await client.aclose();await db.close()

    async def test_cancelled_probe_drains_owned_provider_and_restores_scope(self):
        started=asyncio.Event()
        async def generate(**kwargs):
            self.assertTrue(stateless_completion_active());started.set();await asyncio.Event().wait()
        provider=SimpleNamespace(generate=generate,shutdown=AsyncMock())
        with patch('openagent_core.models.runtime.create_model_from_spec',return_value=provider):
            task=asyncio.create_task(run_provider_smoke_test('fixture',[{'name':'fixture'}],model_id='fixture:one'))
            await started.wait();task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        provider.shutdown.assert_awaited_once()
        self.assertFalse(stateless_completion_active())
