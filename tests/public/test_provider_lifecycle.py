import asyncio
from types import SimpleNamespace
import unittest
from openagent_core.engine import NativeProvider
from openagent_core.models.runtime_db_lifecycle import close_runtime_clients


class Client:
    def __init__(self):self.closed=0
    async def close(self):self.closed+=1


class ProviderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalidation_and_shutdown_drain_overlapping_calls(self):
        provider=NativeProvider('openai:fixture',api_key='fixture')
        first,second=Client(),Client()
        provider._agno_agents['first']=SimpleNamespace(model=SimpleNamespace(async_client=first))
        release=asyncio.Event();entered=asyncio.Event()
        async def ongoing():
            async with provider._call_scope():
                entered.set();await release.wait()
        task=asyncio.create_task(ongoing());await entered.wait()
        provider.set_mcp_toolkits([])
        self.assertEqual(first.closed,0)
        provider._agno_agents['second']=SimpleNamespace(model=SimpleNamespace(async_client=second))
        closing=asyncio.create_task(provider.shutdown());await asyncio.sleep(0)
        self.assertFalse(closing.done());self.assertEqual(second.closed,0)
        with self.assertRaises(RuntimeError):
            async with provider._call_scope():pass
        release.set();await task;await closing
        self.assertEqual((first.closed,second.closed),(1,1))
        await provider.shutdown()
        self.assertEqual((first.closed,second.closed),(1,1))

    async def test_shared_clients_close_once_and_borrowed_transport_survives(self):
        shared,borrowed=Client(),Client()
        runtime=SimpleNamespace(model=SimpleNamespace(async_client=shared),members=[
            SimpleNamespace(model=SimpleNamespace(async_client=shared)),
            SimpleNamespace(model=SimpleNamespace(async_client=borrowed,http_client=object()))])
        self.assertEqual(await close_runtime_clients(runtime),1)
        self.assertEqual((shared.closed,borrowed.closed),(1,0))


if __name__=='__main__':unittest.main()
