import asyncio
from types import SimpleNamespace
import unittest
from openagent_core.engine import NativeProvider
from openagent_core.models.runtime_db_lifecycle import close_runtime_clients


class Client:
    def __init__(self):self.closed=0
    async def close(self):self.closed+=1


class ProviderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_context_invalidation_drains_and_blocks_new_calls_without_history_deletion(self):
        from openagent_core.session_control import SessionControl
        provider=NativeProvider('openai:fixture',api_key='fixture')
        client=Client()
        provider._agno_agents['cached']=SimpleNamespace(model=SimpleNamespace(async_client=client),
                                                      cached_context='PRIVATE OLD CONTEXT')
        release=asyncio.Event();entered=asyncio.Event();next_entered=asyncio.Event()
        async def ongoing():
            async with provider._call_scope():
                entered.set();await release.wait()
        old=asyncio.create_task(ongoing());await entered.wait()
        class Storage:
            def __getattr__(self,name): raise AssertionError('Context invalidation changed storage')
        control=SessionControl(SimpleNamespace(model=provider,memory_db=Storage()))
        clearing=asyncio.create_task(control.invalidate_context('session'))
        await asyncio.sleep(0)
        async def next_call():
            async with provider._call_scope():
                next_entered.set()
                self.assertFalse(provider._agno_agents)
        new=asyncio.create_task(next_call());await asyncio.sleep(0)
        self.assertFalse(clearing.done());self.assertFalse(next_entered.is_set())
        self.assertEqual(client.closed,0)
        release.set();await old;await clearing;await new
        self.assertEqual(client.closed,1)
        self.assertTrue(next_entered.is_set())

    async def test_context_invalidation_propagates_unsupported_and_cleanup_failures(self):
        from openagent_core.models.base import BaseModel
        from openagent_core.models.dispatcher import ModelDispatcher
        from openagent_core.session_control import SessionControl
        with self.assertRaises(NotImplementedError):
            await BaseModel.invalidate_session_context(object(),'session')
        class Broken:
            async def invalidate_session_context(self,session): raise RuntimeError('cleanup failed')
        dispatcher=object.__new__(ModelDispatcher)
        dispatcher._team_providers={'broken':Broken()}
        dispatcher._retired_providers=[]
        dispatcher._last_pick_by_session={'session':'broken'}
        control=SessionControl(SimpleNamespace(model=dispatcher))
        with self.assertRaisesRegex(RuntimeError,'cleanup failed'):
            await control.invalidate_context('session')
        self.assertIn('session',dispatcher._last_pick_by_session)

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

    async def test_router_closes_retired_native_and_team_clients(self):
        from openagent_core.models.dispatcher import TeamRouterProvider
        router=TeamRouterProvider('openai:fixture',[])
        native=NativeProvider('openai:fixture',api_key='fixture')
        first,second=Client(),Client()
        native._agno_agents['first']=SimpleNamespace(model=SimpleNamespace(async_client=first))
        router._session_runtime['native']=native
        router._session_runtime['team']=SimpleNamespace(model=SimpleNamespace(async_client=second))
        async with router._call_scope():
            router._invalidate_session_cache()
            self.assertEqual((first.closed,second.closed),(0,0))
        self.assertEqual((first.closed,second.closed),(1,1))
        await router.shutdown()

    async def test_shared_clients_close_once_and_borrowed_transport_survives(self):
        shared,borrowed=Client(),Client()
        runtime=SimpleNamespace(model=SimpleNamespace(async_client=shared),members=[
            SimpleNamespace(model=SimpleNamespace(async_client=shared)),
            SimpleNamespace(model=SimpleNamespace(async_client=borrowed,http_client=object()))])
        self.assertEqual(await close_runtime_clients(runtime),1)
        self.assertEqual((shared.closed,borrowed.closed),(1,0))


if __name__=='__main__':unittest.main()
