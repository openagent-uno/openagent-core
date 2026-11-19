"""A streamed generator must never transfer ownership of its OS leases."""
import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time

import pytest
from openagent_core.subscriptions import blocking
from openagent_core.subscriptions.admission import StoreSemaphore
from openagent_core.subscriptions.codex.codex_backend import CodexClient
from openagent_core.subscriptions.service import SubscriptionService


@pytest.mark.asyncio
async def test_stream_leases_survive_pool_worker_changes(tmp_path, monkeypatch):
    # Reproduce Starlette's permitted migration between worker threads on every
    # next(). Previously the final release used a different thread-local owner.
    workers=[ThreadPoolExecutor(max_workers=1) for _ in range(2)]
    async def changing_workers(iterator):
        def advance():
            try:return True,next(iterator)
            except StopIteration:return False,None
        i=0
        while True:
            context=contextvars.copy_context()
            ok,value=await asyncio.get_running_loop().run_in_executor(workers[i%2],context.run,advance)
            i+=1
            if not ok:return
            yield value
    monkeypatch.setattr('starlette.responses.iterate_in_threadpool',changing_workers)
    def events(self,payload,**kwargs):
        for _ in range(4):
            yield {'type':'response.output_text.delta','delta':'ok'},{}
        yield {'type':'response.completed','response':{'id':'r','usage':{}}},{}
    monkeypatch.setattr(CodexClient,'stream',events)
    store=tmp_path/'admission'
    service=SubscriptionService.from_access_token('codex','fixture',account_id='a',admission_path=store,
        settings={'upstream_max_concurrency':1,'upstream_min_interval':0,'upstream_queue_timeout':1})
    try:
        for _ in range(3):
            async with service.inference_client('a').stream('POST','/v1/chat/completions',json={
                'model':'gpt-fixture','messages':[{'role':'user','content':'hello'}],'stream':True}) as response:
                body=await response.aread()
            assert b'proxy_error' not in body and b'[DONE]' in body
            slots=StoreSemaphore(store,'active',1)
            assert slots.acquire(blocking=False),'Completed stream stranded an upstream lease'
            slots.release()
    finally:
        await service.close()
        for worker in workers:worker.shutdown(wait=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('stream',[True,False])
async def test_cancelled_waiter_releases_queue_without_waiting_for_admission(tmp_path,monkeypatch,stream):
    store=tmp_path/'admission'
    held=StoreSemaphore(store,'active',1);assert held.acquire(blocking=False)
    service=SubscriptionService.from_access_token('codex','fixture',account_id='a',admission_path=store,
        settings={'upstream_max_concurrency':1,'upstream_max_queue':0,'upstream_min_interval':0,'upstream_queue_timeout':1})
    request=asyncio.create_task(service.inference_client('a').post('/v1/chat/completions',json={
        'model':'fixture','messages':[{'role':'user','content':'hello'}],'stream':stream}))
    queue=StoreSemaphore(store,'queue',1)
    try:
        # Wait until the producer is actually holding the only queue lease.
        for _ in range(100):
            if not queue.acquire(blocking=False):break
            queue.release();await asyncio.sleep(0.01)
        else:pytest.fail('No request reached admission')
        start=time.monotonic();request.cancel()
        with pytest.raises(asyncio.CancelledError):await asyncio.wait_for(request,0.4)
        for _ in range(30):
            if queue.acquire(blocking=False):
                queue.release();break
            await asyncio.sleep(0.01)
        else:pytest.fail('Cancelled request retained its queue lease')
        assert time.monotonic()-start<0.4
    finally:
        held.release();request.cancel();await asyncio.gather(request,return_exceptions=True)
        await service.close()


@pytest.mark.asyncio
async def test_blocking_stream_closes_iterator_on_own_thread_and_propagates_context():
    marker=contextvars.ContextVar('fixture-marker');marker.set('private-scope')
    seen=[];finished=threading.Event()
    def chunks():
        try:
            seen.append((threading.get_ident(),marker.get()))
            while True:yield 'data: fixture\n\n'
        finally:
            seen.append((threading.get_ident(),marker.get()));finished.set()
    stream=blocking.iterate(chunks())
    assert await anext(stream)=='data: fixture\n\n'
    await stream.aclose()
    assert await asyncio.to_thread(finished.wait,1)
    assert len(seen)==2 and seen[0]==seen[1]


@pytest.mark.asyncio
async def test_cancel_after_first_token_closes_socket_and_releases_admission(tmp_path,monkeypatch):
    from openagent_core.subscriptions.codex import codex_backend
    closed=threading.Event();entered=threading.Event();calls=[]
    def post(*args,**kwargs):
        def lines():
            yield 'data: '+json.dumps({'type':'response.output_text.delta','delta':'first'})
            entered.set()
            assert closed.wait(2),'Disconnected request left its upstream read blocked'
        calls.append(True)
        return 200,{},lines(),closed.set
    monkeypatch.setattr(codex_backend,'_post_httpx',post)
    store=tmp_path/'admission'
    service=SubscriptionService.from_access_token('codex','fixture',account_id='a',admission_path=store,
        settings={'transport':'httpx','upstream_max_concurrency':1,'upstream_min_interval':0})
    try:
        async with service.inference_client('a').stream('POST','/v1/chat/completions',json={
            'model':'fixture','messages':[{'role':'user','content':'hello'}],'stream':True}) as response:
            stream=response.aiter_bytes()
            for _ in range(5):
                if b'first' in await anext(stream):break
            else:pytest.fail('No model content reached the caller')
        assert await asyncio.to_thread(closed.wait,0.5)
        slots=StoreSemaphore(store,'active',1)
        for _ in range(50):
            if slots.acquire(blocking=False):
                slots.release();break
            await asyncio.sleep(0.01)
        else:pytest.fail('Interrupted stream stranded its active lease')
        assert calls==[True]
    finally:await service.close()
