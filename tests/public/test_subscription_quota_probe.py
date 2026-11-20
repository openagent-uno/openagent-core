"""Quota telemetry belongs to the Core; consumers never handle raw tokens."""
import asyncio
import json
import math
from pathlib import Path

import httpx
import pytest
from openagent_core.subscriptions.quota import claude_quota
from openagent_core.subscriptions.service import SubscriptionService


def test_quota_headers_are_numeric_allowlisted_and_never_expose_other_headers():
    data=claude_quota({'Anthropic-Ratelimit-Unified-7d-Utilization':'0.72',
        'anthropic-ratelimit-unified-7d-reset':'1900000000',
        'anthropic-ratelimit-unified-5h-utilization':'nan',
        'anthropic-ratelimit-unified-5h-reset':'-1',
        'anthropic-ratelimit-unified-7d-status':'allowed',
        'authorization':'SECRET','set-cookie':'SECRET'})
    assert data['util_7d']==0.72 and data['reset_7d']==1900000000
    assert data['status_7d']=='allowed' and data['observed_at_ms']>0
    assert 'util_5h' not in data and 'reset_5h' not in data
    assert 'SECRET' not in json.dumps(data)
    assert claude_quota({'authorization':'SECRET'})=={}


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[200,429])
async def test_exact_account_probe_preserves_credentials_and_captures_success_or_limit_headers(tmp_path,monkeypatch,status):
    source=tmp_path/'store.json'
    source.write_text(json.dumps({'accounts':[
        {'id':'a','name':'Personal','access_token':'fixture-a','priority':0},
        {'id':'b','name':'Work','access_token':'fixture-b','priority':1}]}))
    original=httpx.Client;used=[]
    def handler(request):
        used.append(request.headers['authorization'])
        return httpx.Response(status,headers={'anthropic-ratelimit-unified-7d-utilization':'0.62',
            'anthropic-ratelimit-unified-7d-reset':'1900000000','secret-header':'SECRET'},json=(
            {'id':'m','content':[{'type':'text','text':'ok'}],'stop_reason':'end_turn','usage':{}}
            if status==200 else {'error':{'type':'rate_limit_error','message':'SECRET'}}))
    def factory(**kwargs):return original(**kwargs,transport=httpx.MockTransport(handler))
    monkeypatch.setattr(httpx,'Client',factory)
    service=SubscriptionService('claude',source,settings={'transport':'httpx','upstream_min_interval':0})
    try:
        result=await service.refresh_usage(account_id='b',model='claude-fixture')
        assert result['fresh'] and result['quota']['util_7d']==0.62
        assert result['status']==status and 'SECRET' not in json.dumps(result)
        assert used==['Bearer fixture-b']
        accounts={a['id']:a for a in await service.accounts()}
        assert 'quota' not in accounts['a'] and accounts['b']['quota']['util_7d']==0.62
        stored=json.loads(source.read_text())['accounts']
        assert [a['access_token'] for a in stored]==['fixture-a','fixture-b']
        assert [a['name'] for a in stored]==['Personal','Work']
    finally:await service.close()


@pytest.mark.asyncio
async def test_concurrent_probes_keep_observers_separate_and_missing_headers_are_unknown(tmp_path,monkeypatch):
    source=tmp_path/'store.json';source.write_text(json.dumps({'accounts':[
        {'id':i,'access_token':'fixture-'+i} for i in ['a','b']]}))
    original=httpx.Client
    def handler(request):
        account=request.headers['authorization'][-1]
        return httpx.Response(200,headers={'anthropic-ratelimit-unified-7d-utilization':{'a':'0.2','b':'0.8'}[account]},
            json={'content':[{'type':'text','text':'ok'}],'usage':{}})
    def factory(**kwargs):return original(**kwargs,transport=httpx.MockTransport(handler))
    monkeypatch.setattr(httpx,'Client',factory)
    service=SubscriptionService('claude',source,settings={'transport':'httpx','upstream_min_interval':0})
    try:
        results=await asyncio.gather(*(service.refresh_usage(account_id=i,model='claude-fixture') for i in ['a','b']))
        assert [r['quota']['util_7d'] for r in results]==[0.2,0.8]
        assert all(r['fresh'] for r in results)
        with pytest.raises(KeyError):await service.refresh_usage(account_id='unknown',model='fixture')
        # A successful response with no quota is unknown; no invented zero.
        monkeypatch.setattr(httpx,'Client',lambda **kw:original(**kw,transport=httpx.MockTransport(
            lambda req:httpx.Response(200,json={'content':[],'usage':{}}))))
        await asyncio.sleep(0.01)
        result=await service.refresh_usage(account_id='a',model='fixture')
        assert not result['fresh'] and result['quota']['util_7d']==0.2
    finally:await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('stream',[True,False])
@pytest.mark.parametrize('transport',['httpx','curl_cffi'])
@pytest.mark.parametrize('chunk_timeout',[None,30.0])
async def test_quota_observation_uses_both_native_transports_and_ephemeral_grants(tmp_path,monkeypatch,stream,transport,chunk_timeout):
    from types import SimpleNamespace
    headers={'anthropic-ratelimit-unified-7d-utilization':'0.45'}
    body={'id':'m','content':[{'type':'text','text':'ok'}],'usage':{},'stop_reason':'end_turn'}
    events=[{'type':'message_start','message':{'id':'m','usage':{}}},
        {'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'ok'}},
        {'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{}}, {'type':'message_stop'}]
    wire=''.join('data: '+json.dumps(e)+'\n\n' for e in events)
    if transport=='httpx':
        original=httpx.Client
        monkeypatch.setattr(httpx,'Client',lambda **kw:original(**kw,transport=httpx.MockTransport(
            lambda request:httpx.Response(200,headers=headers,content=wire if stream else json.dumps(body)))))
    else:
        from curl_cffi import requests
        class Session:
            def __init__(self,**kwargs):pass
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def close(self):pass
            def post(self,*args,**kwargs):return SimpleNamespace(status_code=200,headers=headers,
                json=lambda:body,iter_lines=lambda:iter(wire.splitlines()),close=lambda:None)
        monkeypatch.setattr(requests,'Session',Session)
    service=SubscriptionService.from_access_token('claude','fixture',account_id='a',admission_path=tmp_path/'admission',
        settings={'transport':transport,'upstream_min_interval':0,'chunk_timeout':chunk_timeout})
    try:
        response=await service.inference_client('a').post('/v1/chat/completions',json={
            'model':'fixture','messages':[{'role':'user','content':'hi'}],'stream':stream})
        assert response.status_code==200
        accounts=await service.accounts();assert accounts[0]['quota']['util_7d']==0.45
        assert not (tmp_path/'admission').exists()
    finally:await service.close()


def test_optional_quota_storage_failure_never_breaks_inference():
    from openagent_core.subscriptions.quota import observe,report_claude
    def unavailable(quota):raise OSError('private storage failure')
    with observe(unavailable):
        report_claude({'anthropic-ratelimit-unified-7d-utilization':'0.3'})
