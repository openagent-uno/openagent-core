"""Real HTTP custody boundary: no rotating token leaves its owning host."""
import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import socket

import httpx
import pytest
import uvicorn

from openagent_core.subscriptions.host import create_app, account_path, account_inventory_sha256
from openagent_core.subscriptions.service import SubscriptionService, subscription_for
from openagent_core.subscriptions.remote import RemoteSubscriptionService


@asynccontextmanager
async def hosted(service, **options):
    app = create_app(service, api_key='inference-fixture', admin_api_key='admin-fixture', **options)
    sock = socket.socket(); sock.bind(('127.0.0.1', 0)); sock.listen(128)
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='on'))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        for _ in range(200):
            if server.started: break
            if task.done(): await task
            await asyncio.sleep(.01)
        assert server.started
        yield 'http://127.0.0.1:'+str(sock.getsockname()[1])
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        sock.close()


def reply(_client, body, *, token=None, **kwargs):
    return {'id':'fixture','type':'message','role':'assistant','model':body['model'],
            'content':[{'type':'text','text':token}], 'stop_reason':'end_turn',
            'usage':{'input_tokens':3,'output_tokens':1}}


@pytest.mark.asyncio
async def test_real_host_exact_account_inventory_admin_isolation_and_legacy_compatibility(tmp_path, monkeypatch):
    service = SubscriptionService('claude', tmp_path/'custody.json', settings={
        'upstream_min_interval':0, 'transport':'httpx', 'rate_limit_retries':0})
    first = service.manager.save_account({'access_token':'first-fixture'}, name='Personal')
    second = service.manager.save_account({'access_token':'second-fixture'}, name='Work')
    monkeypatch.setattr('openagent_core.subscriptions.claude.cc_anthropic.ClaudeCodeClient.complete', reply)
    async with hosted(service) as url:
        remote = RemoteSubscriptionService('claude', url+'/v1', 'inference-fixture')
        try:
            inventory = await remote.accounts()
            assert {a['name'] for a in inventory} == {'Personal','Work'}
            assert 'access_token' not in json.dumps(inventory) and 'first-fixture' not in json.dumps(inventory)
            body={'model':'fixture','messages':[{'role':'user','content':'hello'}]}
            result = await remote.complete(body, account_id=second['id'])
            assert result['choices'][0]['message']['content'] == 'second-fixture'
            client = remote.inference_client(first['id'], single_attempt=True)
            response = await client.post('/v1/chat/completions', json=body)
            assert response.status_code == 200 and response.json()['choices'][0]['message']['content'] == 'first-fixture'
            assert await asyncio.to_thread(remote.account_available, first['id'])
            async with httpx.AsyncClient(base_url=url, trust_env=False) as public:
                assert (await public.get('/subscription/accounts')).status_code == 401
                assert (await public.get('/accounts/'+account_path(first['id'])+'/v1/models')).status_code == 401
                inference={'Authorization':'Bearer inference-fixture'}
                assert (await public.get('/accounts/invalid/v1/models', headers=inference)).status_code == 404
                assert (await public.get('/accounts/'+account_path(first['id'])+'/admin/accounts', headers=inference)).status_code == 404
                assert (await public.get('/admin/accounts', headers=inference)).status_code == 401
                assert (await public.get('/admin/accounts', headers={'Authorization':'Bearer admin-fixture'})).status_code == 200
                legacy=await public.post('/v1/chat/completions', headers=inference, json=body)
                assert legacy.status_code == 200
            await service.update_account({'id':second['id'],'enabled':False})
            assert not await asyncio.to_thread(remote.account_available, second['id'])
            result = await remote.inference_client(second['id']).post('/v1/chat/completions', json=body)
            assert result.status_code in {429,503} and 'first-fixture' not in result.text
        finally: await remote.close()
    assert service.client.is_closed


@pytest.mark.asyncio
async def test_remote_adoption_preserves_original_references_and_keeps_custody_remote(tmp_path, monkeypatch):
    from openagent_core.engine import MemoryDB
    from openagent_core.subscriptions.migration import adopt_remote_subscription_accounts
    from openagent_core.administration import ProviderModelAdmin, ManagementContext
    from openagent_core.contracts import PrincipalRef
    from unittest.mock import AsyncMock
    class Policy:
        async def authorize(self, *args, **kwargs): return True
    service = SubscriptionService('codex', tmp_path/'custody.json')
    service.manager.save_account({'access_token':'token-a'}, name='Personal')
    service.manager.save_account({'access_token':'token-b'}, name='Work')
    async with hosted(service) as url:
        remote = RemoteSubscriptionService('codex', url, 'inference-fixture')
        inventory=await remote.accounts();await remote.close()
        digest=account_inventory_sha256(inventory)
        (tmp_path/'consumer').mkdir()
        db=MemoryDB(str(tmp_path/'consumer'/'state.sqlite'));await db.connect()
        try:
            pid=await db.upsert_provider(name='existing',framework='api-based',api_key='inference-fixture',base_url=url+'/v1')
            mid=await db.upsert_model(provider_id=pid,model='gpt-fixture',metadata={'max_tokens':32},is_classifier=True)
            with pytest.raises(ValueError,match='changed'):
                await adopt_remote_subscription_accounts(db,pid,'codex',quiesced=True,expected_accounts_sha256='wrong')
            result=await adopt_remote_subscription_accounts(db,pid,'codex',quiesced=True,expected_accounts_sha256=digest)
            assert result['remote_custody'] and len(result['connections']) == 2
            assert (await db.get_default_router())['id'] == mid
            first=await db.get_provider(pid)
            assert first['base_url']==url+'/v1' and first['metadata']['subscription_remote']
            assert not (tmp_path/'consumer'/'subscriptions').exists()
            assert isinstance(subscription_for(first,db.db_path),RemoteSubscriptionService)
            context=ManagementContext(PrincipalRef('fixture','tenant','owner','user'),'agent')
            admin=ProviderModelAdmin(db,Policy(),reload_catalog=AsyncMock())
            before=(tmp_path/'custody.json').read_bytes()
            await admin.subscription(context,pid,'update_account',{'id':first['metadata']['account_id'],'enabled':False,'name':'Local label'})
            assert (tmp_path/'custody.json').read_bytes()==before
            assert (await db.get_provider(pid))['enabled'] is False
            account=next(a for a in await admin.subscription(context,pid,'accounts') if a['id']==first['metadata']['account_id'])
            assert account['enabled'] is False and account['credential_enabled'] is True and account['name']=='Local label'
            assert (await service.accounts())[0]['enabled'] is True
            with pytest.raises(ValueError,match='match'):
                await admin.subscription(context,pid,'update_account',{'id':'foreign','enabled':True})
            await subscription_for(first,db.db_path).close()
        finally: await db.close()


def test_account_paths_roundtrip_opaque_unicode_and_reject_unbounded_ids():
    assert '/' not in account_path('opaque/\N{SNOWMAN}')
    with pytest.raises(ValueError): account_path('')
    with pytest.raises(ValueError): account_path('x'*1025)


@pytest.mark.asyncio
async def test_native_sdk_uses_exact_remote_accounts_and_rotates_only_pre_output(tmp_path, monkeypatch):
    from openagent_core.engine import MemoryDB
    from openagent_core.models.native_provider import NativeProvider
    from openagent_core.models.providers.message import Message
    from openagent_core.subscriptions.claude.cc_anthropic import AnthropicHTTPError
    from openagent_core.subscriptions.migration import adopt_remote_subscription_accounts
    service = SubscriptionService('claude',tmp_path/'custody.json',settings={'upstream_min_interval':0,'transport':'httpx','rate_limit_retries':2})
    first=service.manager.save_account({'access_token':'first-fixture'},name='Personal',priority=1)
    service.manager.save_account({'access_token':'second-fixture'},name='Work',priority=2)
    used=[]
    def complete(client,body,*,token=None,**kwargs):
        used.append((token,body.get('max_tokens')))
        if token=='first-fixture': raise AnthropicHTTPError(429,{'error':{'type':'rate_limit_error'}})
        return reply(client,body,token=token,**kwargs)
    monkeypatch.setattr('openagent_core.subscriptions.claude.cc_anthropic.ClaudeCodeClient.complete',complete)
    async with hosted(service) as url:
        inventory=await service.accounts()
        (tmp_path/'client').mkdir()
        db=MemoryDB(str(tmp_path/'client'/'state.sqlite'));await db.connect()
        clients=[]
        try:
            pid=await db.upsert_provider(name='legacy',framework='api-based',base_url=url+'/v1',api_key='inference-fixture')
            await db.upsert_model(provider_id=pid,model='claude-sonnet-4-6',metadata={'max_tokens':32},is_classifier=True)
            result=await adopt_remote_subscription_accounts(db,pid,'claude',quiesced=True,expected_accounts_sha256=account_inventory_sha256(inventory))
            second_id=result['connections'][1]['provider_id']
            second_model=(await db.list_models(provider_id=second_id))[0]
            await db.update_model_fields(second_model['id'],{'metadata':{**second_model['metadata'],'max_tokens':128}})
            model=NativeProvider('legacy:claude-sonnet-4-6',providers_config=await db.materialise_providers_config(),db_path=db.db_path).build_runtime_model()
            response=await asyncio.wait_for(model.ainvoke([Message(role='user',content='Hello')],Message(role='assistant')),5)
            assert response.content=='second-fixture'
            assert used==[('first-fixture',32),('second-fixture',128)]
            assert '/once/v1' in model.base_url
            assert not (tmp_path/'client'/'subscriptions').exists()
            for row in await db.list_providers(): clients.append(subscription_for(row,db.db_path))
        finally:
            for client in clients: await client.close()
            await db.close()


@pytest.mark.asyncio
async def test_local_connection_toggle_cannot_disable_other_database_using_same_credential(tmp_path):
    from openagent_core.engine import MemoryDB
    from openagent_core.administration import ManagementContext,ProviderModelAdmin
    from openagent_core.contracts import PrincipalRef
    from unittest.mock import AsyncMock
    class Policy:
        async def authorize(self,*args,**kwargs):return True
    dbs=[MemoryDB(str(tmp_path/(name+'.sqlite'))) for name in ['alpha','beta']]
    services=[]
    try:
        for db in dbs: await db.connect()
        ids=[]
        for label,db in zip(['Alpha','Beta'],dbs):
            pid=await db.upsert_provider(name='account',framework='api-based',metadata={'vendor':'anthropic','access':'subscription','label':label,'account_id':'shared','credential_store_id':1})
            ids.append(pid)
            services.append(subscription_for(await db.get_provider(pid),db.db_path))
        services[0].manager.save_account({'access_token':'fixture'},name='shared')
        before=(tmp_path/'subscriptions'/'1-claude.json').read_bytes()
        context=ManagementContext(PrincipalRef('fixture','tenant','owner','user'),'agent')
        admin=ProviderModelAdmin(dbs[0],Policy(),reload_catalog=AsyncMock())
        await admin.subscription(context,ids[0],'update_account',{'id':'shared','enabled':False,'name':'Local only','priority':5})
        assert (tmp_path/'subscriptions'/'1-claude.json').read_bytes()==before
        assert (await dbs[1].get_provider(ids[1]))['enabled'] is True
        assert (await dbs[1].get_provider(ids[1]))['metadata']['label']=='Beta'
        assert (await services[1].accounts())[0]['enabled'] is True
    finally:
        for service in services: await service.close()
        for db in dbs: await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('provider',['claude','codex'])
@pytest.mark.parametrize('stream',[False,True])
async def test_real_http_completions_and_streams_use_shared_protocol(tmp_path,monkeypatch,provider,stream):
    service=SubscriptionService(provider,tmp_path/'credentials.json',settings={'upstream_min_interval':0,'rate_limit_retries':0})
    account=service.manager.save_account({'access_token':'fixture'},name='Work')
    if provider=='claude':
        def events(client,body,**kwargs):
            yield {'type':'message_start','message':{'id':'fixture','usage':{'input_tokens':2}}}
            yield {'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}}
            yield {'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'host-ok'}}
            yield {'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':1}}
            yield {'type':'message_stop'}
        monkeypatch.setattr('openagent_core.subscriptions.claude.cc_anthropic.ClaudeCodeClient.stream',events)
        def complete(client,body,**kwargs):
            return reply(client,body,token='host-ok')
        monkeypatch.setattr('openagent_core.subscriptions.claude.cc_anthropic.ClaudeCodeClient.complete',complete)
    else:
        def events(client,body,**kwargs):
            yield {'type':'response.output_text.delta','delta':'host-ok'},{}
            yield {'type':'response.completed','response':{'id':'fixture','status':'completed','usage':{'input_tokens':2,'output_tokens':1}}},{}
        monkeypatch.setattr('openagent_core.subscriptions.codex.codex_backend.CodexClient.stream',events)
    async with hosted(service) as url:
        remote=RemoteSubscriptionService(provider,url,'inference-fixture')
        try:
            body={'model':'fixture','messages':[{'role':'user','content':'hello'}]}
            if stream:
                chunks=[chunk async for chunk in remote.stream_chat(body,account_id=account['id'])]
                assert 'host-ok' in ''.join(chunks) and chunks[-1]=='data: [DONE]\n\n'
            else:
                result=await remote.complete(body,account_id=account['id'])
                assert result['choices'][0]['message']['content']=='host-ok'
        finally:await remote.close()


@pytest.mark.asyncio
async def test_remote_claude_catalog_uses_selected_account_not_pool_default(tmp_path):
    service=SubscriptionService('claude',tmp_path/'credentials.json')
    first=service.manager.save_account({'access_token':'first-fixture'},name='Personal')
    second=service.manager.save_account({'access_token':'second-fixture'},name='Work')
    await service._catalog_client.aclose()
    seen=[]
    def catalog(request):
        seen.append(request.headers['Authorization'])
        return httpx.Response(200,json={'data':[{'id':'claude-sonnet-fixture'}],'has_more':False})
    service._catalog_client=httpx.AsyncClient(transport=httpx.MockTransport(catalog))
    async with hosted(service) as url:
        remote=RemoteSubscriptionService('claude',url,'inference-fixture',account_id=second['id'])
        try:
            assert (await remote.models())[0]['id']=='claude-sonnet-fixture'
            assert seen==['Bearer second-fixture']
        finally:await remote.close()


@pytest.mark.asyncio
async def test_exact_codex_calls_share_root_usage_and_metrics_and_flush_on_shutdown(tmp_path,monkeypatch):
    service=SubscriptionService('codex',tmp_path/'credentials.json',settings={'upstream_min_interval':0,'rate_limit_retries':0})
    first=service.manager.save_account({'access_token':'first-fixture'},name='Personal')
    second=service.manager.save_account({'access_token':'second-fixture'},name='Work')
    def events(client,body,**kwargs):
        yield {'type':'response.output_text.delta','delta':'host-ok'},{}
        yield {'type':'response.completed','response':{'id':'fixture','status':'completed','usage':{'input_tokens':2,'output_tokens':1}}},{}
    monkeypatch.setattr('openagent_core.subscriptions.codex.codex_backend.CodexClient.stream',events)
    usage=tmp_path/'pool-usage.json'
    async with hosted(service,usage_file=usage) as url:
        remote=RemoteSubscriptionService('codex',url,'inference-fixture')
        try:
            body={'model':'fixture','messages':[{'role':'user','content':'hello'}]}
            async with httpx.AsyncClient(base_url=url,trust_env=False) as client:
                response=await client.post('/v1/chat/completions',headers={'Authorization':'Bearer inference-fixture'},json=body)
                assert response.status_code==200
                assert (await remote.complete(body,account_id=second['id']))['choices'][0]['message']['content']=='host-ok'
                response=await client.get('/admin/diagnostics',headers={'Authorization':'Bearer admin-fixture'})
                metrics=response.json()['metrics']
                assert metrics['accepted']==2 and metrics['completed']==2 and metrics['active']==0
                response=await client.get('/admin/usage',headers={'Authorization':'Bearer admin-fixture'})
                report=response.json()
                assert report['totals']['turns']==2 and report['totals']['input_tokens']==4
                assert set(report['by_account'])=={first['id'],second['id']}
                # This third call remains below the periodic flush threshold.
                await remote.complete(body,account_id=second['id'])
        finally:await remote.close()
    from openagent_core.subscriptions.codex.usage import UsageLedger
    report=UsageLedger(usage).report()
    assert report['totals']['turns']==3 and report['totals']['input_tokens']==6


def test_explicit_model_schema_can_restrict_a_subscription_preset():
    from openagent_core.models.settings_schema import model_settings_schema,validate_settings
    provider={'name':'work','metadata':{'vendor':'openai','access':'subscription'}}
    model={'kind':'llm','model':'fixture','metadata':{'parameter_schema':[{
        'key':'reasoning_effort','label':'Reasoning effort','type':'enum','options':['low','medium']}]}}
    assert model_settings_schema(provider,model)[0]['options']==['low','medium']
    validate_settings(provider,model,{'reasoning_effort':'medium'})
    with pytest.raises(ValueError):validate_settings(provider,model,{'reasoning_effort':'xhigh'})
