import asyncio
import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio

from openagent_core.engine import MemoryDB
from openagent_core.image_generation import ImageGenerationError
from openagent_core.subscriptions.images import generate_subscription_image

PNG=b'\x89PNG\r\n\x1a\nfixture-image'


@pytest_asyncio.fixture
async def routes(tmp_path,monkeypatch):
    db=MemoryDB(str(tmp_path/'state.sqlite'));await db.connect()
    clients=[];seen=[];services={};pids=[];mids=[]
    for account in ['first','second']:
        pid=await db.upsert_provider(name=account,framework='api-based',metadata={
            'vendor':'openai','access':'subscription','account_id':account,'label':account.title(),
            'account_fallback_group':'fixture-group','account_fallback_kinds':['llm','image']})
        mid=await db.upsert_model(provider_id=pid,model='image-fixture',kind='image')
        pids.append(pid);mids.append(mid)
        def receive(request,account=account):
            seen.append(account)
            if account=='first':return httpx.Response(429,json={'error':{'type':'usage_limit_reached','message':'quota'}})
            return httpx.Response(200,json={'data':[{'b64_json':base64.b64encode(PNG).decode()}]})
        client=httpx.AsyncClient(transport=httpx.MockTransport(receive));clients.append(client)
        service=SimpleNamespace(settings=SimpleNamespace(api_key='fixture'),
            accounts=AsyncMock(return_value=[{'id':account,'enabled':True}]),
            inference_client=lambda identifier,**kwargs:clients[['first','second'].index(identifier)],
            inference_base_url=lambda identifier,**kwargs:'https://fixture.test/accounts/'+identifier+'/v1')
        services[pid]=service
    monkeypatch.setattr('openagent_core.subscriptions.images.subscription_for',lambda row,path:services[row['id']])
    from openagent_core.core.budget_guard import BudgetGuard
    guard=BudgetGuard(db, providers_config=await db.materialise_providers_config(enabled_only=False))
    try:yield db,pids,mids,services,clients,seen,guard
    finally:
        for client in clients:await client.aclose()
        await db.close()


@pytest.mark.asyncio
async def test_declared_image_group_fails_over_once_and_reports_the_actual_account(routes):
    db,pids,mids,services,clients,seen,guard=routes
    image=await generate_subscription_image(db,pids[0],model_id=mids[0],model='image-fixture',prompt='A blue circle',budget_guard=guard)
    assert seen==['first','second'] and image.content==PNG
    assert image.connection=='second' and image.account_id=='second' and image.connection_label=='Second'
    assert not clients[0].is_closed


@pytest.mark.asyncio
async def test_disabled_or_undeclared_group_remains_exact_and_disabled_accounts_are_skipped(routes):
    db,pids,mids,services,clients,seen,guard=routes
    row=await db.get_provider(pids[0]);meta=row['metadata']
    await db.update_provider_fields(pids[0],{'metadata':{**meta,'account_fallback_enabled':False}})
    with pytest.raises(ImageGenerationError) as error:
        await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert error.value.status==429 and seen==['first']
    await db.update_provider_fields(pids[0],{'metadata':meta})
    services[pids[0]].accounts=AsyncMock(return_value=[{'id':'first','enabled':False}]);seen.clear()
    image=await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert image.account_id=='second' and seen==['second']
    await db.update_provider_fields(pids[1],{'enabled':False});seen.clear()
    with pytest.raises(ImageGenerationError):
        await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert not seen


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',[400,500,'invalid','transport','cancel'])
async def test_uncertain_jobs_and_non_account_failures_are_never_replayed(routes,failure):
    db,pids,mids,services,clients,seen,guard=routes
    def receive(request):
        seen.append('first')
        if failure=='transport':raise httpx.ReadError('fixture',request=request)
        if failure=='cancel':raise asyncio.CancelledError()
        if failure=='invalid':return httpx.Response(200,json={'data':[{'b64_json':base64.b64encode(b'not-an-image').decode()}]})
        return httpx.Response(failure,json={'error':{'message':'fixture'}})
    client=httpx.AsyncClient(transport=httpx.MockTransport(receive));clients.append(client)
    services[pids[0]].inference_client=lambda identifier,**kwargs:client
    with pytest.raises((ImageGenerationError,ValueError,httpx.ReadError,asyncio.CancelledError)):
        await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert seen==['first']


@pytest.mark.asyncio
async def test_budget_filter_and_typed_shared_rows_do_not_repeat_an_account(routes):
    db,pids,mids,services,clients,seen,guard=routes
    row=await db.get_provider(pids[0]);await db.update_provider_fields(pids[0],{'metadata':{**row['metadata'],'credential_store_id':pids[0]}})
    duplicate=await db.upsert_provider(name='first-text',framework='api-based',metadata={**row['metadata'],'credential_store_id':pids[0]})
    await db.upsert_model(provider_id=duplicate,model='image-fixture',metadata={'capabilities':['image_generation']})
    services[duplicate]=services[pids[0]]
    await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert seen==['first','second']
    seen.clear()
    await db.add_budget(scope_kind='provider', scope_value='first', metric='tokens', window='day', amount=1)
    await db.record_usage(model='first:image-fixture', input_tokens=2, output_tokens=0, cost=0)
    await guard.refresh(emit_alerts=False)
    await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert seen==['second']
    await db.add_budget(scope_kind='global', metric='tokens', window='day', amount=1)
    await guard.refresh(emit_alerts=False)
    seen.clear()
    with pytest.raises(ImageGenerationError,match='budget'):
        await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert not seen


@pytest.mark.asyncio
async def test_default_budget_admission_uses_the_public_database_and_no_new_history(routes):
    db,pids,mids,services,clients,seen,guard=routes
    before=await (await db._conn.execute('SELECT COUNT(*) FROM sessions')).fetchone()
    image=await generate_subscription_image(db,pids[0],model_id=mids[0],model='image-fixture',prompt='fixture')
    after=await (await db._conn.execute('SELECT COUNT(*) FROM sessions')).fetchone()
    assert image.account_id=='second' and before[0]==after[0]==0


@pytest.mark.asyncio
async def test_older_text_only_policy_does_not_silently_enable_image_rotation(routes):
    db,pids,mids,services,clients,seen,guard=routes
    row=await db.get_provider(pids[0]);meta=dict(row['metadata']);meta.pop('account_fallback_kinds')
    await db.update_provider_fields(pids[0],{'metadata':meta})
    with pytest.raises(ImageGenerationError):
        await generate_subscription_image(db,pids[0],model='image-fixture',prompt='fixture',budget_guard=guard)
    assert seen==['first']


@pytest.mark.parametrize(('kind','key','value'),[('tts','voice_id',False),('stt','language',0)])
def test_audio_model_controls_validate_the_declared_string_type(kind,key,value):
    from openagent_core.models.settings_schema import validate_settings
    provider={'name':'fixture','metadata':{'vendor':'openai','access':'api'}}
    model={'kind':kind,'model':'fixture'}
    with pytest.raises(ValueError):validate_settings(provider,model,{key:value})
    validate_settings(provider,model,{key:''})
