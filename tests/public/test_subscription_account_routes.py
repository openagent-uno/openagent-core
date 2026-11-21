"""Same-model account failover preserves settings, budgets, usage and stream boundaries."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models import account_routes as A
from openagent_core.models.catalog import CatalogModel
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.like import OpenAILike
from openagent_core.models.providers.response import ModelResponse
from openagent_core.models.providers.metrics import MessageMetrics


def entry(name):
    return CatalogModel(name, 'fixture', name + ':fixture', 'platform')


def policy(a, b, guard=None, available=None):
    return A.AccountRoutes('first:fixture', (entry('first'), entry('second')),
                           lambda rid: b, guard, available)


def response(text='OK'):
    return ModelResponse(content=text, response_usage=MessageMetrics(
        input_tokens=100, output_tokens=5, cache_read_tokens=40))


class Script:
    def __init__(self, failure=None, rows=()):
        self.failure, self.rows, self.calls, self.closed = failure, rows, 0, False

    @A.account_call
    def invoke(self):
        self.calls += 1
        if self.failure: raise self.failure
        return response()

    @A.account_acall
    async def ainvoke(self):
        self.calls += 1
        await asyncio.sleep(0)
        if self.failure: raise self.failure
        return response()

    @A.account_stream
    def invoke_stream(self):
        self.calls += 1
        try:
            yield from self.rows
            if self.failure: raise self.failure
        finally:
            self.closed = True

    @A.account_astream
    async def ainvoke_stream(self):
        self.calls += 1
        try:
            for row in self.rows:
                yield row
            if self.failure: raise self.failure
        finally:
            self.closed = True


@pytest.mark.parametrize('status', [401, 403, 429, 529])
@pytest.mark.asyncio
async def test_only_account_failures_rotate_before_any_output(status):
    a, b = Script(ModelProviderError(message='fixture', status_code=status)), Script(rows=[response()])
    a._openagent_account_routes = policy(a, b)
    sink, token = A.open_usage()
    try:
        assert (await a.ainvoke()).content == 'OK'
        assert A.charge_rows(sink, 'first:fixture', (100, 5, 40)) == [('second:fixture', 100, 5, 40)]
        assert A.effective_model('first:fixture') == 'second:fixture'
    finally:
        A.close_usage(token)
    assert a.calls == b.calls == 1
    assert A.effective_model('first:fixture') == 'first:fixture'


@pytest.mark.asyncio
async def test_no_retry_for_bad_request_or_cancel_or_after_even_an_empty_frame():
    for failure, rows in [(ModelProviderError(message='bad request', status_code=400), ()),
                          (asyncio.CancelledError(), ()),
                          (ModelProviderError(message='quota', status_code=429), (ModelResponse(),))]:
        a, b = Script(failure, rows), Script(rows=[response()])
        a._openagent_account_routes = policy(a, b)
        with pytest.raises(type(failure)):
            async for _ in a.ainvoke_stream(): pass
        assert b.calls == 0
        assert a.closed


def test_sync_stream_and_call_respect_the_same_boundary():
    a, b = Script(ModelProviderError(message='quota', status_code=429)), Script(rows=[response()])
    a._openagent_account_routes = policy(a, b)
    assert a.invoke().content == 'OK'
    assert [x.content for x in a.invoke_stream()] == ['OK']
    assert a.closed and b.closed
    a.rows = [ModelResponse()]
    with pytest.raises(ModelProviderError): list(a.invoke_stream())
    assert b.calls == 2


@pytest.mark.asyncio
async def test_budget_and_disabled_accounts_are_not_contacted():
    a, b = Script(), Script()
    guard = SimpleNamespace(blocked_runtime_ids=lambda entries: {'first:fixture'})
    a._openagent_account_routes = policy(a, b, guard)
    assert (await a.ainvoke()).content == 'OK'
    assert a.calls == 0 and b.calls == 1
    a._openagent_account_routes = policy(a, b, available=lambda rid: False)
    with pytest.raises(PermissionError): await a.ainvoke()
    assert a.calls == 0 and b.calls == 1


@pytest.mark.asyncio
async def test_stream_cancel_closes_current_transport_without_contacting_sibling():
    a, b = Script(rows=[response(), response()]), Script()
    a._openagent_account_routes = policy(a, b)
    it = a.ainvoke_stream()
    assert (await anext(it)).content == 'OK'
    await it.aclose()
    assert a.closed and b.calls == 0


@pytest.mark.asyncio
async def test_real_sdk_uses_each_candidates_own_model_settings_and_preserves_tool_messages():
    requests = []
    def handler(label):
        def receive(request):
            import json
            body = json.loads(request.content); requests.append((label, body))
            if label == 'first':
                return httpx.Response(429, json={'error': {'message': 'quota', 'type': 'rate_limit'}})
            return httpx.Response(200, json={'id': 'fixture', 'object': 'chat.completion',
                'created': 1, 'model': 'fixture', 'choices': [{'index': 0,
                    'message': {'role': 'assistant', 'content': 'OK'}, 'finish_reason': 'stop'}],
                'usage': {'prompt_tokens': 100, 'completion_tokens': 5, 'total_tokens': 105,
                    'prompt_tokens_details': {'cached_tokens': 40}}})
        return receive
    clients = [httpx.AsyncClient(transport=httpx.MockTransport(handler(x))) for x in ('first', 'second')]
    a, b = [OpenAILike(id='fixture', api_key='fixture', http_client=c, max_retries=0,
                      max_tokens=n) for c, n in zip(clients, (32, 128))]
    a._openagent_account_routes = policy(a, b)
    messages = [Message(role='user', content='hello'), Message(role='assistant', content=None,
        tool_calls=[{'id': 'call_fixture', 'type': 'function', 'function': {'name': 'lookup', 'arguments': '{}'}}]),
        Message(role='tool', content='already executed', tool_call_id='call_fixture')]
    sink, token = A.open_usage()
    try:
        result = await a.ainvoke(messages, Message(role='assistant'))
        assert result.content == 'OK'
        assert [r[1]['max_tokens'] for r in requests] == [32, 128]
        assert requests[0][1]['messages'] == requests[1][1]['messages']
        assert requests[1][1]['messages'][-1]['content'] == 'already executed'
        assert A.charge_rows(sink, 'first:fixture', (100, 5, 40)) == [('second:fixture', 100, 5, 40)]
    finally:
        A.close_usage(token)
        for c in clients: await c.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('stream', [False, True])
async def test_router_records_actual_account_once_and_preserves_badge(stream):
    from openagent_core.models.base import BaseModel, ModelResponse as Result
    from openagent_core.models.dispatcher import ModelDispatcher, TeamRouterProvider
    from openagent_core.models import stream_usage
    a, b = Script(ModelProviderError(message='quota', status_code=429)), Script()
    a._openagent_account_routes = policy(a, b)
    class Runtime(BaseModel):
        async def generate(self, **kwargs):
            await a.ainvoke()
            return Result(content='OK', input_tokens=100, output_tokens=5, cache_read_tokens=40)
        async def stream(self, *args, **kwargs):
            await a.ainvoke()
            stream_usage.record(input_tokens=100, output_tokens=5, cache_read_tokens=40)
            yield 'OK'
    router = TeamRouterProvider('first:fixture')
    router._ensure_runtime = lambda *args, **kwargs: Runtime()
    router.record_cost = AsyncMock()
    if stream:
        assert [x async for x in router.stream([{'role': 'user', 'content': 'hello'}], session_id='one')] == ['OK']
    else:
        result = await router.generate([{'role': 'user', 'content': 'hello'}], session_id='one')
        assert result.model == 'second:fixture'
    router.record_cost.assert_awaited_once_with(input_tokens=100, output_tokens=5,
        cache_read_tokens=40, session_id='one', runtime_id='second:fixture')
    assert router.effective_model_id('one') == 'second:fixture'
    assert router.effective_model_id('other') == 'first:fixture'
    # The stream's terminal frame and channel footer read the dispatcher,
    # which must retain the provider's actual-account resolution.
    dispatcher = ModelDispatcher()
    dispatcher._last_pick_by_session['one'] = 'first:fixture'
    dispatcher._last_provider_by_session['one'] = router
    assert dispatcher.effective_model_id('one') == 'second:fixture'


def test_dispatcher_badge_uses_only_the_latest_provider_for_each_session():
    from openagent_core.models.dispatcher import ModelDispatcher, TeamRouterProvider
    old = TeamRouterProvider('old:leader')
    old._last_delegation_by_session['one'] = 'old:specialist'
    current = TeamRouterProvider('current:leader')
    current._last_delegation_by_session['one'] = 'current:specialist'
    current._account_models_by_session['one'] = {'current:specialist': 'backup:answer'}
    dispatcher = ModelDispatcher()
    dispatcher._team_providers = {'old:leader': old, 'current:leader': current}
    dispatcher._last_pick_by_session = {'one': 'current:leader', 'other': 'old:leader'}
    dispatcher._last_provider_by_session['one'] = current
    assert dispatcher.effective_model_id('one') == 'backup:answer'
    assert dispatcher.effective_model_id('other') == 'old:leader'
    assert dispatcher.effective_model_id('unknown') is None


def test_mixed_usage_charges_only_the_unattributed_remainder():
    sink = {'models': {'second:fixture': [100, 5, 40]}, 'selected': {}}
    assert A.charge_rows(sink, 'first:fixture', (120, 8, 41)) == [
        ('second:fixture', 100, 5, 40), ('first:fixture', 20, 3, 1)]


@pytest.mark.asyncio
async def test_migrated_native_models_keep_exact_clients_settings_and_explicit_route_group(tmp_path):
    import hashlib, json
    from openagent_core.engine import MemoryDB
    from openagent_core.models.native_provider import NativeProvider
    from openagent_core.subscriptions.migration import adopt_subscription_accounts
    from openagent_core.subscriptions.service import subscription_for
    db = MemoryDB(str(tmp_path / 'state.sqlite')); await db.connect()
    source = tmp_path / 'legacy.json'
    source.write_text(json.dumps({'accounts': [
        {'id': 'one', 'name': 'One', 'access_token': 'fixture-one', 'priority': 0},
        {'id': 'two', 'name': 'Two', 'access_token': 'fixture-two', 'priority': 10}]}))
    services = []
    try:
        pid = await db.upsert_provider(name='legacy', framework='api-based', metadata={'accounts': [
            {'api_key': 'old-pool-one', 'base_url': 'http://old-proxy.invalid/v1'},
            {'api_key': 'old-pool-two', 'base_url': 'http://old-proxy.invalid/v1'}]})
        await db.upsert_model(provider_id=pid, model='claude-sonnet-4-6', metadata={'max_tokens': 32})
        result = await adopt_subscription_accounts(db, pid, 'claude', source, quiesced=True,
            expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
        sibling = result['connections'][1]
        models = await db.list_models(provider_id=sibling['provider_id'])
        await db.update_model_fields(models[0]['id'], {'metadata': {'max_tokens': 128}})
        config = await db.materialise_providers_config()
        model = NativeProvider('legacy:claude-sonnet-4-6', providers_config=config,
            db_path=db.db_path).build_runtime_model()
        routes = model._openagent_account_routes
        candidates = [x async for x in routes.acandidates(model)]
        assert [x[1].max_tokens for x in candidates] == [32, 128]
        for index, (_, candidate) in enumerate(candidates):
            row = await db.get_provider(result['connections'][index]['provider_id'])
            service = subscription_for(row, db.db_path); services.append(service)
            assert candidate.http_client is service.inference_client(row['metadata']['account_id'], single_attempt=True)
            assert candidate.api_key == service.settings.api_key
        assert getattr(candidates[1][1], '_openagent_account_routes', None) is None
        await services[0].update_account({'id': 'one', 'enabled': False})
        assert [rid for rid, _ in [x async for x in routes.acandidates(model)]] == [candidates[1][0]]
        # Removing the declaration restores exact-account behavior.
        for row in config: row['metadata'].pop('account_fallback_group', None)
        exact = NativeProvider('legacy:claude-sonnet-4-6', providers_config=config,
            db_path=db.db_path).build_runtime_model()
        assert getattr(exact, '_openagent_account_routes', None) is None
    finally:
        for service in services: await service.close()
        await db.close()


@pytest.mark.asyncio
async def test_concurrent_turns_do_not_share_usage_or_effective_account():
    async def run(name):
        sink, token = A.open_usage()
        try:
            A.record_response('requested', name, response())
            await asyncio.sleep(0)
            assert A.effective_model('requested') == name
            return A.charge_rows(sink, 'requested', (100, 5, 40))
        finally:
            A.close_usage(token)
    one, two = await asyncio.gather(run('one'), run('two'))
    assert one == [('one', 100, 5, 40)] and two == [('two', 100, 5, 40)]


def test_primary_recovery_in_one_turn_resets_a_prior_fallback_identity_and_billing():
    a=SimpleNamespace(id='a',_openagent_runtime_id='first:a')
    b=SimpleNamespace(id='b',_openagent_runtime_id='second:b')
    sink,token=A.open_usage()
    try:
        A.record_model_result('first:a',b,response())
        A.record_model_result('first:a',a,response())
        assert A.effective_model('first:a')=='first:a'
        assert A.charge_rows(sink,'first:a',(200,10,80))==[
            ('second:b',100,5,40),('first:a',100,5,40)]
    finally:A.close_usage(token)
