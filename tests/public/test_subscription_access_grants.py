"""Host custody, exact ephemeral grants, stream integrity and OAuth ownership."""
import asyncio
import importlib
import json
import threading
from unittest.mock import patch

import pytest

from openagent_core.subscriptions.service import SubscriptionService
from openagent_core.subscriptions.errors import SubscriptionError, validated_events, upstream_failure
from openagent_core.subscriptions.oauth import oauth, TokenEndpointError


@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude', 'codex'])
async def test_access_grant_is_exact_ephemeral_and_cannot_manage_accounts(tmp_path, vendor):
    path = tmp_path / 'private' / 'admission'
    service = SubscriptionService.from_access_token(vendor, 'private-access', account_id='exact',
                admission_path=path, vendor_account_id='vendor-workspace', plan='pro')
    try:
        assert service.manager.select_account().token == 'private-access'
        assert service.manager.account_ids() == ['exact']
        with pytest.raises(Exception):
            service.manager.get_account_token('sibling')
        assert 'private-access' not in json.dumps(await service.accounts())
        with pytest.raises(ValueError, match='host manages'):
            await service.update_account({'id': 'exact', 'enabled': False})
        with pytest.raises(ValueError):
            await service.start_sign_in()
        assert not path.exists()
        assert not list(tmp_path.iterdir())
    finally:
        await service.close()


@pytest.mark.parametrize('vendor,terminal', [('claude','message_stop'), ('codex','response.completed')])
def test_stream_requires_terminal_and_closes_source_without_false_success(vendor, terminal):
    closed = []
    def events():
        try:
            yield {'type': 'partial'}
        finally:
            closed.append(True)
    with pytest.raises(SubscriptionError, match='inference_failed'):
        list(validated_events(vendor, events()))
    assert closed == [True]
    assert list(validated_events(vendor, iter([{'type': terminal}]))) == [{'type': terminal}]
    with pytest.raises(SubscriptionError):
        list(validated_events(vendor, iter([{'type':'error','error':{'type':'overloaded_error','message':'SECRET'}}])))


def test_codex_incomplete_requires_explicit_strictness_and_preserves_headers():
    event = ({'type':'response.incomplete'}, {'x-fixture':'kept'})
    assert list(validated_events('codex', iter([event]))) == [event]
    with pytest.raises(SubscriptionError):
        list(validated_events('codex', iter([event]), require_completed=True))


@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude', 'codex'])
@pytest.mark.parametrize('stream', [False, True])
async def test_grant_inference_uses_shared_protocol_with_exact_identity_and_no_store(tmp_path, vendor, stream):
    path = tmp_path / 'admission'
    service = SubscriptionService.from_access_token(vendor, 'private-access', account_id='exact',
                admission_path=path, vendor_account_id='vendor-workspace')
    service.settings.upstream_min_interval = 0
    # Account-bound factory copies explicit zero pacing into its own adapter.
    client = service.inference_client('exact')
    used = []
    if vendor == 'claude':
        from openagent_core.subscriptions.claude.cc_anthropic import ClaudeCodeClient
        def complete(_self, payload, *, token, **kwargs):
            used.append(token)
            return {'id':'m','content':[{'type':'text','text':'ok'}], 'stop_reason':'end_turn',
                    'usage':{'input_tokens':1,'output_tokens':1}}
        def events(_self, payload, *, token, **kwargs):
            used.append(token)
            yield {'type':'message_start','message':{'id':'m','usage':{'input_tokens':1}}}
            yield {'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'ok'}}
            yield {'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':1}}
            yield {'type':'message_stop'}
        target, method, fake = ClaudeCodeClient, 'stream' if stream else 'complete', events if stream else complete
    else:
        from openagent_core.subscriptions.codex.codex_backend import CodexClient
        def events(_self, payload, *, token, account_id):
            used.append((token,account_id))
            yield {'type':'response.output_text.delta','delta':'ok'}, {}
            yield {'type':'response.completed','response':{'id':'r','usage':{'input_tokens':1,'output_tokens':1}}}, {}
        target, method, fake = CodexClient, 'stream', events
    try:
        with patch.object(target, method, fake):
            response = await client.post('/v1/chat/completions', json={
                'model':'claude-sonnet-4-6' if vendor=='claude' else 'gpt-5.6-sol',
                'messages':[{'role':'user','content':'hello'}], 'stream':stream})
        assert response.status_code == 200, response.text
        assert 'ok' in response.text
        assert used == (['private-access'] if vendor=='claude' else [('private-access','vendor-workspace')])
        assert not path.exists()
        assert not path.with_suffix('.usage.json').exists()
    finally:
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude', 'codex'])
async def test_truncated_grant_stream_has_error_and_no_done(tmp_path, vendor):
    service = SubscriptionService.from_access_token(vendor, 'access', account_id='exact', admission_path=tmp_path/'a')
    if vendor == 'claude':
        from openagent_core.subscriptions.claude.cc_anthropic import ClaudeCodeClient as Client
        def events(*args, **kwargs):
            yield {'type':'message_start', 'message':{'id':'m'}}
    else:
        from openagent_core.subscriptions.codex.codex_backend import CodexClient as Client
        def events(*args, **kwargs):
            yield {'type':'response.output_text.delta','delta':'partial'}, {}
    try:
        with patch.object(Client, 'stream', events):
            response = await service.client.post('/v1/chat/completions', json={
                'model':'claude-sonnet-4-6' if vendor=='claude' else 'gpt-5.6-sol',
                'messages':[{'role':'user','content':'hello'}], 'stream':True})
        assert 'subscription_inference_failed' in response.text
        assert '[DONE]' not in response.text
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_oauth_reuses_native_exchange_off_loop_with_explicit_host_redirect():
    main_thread=threading.get_ident()
    calls=[]
    def post(endpoint, body):
        assert threading.get_ident()!=main_thread
        calls.append((endpoint,body))
        return {'access_token':'access','refresh_token':'refresh','expires_in':3600,
                'account':{'uuid':'identity','email_address':'person@example.test'}}
    with patch('openagent_core.subscriptions.claude.auth._post_token', post):
        result=await oauth('claude','exchange',{'code':'c','state':'s','verifier':'v'},
                           redirect_uri='https://platform.claude.com/oauth/code/callback')
        await oauth('claude','refresh',{'refresh_token':'refresh'},refresh_scope='user:inference')
    assert calls[0][1]['redirect_uri']=='https://platform.claude.com/oauth/code/callback'
    assert calls[0][1]['state']=='s'
    assert calls[1][1]['scope']=='user:inference'
    assert result['account_id']=='identity' and result['email']=='person@example.test'


@pytest.mark.asyncio
@pytest.mark.parametrize('operation,fields', [('exchange',{'code':'c','state':'s','verifier':'v'}),('refresh',{'refresh_token':'r'})])
async def test_oauth_denial_is_sanitized_and_not_retried_at_another_endpoint(operation, fields):
    with patch('openagent_core.subscriptions.claude.auth._post_token', side_effect=TokenEndpointError(400,b'{"error":"invalid_grant","description":"SECRET"}')) as post:
        with pytest.raises(SubscriptionError) as caught:
            await oauth('claude',operation,fields)
    assert caught.value.status==400
    assert caught.value.code=='subscription_'+('code' if operation=='exchange' else 'refresh')+'_rejected'
    assert post.call_count==1
    assert 'SECRET' not in json.dumps(caught.value.public())


def test_quota_error_has_safe_reset_retry_and_public_core_classification():
    from openagent_core.core.public_errors import classify_run_error
    error=upstream_failure('codex',429,b'{"error":{"type":"usage_limit_reached","resets_in_seconds":3600,"message":"SECRET"}}',now_s=1000)
    assert error.resets_at_ms==4600000
    assert error.headers()['x-should-retry']=='false'
    assert 'SECRET' not in json.dumps(error.public())
    assert classify_run_error(RuntimeError('Error code: 429 - '+json.dumps({'error':error.public()}))).code=='quota_exhausted'

@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude', 'codex'])
async def test_grant_http_quota_is_safe_and_retains_retry_metadata(tmp_path, vendor):
    service=SubscriptionService.from_access_token(vendor,'private-access',account_id='a',admission_path=tmp_path/'a')
    body={'error':{'type':'usage_limit_reached','resets_in_seconds':3600,'message':'SECRET'}}
    if vendor=='claude':
        from openagent_core.subscriptions.claude.cc_anthropic import ClaudeCodeClient as Client, AnthropicHTTPError
        def fail(*args,**kwargs): raise AnthropicHTTPError(429,body)
        method='complete'
    else:
        from openagent_core.subscriptions.codex.codex_backend import CodexClient as Client, CodexHTTPError
        def fail(*args,**kwargs): raise CodexHTTPError(429,body)
        method='stream'
    try:
        with patch.object(Client,method,fail):
            response=await service.client.post('/v1/chat/completions',json={
                'model':'claude-sonnet-4-6' if vendor=='claude' else 'gpt-5.6-sol',
                'messages':[{'role':'user','content':'hello'}]})
        assert response.status_code==429, response.text
        assert response.json()['error']['code']=='subscription_usage_limit'
        assert response.headers['x-should-retry']=='false'
        assert int(response.headers['retry-after'])>=3590
        assert 'SECRET' not in response.text
    finally:
        await service.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('stream', [False, True])
async def test_claude_transport_retains_quota_headers_for_host_and_never_follows_redirect(tmp_path, stream):
    import httpx
    from openagent_core.subscriptions.claude.config import Settings
    now=__import__('time').time()
    headers={'anthropic-ratelimit-unified-status':'rejected','anthropic-ratelimit-unified-reset':str(int(now+3600))}
    original=httpx.Client
    seen=[]
    def factory(**kwargs):
        assert kwargs['trust_env'] is False and kwargs['follow_redirects'] is False
        return original(**kwargs,transport=httpx.MockTransport(lambda request:(seen.append(request) or httpx.Response(429,content=b'not JSON SECRET',headers=headers))))
    service=SubscriptionService.from_access_token('claude','private-access',account_id='exact',admission_path=tmp_path/'a',settings=Settings(creds_file=tmp_path/'a',transport='httpx'))
    try:
        with patch('httpx.Client', factory):
            response=await service.client.post('/v1/chat/completions',json={'model':'claude-sonnet-4-6','messages':[{'role':'user','content':'hello'}],'stream':stream})
        assert len(seen)==1
        assert 'subscription_usage_limit' in response.text
        assert 'resets_at_ms' in response.text
        assert 'SECRET' not in response.text
        if stream: assert '[DONE]' not in response.text
        else:
            assert response.status_code==429
            assert int(response.headers['retry-after'])>=3590
    finally:
        await service.close()


def test_bounded_raw_stream_counts_ignored_lines_and_closes_on_limit():
    from openagent_core.subscriptions.errors import bounded_sse_lines
    closed=[]
    def lines():
        try:
            yield ': heartbeat'
            yield 'malformed not even JSON'
        finally: closed.append(True)
    with pytest.raises(SubscriptionError):
        list(bounded_sse_lines(lines(),max_line_bytes=100,max_total_bytes=15))
    assert closed==[True]


@pytest.mark.asyncio
async def test_failed_codex_after_partial_output_is_sanitized_and_not_retried(tmp_path):
    from openagent_core.subscriptions.codex.codex_backend import CodexClient, CodexStreamError
    calls=[]
    def events(*args,**kwargs):
        calls.append(True)
        yield {'type':'response.output_text.delta','delta':'partial'}, {}
        raise CodexStreamError({'type':'usage_limit_reached','message':'SECRET'})
    service=SubscriptionService.from_access_token('codex','private',account_id='exact',admission_path=tmp_path/'a')
    try:
        with patch.object(CodexClient,'stream',events):
            response=await service.client.post('/v1/chat/completions',json={'model':'gpt-5.6-sol','messages':[{'role':'user','content':'hello'}],'stream':True})
        assert calls==[True]
        assert 'partial' in response.text and 'subscription_usage_limit' in response.text
        assert 'SECRET' not in response.text and '[DONE]' not in response.text
    finally:
        await service.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('stream', [False, True])
async def test_public_completion_prefetch_keeps_provider_rejection_before_host_success(tmp_path, stream):
    from openagent_core.subscriptions.claude.cc_anthropic import ClaudeCodeClient, AnthropicHTTPError
    service=SubscriptionService.from_access_token('claude','private',account_id='exact',admission_path=tmp_path/'a')
    def fail(*args,**kwargs):
        raise AnthropicHTTPError(429, {'error':{'type':'usage_limit_reached','message':'SECRET','resets_in_seconds':3600}})
    try:
        with patch.object(ClaudeCodeClient,'stream' if stream else 'complete',fail):
            with pytest.raises(SubscriptionError) as caught:
                body={'model':'claude-sonnet-4-6','messages':[{'role':'user','content':'hello'}]}
                if stream:
                    iterator=service.stream_chat(body)
                    try: await anext(iterator)
                    finally: await iterator.aclose()
                else: await service.complete(body)
        assert caught.value.status==429 and caught.value.code=='subscription_usage_limit'
        assert caught.value.resets_at_ms>__import__('time').time()*1000
        assert 'SECRET' not in json.dumps(caught.value.public())
    finally:
        await service.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude','codex'])
async def test_host_settings_mapping_is_explicit_and_cannot_rebind_the_path(tmp_path,vendor):
    service=SubscriptionService.from_access_token(vendor,'private',account_id='a',admission_path=tmp_path/'a',
        settings={'chunk_timeout':60,'upstream_min_interval':0})
    try:
        assert service.settings.chunk_timeout==60
        assert service.settings.upstream_min_interval==0
        with pytest.raises(ValueError):
            SubscriptionService.from_access_token(vendor,'private',account_id='a',admission_path=tmp_path/'a',settings={'creds_file':tmp_path/'foreign'})
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_codex_host_read_deadline_survives_shared_protocol_transport(tmp_path):
    import httpx
    original=httpx.Client
    seen=[]
    wire='data: '+json.dumps({'type':'response.output_text.delta','delta':'ok'})+'\n\n'+'data: '+json.dumps({'type':'response.completed','response':{'id':'r','usage':{}}})+'\n\n'
    def factory(**kwargs):
        seen.append(kwargs['timeout'].read)
        assert kwargs['trust_env'] is False and kwargs['follow_redirects'] is False
        return original(**kwargs,transport=httpx.MockTransport(lambda request:httpx.Response(200,content=wire)))
    service=SubscriptionService.from_access_token('codex','private',account_id='a',admission_path=tmp_path/'a',settings={'chunk_timeout':60})
    try:
        with patch('httpx.Client',factory):
            result=await service.complete({'model':'gpt-5.6-sol','messages':[{'role':'user','content':'hello'}]})
        assert result['choices'][0]['message']['content']=='ok'
        assert seen==[60]
    finally:
        await service.close()
