"""Instance isolation, exact account routing and streaming without a proxy process."""
import asyncio
import os
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from openagent_core.subscriptions.service import SubscriptionService
from openagent_core.subscriptions.transport import SubscriptionTransport


@pytest.mark.asyncio
@pytest.mark.parametrize("vendor", ["claude", "codex"])
async def test_import_and_construction_are_inert_and_private(tmp_path, vendor, monkeypatch):
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "foreign-secret")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "foreign-secret")
    service = SubscriptionService(vendor, tmp_path / "owner" / "credentials.json")
    other = SubscriptionService(vendor, tmp_path / "other" / "credentials.json")
    try:
        assert not list(tmp_path.iterdir())
        assert await service.accounts() == []
        assert await other.accounts() == []
        assert await service.models()
        service.manager.save_account({"access_token": "test-secret", "expires_at_ms": 0}, name="Work")
        accounts = await service.accounts()
        assert len(accounts) == 1 and accounts[0]["name"] == "Work"
        assert "test-secret" not in json.dumps(accounts)
        assert "access_token" not in json.dumps(accounts)
        assert await other.accounts() == []
        if os.name != "nt":
            assert (tmp_path / "owner" / "credentials.json").stat().st_mode & 0o777 == 0o600
    finally:
        await service.close()
        await other.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("vendor", ["claude", "codex"])
async def test_expired_unknown_and_cross_instance_login_states_are_rejected(tmp_path, vendor):
    first = SubscriptionService(vendor, tmp_path / "a.json")
    second = SubscriptionService(vendor, tmp_path / "b.json")
    try:
        login = await first.start_sign_in()
        assert login["authorize_url"].startswith("https://")
        assert "verifier" not in login
        with pytest.raises(ValueError):
            await second.complete_sign_in({"state": login["state"], "code": "not-a-code"})
        with pytest.raises(ValueError, match="state mismatch"):
            await first.complete_sign_in({"state": login["state"], "code": "http://localhost/auth/callback?code=x&state=foreign"})
        with pytest.raises(ValueError):
            await first.complete_sign_in({"state": "expired", "code": "not-a-code"})
        assert not (tmp_path / "a.json").exists()
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_claude_model_card_selects_only_its_account_and_disabled_accounts_do_not_rotate(tmp_path):
    service = SubscriptionService("claude", tmp_path / "a.json")
    first = service.manager.save_account({"access_token": "token-a"}, name="Personal")
    second = service.manager.save_account({"access_token": "token-b"}, name="Work")
    used = []

    def complete(_client, body, *, token=None, **kwargs):
        used.append(token)
        return {"id": "msg-fixture", "type": "message", "role": "assistant", "model": body["model"],
                "content": [{"type": "text", "text": "ok"}], "stop_reason": "end_turn",
                "usage": {"input_tokens": 3, "output_tokens": 1}}

    try:
        from openagent_core.subscriptions.claude.cc_anthropic import ClaudeCodeClient
        with patch.object(ClaudeCodeClient, "complete", complete):
            response = await service.inference_client(second["id"]).post("/v1/chat/completions", json={
                "model": "fixture", "messages": [{"role": "user", "content": "hello"}], "stream": False})
            assert response.status_code == 200 and used == ["token-b"]
            await service.update_account({"id": second["id"], "enabled": False})
            response = await service.inference_client(second["id"]).post("/v1/chat/completions", json={
                "model": "fixture", "messages": [{"role": "user", "content": "hello"}], "stream": False})
            assert response.status_code in {429, 503}
            assert used == ["token-b"]  # Never spends the sibling's quota.
        accounts = await service.accounts()
        assert next(a for a in accounts if a["id"] == first["id"])["enabled"] is True
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_transport_delivers_first_chunk_before_completion_and_cancels_upstream():
    waiting, closed = asyncio.Event(), asyncio.Event()

    async def app(scope, receive, send):
        assert (b"authorization", b"Bearer fixture") in scope["headers"]
        try:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"first", "more_body": True})
            await waiting.wait()
        finally:
            closed.set()

    async with httpx.AsyncClient(transport=SubscriptionTransport(app)) as client:
        async with client.stream("GET", "http://subscription.internal/stream", headers={"Authorization": "Bearer fixture"}) as response:
            iterator = response.aiter_bytes()
            assert await asyncio.wait_for(anext(iterator), 1) == b"first"
            assert not waiting.is_set() and not closed.is_set()
        await asyncio.wait_for(closed.wait(), 1)


@pytest.mark.asyncio
async def test_corrupt_credential_store_is_preserved(tmp_path):
    path = tmp_path / "credentials.json"
    path.write_text("{broken")
    service = SubscriptionService("codex", path)
    try:
        with pytest.raises(ValueError):
            await service.accounts()
        assert path.read_text() == "{broken"
    finally:
        await service.close()


def _rotation_worker(vendor, path, start, results):
    import importlib
    import time
    auth = importlib.import_module(f'openagent_core.subscriptions.{vendor}.auth')
    def refresh(token):
        results.put(('refresh', token))
        time.sleep(0.1)
        return {'access_token': 'rotated-test-access', 'refresh_token': 'rotated-test-refresh',
                'expires_at_ms': int(time.time() * 1000) + 3600000}
    auth.refresh_token = refresh
    manager = auth.TokenManager(Path(path))
    start.wait(10)
    try:
        results.put(('token', manager.get_account_token('a')))
    except Exception as error:
        results.put(('error', type(error).__name__))


@pytest.mark.parametrize('vendor', ['claude', 'codex'])
def test_refresh_token_is_rotated_once_across_processes(tmp_path, vendor):
    import multiprocessing
    from openagent_core.subscriptions.credential_storage import write_credentials
    path = tmp_path / 'credentials.json'
    write_credentials(path, {'accounts': [{'id': 'a', 'name': 'Fixture', 'access_token': 'expired-test-access',
        'refresh_token': 'original-test-refresh', 'expires_at_ms': 1}]})
    context = multiprocessing.get_context('spawn')
    start, results = context.Event(), context.Queue()
    workers = [context.Process(target=_rotation_worker, args=(vendor, str(path), start, results)) for _ in range(3)]
    try:
        for worker in workers: worker.start()
        start.set()
        output = [results.get(timeout=20) for _ in range(4)]
        for worker in workers:
            worker.join(timeout=10)
            assert worker.exitcode == 0
        assert output.count(('refresh', 'original-test-refresh')) == 1
        assert output.count(('token', 'rotated-test-access')) == 3
        saved = json.loads(path.read_text())['accounts'][0]
        assert saved['refresh_token'] == 'rotated-test-refresh'
        if os.name != "nt":
            assert path.stat().st_mode & 0o777 == 0o600
    finally:
        for worker in workers:
            if worker.is_alive(): worker.terminate()
            worker.join(timeout=5)
        results.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('start_headers', [False, True])
async def test_transport_preserves_httpx_read_deadline_and_closes_stalled_producer(start_headers):
    closed = asyncio.Event()
    async def app(scope, receive, send):
        try:
            if start_headers:
                await send({'type':'http.response.start', 'status':200, 'headers':[]})
                await send({'type':'http.response.body', 'body':b'first', 'more_body':True})
            await asyncio.Event().wait()
        finally:
            closed.set()
    async with httpx.AsyncClient(transport=SubscriptionTransport(app), timeout=0.02) as client:
        with pytest.raises(httpx.ReadTimeout):
            await asyncio.wait_for(client.get('http://subscription.internal/stalled'), 2)
        await asyncio.wait_for(closed.wait(), 1)


@pytest.mark.asyncio
async def test_transport_rejects_an_app_that_returns_without_a_response():
    async def app(scope, receive, send): pass
    async with httpx.AsyncClient(transport=SubscriptionTransport(app)) as client:
        with pytest.raises(RuntimeError, match='no response'):
            await asyncio.wait_for(client.get('http://subscription.internal/broken'), 1)


@pytest.mark.asyncio
@pytest.mark.parametrize('vendor', ['claude', 'codex'])
async def test_reauthentication_keeps_account_identity_name_and_disabled_state(tmp_path, vendor):
    import importlib
    service = SubscriptionService(vendor, tmp_path / 'store.json')
    try:
        first = service.manager.save_account({'access_token':'test-old-token'}, name='Work')
        await service.update_account({'id':first['id'], 'enabled':False})
        login = await service.start_sign_in()
        adapter = importlib.import_module(f'openagent_core.subscriptions.{vendor}.adapter')
        with patch.object(adapter, 'exchange_code', return_value={'access_token':'test-renewed', 'refresh_token':'test-refresh'}):
            result = await service.complete_sign_in({'state':login['state'], 'code':'fixture-code', 'name':'Renamed work'}, account_id=first['id'])
        assert result['account']['id'] == first['id']
        accounts = await service.accounts()
        assert len(accounts) == 1 and accounts[0]['name'] == 'Renamed work'
        assert accounts[0]['enabled'] is False
        assert 'test-renewed' not in json.dumps(result)
    finally:
        await service.close()
