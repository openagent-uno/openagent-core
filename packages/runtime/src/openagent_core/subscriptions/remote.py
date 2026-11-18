"""Exact-account connection to an explicitly configured Core custody host."""
from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx

from .host import account_path
from .service import SubscriptionService


def remote_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise ValueError('An explicit HTTP(S) custody host URL is required')
    value = value.rstrip('/')
    return value[:-3] if value.endswith('/v1') else value


class RemoteSubscriptionService(SubscriptionService):
    """No credential file, token exchange or discovery on the connecting host."""
    def __init__(self, provider: str, base_url: str, api_key: str, *, account_id=None, allow_unkeyed=False):
        if provider not in {'claude', 'codex'} or (not isinstance(api_key, str) or not api_key) and not allow_unkeyed:
            raise ValueError('A supported provider and explicit custody-host key are required')
        self.provider, self.base_url = provider, remote_base_url(base_url)
        self.account_id = account_id
        self.settings = SimpleNamespace(api_key=api_key or "unkeyed", admin_api_key=api_key or "unkeyed", request_timeout=1800)
        self._access_grant = False
        self._catalog_client = None
        self._inference_clients = {}
        self.client = self._client(self.base_url, timeout=15)

    def _client(self, base_url, timeout):
        return httpx.AsyncClient(base_url=base_url, timeout=timeout, follow_redirects=False,
            trust_env=False, headers={'Authorization': 'Bearer '+self.settings.api_key})

    def inference_base_url(self, account_id=None, *, single_attempt=False):
        if not account_id: raise ValueError('Remote subscription connections require an exact account')
        return self.base_url+'/accounts/'+account_path(account_id)+('/once' if single_attempt else '')+'/v1'

    def inference_client(self, account_id=None, *, single_attempt=False):
        if not account_id: raise ValueError('Remote subscription connections require an exact account')
        key = (account_id, single_attempt)
        if key not in self._inference_clients:
            url = self.inference_base_url(account_id)[:-3]
            if single_attempt: url += '/once'
            self._inference_clients[key] = self._client(url, self.settings.request_timeout)
        return self._inference_clients[key]

    async def _read(self, path, **kwargs):
        async with self.client.stream('GET', path, **kwargs) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 2 << 20: raise ValueError('Custody host response exceeds limit')
        import json
        return json.loads(body)

    async def capabilities(self):
        result = await self._read("/subscription/accounts")
        return {"sign_in": result.get("sign_in") is True, "credential_management": "remote_host"}

    async def accounts(self):
        result = await self._read('/subscription/accounts')
        fields = {'id','name','enabled','priority','plan','managed','limited','limited_until_ms',
                  'limited_for_s','expires_at_ms','has_refresh_token','quota','status'}
        accounts = result.get('accounts') if isinstance(result, dict) else None
        if not isinstance(result, dict) or result.get('provider') != self.provider or not isinstance(accounts, list) or len(accounts) > 256:
            raise ValueError('Invalid custody account inventory')
        filtered = []
        for account in accounts:
            if not isinstance(account, dict): raise ValueError('Invalid custody account')
            account_path(account.get('id'))
            filtered.append({k:v for k,v in account.items() if k in fields})
        return filtered

    async def models(self, *, account_id=None):
        result = await self._read('/subscription/models', params={'account_id':account_id or self.account_id} if account_id or self.account_id else {})
        if not isinstance(result, dict) or not isinstance(result.get('data'), list):
            raise ValueError('Invalid custody model catalog')
        return result['data']

    def account_available(self, identifier):
        # AccountRoutes runs synchronous availability in its persistence worker.
        # This is a fresh credential-state read, not an authorization cache.
        with httpx.Client(base_url=self.base_url, timeout=15, trust_env=False, follow_redirects=False,
                          headers={'Authorization': 'Bearer '+self.settings.api_key}) as client:
            response = client.get('/subscription/accounts')
            response.raise_for_status()
            if len(response.content) > 2 << 20: raise ValueError('Custody inventory exceeds limit')
            data = response.json()
            if data.get('provider') != self.provider: raise ValueError('Custody provider mismatch')
            return any(a.get('id') == identifier and a.get('enabled') is not False for a in data.get('accounts', []))

    async def update_account(self, fields):
        raise ValueError('Credential administration belongs to the custody host')
