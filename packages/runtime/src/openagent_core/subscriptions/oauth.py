"""Public token exchange for a host that owns PKCE and encrypted custody.

No browser, callback listener, environment lookup or credential persistence.
Hosts verify state, keep the PKCE verifier private, and authorize each use.
"""
from __future__ import annotations
import importlib
import json
import time
from .errors import ERROR_BODY_LIMIT, SubscriptionError, upstream_failure


class TokenEndpointError(RuntimeError):
    def __init__(self, status, body=b''):
        self.status = status
        self.body = body[:ERROR_BODY_LIMIT]
        # Native refresh guards recognize invalid_grant, without retaining an
        # upstream description in the exception message or host logs.
        try:
            data = json.loads(self.body)
            code = data.get('error') if isinstance(data, dict) else None
        except ValueError:
            code = None
        super().__init__('invalid_grant' if code == 'invalid_grant' else 'Subscription token endpoint failed')


def post_token(vendor, endpoint, body):
    """Shared bounded token transport; only provider-owned URLs reach here."""
    agent = ('claude-cli/' + importlib.import_module(
        'openagent_core.subscriptions.claude.cc_anthropic').CC_VERSION + ' (external, sdk-cli)'
        if vendor == 'claude' else 'codex_cli_rs/0.149.0')
    headers = {'content-type': 'application/json', 'accept': 'application/json', 'user-agent': agent}
    payload = json.dumps(body).encode()
    try:
        from curl_cffi import requests
    except ImportError:
        import urllib.request
        import urllib.error
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            response = opener.open(urllib.request.Request(endpoint, data=payload, headers=headers), timeout=20)
        except urllib.error.HTTPError as error:
            raise TokenEndpointError(error.code, error.read(ERROR_BODY_LIMIT)) from None
        with response:
            status, content = response.status, response.read(ERROR_BODY_LIMIT + 1)
    else:
        with requests.Session(impersonate='chrome', trust_env=False) as session:
            response = session.post(endpoint, data=payload, headers=headers, timeout=20, allow_redirects=False)
            status, content = response.status_code, response.content
    if status != 200 or len(content) > ERROR_BODY_LIMIT:
        raise TokenEndpointError(status, content)
    try:
        data = json.loads(content)
    except ValueError:
        raise TokenEndpointError(502) from None
    if not isinstance(data, dict) or not isinstance(data.get('access_token'), str) or not data['access_token']:
        raise TokenEndpointError(502)
    return data


async def oauth(provider, operation, fields, *, redirect_uri=None, refresh_scope=None):
    """Exchange or refresh through the same protocol used by native accounts.

    Explicit redirect/scope must match the host's issued authorization request.
    Return credential material only to the trusted host; never expose it in a
    public UI response. This operation does not read or write any store.
    """
    if provider not in {'claude', 'codex'} or not isinstance(fields, dict):
        raise ValueError('Invalid subscription provider or fields')
    expected = {'code', 'state', 'verifier'} if operation == 'exchange' else {'refresh_token'}
    if operation not in {'exchange', 'refresh'} or set(fields) != expected or any(
        not isinstance(value, str) or not value or len(value) > 8192 for value in fields.values()
    ):
        raise ValueError('Invalid subscription OAuth operation or fields')
    auth = importlib.import_module(f'openagent_core.subscriptions.{provider}.auth')
    from openagent_core.persistence import run_sync
    def perform():
        if operation == 'refresh':
            return auth.refresh_token(fields['refresh_token'], **({'scope': refresh_scope} if refresh_scope is not None else {}))
        kwargs = {'redirect_uri': redirect_uri} if redirect_uri is not None else {}
        if provider == 'claude':
            return auth.exchange_code(fields['code'], fields['verifier'], fields['state'], **kwargs)
        return auth.exchange_code(fields['code'], fields['verifier'], **kwargs)
    try:
        result = await run_sync(perform)
        if provider == 'codex' and operation == 'exchange' and not result.get('account_id'):
            raise SubscriptionError()
        return result
    except SubscriptionError:
        raise
    except TokenEndpointError as error:
        failure = upstream_failure(provider, error.status, error.body)
        if error.status in {400, 401}:
            raise SubscriptionError(400, 'subscription_code_rejected' if operation == 'exchange'
                                    else 'subscription_refresh_rejected') from None
        raise failure from None
    except Exception as error:
        if isinstance(error.__cause__, TokenEndpointError) and error.__cause__.status in {400, 401}:
            raise SubscriptionError(400, 'subscription_refresh_rejected') from None
        raise SubscriptionError() from None
