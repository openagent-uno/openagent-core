"""Explicit optional HTTP custody host for shared subscription accounts.

Importing this module starts nothing. One drained legacy writer can be replaced
by this Core host without copying rotating refresh tokens to each consumer.
"""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import json
import secrets
from pathlib import Path


def account_path(identifier: str) -> str:
    if not isinstance(identifier, str) or not identifier or len(identifier.encode()) > 1024:
        raise ValueError('A bounded account identifier is required')
    return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip('=')


def account_inventory_sha256(accounts):
    """Hash reviewed identity/configuration, excluding transient quota clocks."""
    import hashlib
    fields = ('id', 'name', 'enabled', 'priority', 'managed')
    rows = [{k:a[k] for k in fields if k in a} for a in accounts]
    return hashlib.sha256(json.dumps(sorted(rows, key=lambda a:a['id']),
        sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def create_app(service, *, api_key: str | None, admin_api_key: str | None = None,
               allow_unkeyed: bool = False,
               usage_file: Path | None = None):
    """Own the supplied service's lifespan; preserve separately keyed admin API.

    Exact-account routes only forward /v1 inference paths. Management never
    inherits inference authorization. Root legacy inference preserves pool
    selection, while named connections use the encoded exact-account routes.
    """
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route
    if not api_key and not allow_unkeyed or api_key is not None and not isinstance(api_key, str):
        raise ValueError('The HTTP custody host requires an explicit inference key')
    settings = replace(service.settings, api_key=api_key, admin_api_key=admin_api_key)
    if usage_file is not None:
        if service.provider != 'codex' or not usage_file.is_absolute():
            raise ValueError('Only Codex supports an explicit absolute usage path')
        settings.usage_file = str(usage_file)
    root = service._adapter.create_app(settings, token_manager=service.manager)

    def authorized(scope):
        if not api_key: return allow_unkeyed
        headers = dict(scope.get('headers', []))
        return secrets.compare_digest(headers.get(b'authorization', b''), ('Bearer '+api_key).encode())

    async def inventory(request: Request):
        if not authorized(request.scope):
            return JSONResponse({'error': 'Inference authentication required'}, status_code=401)
        accounts = await service.accounts()
        return JSONResponse({'provider': service.provider, 'accounts': accounts,
                             'inventory_sha256': account_inventory_sha256(accounts), 'sign_in': bool(api_key and (admin_api_key == api_key or service.provider == 'codex' and admin_api_key is None))})

    async def models(request: Request):
        if not authorized(request.scope):
            return JSONResponse({'error': 'Inference authentication required'}, status_code=401)
        identifier = request.query_params.get('account_id')
        if identifier and not any(a['id'] == identifier for a in await service.accounts()):
            return JSONResponse({'error':'Unknown account'}, status_code=404)
        return JSONResponse({'data': await service.models(account_id=identifier)})

    class ExactAccount:
        async def __call__(self, scope, receive, send):
            if scope['type'] != 'http':
                from starlette.responses import PlainTextResponse
                return await PlainTextResponse('Not found', 404)(scope, receive, send)
            if not authorized(scope):
                return await JSONResponse({'error': 'Inference authentication required'}, status_code=401)(scope, receive, send)
            path = scope['path']
            # Starlette versions differ in whether a mount trims path; root_path
            # always identifies the consumed mount prefix.
            prefix = scope.get('root_path', '')
            if prefix and path.startswith(prefix): path = path[len(prefix):]
            pieces = path.lstrip('/').split('/', 2)
            once = len(pieces) == 3 and pieces[1] == 'once'
            encoded = pieces[0] if pieces else ''
            rest = ('/'+pieces[2]) if once else ('/'+path.lstrip('/').split('/', 1)[1] if '/' in path.lstrip('/') else '')
            try:
                identifier = base64.b64decode(encoded+'='*(-len(encoded)%4), altchars=b'-_', validate=True).decode()
                if account_path(identifier) != encoded: raise ValueError()
            except (ValueError, UnicodeError):
                return await JSONResponse({'error': 'Unknown account'}, status_code=404)(scope, receive, send)
            if not rest.startswith('/v1/') or not any(a['id'] == identifier for a in await service.accounts()):
                return await JSONResponse({'error': 'Unknown account route'}, status_code=404)(scope, receive, send)
            forwarded = {**scope, 'path': rest, 'raw_path': rest.encode(), 'root_path': '',
                'headers': [(k,v) for k,v in scope['headers'] if k.lower() != b'authorization']+
                           [(b'authorization', ('Bearer '+service.settings.api_key).encode())]}
            await service.inference_app(identifier, single_attempt=once)(forwarded, receive, send)

    @asynccontextmanager
    async def lifespan(app):
        try: yield
        finally: await service.close()

    return Starlette(routes=[Route('/subscription/accounts', inventory), Route('/subscription/models', models),
                             Mount('/accounts', app=ExactAccount()), Mount('/', app=root)], lifespan=lifespan)


def main(argv=None):
    """Start only from explicit CLI paths and explicitly named key variables."""
    import argparse
    import os
    import re
    import uvicorn
    from .service import SubscriptionService
    from .settings import native_settings
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', required=True, choices=['claude', 'codex'])
    parser.add_argument('--credentials', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--allow-unkeyed', action='store_true', help='Explicitly preserve an already unkeyed, network-isolated endpoint')
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args(argv)
    if not args.config.is_absolute() or not args.credentials.is_absolute():
        parser.error('Config and credential paths must be absolute')
    config = json.loads(args.config.read_text())
    if not isinstance(config, dict) or set(config)-{'settings','api_key_env','admin_api_key_env','usage_file'}:
        parser.error('Unknown host configuration fields')
    def key(field, required=False):
        name = config.get(field)
        if name is None and not required: return None
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', name):
            parser.error('An explicit key environment variable name is required')
        value = os.environ.get(name)
        if not value: parser.error('A configured key environment variable is empty')
        return value
    inference_key, admin_key = key('api_key_env', not args.allow_unkeyed), key('admin_api_key_env')
    settings = native_settings(args.provider, args.credentials, config.get('settings'))
    service = SubscriptionService(args.provider, args.credentials, settings=settings)
    usage = Path(config['usage_file']) if config.get('usage_file') else None
    uvicorn.run(create_app(service, api_key=inference_key, admin_api_key=admin_key, usage_file=usage, allow_unkeyed=args.allow_unkeyed),
                host=args.host, port=args.port, access_log=False)


if __name__ == '__main__':
    main()
