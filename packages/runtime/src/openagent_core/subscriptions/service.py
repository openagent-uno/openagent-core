"""Explicit, instance-owned Claude/Codex subscription lifecycle and OAuth API."""
from __future__ import annotations

import importlib
import secrets
from pathlib import Path

from openagent_core.instance_state import InstanceMapping

_SERVICES = InstanceMapping("model-subscriptions")


class SubscriptionService:
    """Host supplies the credential path; no environment, keychain or CLI imports.

    The optional protocol app is invoked in memory, including streamed chunks.
    It is never mounted on a host gateway or started as an HTTP listener.
    """
    def __init__(self, provider: str, credentials_path: Path, *, settings=None, _access_grant=None):
        if provider not in {"claude", "codex"}:
            raise ValueError("Unknown subscription provider")
        if not credentials_path.is_absolute():
            raise ValueError("An absolute, host-owned credential path is required")
        import httpx
        from .transport import SubscriptionTransport
        base = f"openagent_core.subscriptions.{provider}"
        config = importlib.import_module(base + ".config")
        auth = importlib.import_module(base + ".auth")
        adapter = importlib.import_module(base + ".adapter")
        if isinstance(settings, dict):
            if "creds_file" in settings or "admission_path" in settings:
                raise ValueError("The host supplies the credential path separately")
            settings = config.Settings(creds_file=credentials_path, **settings)
        settings = settings or config.Settings(creds_file=credentials_path)
        if Path(settings.creds_file) != credentials_path:
            raise ValueError("The credential path must match the protocol settings")
        if provider == "codex":
            settings.usage_file = str(credentials_path.with_suffix(".usage.json"))
        settings.api_key = secrets.token_urlsafe(32)
        settings.admin_api_key = secrets.token_urlsafe(32)
        self.provider, self.settings = provider, settings
        self._inference_clients = {}
        self._inference_apps = {}
        self._catalog_client = httpx.AsyncClient(timeout=10) if provider == "claude" else None
        self._adapter = adapter
        self._access_grant = _access_grant is not None
        if self._access_grant:
            from .grants import AccessGrantManager
            self.manager = AccessGrantManager(auth, **_access_grant)
        else:
            self.manager = auth.TokenManager(credentials_path, **({"settings": settings} if provider == "codex" else {}))
        self.app = adapter.create_app(settings, token_manager=self.manager)
        self.client = httpx.AsyncClient(
            transport=SubscriptionTransport(self.app), base_url="http://subscription.internal",
            headers={"Authorization": "Bearer " + settings.api_key}, timeout=settings.request_timeout,
        )

    @classmethod
    def from_access_token(cls, provider: str, access_token: str, *, account_id: str,
                          admission_path: Path, vendor_account_id='', plan='', settings=None):
        """Use a host's authorized grant without copying its credential store.

        This inference-only service never persists tokens or refresh material.
        The explicit admission path coordinates workers and may hold nonsecret
        pacing/lease files. The host owns authorization, renewal and storage.
        """
        if provider not in {'claude', 'codex'}:
            raise ValueError('Unknown subscription provider')
        from dataclasses import replace
        config = importlib.import_module(f'openagent_core.subscriptions.{provider}.config')
        if isinstance(settings, dict):
            if "creds_file" in settings:
                raise ValueError("The host supplies the admission path separately")
            options = config.Settings(creds_file=admission_path, **settings)
        else:
            options = replace(settings) if settings is not None else config.Settings(creds_file=admission_path)
        if Path(options.creds_file) != admission_path or options.admission_path is not None and Path(options.admission_path) != admission_path:
            raise ValueError('The admission path must match the protocol settings')
        options.rate_limit_retries = 0
        options.require_completed = True
        if provider == 'codex': options.usage_enabled = False
        return cls(provider, admission_path, settings=options, _access_grant={
            'access_token':access_token, 'identifier':account_id,
            'vendor_account_id':vendor_account_id, 'plan':plan})

    async def complete(self, body: dict, *, account_id: str | None = None) -> dict:
        """One shared Chat Completions result for a trusted host adapter."""
        from .errors import SubscriptionError, completion_failure
        response = await self.inference_client(account_id).post(
            "/v1/chat/completions", json={**body, "stream": False})
        if len(response.content) > 32 << 20:
            raise SubscriptionError()
        try:
            result = response.json()
        except ValueError:
            raise SubscriptionError() from None
        if not response.is_success or (isinstance(result, dict) and result.get("error")):
            raise completion_failure(self.provider, result, response.status_code, response.headers)
        if not isinstance(result, dict):
            raise SubscriptionError()
        return result

    async def stream_chat(self, body: dict, *, account_id: str | None = None):
        """Validated OpenAI SSE frames; prefetch keeps early errors as errors.

        A host can await the first frame before committing its HTTP status.
        Closing this iterator closes the shared upstream response. Heartbeat
        comments do not prematurely commit a host's successful inference.
        """
        import json
        from .errors import SubscriptionError, completion_failure
        async with self.inference_client(account_id).stream(
                "POST", "/v1/chat/completions", json={**body, "stream": True}) as response:
            if not response.is_success:
                content = await response.aread()
                try:
                    result = json.loads(content) if len(content) <= 64 << 10 else {}
                except ValueError:
                    result = {}
                raise completion_failure(self.provider, result, response.status_code, response.headers)
            total, pending, completed = 0, [], False
            async for line in response.aiter_lines():
                size = len(line.encode("utf-8"))
                total += size
                if size > 4 << 20 or total > 32 << 20:
                    raise SubscriptionError()
                if line.startswith("data:"):
                    pending.append(line[5:].lstrip())
                elif not line and pending:
                    data = "\n".join(pending)
                    pending.clear()
                    if data.strip() == "[DONE]":
                        completed = True
                        yield "data: [DONE]\n\n"
                        break
                    try:
                        event = json.loads(data)
                    except ValueError:
                        raise SubscriptionError() from None
                    if not isinstance(event, dict):
                        raise SubscriptionError()
                    if event.get("error"):
                        raise completion_failure(self.provider, event, 200, response.headers)
                    yield "data: " + data + "\n\n"
            if not completed:
                raise SubscriptionError(code="subscription_inference_failed",
                                        message="The subscription provider stopped this answer before it completed.")

    async def request(self, method: str, path: str, **kwargs):
        if self._access_grant and path.startswith('/admin/') and method != 'GET':
            raise ValueError('The host manages this account; this service only accepts access grants')
        response = await self.client.request(method, path,
            headers={"Authorization": "Bearer " + self.settings.admin_api_key}, **kwargs)
        if not response.is_success:
            # Never surface credential exchange bodies or upstream exception strings.
            raise ValueError("Subscription operation failed; reconnect the account" if response.status_code != 400
                             else "Invalid or expired subscription sign-in; start again")
        return response.json()

    async def capabilities(self):
        return {"sign_in": True, "credential_management": "local"}

    async def accounts(self):
        result = await self.request("GET", "/admin/accounts")
        fields = {"id", "name", "enabled", "priority", "plan", "managed", "limited", "limited_until_ms", "limited_for_s", "expires_at_ms", "has_refresh_token", "quota", "status"}
        return [{key: value for key, value in account.items() if key in fields}
                for account in result.get("accounts", []) if isinstance(account, dict)]

    def inference_base_url(self, account_id=None, *, single_attempt=False):
        return "http://subscription.internal/v1"

    def inference_app(self, account_id: str, *, single_attempt: bool = False):
        """Exact-account ASGI protocol, reusable by an explicitly started host."""
        if not isinstance(account_id, str) or not account_id or len(account_id) > 1024:
            raise ValueError("A bounded account identifier is required")
        key = (account_id, single_attempt)
        if key not in self._inference_apps:
            from dataclasses import replace
            manager = self.manager

            class BoundAccount:
                def __getattr__(self, name):
                    return getattr(manager, name)

                def account_ids(self):
                    return [account_id]

                def select_account(self, **kwargs):
                    exclude = set(kwargs.pop("exclude_ids", set()) or set())
                    exclude.update(a["id"] for a in manager.list_accounts() if a["id"] != account_id)
                    return manager.select_account(exclude_ids=exclude, **kwargs)

                def get_token(self):
                    return self.select_account().token

            settings = replace(self.settings)
            # The route policy owns account failover. Do not sleep and retry
            # the same exhausted account before it can choose its next account.
            if single_attempt:
                settings.rate_limit_retries = 0
            if self.provider == "codex":
                # Core owns usage accounting; avoid multiple file-ledger writers.
                settings.usage_enabled = False
            app = self._adapter.create_app(settings, token_manager=BoundAccount())
            self._inference_apps[key] = app
        return self._inference_apps[key]

    def inference_client(self, account_id: str | None = None, *, single_attempt: bool = False):
        """Bind selection and retries to the card's exact account, never a sibling."""
        if account_id is None:
            return self.client
        client_key = (account_id, True) if single_attempt else account_id
        if client_key not in self._inference_clients:
            import httpx
            from .transport import SubscriptionTransport
            app = self.inference_app(account_id, single_attempt=single_attempt)
            self._inference_clients[client_key] = httpx.AsyncClient(
                transport=SubscriptionTransport(app), base_url="http://subscription.internal",
                headers={"Authorization": "Bearer " + self.settings.api_key}, timeout=self.settings.request_timeout)
        return self._inference_clients[client_key]

    async def _claude_models(self, account_id=None):
        """Read the authenticated provider catalog rather than a version snapshot."""
        import asyncio
        import json
        from openagent_core.persistence import run_sync
        try:
            if account_id is None:
                token = await run_sync(self.manager.get_token)
            else:
                def selected_token():
                    exclude = {a['id'] for a in self.manager.list_accounts() if a['id'] != account_id}
                    return self.manager.select_account(exclude_ids=exclude).token
                token = await run_sync(selected_token)
            entries, seen, cursor = [], set(), None
            async with asyncio.timeout(15):
                for _ in range(5):
                    params = {"limit": 100}
                    if cursor: params["after_id"] = cursor
                    async with self._catalog_client.stream("GET", "https://api.anthropic.com/v1/models",
                            params=params, headers={"Authorization": "Bearer " + token,
                            "anthropic-version": "2023-06-01", "anthropic-beta": "oauth-2025-04-20"}) as response:
                        response.raise_for_status()
                        payload = bytearray()
                        async for chunk in response.aiter_bytes():
                            payload.extend(chunk)
                            if len(payload) > 2 << 20: raise ValueError("Catalog exceeds limit")
                    page = json.loads(payload)
                    for item in page.get("data", []):
                        identifier = item.get("id")
                        if not isinstance(identifier, str) or not identifier or len(identifier) > 256: continue
                        if identifier in seen: continue
                        seen.add(identifier)
                        known_media = identifier.startswith(("claude-opus-", "claude-sonnet-", "claude-haiku-"))
                        entries.append({"id": identifier, "object": "model", "kind": "llm",
                            "display_name": item.get("display_name") or identifier,
                            "owned_by": "anthropic-subscription", "catalog_source": "provider",
                            "input_modalities": ["text", "image", "file"] if known_media else ["text"]})
                    if not page.get("has_more"): return entries
                    next_cursor = page.get("last_id")
                    if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                        raise ValueError("Invalid catalog cursor")
                    cursor = next_cursor
            raise ValueError("Catalog exceeds page limit")
        except Exception:
            # Preserve cancellation and never expose token exchange/upstream bodies.
            raise ValueError("Subscription model discovery failed; retry or reconnect the account") from None

    async def models(self, *, account_id=None):
        # Before sign-in, the declared catalog is a credential-free preview.
        # Signed-in Claude connections use the actual account's live catalog.
        if self.provider == "claude":
            from openagent_core.persistence import run_sync
            if await run_sync(self.manager.list_accounts):
                return await self._claude_models(account_id=account_id)
        result = await self.request("GET", "/v1/models")
        entries = result.get("data", [])
        # One upstream reasoning model may expose image generation too. Give that
        # capability its own typed catalog identity without colliding with text.
        images = [{**entry, "id": "image/" + entry["id"], "kind": "image",
                   "image_model_id": entry["id"], "display_name": entry["id"] + " · Images"}
                  for entry in entries if "image_generation" in entry.get("capabilities", [])]
        return entries + images

    async def start_sign_in(self):
        return await self.request("POST", "/admin/oauth/start")

    async def complete_sign_in(self, fields, *, account_id: str | None = None):
        fields = dict(fields)
        code = str(fields.get("code") or "").strip()
        if code.startswith(("http://", "https://")):
            from urllib.parse import parse_qs, urlsplit
            query = parse_qs(urlsplit(code).query)
            if query.get("state", [fields.get("state")])[0] != fields.get("state"):
                raise ValueError("OAuth state mismatch")
            fields["code"] = query.get("code", [""])[0]
        allowed = {key: fields[key] for key in ("state", "code", "name") if key in fields}
        if account_id is not None:
            allowed["account_id"] = account_id
        return await self.request("POST", "/admin/oauth/complete", json=allowed)

    async def update_account(self, fields):
        """Update the exact credential without exposing or replacing its tokens."""
        import asyncio
        if self._access_grant:
            raise ValueError("The host manages this account")
        identifier = fields.get("id")
        patch = {key: fields[key] for key in ("name", "enabled", "priority") if key in fields}
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("Account id is required")
        if "enabled" in patch and not isinstance(patch["enabled"], bool):
            raise ValueError("enabled must be boolean")
        if "name" in patch and (not isinstance(patch["name"], str) or not patch["name"].strip()):
            raise ValueError("Account name is required")
        def update():
            with self.manager._lock:
                store = self.manager._load_store_unlocked()
                account = self.manager._find_account_unlocked(identifier)
                if account is None: raise LookupError("Account not found")
                account.update(patch)
                self.manager._save_store_unlocked(store)
            return {"ok": True}
        return await asyncio.to_thread(update)

    async def close(self):
        for client in self._inference_clients.values():
            await client.aclose()
        self._inference_clients.clear()
        await self.client.aclose()
        if self._catalog_client is not None:
            await self._catalog_client.aclose()


def subscription_for(row: dict, db_path: str | Path) -> SubscriptionService | None:
    """Resolve a persisted connection. Legacy external endpoints stay explicit."""
    metadata = row.get("metadata") or {}
    if metadata.get("access") != "subscription":
        return None
    vendor = metadata.get("vendor", row.get("name"))
    provider = {"anthropic": "claude", "openai": "codex"}.get(vendor)
    if provider is None:
        raise ValueError("Subscription access is supported by Anthropic and OpenAI")
    if metadata.get('subscription_remote') is True:
        from .remote import RemoteSubscriptionService
        key = ('remote', provider, row.get('base_url'), row.get('api_key'), metadata.get('account_id'), metadata.get('subscription_remote_unkeyed', False))
        if key not in _SERVICES or _SERVICES[key].client.is_closed:
            _SERVICES[key] = RemoteSubscriptionService(provider, row.get('base_url') or '', row.get('api_key'), account_id=metadata.get('account_id'), allow_unkeyed=metadata.get('subscription_remote_unkeyed') is True)
        return _SERVICES[key]
    if not db_path:
        raise ValueError("Subscription access requires an explicit host database path")
    path = Path(db_path)
    if not path.is_absolute() or str(path) == ":memory:":
        raise ValueError("Subscription access requires an explicit persistent host database path")
    # IDs remain stable across renames; no user-controlled path component.
    identifier = int(metadata.get("credential_store_id", row["id"]))
    from .settings import native_settings, validate_settings
    import json
    options = validate_settings(provider, metadata.get('subscription_settings'))
    admission_id = metadata.get('subscription_admission_id')
    if admission_id is not None and (type(admission_id) is not int or admission_id < 1):
        raise ValueError('subscription_admission_id must be a positive identifier')
    directory = path.parent / 'subscriptions'
    admission = directory / f'admission-{admission_id}-{provider}' if admission_id else None
    store = directory / f'{identifier}-{provider}.json'
    key = (str(path), identifier, provider, admission_id, json.dumps(options, sort_keys=True))
    if key not in _SERVICES or _SERVICES[key].client.is_closed:
        _SERVICES[key] = SubscriptionService(provider, store,
            settings=native_settings(provider, store, options, admission_path=admission))
    return _SERVICES[key]
