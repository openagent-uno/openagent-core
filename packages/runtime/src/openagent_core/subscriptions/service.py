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
    def __init__(self, provider: str, credentials_path: Path, *, settings=None):
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
        settings = settings or config.Settings(creds_file=credentials_path)
        if provider == "codex":
            settings.usage_file = str(credentials_path.with_suffix(".usage.json"))
        settings.api_key = secrets.token_urlsafe(32)
        settings.admin_api_key = secrets.token_urlsafe(32)
        self.provider, self.settings = provider, settings
        self._inference_clients = {}
        self._catalog_client = httpx.AsyncClient(timeout=10) if provider == "claude" else None
        self._adapter = adapter
        self.manager = auth.TokenManager(credentials_path, **({"settings": settings} if provider == "codex" else {}))
        self.app = adapter.create_app(settings, token_manager=self.manager)
        self.client = httpx.AsyncClient(
            transport=SubscriptionTransport(self.app), base_url="http://subscription.internal",
            headers={"Authorization": "Bearer " + settings.api_key}, timeout=settings.request_timeout,
        )

    async def request(self, method: str, path: str, **kwargs):
        response = await self.client.request(method, path,
            headers={"Authorization": "Bearer " + self.settings.admin_api_key}, **kwargs)
        if not response.is_success:
            # Never surface credential exchange bodies or upstream exception strings.
            raise ValueError("Subscription operation failed; reconnect the account" if response.status_code != 400
                             else "Invalid or expired subscription sign-in; start again")
        return response.json()

    async def accounts(self):
        result = await self.request("GET", "/admin/accounts")
        fields = {"id", "name", "enabled", "priority", "plan", "managed", "limited", "limited_until_ms", "limited_for_s", "expires_at_ms", "has_refresh_token", "quota", "status"}
        return [{key: value for key, value in account.items() if key in fields}
                for account in result.get("accounts", []) if isinstance(account, dict)]

    def inference_client(self, account_id: str | None = None):
        """Bind selection and retries to the card's exact account, never a sibling."""
        if account_id is None:
            return self.client
        if account_id not in self._inference_clients:
            from dataclasses import replace
            import httpx
            from .transport import SubscriptionTransport
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
            if self.provider == "codex":
                # Core owns usage accounting; avoid multiple file-ledger writers.
                settings.usage_enabled = False
            app = self._adapter.create_app(settings, token_manager=BoundAccount())
            self._inference_clients[account_id] = httpx.AsyncClient(
                transport=SubscriptionTransport(app), base_url="http://subscription.internal",
                headers={"Authorization": "Bearer " + settings.api_key}, timeout=settings.request_timeout)
        return self._inference_clients[account_id]

    async def _claude_models(self):
        """Read the authenticated provider catalog rather than a version snapshot."""
        import asyncio
        import json
        from openagent_core.persistence import run_sync
        try:
            token = await run_sync(self.manager.get_token)
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

    async def models(self):
        # Before sign-in, the declared catalog is a credential-free preview.
        # Signed-in Claude connections use the actual account's live catalog.
        if self.provider == "claude":
            from openagent_core.persistence import run_sync
            if await run_sync(self.manager.list_accounts):
                return await self._claude_models()
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
    if not db_path:
        raise ValueError("Subscription access requires an explicit host database path")
    path = Path(db_path)
    if not path.is_absolute() or str(path) == ":memory:":
        raise ValueError("Subscription access requires an explicit persistent host database path")
    # IDs remain stable across renames; no user-controlled path component.
    identifier = int(metadata.get("credential_store_id", row["id"]))
    key = (str(path), identifier, provider)
    if key not in _SERVICES:
        _SERVICES[key] = SubscriptionService(provider, path.parent / "subscriptions" / f"{identifier}-{provider}.json")
    return _SERVICES[key]
