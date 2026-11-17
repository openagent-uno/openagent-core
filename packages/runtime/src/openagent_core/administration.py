"""Optional host-authorized provider/model administration, independent of HTTP/UI."""
from __future__ import annotations
import asyncio
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable, Awaitable
from .contracts import PrincipalRef, ResourceRef, require_authorized


@dataclass(frozen=True, slots=True)
class ManagementContext:
    authority: PrincipalRef
    agent_id: str

    @property
    def tenant_id(self) -> str:
        return self.authority.tenant_id


class ProviderModelAdmin:
    def __init__(self, db: Any, authorizer: Any, *, reload_catalog: Callable[[], Awaitable[None]], runtime: Any = None):
        self.db, self.authorizer, self.reload_catalog, self.runtime = db, authorizer, reload_catalog, runtime
        self._mutations = asyncio.Lock()

    async def authorize(self, context: ManagementContext, action: str) -> None:
        await require_authorized(self.authorizer, context, action, ResourceRef("agent", context.tenant_id, context.agent_id))

    def scope(self):
        from .runtime import runtime_scope
        return runtime_scope(self.runtime) if self.runtime is not None else nullcontext()

    async def providers(self, context):
        await self.authorize(context, "provider.read")
        return [public_provider(r) for r in await self.db.list_providers()]

    async def provider(self, context, provider_id: int):
        await self.authorize(context, "provider.read")
        row = await self.db.get_provider(provider_id)
        if row is None: raise LookupError("Provider not found")
        return public_provider(row)

    async def create_provider(self, context, fields: dict):
        await self.authorize(context, "provider.write")
        if set(fields) - {"name","framework","kind","api_key","base_url","enabled","metadata"}: raise ValueError("Unknown provider fields")
        if not isinstance(fields.get("name"),str) or not fields["name"].strip() or not isinstance(fields.get("framework"),str):
            raise ValueError("Provider name and framework are required")
        for key in ("api_key","base_url"):
            if fields.get(key) is not None and not isinstance(fields[key],str): raise ValueError("Provider credentials and endpoint must be strings")
        if "enabled" in fields and not isinstance(fields["enabled"],bool): raise ValueError("enabled must be boolean")
        if "metadata" in fields and not isinstance(fields["metadata"],dict): raise ValueError("metadata must be an object")
        from .models.connections import validate_connection
        validate_connection(fields.get("metadata") or {})
        framework="api-based" if fields["framework"] in {"agno","litellm"} else fields["framework"]
        async with self._mutations:
            if any(r["name"]==fields["name"].strip() and r["framework"]==framework for r in await self.db.list_providers()):
                raise ValueError("Provider already exists; update its stable id")
            pid = await self.db.upsert_provider(**fields)
            with self.scope(): await self.reload_catalog()
        return await self.provider(context, pid)

    async def update_provider(self, context, provider_id: int, fields: dict):
        await self.authorize(context, "provider.write")
        if "metadata" in fields:
            from .models.connections import validate_connection
            validate_connection(fields["metadata"])
        async with self._mutations:
            await self.db.update_provider_fields(provider_id, fields)
            with self.scope(): await self.reload_catalog()
        return await self.provider(context, provider_id)

    async def delete_provider(self, context, provider_id: int):
        await self.authorize(context, "provider.write")
        async with self._mutations:
            if await self.db.get_provider(provider_id) is None: raise LookupError("Provider not found")
            await self.db.delete_provider(provider_id)
            with self.scope(): await self.reload_catalog()

    async def models(self, context, **filters):
        await self.authorize(context, "model.read")
        with self.scope():
            return [public_model(r) for r in await self.db.list_models_enriched(**filters)]

    async def model(self, context, model_id: int):
        await self.authorize(context, "model.read")
        row = await self.db.get_model_enriched(model_id)
        if row is None: raise LookupError("Model not found")
        with self.scope(): return public_model(row)

    async def default_model(self, context):
        """The language model unpinned sessions start on, or ``None``.

        At most one model is ever stored as the default router (its
        ``is_classifier`` flag), and only an enabled model of an enabled
        provider can hold it. ``reason`` says whether that stored default
        applies (``"router_flag"``) or, with none set, the first enabled
        language model in catalog order (``"first_enabled"``).
        """
        await self.authorize(context, "model.read")
        row = await self.db.get_default_router()
        if row is None: return None
        with self.scope(): return {**public_model(row), "reason": row["reason"]}

    async def create_model(self, context, fields: dict):
        await self.authorize(context, "model.write")
        if set(fields) - {"provider_id","model","display_name","tier_hint","enabled","is_classifier","metadata","kind"}: raise ValueError("Unknown model fields")
        if not isinstance(fields.get("model"),str) or not fields["model"].strip(): raise ValueError("Model name is required")
        if isinstance(fields.get("provider_id"),bool) or not isinstance(fields.get("provider_id"),int): raise ValueError("Provider id is required")
        for key in ("enabled","is_classifier"):
            if key in fields and not isinstance(fields[key],bool): raise ValueError("Model flags must be boolean")
        if "metadata" in fields and not isinstance(fields["metadata"],dict): raise ValueError("metadata must be an object")
        fields = dict(fields)
        provider = await self.db.get_provider(fields["provider_id"])
        if provider is None: raise LookupError("Provider not found")
        from .models.settings_schema import validate_settings
        from .models.connections import connection_info
        info = connection_info(provider)
        kind = fields.get("kind", "llm")
        if info["access"] == "subscription" and kind not in ({"llm", "image"} if info["vendor"] == "openai" else {"llm"}):
            raise ValueError("This subscription does not expose a standalone model of this type")
        if kind == "image" and str(fields["model"]).startswith("image/"):
            fields["metadata"] = {**(fields.get("metadata") or {}), "image_model_id": fields["model"][6:]}
        validate_settings(provider, fields, fields.get("metadata") or {})
        if fields.get("kind", "llm") == "llm":
            fields["metadata"] = await self._seed_capabilities(fields.get("provider_id"), fields["model"].strip(), fields.get("metadata"))
        async with self._mutations:
            if await self.db.get_provider(fields["provider_id"]) is None: raise LookupError("Provider not found")
            if any(r["model"]==fields["model"].strip() for r in await self.db.list_models(provider_id=fields["provider_id"])):
                raise ValueError("Model already exists; update its stable id")
            mid = await self.db.upsert_model(**fields)
            with self.scope(): await self.reload_catalog()
        return await self.model(context, mid)

    async def update_model(self, context, model_id: int, fields: dict):
        await self.authorize(context, "model.write")
        if isinstance(fields.get("metadata"), dict):
            from .models.settings_schema import validate_settings
            before = await self.db.get_model(model_id)
            if before is None: raise LookupError("Model not found")
            provider = await self.db.get_provider(before["provider_id"])
            # Historical unsupported values may remain untouched; new overrides are validated.
            changed = {key: value for key, value in fields["metadata"].items()
                       if value != (before.get("metadata") or {}).get(key)}
            validate_settings(provider, before, changed)
        if isinstance(fields.get("metadata"), dict):
            from .models.media_capabilities import mark_manual_modalities
            fields = {**fields, "metadata": mark_manual_modalities(fields["metadata"])}
        async with self._mutations:
            await self.db.update_model_fields(model_id, fields)
            with self.scope(): await self.reload_catalog()
        return await self.model(context, model_id)

    async def delete_model(self, context, model_id: int):
        await self.authorize(context, "model.write")
        async with self._mutations:
            if await self.db.get_model(model_id) is None: raise LookupError("Model not found")
            await self.db.delete_model(model_id)
            with self.scope(): await self.reload_catalog()

    async def supported_providers(self, context):
        await self.authorize(context, "model.read")
        from .models.catalog import supported_providers
        with self.scope(): return supported_providers(await self.db.materialise_providers_config(enabled_only=False))

    async def catalog(self, context, provider: str = ""):
        await self.authorize(context, "model.read")
        from .models.catalog import iter_configured_models, get_model_pricing
        from .models.media_capabilities import input_modalities_for
        with self.scope():
            cfg = await self.db.materialise_providers_config(enabled_only=False)
            out = []
            for entry in iter_configured_models(cfg):
                if provider and entry.provider != provider: continue
                out.append({"provider":entry.provider,"framework":entry.framework,"model":entry.model_id,
                    "runtime_id":entry.runtime_id,"history_mode":entry.history_mode,"tier_hint":entry.tier_hint,
                    "input_modalities":sorted(input_modalities_for(entry)), **get_model_pricing(entry.runtime_id, cfg)})
            return out

    async def discover(self, context, provider_id: int):
        await self.authorize(context, "model.discover")
        row = await self.db.get_provider(provider_id)
        if row is None: raise LookupError("Provider not found")
        result = await self._discovered_entries(row)
        await self.authorize(context, "model.discover")
        return result

    async def _seed_capabilities(self, provider_id, model: str, metadata: dict | None) -> dict:
        """A new model's media declaration: a person's, else the provider's, else the default."""
        from .models.media_capabilities import mark_manual_modalities, refreshed_model_metadata
        out = mark_manual_modalities(dict(metadata or {}))
        if out.get("input_modalities_source") == "manual":
            return out
        row = await self.db.get_provider(provider_id) if isinstance(provider_id, int) and not isinstance(provider_id, bool) else None
        if row is None:
            return out
        return refreshed_model_metadata(out, provider=row["name"], model=model,
                                        discovered=await self._discovered_entry(row, model))

    async def _discovered_entry(self, row: dict, model: str) -> dict | None:
        entries = await self._discovered_entries(row)
        return next((entry for entry in entries if entry.get("id") == model), None)

    async def _discovered_entries(self, row: dict) -> list[dict]:
        from .models.discovery import list_provider_models
        from .models.connections import connection_info
        try:
            with self.scope():
                from .subscriptions.service import subscription_for
                service = subscription_for(row, self.db.db_path)
                if service is not None:
                    entries = await service.models()
                    return [{**entry, "kind": entry.get("kind", "llm")} for entry in entries]
                async with asyncio.timeout(10):
                    return await list_provider_models(connection_info(row)["vendor"], row.get("api_key"), row.get("base_url"))
        except (TimeoutError, OSError, ValueError):
            # An unreachable provider keeps the stored declaration.
            return []

    async def refresh_capabilities(self, context, provider_id: int) -> dict:
        """Re-read every model's media declaration from its provider.

        Run after a login or a provider change. Rows a person set by hand
        (``input_modalities_source: manual``) are never touched.
        """
        await self.authorize(context, "model.write")
        from .models.media_capabilities import refreshed_model_metadata
        row = await self.db.get_provider(provider_id)
        if row is None: raise LookupError("Provider not found")
        entries = {entry.get("id"): entry for entry in await self._discovered_entries(row)}
        await self.authorize(context, "model.write")
        updated, manual = [], 0
        async with self._mutations:
            for model in await self.db.list_models_enriched(provider_id=provider_id, kind="llm"):
                before = dict(model.get("metadata") or {})
                if before.get("input_modalities_source") == "manual":
                    manual += 1
                    continue
                if model["model"] not in entries:
                    # Unreachable provider, or a model it no longer lists:
                    # keep the stored declaration.
                    continue
                after = refreshed_model_metadata(before, provider=row["name"], model=model["model"],
                                                 discovered=entries.get(model["model"]))
                if after != before:
                    await self.db.update_model_fields(model["id"], {"metadata": after})
                    updated.append(model["runtime_id"])
            if updated:
                with self.scope(): await self.reload_catalog()
        return {"provider_id": provider_id, "reachable": bool(entries), "updated": updated, "manual": manual}

    async def subscription(self, context, provider_id: int, operation: str, fields: dict | None = None):
        """Authorized subscription administration shared by SDK and all host transports."""
        if operation not in {"accounts", "start", "complete", "update_account"}:
            raise ValueError("Unknown subscription operation")
        await self.authorize(context, "provider.read" if operation == "accounts" else "provider.write")
        row = await self.db.get_provider(provider_id)
        if row is None: raise LookupError("Provider not found")
        from .subscriptions.service import subscription_for
        with self.scope():
            service = subscription_for(row, self.db.db_path)
            if service is None: raise ValueError("This is not a built-in subscription connection")
            if operation == "accounts": return await service.accounts()
            if operation == "start": return await service.start_sign_in()
            if operation == "update_account":
                patch = fields or {}
                result = await service.update_account(patch)
                if patch.get("id") == (row.get("metadata") or {}).get("account_id") and ({"enabled", "name"} & patch.keys()):
                    owner = (row.get("metadata") or {}).get("credential_store_id", row["id"])
                    for linked in await self.db.list_providers():
                        meta = linked.get("metadata") or {}
                        if meta.get("access") == "subscription" and meta.get("credential_store_id", linked["id"]) == owner and meta.get("account_id") == patch["id"]:
                            changes = {}
                            if "enabled" in patch: changes["enabled"] = patch["enabled"]
                            if "name" in patch: changes["metadata"] = {**meta, "label": patch["name"].strip()}
                            await self.db.update_provider_fields(linked["id"], changes)
                    with self.scope(): await self.reload_catalog()
                return result
            metadata = row.get("metadata") or {}
            submitted = {**(fields or {})}
            submitted["name"] = submitted.get("name") or metadata.get("label") or row["name"]
            result = await service.complete_sign_in(submitted, account_id=metadata.get("account_id"))
        await self.authorize(context, "provider.write")
        account = result.get("account") or {}
        if account.get("id"):
            owner = (row.get("metadata") or {}).get("credential_store_id", row["id"])
            linked_rows = await self.db.list_providers()
            for linked in linked_rows:
                meta = linked.get("metadata") or {}
                if linked["id"] == provider_id or (meta.get("access") == "subscription"
                        and meta.get("credential_store_id", linked["id"]) == owner
                        and meta.get("account_id") == account["id"]):
                    await self.db.update_provider_fields(linked["id"], {"metadata": {
                        **meta, "account_id": account["id"],
                        "label": account.get("name") or meta.get("label", linked["name"])}})
            with self.scope(): await self.reload_catalog()
        await self.refresh_capabilities(context, provider_id)
        return result

    async def test_provider(self, context, provider_id: int, model_ref: str | None = None):
        await self.authorize(context, "provider.test")
        row = await self.db.get_provider(provider_id)
        if row is None: raise LookupError("Provider not found")
        if row.get("kind", "llm") != "llm": raise ValueError("Provider test requires a language-model provider")
        candidates = await self.db.list_models_enriched(provider_id=provider_id, enabled_only=True, kind="llm")
        if model_ref:
            candidates = [r for r in candidates if model_ref in {r["runtime_id"], r["model"]}]
        if not candidates: raise ValueError("Configure and enable a model for this provider first")
        selected = candidates[0]["runtime_id"]
        from .inference import InferenceRequest, StatelessInference
        from .models.runtime import create_model_from_spec
        with self.scope():
            provider = create_model_from_spec(selected, providers_config=await self.db.materialise_providers_config())
            async with asyncio.timeout(30):
                response = await StatelessInference(provider).complete(InferenceRequest(({
                    "role":"user", "content":"Say 'ok' and nothing else.",
                },)))
        await self.authorize(context, "provider.test")
        return {"ok":True,"model":selected,"response":response.content}


def _key_display(key) -> str:
    return "—" if key is None else (key if isinstance(key,str) and key.startswith("${") else "****" + (key[-4:] if isinstance(key,str) and len(key)>4 else ""))


def _public_metadata(metadata):
    """Provider metadata without credentials.

    ``metadata.accounts`` is a credential pool (see ``models.credential_pool``):
    every entry's ``api_key`` is write-only, like the provider's own key, and is
    replaced by ``api_key_display``. An account's own ``base_url`` is kept.
    """
    if not isinstance(metadata, dict) or not isinstance(metadata.get("accounts"), list):
        return metadata
    out = dict(metadata)
    out["accounts"] = [
        {**{k: v for k, v in account.items() if k != "api_key"}, "api_key_display": _key_display(account.get("api_key"))}
        if isinstance(account, dict) else account
        for account in metadata["accounts"]
    ]
    return out


def public_provider(row: dict) -> dict:
    from .models.connections import connection_info
    out = {k:row.get(k) for k in ("id","name","framework","kind","base_url","metadata","created_at","updated_at")}
    out["metadata"] = _public_metadata(out["metadata"])
    out["enabled"] = bool(row.get("enabled",True))
    out["api_key_display"] = _key_display(row.get("api_key"))
    out.update(connection_info(row))
    return out


def public_model(row: dict) -> dict:
    from .models.catalog import get_model_pricing
    out = {k:row.get(k) for k in ("id","provider_id","provider_name","framework","kind","runtime_id","model","display_name","tier_hint","metadata","created_at","updated_at")}
    for key in ("enabled","is_classifier","provider_enabled"): out[key] = bool(row.get(key, key!="is_classifier"))
    if not out["display_name"] and row.get("kind") in {"tts","stt"}: out["display_name"] = f'{row["provider_name"]}: {row["model"]}'
    from .models.settings_schema import model_settings_schema
    out["settings_schema"] = model_settings_schema({"name": row["provider_name"], "metadata": row.get("provider_metadata") or {}}, row)
    out.update(get_model_pricing(row["runtime_id"]))
    return out
