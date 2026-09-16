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
        async with self._mutations:
            pid = await self.db.upsert_provider(**fields)
            with self.scope(): await self.reload_catalog()
        return await self.provider(context, pid)

    async def update_provider(self, context, provider_id: int, fields: dict):
        await self.authorize(context, "provider.write")
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

    async def create_model(self, context, fields: dict):
        await self.authorize(context, "model.write")
        if set(fields) - {"provider_id","model","display_name","tier_hint","enabled","is_classifier","metadata","kind"}: raise ValueError("Unknown model fields")
        async with self._mutations:
            mid = await self.db.upsert_model(**fields)
            with self.scope(): await self.reload_catalog()
        return await self.model(context, mid)

    async def update_model(self, context, model_id: int, fields: dict):
        await self.authorize(context, "model.write")
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
        from .models.discovery import list_provider_models
        with self.scope():
            async with asyncio.timeout(30):
                result = await list_provider_models(row["name"], row.get("api_key"), row.get("base_url"))
        await self.authorize(context, "model.discover")
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
        from .core.execution_profile import stateless_completion_scope
        from .models.runtime import run_provider_smoke_test
        with self.scope(), stateless_completion_scope():
            async with asyncio.timeout(30):
                runtime_id, response = await run_provider_smoke_test(row["name"], await self.db.materialise_providers_config(),
                    model_id=selected, framework=row["framework"])
        await self.authorize(context, "provider.test")
        return {"ok":True,"model":runtime_id,"response":response.content}


def public_provider(row: dict) -> dict:
    out = {k:row.get(k) for k in ("id","name","framework","kind","base_url","metadata","created_at","updated_at")}
    out["enabled"] = bool(row.get("enabled",True))
    key = row.get("api_key")
    out["api_key_display"] = "—" if key is None else (key if isinstance(key,str) and key.startswith("${") else "****" + (key[-4:] if isinstance(key,str) and len(key)>4 else ""))
    return out


def public_model(row: dict) -> dict:
    from .models.catalog import get_model_pricing
    out = {k:row.get(k) for k in ("id","provider_id","provider_name","framework","kind","runtime_id","model","display_name","tier_hint","metadata","created_at","updated_at")}
    for key in ("enabled","is_classifier","provider_enabled"): out[key] = bool(row.get(key, key!="is_classifier"))
    if not out["display_name"] and row.get("kind") in {"tts","stt"}: out["display_name"] = f'{row["provider_name"]}: {row["model"]}'
    out.update(get_model_pricing(row["runtime_id"]))
    return out
