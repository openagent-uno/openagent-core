"""Declared subscription image failover at a complete HTTP response boundary."""
from __future__ import annotations

from dataclasses import replace

from ..image_generation import (ImageGenerationError, generate_image_bytes,
                                provider_image_model_id, supports_image_generation)
from ..models.catalog import CatalogModel, build_runtime_model_id
from .service import subscription_for


def _wire_model(provider, model):
    identifier = model['model']
    prefix = provider['name']+':'
    if identifier.startswith(prefix): identifier = identifier[len(prefix):]
    return provider_image_model_id(identifier, model.get('metadata') or {})


async def generate_subscription_image(db, provider_id: int, *, model: str, prompt: str,
                                      model_id: int | None = None, size='1024x1024',
                                      quality='auto', budget_guard=None):
    """Use exact accounts; rotate only on explicit 401/403/429/529 refusals.

    Invalid images, server errors, transport failures and cancellation are never
    replayed. An undeclared/disabled group remains exact-account. The caller
    retains ownership of its database and borrowed subscription services.
    """
    providers = await db.list_providers()
    primary = next((p for p in providers if p['id'] == provider_id), None)
    if primary is None or not primary.get('enabled'):
        raise ImageGenerationError(403, 'The image connection is disabled or missing', 'subscription_image_unavailable')
    metadata = primary.get('metadata') or {}
    if metadata.get('access') != 'subscription' or metadata.get('vendor') != 'openai':
        raise ValueError('A supported subscription image connection is required')
    group = metadata.get('account_fallback_group') if metadata.get('account_fallback_enabled') is not False and 'image' in metadata.get('account_fallback_kinds', ['llm']) else None
    candidates = []
    for provider in providers:
        meta = provider.get('metadata') or {}
        if (not provider.get('enabled') or meta.get('access') != 'subscription'
                or meta.get('vendor') != 'openai' or not meta.get('account_id')):
            continue
        if provider['id'] != provider_id and (not group or meta.get('account_fallback_group') != group
                                               or meta.get('account_fallback_enabled') is False or 'image' not in meta.get('account_fallback_kinds', ['llm'])):
            continue
        models = await db.list_models(provider_id=provider['id'])
        matching = [m for m in models if m.get('enabled')
                    and supports_image_generation(m.get('kind','llm'), m.get('metadata'))
                    and _wire_model(provider, m) == model]
        if provider['id'] == provider_id and model_id is not None:
            matching = [m for m in matching if m['id'] == model_id]
        if not matching: continue
        selected = min(matching, key=lambda m:(m.get('kind') != 'image', m['id']))
        entry = CatalogModel(provider['name'], selected['model'],
            build_runtime_model_id(provider['name'], selected['model']), 'platform',
            metadata=selected.get('metadata'), provider_id=provider['id'])
        candidates.append((provider, selected, entry))
    candidates.sort(key=lambda row:(row[0]['id'] != provider_id,
        int((row[0].get('metadata') or {}).get('account_fallback_priority') or 0), row[0]['id']))
    # Typed rows may share one credential. A refused account is attempted once.
    unique, seen = [], set()
    for candidate in candidates:
        p = candidate[0]; meta = p['metadata']
        if meta.get('subscription_remote'):
            from .remote import remote_base_url
            owner = ('remote', remote_base_url(p.get('base_url') or ''))
        else:
            owner = ('native', meta.get('credential_store_id', p['id']))
        key = (owner, meta['account_id'])
        if key not in seen:
            unique.append(candidate); seen.add(key)
    candidates = unique
    if budget_guard is None:
        from ..core.budget_guard import BudgetGuard
        budget_guard = BudgetGuard(db, providers_config=await db.materialise_providers_config(enabled_only=False))
        await budget_guard.refresh(emit_alerts=False)
    allowed = {entry.runtime_id for entry in budget_guard.filter_catalog([c[2] for c in candidates])}
    if candidates and not allowed:
        raise ImageGenerationError(429, "The configured budget excludes every matching image model", "budget_exceeded")
    last = None
    for provider, selected, entry in candidates:
        if entry.runtime_id not in allowed: continue
        meta = provider['metadata']
        account_id = meta['account_id']
        service = subscription_for(provider, db.db_path)
        accounts = await service.accounts()
        if not any(a.get('id') == account_id and a.get('enabled') is not False for a in accounts):
            continue
        options = {'single_attempt':True} if group else {}
        try:
            image = await generate_image_bytes(
                endpoint=service.inference_base_url(account_id, **options)+'/images/generations',
                api_key=service.settings.api_key, model=_wire_model(provider, selected),
                prompt=prompt, size=size, quality=quality,
                client=service.inference_client(account_id, **options))
        except ImageGenerationError as error:
            if error.status not in {401,403,429,529}: raise
            last = error
            continue
        return replace(image, connection=provider['name'],
                       connection_label=meta.get('label') or provider['name'], account_id=account_id)
    if last is not None: raise last
    raise ImageGenerationError(429, 'No enabled image account is eligible for this route', 'subscription_image_unavailable')
