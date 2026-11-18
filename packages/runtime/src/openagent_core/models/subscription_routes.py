"""Resolve an explicitly declared same-model subscription account group."""
from __future__ import annotations

from .account_routes import AccountRoutes
from .catalog import _iter_provider_entries, iter_configured_models
from .connections import subscription_model_id


def account_routes_for(row, requested, config, db_path, guard=None):
    metadata = row.get('metadata') or {}
    group = metadata.get('account_fallback_group')
    if not group or metadata.get('access') != 'subscription' or metadata.get('account_fallback_enabled') is False:
        return None
    vendor = metadata.get('vendor')
    rows = {r['name']: r for r in _iter_provider_entries(config)}
    entries = list(iter_configured_models(config))
    primary = next((e for e in entries if e.runtime_id == requested), None)
    if primary is None:
        return None
    wire_model = subscription_model_id(vendor, primary.model_id)
    candidates = []
    for entry in entries:
        candidate = rows.get(entry.provider, {})
        meta = candidate.get('metadata') or {}
        if (entry.disabled or candidate.get('enabled') is False
                or meta.get('access') != 'subscription'
                or meta.get('vendor') != vendor
                or meta.get('account_fallback_group') != group
                or meta.get('account_fallback_enabled') is False
                or not meta.get('account_id')
                or subscription_model_id(vendor, entry.model_id) != wire_model):
            continue
        candidates.append(entry)
    candidates.sort(key=lambda e: (e.runtime_id != requested,
        int((rows[e.provider].get('metadata') or {}).get('account_fallback_priority') or 0), e.runtime_id))
    if len(candidates) < 2:
        return None

    def build(runtime_id):
        from .native_provider import NativeProvider
        return NativeProvider(runtime_id, providers_config=config, db_path=db_path,
                              budget_guard=guard).build_runtime_model(account_fallback=False)
    def available(runtime_id):
        from ..subscriptions.service import subscription_for
        entry = next(e for e in candidates if e.runtime_id == runtime_id)
        source = rows[entry.provider]
        account_id = source['metadata']['account_id']
        service = subscription_for(source, db_path)
        if hasattr(service, 'account_available'):
            return service.account_available(account_id)
        return any(a['id'] == account_id and a.get('enabled') is not False
                   for a in service.manager.list_accounts())
    return AccountRoutes(requested, tuple(candidates), build, guard, available)
