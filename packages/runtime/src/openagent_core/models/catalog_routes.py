"""Eligible typed catalog routes shared by generation and audio services."""
from __future__ import annotations

from .catalog import CatalogModel


def route_health_key(db, kind, row):
    import hashlib
    scope = str(getattr(db, "db_path", id(db)))
    return kind + ":" + hashlib.sha256((scope + ":" + row["runtime_id"]).encode()).hexdigest()[:24]


def route_failed(db, kind, row):
    from .providers.fallback import _breaker_record_failure
    _breaker_record_failure(route_health_key(db, kind, row))


def route_succeeded(db, kind, row):
    from .providers.fallback import _breaker_record_success
    _breaker_record_success(route_health_key(db, kind, row))


async def authorized_model_use(runtime_id):
    from ..runtime import current_runtime, current_execution_context
    from ..contracts import ResourceRef, require_authorized
    runtime, context = current_runtime(), current_execution_context()
    if runtime is not None and context is not None:
        await require_authorized(runtime.services.authorizer, context, "model.use",
                                 ResourceRef("model", context.tenant_id, runtime_id))


async def catalog_routes(db, kind, *, preferred_id=None, guard=None):
    """Keep configured preference, then recover through enabled compatible rows."""
    rows = await db.list_models_enriched(enabled_only=True)
    if kind == "image":
        from ..image_generation import supports_image_generation
        rows = [row for row in rows if supports_image_generation(row.get("kind", "llm"), row.get("metadata"))]
        rows.sort(key=lambda row: (row.get("kind") != "image", row["id"]))
    else:
        rows = [row for row in rows if row.get("kind") == kind]
        rows.sort(key=lambda row: (str(row.get("updated_at") or ""), row["id"]), reverse=True)
    rows = [row for row in rows if row.get("enabled", True) and row.get("provider_enabled", True)]
    if preferred_id is not None:
        rows.sort(key=lambda row: row["id"] != preferred_id)
    primary = rows[0]["id"] if rows else None
    rows = [row for row in rows if row["id"] == primary or (row.get("metadata") or {}).get("auto_fallback", True) is not False]
    if guard is None:
        from ..core.budget_guard import BudgetGuard
        guard = BudgetGuard(db, providers_config=await db.materialise_providers_config(enabled_only=False))
    await guard.ensure_fresh()
    entries = [CatalogModel(row["provider_name"], row["model"], row["runtime_id"], "platform",
                           metadata={**(row.get("metadata") or {}), "kind": row.get("kind", kind)},
                           provider_id=row["provider_id"]) for row in rows]
    blocked = guard.blocked_runtime_ids(entries)
    from .providers.fallback import breaker_snapshot
    cooling = breaker_snapshot()
    allowed = []
    for row in rows:
        if row["runtime_id"] in blocked:
            continue
        if cooling.get(route_health_key(db, kind, row), 0) > 0:
            continue
        try:
            await authorized_model_use(row["runtime_id"])
        except PermissionError:
            continue
        allowed.append(row)
    return allowed
