"""Automatic request recovery from the enabled catalog, without changing pins."""
from __future__ import annotations

from copy import copy

from .catalog import iter_configured_models, _iter_provider_entries
from .media_capabilities import input_modalities_for, native_modalities_for_payload
from .providers.fallback import FallbackConfig


def unavailable_model_error(error) -> bool:
    """Connection/model failures, never a content refusal or malformed request."""
    status = getattr(error, "status_code", None)
    if status == 401:
        return True
    message = str(getattr(error, "message", error)).lower()
    if status in {400, 403, 404}:
        return any(marker in message for marker in (
            "model_not_found", "unknown model", "model not found",
            "invalid_api_key", "refresh_token_invalidated", "token expired",
            "model is not supported", "model' is not supported",
            "model does not exist", "not supported when using codex",
        ))
    return False


def automatic_fallback_config(providers, configured, *, db_path=None, guard=None):
    """Use enabled language models; explicit backup order retains precedence."""
    from .native_provider import NativeProvider

    catalog = {entry.runtime_id: entry for entry in iter_configured_models(providers, include_disabled=True)}
    disabled_providers = {row['name'] for row in _iter_provider_entries(providers)
                          if row.get('enabled') is False and row.get('name')}
    entries = [entry for entry in catalog.values()
               if not entry.disabled and (entry.metadata or {}).get("kind", "llm") == "llm"]
    config = copy(configured) if configured is not None else FallbackConfig()
    config.recover_unavailable = True
    config.first_output_timeout = 60.0
    async def prepare():
        if guard is not None:
            await guard.ensure_fresh()
    config.prepare = prepare
    by_runtime = {entry.runtime_id: entry for entry in entries}
    models = []
    for entry in entries:
        if (entry.metadata or {}).get('auto_fallback', True) is False:
            continue
        try:
            model = NativeProvider(entry.runtime_id, providers_config=providers,
                                   db_path=db_path, budget_guard=guard).build_runtime_model()
        except (ValueError, ImportError, LookupError):
            continue
        model._openagent_automatic_backup = True
        models.append(model)

    for field in ("on_error", "on_rate_limit"):
        previous = list(getattr(config, field) or [])
        existing = {getattr(model, "_openagent_runtime_id", None) for model in previous}
        setattr(config, field, previous + [model for model in models
            if getattr(model, "_openagent_runtime_id", None) not in existing])

    def eligible(model, kwargs):
        runtime_id = getattr(model, "_openagent_runtime_id", None)
        if runtime_id and runtime_id.split(':', 1)[0] in disabled_providers:
            return False
        entry = by_runtime.get(runtime_id)
        if entry is None:
            if runtime_id in catalog:
                return False
            return True  # Explicit non-catalog backups keep their host policy.
        if guard is not None and runtime_id in guard.blocked_runtime_ids(entries):
            return False
        capabilities = (entry.metadata or {}).get("capabilities")
        if isinstance(capabilities, list) and "tools" not in capabilities and kwargs.get("tools"):
            return False
        required = set()
        for message in kwargs.get("messages") or []:
            def value(key):
                return message.get(key) if isinstance(message, dict) else getattr(message, key, None)
            required.update(native_modalities_for_payload(
                images=value("images"), audio=value("audio"), videos=value("videos"), files=value("files")))
            content = value("content")
            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    modality = {"image_url": "image", "input_audio": "audio", "video_url": "video",
                                "file": "file"}.get(block.get("type"))
                    if modality:
                        required.add(modality)
        return required <= input_modalities_for(entry)

    config.eligible = eligible
    async def authorize(model):
        from .catalog_routes import authorized_model_use
        await authorized_model_use(getattr(model, "_openagent_runtime_id", None) or model.id)
    config.authorize_model = authorize
    return config
