"""A connection's identity is independent of its vendor and model capabilities."""
from __future__ import annotations

import re

ACCESS_TYPES = frozenset({"api", "subscription", "local"})
MODEL_KINDS = ("llm", "image", "tts", "stt")


def subscription_model_id(vendor: str, model: str) -> str:
    """Resolve versioned subscription aliases without changing generations."""
    if vendor == "anthropic" and re.fullmatch(r"(?:opus|sonnet|haiku)-\d+(?:-\d+)*(?:-latest)?", model):
        return "claude-" + model
    return model


def connection_info(row: dict) -> dict:
    metadata = row.get("metadata") or {}
    access = metadata.get("access") or ("local" if row.get("name") == "local" else "api")
    return {"vendor": metadata.get("vendor") or row.get("name", ""),
            "access": access, "label": metadata.get("label") or row.get("name", ""),
            "account_id": metadata.get("account_id")}


def validate_connection(metadata: dict) -> None:
    if "access" in metadata and metadata["access"] not in ACCESS_TYPES:
        raise ValueError("access must be api, subscription or local")
    if metadata.get("access") == "subscription" and metadata.get("vendor") not in {"anthropic", "openai"}:
        raise ValueError("Unsupported subscription vendor")
    if "credential_store_id" in metadata and (isinstance(metadata["credential_store_id"], bool) or not isinstance(metadata["credential_store_id"], int) or metadata["credential_store_id"] < 1):
        raise ValueError("credential_store_id must be a positive identifier")
    if "account_fallback_enabled" in metadata and type(metadata['account_fallback_enabled']) is not bool:
        raise ValueError('account_fallback_enabled must be a boolean')
    if "account_fallback_priority" in metadata and (type(metadata['account_fallback_priority']) is not int or not -1_000_000 <= metadata['account_fallback_priority'] <= 1_000_000):
        raise ValueError('account_fallback_priority must be an integer between -1000000 and 1000000')
    if "subscription_admission_id" in metadata and (type(metadata['subscription_admission_id']) is not int or metadata['subscription_admission_id'] < 1):
        raise ValueError('subscription_admission_id must be a positive identifier')
    if 'subscription_settings' in metadata:
        from ..subscriptions.settings import validate_settings
        provider = {'anthropic': 'claude', 'openai': 'codex'}.get(metadata.get('vendor'))
        validate_settings(provider, metadata['subscription_settings'])
    for field in ("vendor", "label", "account_id", "budget_group", "account_fallback_group"):
        if field in metadata and (not isinstance(metadata[field], str) or not metadata[field].strip()):
            raise ValueError(f"{field} must be a nonempty string")


from openagent_core.instance_state import InstanceMapping
_PRICING_CONNECTIONS = InstanceMapping("model-pricing-connections")

def register_pricing_connection(row: dict) -> None:
    _PRICING_CONNECTIONS[row["name"]] = connection_info(row)

def pricing_reference(runtime_id: str) -> str | None:
    name, separator, model = runtime_id.partition(":")
    info = _PRICING_CONNECTIONS.get(name)
    if not info or not separator:
        return runtime_id
    if info["access"] == "subscription":
        return None  # Included subscription usage is not a metered API charge.
    return f'{info["vendor"]}:{model}'


def budget_scope_keys(entry, providers_config) -> set[tuple[str, str]]:
    from .catalog import _iter_provider_entries
    provider = getattr(entry, "provider", "")
    rid = getattr(entry, "runtime_id", "")
    keys = {("provider", provider), ("model", rid)}
    metadata = getattr(entry, "metadata", None) or {}
    group = metadata.get("budget_model_group")
    if group: keys.add(("model", group))
    for row in _iter_provider_entries(providers_config):
        if row.get("name") != provider: continue
        group = (row.get("metadata") or {}).get("budget_group")
        if group: keys.add(("provider", group))
        vendor = (row.get("metadata") or {}).get("vendor")
        if vendor: keys.add(("provider", vendor))
    return keys
