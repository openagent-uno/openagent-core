"""Conservative model settings contracts shared by every host and UI.

Unknown adapters expose no guessed sampling controls. A provider may declare
the supported subset in model metadata; this does not grant endpoint access.
"""
from __future__ import annotations

SAMPLING_KEYS = frozenset({"temperature", "top_p", "top_k", "min_p", "max_tokens",
    "presence_penalty", "frequency_penalty", "repeat_penalty", "seed", "reasoning_effort"})


def model_settings_schema(provider: dict, model: dict) -> list[dict]:
    from .connections import connection_info
    info = connection_info(provider)
    vendor, access = info["vendor"], info["access"]
    kind, identifier = model.get("kind", "llm"), model.get("model", "").lower()
    if kind == "tts":
        return [{"key": "voice_id", "label": "Voice", "type": "string"}]
    if kind == "stt":
        return [{"key": "language", "label": "Language", "type": "string"}]
    if kind != "llm":
        return []
    declared = (model.get("metadata") or {}).get("parameter_schema")
    if isinstance(declared, list):
        return [{k: v for k, v in field.items() if k in {"key", "label", "type", "min", "max", "options"}}
                for field in declared if isinstance(field, dict) and field.get("key") in SAMPLING_KEYS]
    if access == "subscription" and vendor == "openai":
        return [{"key": "reasoning_effort", "label": "Reasoning effort", "type": "enum",
                 "options": ["low", "medium", "high", "xhigh"]}]
    if vendor == "anthropic":
        return [{"key": "max_tokens", "label": "Maximum output tokens", "type": "integer", "min": 1}]
    if vendor == "openai" and (identifier.startswith("gpt-5") or identifier.startswith(("o1", "o3", "o4"))):
        return [{"key": "reasoning_effort", "label": "Reasoning effort", "type": "enum",
                 "options": ["low", "medium", "high"]}]
    if vendor in {"openai", "groq", "mistral", "deepseek", "google", "openrouter"}:
        return [{"key": "temperature", "label": "Temperature", "type": "number", "min": 0, "max": 2},
                {"key": "top_p", "label": "Top P", "type": "number", "min": 0, "max": 1},
                {"key": "max_tokens", "label": "Maximum output tokens", "type": "integer", "min": 1}]
    return []


def validate_settings(provider: dict, model: dict, metadata: dict) -> None:
    import math
    if "auto_fallback" in metadata and type(metadata["auto_fallback"]) is not bool:
        raise ValueError("auto_fallback must be boolean")
    schema = {field["key"]: field for field in model_settings_schema(provider, model)}
    for key in (SAMPLING_KEYS & metadata.keys()) | (schema.keys() & metadata.keys()):
        if key not in schema:
            raise ValueError(f"{key} is not supported by this model connection")
        value, field = metadata[key], schema[key]
        if field["type"] == "string":
            if not isinstance(value, str): raise ValueError(f"Invalid {key}")
        elif field["type"] == "enum":
            if value not in field.get("options", []):
                raise ValueError(f"Invalid {key}")
        elif (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
              or (field["type"] == "integer" and int(value) != value)
              or ("min" in field and value < field["min"]) or ("max" in field and value > field["max"])):
            raise ValueError(f"Invalid {key}")
