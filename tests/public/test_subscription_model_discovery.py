"""Proxy media declarations survive discovery into product registration."""

from openagent_core.models.discovery import _openai_models_url, _parse_openai_style


def test_subscription_proxy_catalog_keeps_supported_media_only():
    result = _parse_openai_style({"data": [
        {"id": "claude-opus-4-8", "input_modalities": ["text", "image", "file"],
         "capabilities": ["chat", "vision", "file"]},
        {"id": "gpt-5.6-sol:high", "input_modalities": ["text", "image"],
         "capabilities": ["chat", "vision", "image_generation"]},
    ]})
    assert result[0]["input_modalities"] == ["text", "image", "file"]
    assert result[1]["capabilities"] == ["chat", "vision", "image_generation"]
    assert "image_generation" not in result[0]["capabilities"]


def test_configured_proxy_models_url_uses_one_v1_segment():
    assert _openai_models_url("openai", "http://claude-proxy:8787/v1") == "http://claude-proxy:8787/v1/models"
    assert _openai_models_url("codex", "http://codex-proxy:8788/v1") == "http://codex-proxy:8788/v1/models"
    assert _openai_models_url("codex", "http://codex-proxy:8788") == "http://codex-proxy:8788/v1/models"
