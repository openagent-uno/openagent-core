"""Image input declarations survive custom provider names and explicit overrides."""

from openagent_core.models.media_capabilities import default_input_modalities, normalize_model_metadata


def test_claude_proxy_defaults_to_vision():
    for model in ("claude-opus-4-8", "claude-sonnet-4-6", "claude-haiku-4-5"):
        assert default_input_modalities("local", model) == ["text", "image", "file"]
        assert normalize_model_metadata(None, provider="local", model=model)["input_modalities"] == ["text", "image", "file"]


def test_explicit_proxy_capabilities_win():
    assert normalize_model_metadata(
        {"input_modalities": ["text"]}, provider="local", model="claude-opus-4-8",
    )["input_modalities"] == ["text"]
    assert default_input_modalities("local", "unknown-llm") == ["text"]
