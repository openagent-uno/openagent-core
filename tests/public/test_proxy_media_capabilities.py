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


def test_current_claude_generations_default_to_vision():
    for model in ("claude-opus-5", "claude-sonnet-5", "claude-haiku-5-5", "claude-opus-5-5"):
        assert default_input_modalities("local", model) == ["text", "image", "file"]
    assert default_input_modalities("local", "claude-2.1") == ["text"]


def test_a_declared_vision_capability_accepts_images():
    # Rows registered while the default missed a model family persisted a
    # text-only declaration next to the provider's own "vision" capability.
    from types import SimpleNamespace
    from openagent_core.models.media_capabilities import input_modalities_for

    entry = SimpleNamespace(provider="local", model_id="claude-opus-5", metadata={
        "capabilities": ["chat", "streaming", "tools", "vision"], "input_modalities": ["text"]})
    assert input_modalities_for(entry) == frozenset({"text", "image"})
    text_only = SimpleNamespace(provider="local", model_id="claude-opus-4-8", metadata={
        "capabilities": ["chat", "tools"], "input_modalities": ["text"]})
    assert input_modalities_for(text_only) == frozenset({"text"})
