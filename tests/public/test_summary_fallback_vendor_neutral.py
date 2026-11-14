"""The compaction summariser's fallback is chosen by properties, never by vendor name."""

import pytest

from openagent_core.core import compaction
from openagent_core.models import catalog
import openagent_core.models.native_provider as native_provider


class _Agent:
    def __init__(self, providers_config):
        self._providers_config = providers_config
        self._db = None


def _providers(*names_models):
    return [
        {"id": i, "name": name, "framework": "api-based", "enabled": True,
         "models": [{"id": i * 100 + 1, "model": model}]}
        for i, (name, model) in enumerate(names_models, start=1)
    ]


@pytest.fixture
def picked(monkeypatch):
    captured = {}

    class FakeProvider:
        def __init__(self, *, model, providers_config, db_path):
            captured["model"] = model
            self.model = model

    prices = {}
    monkeypatch.setattr(native_provider, "NativeProvider", FakeProvider)
    monkeypatch.setattr(
        catalog, "get_model_pricing",
        lambda ref, cfg=None: {"input_cost_per_million": prices.get(ref, 0.0),
                               "output_cost_per_million": 0.0},
    )
    monkeypatch.delenv(compaction._SUMMARY_FALLBACK_MODEL_ENV, raising=False)
    return captured, prices


def test_no_vendor_is_preferred_by_name(picked):
    captured, _ = picked
    # Two equally priced cloud rows of other providers: configuration order decides,
    # whatever the vendor is called.
    for order in (("vendor-a", "deepseek"), ("deepseek", "vendor-a")):
        cfg = _providers(("anthropic", "claude-haiku-4-5"),
                         (order[0], "gpt-5-mini"), (order[1], "deepseek-chat"))
        captured.clear()
        assert compaction._summary_fallback_model(_Agent(cfg), exclude_provider="anthropic")
        first = cfg[1]["name"] + ":" + cfg[1]["models"][0]["model"]
        assert captured["model"] == first


def test_cheapest_other_provider_wins(picked):
    captured, prices = picked
    cfg = _providers(("anthropic", "claude-haiku-4-5"),
                     ("vendor-a", "gpt-5-mini"), ("vendor-b", "gemini-2.5-flash"))
    prices.update({"vendor-a:gpt-5-mini": 3.0, "vendor-b:gemini-2.5-flash": 1.0,
                   "anthropic:claude-haiku-4-5": 0.0})
    compaction._summary_fallback_model(_Agent(cfg), exclude_provider="anthropic")
    # The failed provider is the cheapest overall but is never picked again.
    assert captured["model"] == "vendor-b:gemini-2.5-flash"


def test_self_hosted_other_provider_first(picked):
    captured, _ = picked
    cfg = _providers(("anthropic", "claude-haiku-4-5"),
                     ("vendor-a", "gpt-5-mini"), ("local-gpu", "qwen3-moe"))
    compaction._summary_fallback_model(_Agent(cfg), exclude_provider="anthropic")
    assert captured["model"] == "local-gpu:qwen3-moe"


def test_operator_choice_and_no_distinct_provider(picked, monkeypatch):
    captured, _ = picked
    cfg = _providers(("anthropic", "claude-haiku-4-5"),
                     ("vendor-a", "gpt-5-mini"), ("vendor-b", "gemini-2.5-flash"))
    monkeypatch.setenv(compaction._SUMMARY_FALLBACK_MODEL_ENV, "vendor-b:gemini-2.5-flash")
    compaction._summary_fallback_model(_Agent(cfg), exclude_provider="anthropic")
    assert captured["model"] == "vendor-b:gemini-2.5-flash"
    monkeypatch.delenv(compaction._SUMMARY_FALLBACK_MODEL_ENV)
    only = _providers(("anthropic", "claude-haiku-4-5"))
    assert compaction._summary_fallback_model(_Agent(only), exclude_provider="anthropic") is None
