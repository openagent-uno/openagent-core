"""A failed compaction summary picks its fallback by the KIND of failure.

A quota, rate-limit or auth failure is shared by every model of the provider,
so only another provider can help. A timeout (or another call-specific failure)
is not: a different model of the same provider is a valid retry, and on an
install with a single working provider it is the only one. The operator's
OPENAGENT_COMPACTION_FALLBACK_MODEL is honoured whatever its provider.
"""

import asyncio

import pytest

from openagent_core.core import compaction
from openagent_core.models import catalog
import openagent_core.models.native_provider as native_provider


class _Agent:
    def __init__(self, providers_config):
        self._providers_config = providers_config
        self._db = None


def _providers(spec):
    """spec: [(provider, [models...]), ...]"""
    out = []
    for i, (name, models) in enumerate(spec, start=1):
        out.append({"id": i, "name": name, "framework": "api-based", "enabled": True,
                    "models": [{"id": i * 100 + j, "model": m} for j, m in enumerate(models, 1)]})
    return out


class _Model:
    def __init__(self, model, *, fail=None, summary="recap"):
        self.model = model
        self.fail = fail
        self.summary = summary
        self.calls = 0

    async def generate(self, messages, **kwargs):
        self.calls += 1
        if self.fail is not None:
            raise self.fail
        return type("R", (), {"content": self.summary})()


@pytest.fixture
def env(monkeypatch):
    built = {}

    class FakeProvider(_Model):
        def __init__(self, *, model, providers_config, db_path):
            super().__init__(model, summary=f"recap by {model}")
            built[model] = self

    monkeypatch.setattr(native_provider, "NativeProvider", FakeProvider)
    monkeypatch.setattr(
        catalog, "get_model_pricing",
        lambda ref, cfg=None: {"input_cost_per_million": 0.0, "output_cost_per_million": 0.0},
    )
    monkeypatch.delenv(compaction._SUMMARY_FALLBACK_MODEL_ENV, raising=False)
    monkeypatch.setenv(compaction._SUMMARY_TIMEOUT_ENV, "5")
    return built


_RUNS = [{"content": "hi", "messages": [{"role": "user", "content": "please fix the build"}]}]


def _summarize(primary, agent, monkeypatch):
    monkeypatch.setattr(compaction, "_pick_summary_model", lambda a, *, fallback: primary)
    return asyncio.run(compaction._summarize_runs(_RUNS, primary, agent))


def test_timeout_on_single_provider_retries_a_sibling_model(env, monkeypatch):
    agent = _Agent(_providers([("proxy", ["slow-model", "small-model"])]))
    primary = _Model("proxy:slow-model", fail=TimeoutError("compaction summary exceeded 45s"))
    out = _summarize(primary, agent, monkeypatch)
    assert out == "recap by proxy:small-model"
    assert primary.calls == 1


def test_quota_failure_never_retries_the_same_provider(env, monkeypatch):
    agent = _Agent(_providers([("proxy", ["model-a", "model-b"])]))
    primary = _Model("proxy:model-a", fail=RuntimeError("usage limit reached for this account"))
    out = _summarize(primary, agent, monkeypatch)
    assert out == ""
    assert "proxy:model-b" not in env


def test_pool_exhaustion_wording_is_provider_wide(env, monkeypatch):
    agent = _Agent(_providers([("proxy", ["model-a", "model-b"]), ("other", ["model-c"])]))
    primary = _Model("proxy:model-a", fail=RuntimeError("No available OAuth accounts"))
    out = _summarize(primary, agent, monkeypatch)
    assert out == "recap by other:model-c"
    assert "proxy:model-b" not in env


def test_operator_fallback_honoured_even_on_the_same_provider(env, monkeypatch):
    agent = _Agent(_providers([("proxy", ["model-a", "model-b", "model-c"])]))
    monkeypatch.setenv(compaction._SUMMARY_FALLBACK_MODEL_ENV, "proxy:model-c")
    primary = _Model("proxy:model-a", fail=TimeoutError("slow"))
    out = _summarize(primary, agent, monkeypatch)
    assert out == "recap by proxy:model-c"
    assert "proxy:model-b" not in env


def test_fallbacks_are_bounded_and_never_repeat_the_failed_model(env, monkeypatch):
    agent = _Agent(_providers([("proxy", ["model-a", "model-b"]), ("other", ["model-c"])]))

    def failing(*, model, providers_config, db_path):
        m = _Model(model, fail=TimeoutError("slow"))
        env[model] = m
        return m

    monkeypatch.setattr(native_provider, "NativeProvider", failing)
    primary = _Model("proxy:model-a", fail=TimeoutError("slow"))
    out = _summarize(primary, agent, monkeypatch)
    assert out == ""
    assert primary.calls == 1
    assert set(env) == {"proxy:model-b", "other:model-c"}
    assert all(m.calls == 1 for m in env.values())
    assert len(env) <= compaction._MAX_SUMMARY_FALLBACKS
