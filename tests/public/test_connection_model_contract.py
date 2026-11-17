"""One vendor can have independent API/subscription connections and all model kinds."""
import pytest
from unittest.mock import AsyncMock, patch

from openagent_core.engine import MemoryDB, NativeProvider
from openagent_core.models.settings_schema import model_settings_schema, validate_settings


@pytest.mark.asyncio
async def test_all_model_kinds_share_a_connection_without_entering_the_text_router(tmp_path):
    db = MemoryDB(str(tmp_path / "state.sqlite"))
    await db.connect()
    try:
        pid = await db.upsert_provider(name="openai-work", framework="api-based", metadata={"vendor": "openai", "access": "api", "label": "Work"}, api_key="fixture")
        for kind in ("llm", "image", "tts", "stt"):
            await db.upsert_model(provider_id=pid, model=f"fixture-{kind}", kind=kind)
        assert {r["kind"] for r in await db.list_models_enriched(provider_id=pid)} == {"llm", "image", "tts", "stt"}
        catalog = await db.materialise_providers_config()
        assert [m["model"] for m in catalog[0]["models"]] == ["fixture-llm"]
        audio = await db.latest_audio_model("tts")
        assert audio["provider_id"] == pid and audio["provider_metadata"]["vendor"] == "openai"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_same_model_settings_and_driver_remain_bound_to_the_connection(tmp_path):
    path = str(tmp_path / "state.sqlite")
    db = MemoryDB(path)
    await db.connect()
    try:
        for name, value in (("openai-personal", 0), ("openai-work", 0.7)):
            pid = await db.upsert_provider(name=name, framework="api-based", metadata={"vendor": "openai", "access": "api"}, api_key="fixture")
            await db.upsert_model(provider_id=pid, model="fixture", metadata={"temperature": value})
        config = await db.materialise_providers_config()
        models = [NativeProvider(f"{name}:fixture", providers_config=config, db_path=path) for name in ("openai-personal", "openai-work")]
        assert [m._sampling_from_model_row()["temperature"] for m in models] == [0, 0.7]
        assert [m.build_runtime_model().api_key for m in models] == ["fixture", "fixture"]
        assert all("OpenAI" in m.build_runtime_model().get_provider() for m in models)
    finally:
        await db.close()


def test_settings_are_capability_specific_and_zero_is_not_inheritance():
    subscription = {"name": "work", "metadata": {"vendor": "openai", "access": "subscription"}}
    assert [f["key"] for f in model_settings_schema(subscription, {"kind": "llm"})] == ["reasoning_effort"]
    with pytest.raises(ValueError, match="not supported"):
        validate_settings(subscription, {"kind": "llm"}, {"temperature": 0})
    validate_settings(subscription, {"kind": "llm"}, {"reasoning_effort": "high"})
    api = {"name": "openai"}
    validate_settings(api, {"kind": "llm", "model": "gpt-4o"}, {"temperature": 0})
    with pytest.raises(ValueError):
        validate_settings(api, {"kind": "llm"}, {"temperature": float("nan")})
    assert model_settings_schema(api, {"kind": "tts"})[0]["key"] == "voice_id"
    assert model_settings_schema(api, {"kind": "stt"})[0]["key"] == "language"
    assert model_settings_schema(api, {"kind": "image"}) == []


@pytest.mark.parametrize(('model', 'expected'), [
    ('gpt-fixture:high', 'work:gpt-fixture:high'),
    ('image/gpt-fixture', 'work:image/gpt-fixture'),
    ('vendor/model', 'work:vendor/model'),
    ('work:gpt-fixture:high', 'work:gpt-fixture:high'),
    ('work/gpt-fixture', 'work:gpt-fixture'),
])
def test_runtime_identity_belongs_to_the_explicit_connection(model, expected):
    from openagent_core.models.catalog import build_runtime_model_id
    assert build_runtime_model_id('work', model) == expected


@pytest.mark.asyncio
async def test_legacy_qualified_model_keeps_its_exact_settings(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    db = MemoryDB(path)
    await db.connect()
    try:
        pid = await db.upsert_provider(name='work', framework='api-based', api_key='fixture')
        await db.upsert_model(provider_id=pid, model='work:gpt-fixture:high', metadata={'max_tokens': 321})
        runtime = NativeProvider('work:gpt-fixture:high', providers_config=await db.materialise_providers_config(), db_path=path)
        assert runtime._model_row_metadata()['max_tokens'] == 321
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_non_text_budget_does_not_claim_reasoning_routing_enforcement(tmp_path):
    from openagent_core.core.budget_guard import compute_budget_usage
    db = MemoryDB(str(tmp_path / 'state.sqlite')); await db.connect()
    try:
        pid = await db.upsert_provider(name='api', framework='api-based')
        await db.upsert_model(provider_id=pid, model='image-fixture', kind='image')
        await db.upsert_model(provider_id=pid, model='text-fixture', kind='llm')
        for model in ['image-fixture', 'text-fixture']:
            await db.add_budget(scope_kind='model', scope_value=f'api:{model}', amount=5)
        rules = {r['scope_value']:r for r in await compute_budget_usage(db)}
        assert rules['api:image-fixture']['enforced'] is False
        assert 'hard spend cap' in rules['api:image-fixture']['metering_warning']
        assert rules['api:text-fixture']['enforced'] is True
        assert rules['api:text-fixture']['enforced_kinds'] == ['llm']
    finally:
        await db.close()
