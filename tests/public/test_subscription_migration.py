import hashlib
import json

import pytest
from openagent_core.engine import MemoryDB
from openagent_core.subscriptions.migration import adopt_subscription_accounts
from openagent_core.subscriptions.service import subscription_for


@pytest.mark.asyncio
async def test_adoption_preserves_identity_models_pins_source_and_separates_accounts(tmp_path):
    db = MemoryDB(str(tmp_path / "state.sqlite"))
    await db.connect()
    source = tmp_path / "old-credentials.json"
    source.write_text(json.dumps({"version": 2, "accounts": [
        {"id": "personal", "name": "Personal", "access_token": "first-test-token", "priority": 1},
        {"id": "work", "name": "Work", "access_token": "second-test-token", "priority": 2}]}))
    original = source.read_bytes()
    try:
        pid = await db.upsert_provider(name="legacy", framework="api-based", base_url="http://localhost:8787/v1", api_key="legacy-proxy-key")
        mid = await db.upsert_model(provider_id=pid, model="fixture", metadata={"max_tokens": 100}, is_classifier=True)
        with pytest.raises(ValueError, match="Drain"):
            await adopt_subscription_accounts(db, pid, "claude", source, quiesced=False, expected_sha256=hashlib.sha256(original).hexdigest())
        result = await adopt_subscription_accounts(db, pid, "claude", source, quiesced=True, expected_sha256=hashlib.sha256(original).hexdigest())
        assert len(result["connections"]) == 2
        assert source.read_bytes() == original
        first = await db.get_provider(pid)
        assert first["name"] == "legacy" and first["api_key"] == "legacy-proxy-key"
        assert (await db.get_model(mid))["is_classifier"]
        assert (await db.get_model(mid))["metadata"]["max_tokens"] == 100
        sibling = await db.get_provider(result["connections"][1]["provider_id"])
        sibling_models = await db.list_models(provider_id=sibling["id"])
        assert len(sibling_models) == 1 and not sibling_models[0]["is_classifier"]
        await db.record_usage(model="legacy:fixture", input_tokens=10, output_tokens=2, cost=0)
        await db.record_usage(model=f"{sibling['name']}:fixture", input_tokens=20, output_tokens=3, cost=0)
        assert await db.get_scope_spend(scope_kind="provider", scope_value="legacy", metric="tokens", since_epoch=0) == 35
        assert await db.get_scope_spend(scope_kind="provider", scope_value="anthropic", metric="tokens", since_epoch=0) == 35
        assert await db.get_scope_spend(scope_kind="model", scope_value="legacy:fixture", metric="tokens", since_epoch=0) == 35
        from openagent_core.models.catalog import iter_configured_models
        from openagent_core.models.connections import budget_scope_keys
        config = await db.materialise_providers_config()
        for entry in iter_configured_models(config):
            scopes = budget_scope_keys(entry, config)
            assert ("provider", "legacy") in scopes and ("provider", "anthropic") in scopes
            assert ("model", "legacy:fixture") in scopes
        first_service = subscription_for(first, db.db_path)
        sibling_service = subscription_for(sibling, db.db_path)
        assert [a["id"] for a in await first_service.accounts()] == ["personal"]
        assert [a["id"] for a in await sibling_service.accounts()] == ["work"]
        await first_service.close()
        await sibling_service.close()
        assert b"test-token" not in json.dumps(result).encode()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_migration_refuses_to_overwrite_and_rolls_back_catalog(tmp_path):
    db = MemoryDB(str(tmp_path / "state.sqlite"))
    await db.connect()
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"accounts": [{"id": "a", "access_token": "fixture"}]}))
    try:
        pid = await db.upsert_provider(name="legacy", framework="api-based")
        target = tmp_path / "subscriptions" / f"{pid}-codex.json"
        target.parent.mkdir()
        target.write_text("do not overwrite")
        with pytest.raises(ValueError, match="already exists"):
            await adopt_subscription_accounts(db, pid, "codex", source, quiesced=True, expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
        assert (await db.get_provider(pid))["metadata"] == {}
        assert target.read_text() == "do not overwrite"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_adoption_reuses_one_rotating_store_for_text_and_image_rows(tmp_path):
    from openagent_core.models.catalog import iter_configured_models
    db = MemoryDB(str(tmp_path / 'state.sqlite'))
    await db.connect()
    source = tmp_path / 'source.json'
    source.write_text(json.dumps({'accounts': [
        {'id': 'a', 'name': 'Personal', 'access_token': 'test-a', 'refresh_token': 'rotate-a'},
        {'id': 'b', 'name': 'Work', 'access_token': 'test-b', 'refresh_token': 'rotate-b'}]}))
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    try:
        text_id = await db.upsert_provider(name='legacy', framework='api-based')
        image_id = await db.upsert_provider(name='legacy-images', framework='api-based')
        mid = await db.upsert_model(provider_id=text_id, model='legacy:gpt-fixture:high', metadata={'reasoning_effort': 'high'})
        await db.upsert_model(provider_id=image_id, model='gpt-fixture', kind='image')
        first = await adopt_subscription_accounts(db, text_id, 'codex', source, quiesced=True, expected_sha256=digest)
        owners = {c['account_id']: c['credential_store_id'] for c in first['connections']}
        second = await adopt_subscription_accounts(db, image_id, 'codex', source, quiesced=True, expected_sha256=digest, credential_owners=owners)
        assert len(list((tmp_path / 'subscriptions').glob('*.json'))) == 2
        for a, b in zip(first['connections'], second['connections']):
            p1, p2 = await db.get_provider(a['provider_id']), await db.get_provider(b['provider_id'])
            assert subscription_for(p1, db.db_path) is subscription_for(p2, db.db_path)
            await subscription_for(p1, db.db_path).close()
        entries = list(iter_configured_models(await db.materialise_providers_config()))
        assert len(entries) == 2 and len({e.runtime_id for e in entries}) == 2
        assert entries[0].runtime_id == 'legacy:gpt-fixture:high'
        assert entries[1].runtime_id.endswith(':gpt-fixture:high')
        assert (await db.get_model(mid))['model'] == 'legacy:gpt-fixture:high'
        cloned = await db.list_models(provider_id=first['connections'][1]['provider_id'])
        assert cloned[0]['model'] == 'gpt-fixture:high'
        assert cloned[0]['metadata']['budget_model_group'] == 'legacy:gpt-fixture:high'
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_original_references_bind_to_the_first_available_account_not_a_disabled_one(tmp_path):
    db = MemoryDB(str(tmp_path / 'state.sqlite')); await db.connect()
    source = tmp_path / 'source.json'
    source.write_text(json.dumps({'accounts': [
        {'id':'disabled', 'access_token':'fixture-disabled', 'priority':0, 'enabled':False},
        {'id':'available', 'access_token':'fixture-available', 'priority':1, 'enabled':True}]}))
    try:
        pid = await db.upsert_provider(name='legacy', framework='api-based')
        await db.upsert_model(provider_id=pid, model='fixture', is_classifier=True)
        result = await adopt_subscription_accounts(db, pid, 'codex', source, quiesced=True, expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
        assert (await db.get_provider(pid))['metadata']['account_id'] == 'available'
        assert (await db.get_provider(result['connections'][1]['provider_id']))['enabled'] is False
    finally:
        await db.close()
