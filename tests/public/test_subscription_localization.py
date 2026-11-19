"""Offline retirement of a custody listener, without rebuilding catalogs."""
import hashlib
import json

import pytest
from openagent_core.engine import MemoryDB
from openagent_core.subscriptions.migration import localize_subscription_accounts
from openagent_core.subscriptions.service import subscription_for


@pytest.mark.asyncio
async def test_localization_preserves_catalog_and_shares_atomic_rotations(tmp_path):
    source = tmp_path / 'old.json'
    source.write_text(json.dumps({'accounts': [
        {'id':'a', 'name':'Personal', 'access_token':'fixture-a', 'refresh_token':'rotate-a'},
        {'id':'b', 'name':'Work', 'access_token':'fixture-b', 'enabled':False}]}))
    original = source.read_bytes()
    dbs, services = [], []
    shared = tmp_path / 'private'; shared.mkdir()
    try:
        for index in range(2):
            root = tmp_path / str(index); root.mkdir()
            (root / 'subscriptions').symlink_to(shared, target_is_directory=True)
            db = MemoryDB(str(root / 'state.sqlite')); await db.connect(); dbs.append(db)
            ids = []
            for account in ('a', 'b'):
                pid = await db.upsert_provider(name='connection-'+account, framework='api-based',
                    base_url='http://localhost:8787/v1', enabled=account=='a', metadata={
                        'access':'subscription', 'vendor':'anthropic', 'subscription_remote':True,
                        'subscription_remote_unkeyed':True, 'account_id':account,
                        'budget_group':'original', 'account_fallback_group':'keep',
                        'subscription_settings':{'max_tools':12}})
                ids.append(pid)
                await db.upsert_model(provider_id=pid, model='fixture', is_classifier=account=='a',
                    metadata={'reasoning_effort':'high','budget_model_group':'old:fixture'})
            api = await db.upsert_provider(name='anthropic-api',framework='api-based',base_url='https://api.anthropic.com')
            before_models = await db.list_models()
            before_rows = [await db.get_provider(pid) for pid in ids]
            result = await localize_subscription_accounts(db, 'claude', source, quiesced=True,
                expected_sha256=hashlib.sha256(original).hexdigest(), credential_owners={'a':71,'b':72},
                provider_ids=ids, admission_id=70, subscription_settings={'request_timeout':120})
            assert result['provider_ids']==ids and result['accounts']==2
            assert await db.list_models()==before_models
            assert (await db.get_provider(api))['base_url']=='https://api.anthropic.com'
            for before in before_rows:
                after=await db.get_provider(before['id'])
                assert after['enabled']==before['enabled']
                assert after['name']==before['name'] and after['base_url'] is None
                m=after['metadata']; assert 'subscription_remote' not in m
                assert m['budget_group']=='original' and m['account_fallback_group']=='keep'
                assert m['subscription_settings']=={'request_timeout':120,'max_tools':12}
            services.append(subscription_for(await db.get_provider(ids[0]), db.db_path))
        assert len(list(shared.glob('*.json')))==2 and source.read_bytes()==original
        assert services[0].manager.creds_file.samefile(services[1].manager.creds_file)
        from openagent_core.subscriptions.credential_storage import write_credentials
        target=shared/'71-claude.json'; data=json.loads(target.read_text())
        data['accounts'][0]['access_token']='rotated-fixture'
        write_credentials(target,data)
        # Core's manager reopens the atomic replacement instead of retaining a stale token.
        assert services[1].manager.list_accounts()[0]['access_token']=='rotated-fixture'
        assert b'fixture-a' not in json.dumps(result).encode()
    finally:
        for service in services: await service.close()
        for db in dbs: await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['running','digest','missing-row','conflict','symlink','settings'])
async def test_localization_refuses_unreviewed_or_conflicting_state(tmp_path, failure):
    source=tmp_path/'old.json'; source.write_text(json.dumps({'accounts':[{'id':'a','access_token':'fixture'}]}))
    db=MemoryDB(str(tmp_path/'db.sqlite')); await db.connect()
    try:
        pid=await db.upsert_provider(name='native',framework='api-based',metadata={
            'access':'subscription','vendor':'anthropic','account_id':'a','subscription_remote':True})
        before=await db.get_provider(pid)
        target=tmp_path/'subscriptions'/'1-claude.json'
        if failure in {'conflict','symlink'}:
            target.parent.mkdir()
            if failure=='conflict': target.write_text('{"accounts":[]}')
            else: target.symlink_to(source)
        with pytest.raises(ValueError):
            await localize_subscription_accounts(db,'claude',source,
                quiesced=failure!='running', expected_sha256='bad' if failure=='digest' else hashlib.sha256(source.read_bytes()).hexdigest(),
                credential_owners={'a':1},provider_ids=[999 if failure=='missing-row' else pid],admission_id=1,
                subscription_settings={'api_key':'private'} if failure=='settings' else {})
        assert await db.get_provider(pid)==before
        if failure not in {'conflict','symlink'}: assert not target.exists()
    finally: await db.close()


@pytest.mark.asyncio
async def test_source_change_rolls_back_database_and_only_new_files(tmp_path, monkeypatch):
    from openagent_core.subscriptions import migration
    source=tmp_path/'source.json'; source.write_text(json.dumps({'accounts':[{'id':'a','access_token':'fixture'}]}))
    db=MemoryDB(str(tmp_path/'db.sqlite')); await db.connect()
    try:
        pid=await db.upsert_provider(name='account',framework='api-based',metadata={
            'access':'subscription','vendor':'anthropic','account_id':'a','subscription_remote':True})
        before=await db.get_provider(pid)
        real_write=migration.write_credentials
        def changing_writer(path, value):
            real_write(path, value)
            source.write_text(source.read_text()+' ')
        monkeypatch.setattr(migration,'write_credentials',changing_writer)
        with pytest.raises(ValueError,match='writer was not stopped'):
            await localize_subscription_accounts(db,'claude',source,quiesced=True,
                expected_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),credential_owners={'a':1},
                provider_ids=[pid],admission_id=1)
        assert await db.get_provider(pid)==before
        assert not (tmp_path/'subscriptions'/'1-claude.json').exists()
    finally: await db.close()
