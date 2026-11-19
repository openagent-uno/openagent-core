"""The host's account label and shared model bindings survive browser renewal."""
import importlib
from unittest.mock import AsyncMock, patch
import pytest
from openagent_core.administration import ManagementContext, ProviderModelAdmin
from openagent_core.contracts import PrincipalRef
from openagent_core.engine import MemoryDB
from openagent_core.subscriptions.service import subscription_for

class Policy:
    async def authorize(self, *args, **kwargs): return True

@pytest.mark.asyncio
@pytest.mark.parametrize("layout", ["flat", "provider"])
@pytest.mark.parametrize(('vendor','adapter'), [('openai','codex'),('anthropic','claude')])
async def test_sign_in_name_renewal_and_rename_keep_shared_connections_consistent(tmp_path, vendor, adapter, layout):
    db = MemoryDB(str(tmp_path / 'state.sqlite')); await db.connect()
    admin = ProviderModelAdmin(db, Policy(), reload_catalog=AsyncMock())
    context = ManagementContext(PrincipalRef('fixture','tenant','owner','user'), 'agent')
    pid = await db.upsert_provider(name='work', framework='api-based', metadata={'vendor':vendor,'access':'subscription','label':'Work', 'subscription_store_layout':layout})
    service = subscription_for(await db.get_provider(pid), db.db_path)
    implementation = importlib.import_module(f'openagent_core.subscriptions.{adapter}.adapter')
    async def signin(provider, name=None):
        login = await admin.subscription(context, provider, 'start')
        fields = {'state':login['state'], 'code':'fixture-code'}
        if name: fields['name'] = name
        with patch.object(implementation, 'exchange_code', return_value={'access_token':'test-access','refresh_token':'test-refresh'}):
            return await admin.subscription(context, provider, 'complete', fields)
    try:
        result = await signin(pid)
        account_id = result['account']['id']
        assert result['account']['name'] == 'Work'
        first = await db.get_provider(pid)
        image_id = await db.upsert_provider(name='images', framework='api-based', metadata={**first['metadata'], 'credential_store_id':pid})
        unrelated_id = await db.upsert_provider(name='isolated', framework='api-based', metadata={**first['metadata'], 'credential_store_id':pid, 'subscription_store_layout':'provider' if layout == 'flat' else 'flat'})
        await admin.subscription(context, pid, 'update_account', {'id':account_id,'enabled':False})
        renewed = await signin(image_id, 'Renamed work')
        assert renewed['account']['id'] == account_id
        accounts = await service.accounts()
        assert len(accounts) == 1 and accounts[0]['name'] == 'Renamed work' and accounts[0]['enabled'] is True
        for identifier in [pid,image_id]:
            row = await db.get_provider(identifier)
            assert row['metadata']['account_id'] == account_id
            assert row['metadata']['label'] == 'Renamed work'
            assert row['enabled'] is False
        assert (await db.get_provider(unrelated_id))['enabled'] is True
        assert (await db.get_provider(unrelated_id))['metadata']['label'] == 'Work'
        await admin.subscription(context, image_id, 'update_account', {'id':account_id,'name':'Display name','fallback_enabled':False})
        for identifier in [pid,image_id]:
            assert (await db.get_provider(identifier))['metadata']['label'] == 'Display name'
            assert (await db.get_provider(identifier))['metadata']['account_fallback_enabled'] is False
    finally:
        await service.close(); await db.close()
