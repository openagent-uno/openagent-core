import math
import pytest
from openagent_core.subscriptions.settings import native_settings, validate_settings


@pytest.mark.parametrize('options', [{'creds_file': '/other'}, {'api_key': 'secret'},
    {'admin_api_key': 'secret'}, {'admission_path': '/other'}, {'request_timeout': math.nan},
    {'upstream_max_concurrency': True}, {'model_map': {'alias': 'bad\nheader'}},
    {'rate_limit_retries': -1}, {'unknown': 1}])
def test_catalog_protocol_settings_reject_private_fields_and_invalid_values(options):
    with pytest.raises(ValueError): validate_settings('claude', options)


def test_settings_preserve_explicit_pacing_model_map_and_shared_admission_without_credentials(tmp_path):
    store, admission = tmp_path / 'one.json', tmp_path / 'shared-admission'
    options = {'upstream_max_concurrency': 2, 'upstream_min_interval': 1.0,
        'model_map': {'opus-4-8': 'claude-opus-4-8'}, 'transport': 'httpx'}
    settings = native_settings('claude', store, options, admission_path=admission)
    assert settings.creds_file == store and settings.admission_path == admission
    assert settings.upstream_max_concurrency == 2 and settings.upstream_min_interval == 1
    assert settings.model_map == options['model_map']
    assert not store.exists() and not admission.exists()
    assert not settings.api_key and not settings.admin_api_key


@pytest.mark.asyncio
async def test_separate_rotating_accounts_share_the_original_pool_admission(tmp_path, monkeypatch):
    from openagent_core.subscriptions import admission
    from openagent_core.subscriptions.service import SubscriptionService
    original = admission.StoreSemaphore
    leases = []
    def semaphore(store, label, capacity):
        obj = original(store, label, capacity)
        if label == 'active': leases.append(obj)
        return obj
    monkeypatch.setattr(admission, 'StoreSemaphore', semaphore)
    common = tmp_path / 'admission-one'
    services = [SubscriptionService('claude', tmp_path / name,
        settings=native_settings('claude', tmp_path / name,
            {'upstream_max_concurrency': 1}, admission_path=common))
        for name in ('first.json', 'second.json')]
    try:
        assert len(leases) == 2 and all(lease.store == common for lease in leases)
        assert leases[0].acquire(blocking=False)
        try: assert not leases[1].acquire(blocking=False)
        finally: leases[0].release()
        assert leases[1].acquire(blocking=False)
        leases[1].release()
    finally:
        for service in services: await service.close()
