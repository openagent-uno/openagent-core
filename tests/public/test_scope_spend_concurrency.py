"""Budget reads remain current, isolated and cancellable while sharing active work."""
import asyncio
import pytest
from openagent_core.engine import MemoryDB

@pytest.mark.asyncio
async def test_provider_scopes_are_exact_and_new_writes_are_immediately_visible(tmp_path):
    db = MemoryDB(str(tmp_path / 'state.sqlite'))
    await db.connect()
    try:
        for name, tokens in [('work_1', 10), ('workX1', 100), ('personal', 20)]:
            await db.upsert_provider(name=name, framework='api-based', metadata={'vendor':'openai', 'budget_group':'shared'} if name != 'workX1' else {})
            await db.record_usage(model=f'{name}:fixture', input_tokens=tokens, output_tokens=0, cost=tokens / 10)
        request = {'scope_kind':'provider','scope_value':'work_1','metric':'tokens','since_epoch':0}
        assert await db.get_scope_spend(**request) == 10
        assert await db.get_scope_spend(**{**request, 'scope_value':'shared'}) == 30
        assert await db.get_scope_spend(**{**request, 'scope_value':'openai'}) == 30
        await db.record_usage(model='work_1:fixture', input_tokens=7, output_tokens=1, cost=0.8)
        assert await db.get_scope_spend(**request) == 18
        assert await db.get_scope_spend(**{**request, 'metric':'cost_usd'}) == pytest.approx(1.8)
    finally:
        await db.close()

@pytest.mark.asyncio
async def test_cancelling_one_meter_does_not_cancel_another_or_retain_completed_results(tmp_path):
    db = MemoryDB(str(tmp_path / 'state.sqlite')); await db.connect()
    try:
        await db.record_usage(model='work:fixture', input_tokens=7, output_tokens=1, cost=1)
        original = db._read_scope_spend
        started, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def delayed(**request):
            calls.append(request); started.set(); await release.wait(); return await original(**request)
        db._read_scope_spend = delayed
        request = {'scope_kind':'provider','scope_value':'work','metric':'tokens','since_epoch':0}
        a = asyncio.create_task(db.get_scope_spend(**request)); b = asyncio.create_task(db.get_scope_spend(**request))
        await started.wait(); a.cancel()
        with pytest.raises(asyncio.CancelledError): await a
        release.set(); assert await b == 8
        assert len(calls) == 1
        assert await db.get_scope_spend(**request) == 8
        assert len(calls) == 2
        assert not db._scope_reads
    finally:
        await db.close()
