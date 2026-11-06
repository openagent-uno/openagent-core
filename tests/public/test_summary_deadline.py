"""Optional maintenance is bounded and cannot mutate a completed turn later."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import pytest
from openagent_core.memory.sessions.summary import SessionSummaryManager, SessionSummary
from openagent_core.models.providers.base import Model


def fixture(timeout=0.02):
    model = MagicMock(spec=Model)
    model.supports_native_structured_outputs = False
    model.supports_json_schema_outputs = False
    model.aresponse = AsyncMock()
    manager = SessionSummaryManager(model=model, async_timeout_seconds=timeout)
    previous = SessionSummary('durable previous summary')
    session = SimpleNamespace(summary=previous, get_messages=lambda **_: [])
    return manager, model, session, previous


@pytest.mark.asyncio
async def test_deadline_cancels_provider_and_keeps_previous_summary():
    manager, model, session, previous = fixture()
    cancelled = asyncio.Event()
    async def blocked(**_):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    model.aresponse.side_effect = blocked
    manager.summaries_updated = True
    assert await asyncio.wait_for(manager.acreate_session_summary(session), 1) is None
    assert cancelled.is_set()
    assert session.summary is previous
    assert manager.summaries_updated is False


@pytest.mark.asyncio
async def test_caller_cancellation_propagates_and_awaits_provider_cleanup():
    manager, model, session, previous = fixture(None)
    started, cancelled = asyncio.Event(), asyncio.Event()
    async def blocked(**_):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    model.aresponse.side_effect = blocked
    task = asyncio.create_task(manager.acreate_session_summary(session))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    assert session.summary is previous


@pytest.mark.asyncio
async def test_fast_summary_updates_normally():
    manager, model, session, _ = fixture(1)
    model.aresponse.return_value = SimpleNamespace(content='{"summary":"new","topics":["topic"]}')
    summary = await manager.acreate_session_summary(session)
    assert summary.summary == 'new'
    assert session.summary is summary
    assert manager.summaries_updated


@pytest.mark.asyncio
async def test_provider_timeout_is_not_misreported_as_maintenance_deadline():
    manager, model, session, previous = fixture(1)
    model.aresponse.side_effect = TimeoutError('provider timeout')
    with pytest.raises(TimeoutError, match='provider timeout'):
        await manager.acreate_session_summary(session)
    assert session.summary is previous


@pytest.mark.parametrize('value', [0, -1, float('nan'), float('inf'), True])
def test_deadline_configuration_rejects_unbounded_or_invalid_numbers(value):
    with pytest.raises(ValueError):
        SessionSummaryManager(async_timeout_seconds=value)
