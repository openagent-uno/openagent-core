"""An explicit stop terminates a batched observer before the runner starts."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openagent_core.stream.session import StreamSession
from openagent_core.stream.events import Interrupt, TextFinal, TurnComplete


@pytest.mark.asyncio
async def test_stop_before_runner_entry_publishes_one_cancelled_terminal_and_next_turn_is_separate():
    calls = []
    async def stream(message, **kwargs):
        calls.append(message)
        yield {'kind': 'done', 'text': 'next answer'}
    agent = SimpleNamespace(config={}, name='fixture', run_stream=stream,
                            request_cancel=AsyncMock(return_value=False))
    session = StreamSession(agent, client_id='client', session_id='chat',
                            coalesce_window_ms=0, speak_enabled=False)
    await session.start()
    try:
        # Both queue entries arrive before the dispatch pump runs. The first
        # runner task is cancelled before it can execute a finally block.
        await session.push_in(TextFinal(session_id='chat', text='old', source='stt'))
        await session.push_in(Interrupt(session_id='chat', reason='manual'))
        event = await asyncio.wait_for(session.outbound.get(), 3)
        assert isinstance(event, TurnComplete) and event.reason == 'cancelled'
        assert calls == []
        assert session.outbound.empty()
        await session.push_in(TextFinal(session_id='chat', text='new', source='stt'))
        async with asyncio.timeout(3):
            while True:
                event = await session.outbound.get()
                if isinstance(event, TurnComplete):
                    assert event.reason == 'completed'
                    break
        assert calls == ['new']
    finally:
        await session.close()
