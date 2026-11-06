from __future__ import annotations

import asyncio
from typing import Any

import pytest

from openagent_core.models.dispatcher import _arun_runtime_stream


class _Runtime:
    def __init__(self, events: list[Any]) -> None:
        self.events = events

    def arun(self, _prompt: str, **_kwargs: Any):
        async def _stream():
            for event in self.events:
                yield event

        return _stream()


async def _drain(events: list[Any]) -> str:
    chunks = []
    async for chunk in _arun_runtime_stream(
        _Runtime(events),
        prompt="hello",
        session_id="session-1",
        user_id="user-1",
        on_status=None,
        error_event="test.runtime_stream_error",
    ):
        chunks.append(chunk)
    return "".join(chunks)


@pytest.mark.asyncio
@pytest.mark.parametrize("team", [False, True])
async def test_terminal_error_is_not_treated_as_an_empty_stream(team: bool) -> None:
    if team:
        from openagent_core.core._run_state.team import RunErrorEvent
    else:
        from openagent_core.core._run_state.agent import RunErrorEvent

    event = RunErrorEvent(
        session_id="session-1",
        content="provider rate limit",
    )
    with pytest.raises(RuntimeError, match="provider rate limit"):
        await _drain([event])


@pytest.mark.asyncio
@pytest.mark.parametrize("team", [False, True])
async def test_terminal_cancellation_propagates_without_fallback(team: bool) -> None:
    if team:
        from openagent_core.core._run_state.team import RunCancelledEvent
    else:
        from openagent_core.core._run_state.agent import RunCancelledEvent

    event = RunCancelledEvent(session_id="session-1", reason="stopped")
    with pytest.raises(asyncio.CancelledError, match="stopped"):
        await _drain([event])


@pytest.mark.asyncio
@pytest.mark.parametrize("team", [False, True])
@pytest.mark.parametrize("error_type,content,code", [
    ("ModelRateLimitError", "No account available PRIVATE-KEY", "rate_limit"),
    ("ModelRateLimitError", "exceeded_current_quota_error PRIVATE-KEY", "quota_exhausted"),
    ("ModelAuthenticationError", "PRIVATE-KEY", "auth"),
    ("UnknownProviderError", "PRIVATE-KEY", "generic"),
])
async def test_terminal_error_preserves_safe_provider_category(team, error_type, content, code):
    from openagent_core.core.public_errors import RunTurnError, classify_run_error, PUBLIC_MESSAGES
    if team:
        from openagent_core.core._run_state.team import RunErrorEvent
    else:
        from openagent_core.core._run_state.agent import RunErrorEvent
    with pytest.raises(RunTurnError) as raised:
        await _drain([RunErrorEvent(session_id="session-1", content=content, error_type=error_type)])
    failure = classify_run_error(raised.value)
    assert failure.code == code
    assert failure.public_message == PUBLIC_MESSAGES[code]
    assert "PRIVATE" not in failure.public_message
