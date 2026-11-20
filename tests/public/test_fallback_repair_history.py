"""Request repair retains generated evidence without rewriting the input."""
import asyncio
import copy
from types import SimpleNamespace

import pytest

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.providers.fallback import (
    FallbackConfig, acall_model_stream_with_fallback, acall_model_with_fallback,
    call_model_stream_with_fallback, call_model_with_fallback, reset_breaker,
)
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.chat import OpenAIChat
from openagent_core.models.providers.response import ModelResponse


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_repaired_request_retains_only_generated_messages(stream, outcome):
    original = Message(role="user", content=[{"type": "text", "text": "Compare these"},
                       {"type": "image_url", "image_url": {"url": "https://example.invalid/fixture.png"}}])
    before = copy.deepcopy(original.model_dump())
    messages, attempts, effects = [original], [], []
    failure = ModelProviderError("provider failed after tool", status_code=503)
    class Repaired(OpenAIChat):
        async def attempt(self, supplied):
            attempts.append(supplied)
            if len(attempts) == 1:
                raise ModelProviderError("image too large", status_code=400)
            assert supplied[0] is not original
            assert all(p.get("type") != "image_url" for p in supplied[0].content)
            supplied.extend([Message(role="assistant", tool_calls=[{"id": "call"}]),
                             Message(role="tool", tool_call_id="call", content="effect-719")])
            effects.append("read")
            if outcome == "failure": raise failure
            if outcome == "cancel": raise asyncio.CancelledError()
            supplied.append(Message(role="assistant", content="Repaired answer"))
        async def aresponse(self, **kwargs):
            await self.attempt(kwargs["messages"])
            return ModelResponse(content="Repaired answer")
        async def aresponse_stream(self, **kwargs):
            await self.attempt(kwargs["messages"])
            yield ModelResponse(content="Repaired answer")
    class Forbidden(OpenAIChat):
        async def aresponse(self, **kwargs): pytest.fail("A committed tool was replayed")
        async def aresponse_stream(self, **kwargs):
            pytest.fail("A committed tool was replayed")
            yield
    primary = Repaired(id="repair", api_key="fixture")
    config = FallbackConfig(on_error=[Forbidden(id="forbidden", api_key="fixture")])
    async def execute():
        if stream:
            return [e async for e in acall_model_stream_with_fallback(primary, config, messages=messages)]
        return await acall_model_with_fallback(primary, config, messages=messages)
    if outcome == "success": await execute()
    else:
        with pytest.raises(ModelProviderError if outcome == "failure" else asyncio.CancelledError):
            await execute()
    assert original.model_dump() == before
    assert messages[0] is original
    assert effects == ["read"]
    assert [m.role for m in messages] == ["user", "assistant", "tool"] + (["assistant"] if outcome == "success" else [])
    assert messages[2].content == "effect-719"


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("backup_effect", [False, True])
def test_sync_fallback_never_replays_committed_tools(stream, backup_effect):
    reset_breaker()
    messages = [Message(role="user", content="Synthetic request")]
    invoked = []
    class Scripted(OpenAIChat):
        def attempt(self, supplied):
            invoked.append(self.id)
            if self.id == ("backup" if backup_effect else "primary"):
                supplied.extend([Message(role="assistant", tool_calls=[{"id": "call"}]),
                                 Message(role="tool", tool_call_id="call", content="effect")])
            raise ModelProviderError("rate_limit_error", status_code=429)
        def response(self, **kwargs): self.attempt(kwargs["messages"])
        def response_stream(self, **kwargs):
            self.attempt(kwargs["messages"])
            yield
    primary, backup, forbidden = [Scripted(id=name, api_key="fixture") for name in ("primary", "backup", "forbidden")]
    config = FallbackConfig(on_rate_limit=[backup, forbidden])
    try:
        with pytest.raises(ModelProviderError):
            if stream: list(call_model_stream_with_fallback(primary, config, messages=messages))
            else: call_model_with_fallback(primary, config, messages=messages)
        assert invoked == (["primary", "backup"] if backup_effect else ["primary"])
        assert messages[-1].content == "effect"
    finally: reset_breaker()


@pytest.mark.asyncio
@pytest.mark.parametrize("backup_effect", [False, True])
async def test_async_stream_never_replays_a_tool_without_a_visible_event(backup_effect):
    reset_breaker()
    messages, invoked = [Message(role="user", content="Synthetic request")], []
    class Scripted(OpenAIChat):
        async def aresponse_stream(self, **kwargs):
            invoked.append(self.id)
            if self.id == ("backup" if backup_effect else "primary"):
                kwargs["messages"].extend([Message(role="assistant", tool_calls=[{"id": "call"}]),
                                          Message(role="tool", tool_call_id="call", content="effect")])
            raise ModelProviderError("rate_limit_error", status_code=429)
            yield
    primary, backup, forbidden = [Scripted(id=name, api_key="fixture") for name in ("primary", "backup", "forbidden")]
    try:
        with pytest.raises(ModelProviderError):
            async for _ in acall_model_stream_with_fallback(primary, FallbackConfig(on_rate_limit=[backup, forbidden]), messages=messages):
                pass
        assert invoked == (["primary", "backup"] if backup_effect else ["primary"])
        assert messages[-1].content == "effect"
    finally: reset_breaker()


@pytest.mark.asyncio
async def test_rotated_account_cannot_replay_a_tool_before_first_visible_event():
    invoked, rotations = [], []
    messages = [Message(role="user", content="Synthetic request")]
    class Scripted(OpenAIChat):
        async def aresponse_stream(self, **kwargs):
            invoked.append(self.api_key)
            if self.api_key == "rotated-first":
                kwargs["messages"].extend([Message(role="assistant", tool_calls=[{"id": "call"}]),
                                          Message(role="tool", tool_call_id="call", content="effect")])
            raise ModelProviderError("rate_limit_error", status_code=429)
            yield
    def rotate(**kwargs):
        rotations.append(True)
        return SimpleNamespace(api_key="rotated-first" if len(rotations) == 1 else "must-not-run", base_url="https://example.invalid")
    primary = Scripted(id="primary", api_key="initial")
    primary._openagent_cred_pool = SimpleNamespace(mark_exhausted_and_rotate=rotate)
    with pytest.raises(ModelProviderError):
        async for _ in acall_model_stream_with_fallback(primary, None, messages=messages): pass
    assert invoked == ["initial", "rotated-first"]
    assert len(rotations) == 1
    assert messages[-1].content == "effect"
