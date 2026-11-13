"""A transient stream failure before any output is retried once on the same model."""

import pytest

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.providers import fallback
from openagent_core.models.providers.fallback import acall_model_stream_with_fallback


class FlakyModel:
    id = "flaky"
    api_key = None

    def __init__(self, failures, *, status=None, emit_before_failure=False):
        self.failures = list(failures)
        self.status = status
        self.emit_before_failure = emit_before_failure
        self.calls = 0

    async def aresponse_stream(self, **kwargs):
        self.calls += 1
        if self.failures and self.failures.pop(0):
            if self.emit_before_failure:
                yield "partial"
            raise ModelProviderError(message="An error occurred during streaming", status_code=self.status,
                                     model_name="m", model_id="flaky")
        yield "ok"


async def _collect(model):
    return [e async for e in acall_model_stream_with_fallback(model, None, messages=[])]


@pytest.fixture(autouse=True)
def _no_delay(monkeypatch):
    monkeypatch.setattr(fallback, "TRANSIENT_STREAM_RETRY_DELAY_S", 0)


@pytest.mark.asyncio
async def test_zero_output_network_failure_is_retried_once():
    model = FlakyModel([True])
    assert await _collect(model) == ["ok"]
    assert model.calls == 2


@pytest.mark.asyncio
async def test_server_error_is_retried_but_client_error_is_not():
    assert await _collect(FlakyModel([True], status=503)) == ["ok"]
    client = FlakyModel([True], status=400)
    with pytest.raises(ModelProviderError):
        await _collect(client)
    assert client.calls == 1


@pytest.mark.asyncio
async def test_only_one_retry_then_the_error_propagates():
    model = FlakyModel([True, True, True])
    with pytest.raises(ModelProviderError):
        await _collect(model)
    assert model.calls == 2


@pytest.mark.asyncio
async def test_failure_after_output_is_not_retried():
    model = FlakyModel([True], emit_before_failure=True)
    events = []
    with pytest.raises(ModelProviderError):
        async for event in acall_model_stream_with_fallback(model, None, messages=[]):
            events.append(event)
    assert events == ["partial"] and model.calls == 1
