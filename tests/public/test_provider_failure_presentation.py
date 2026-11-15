"""Empty provider accounts and oversized requests reach chat users as such.

A provider that answers HTTP 402 or an endpoint that rejects the request
body with HTTP 413 used to fall through to the generic "please retry"
message, which is wrong twice: it hides the cause and retrying cannot help.
"""
import pytest

from openagent_core import ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.core.public_errors import (
    GENERIC,
    NON_RETRYABLE_CODES,
    PUBLIC_MESSAGES,
    RunTurnError,
    classify_run_error,
    is_retryable,
    run_error_from_runtime,
)
from openagent_core.core.runtime_errors import (
    ContextWindowExceededError,
    ModelProviderError,
    ModelRateLimitError,
)
from openagent_core.models.native_provider import NativeProviderError
from openagent_storage_sqlite import SqliteRuntimeStore


class _Status(Exception):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


@pytest.mark.parametrize("error", [
    # The runtime rewrites a 402 into a clean provider message.
    NativeProviderError(
        "Provider 'example' returned HTTP 402 (Insufficient Balance). This is a billing "
        "issue — refill the provider account. Retrying won't help. PRIVATE-KEY"
    ),
    # A retry wrapper may re-raise the 402 as a rate-limit error; the status wins.
    ModelRateLimitError("PRIVATE-BODY", status_code=402),
    ModelRateLimitError("Insufficient balance PRIVATE-BODY"),
    ModelProviderError("Error code: 402 - {'error': {'message': 'PRIVATE'}}", status_code=402),
    RuntimeError("payment required PRIVATE"),
    _Status("PRIVATE", 402),
])
def test_empty_provider_account_is_credit_exhausted(error):
    failure = classify_run_error(error)
    assert failure.code == "credit_exhausted"
    assert failure.retryable is False
    message = failure.public_message
    assert "credit" in message and "will not help" in message
    assert "PRIVATE" not in message and "402" not in message


@pytest.mark.parametrize("error", [
    ModelProviderError("Maximum request body size 524288 exceeded PRIVATE-BODY", status_code=413),
    # The runtime classifies "payload too large" as a context-window error;
    # the 413 still names the endpoint's request limit.
    ContextWindowExceededError("payload too large PRIVATE", status_code=413),
    RuntimeError("Request Entity Too Large PRIVATE"),
    RuntimeError("Maximum request body size 524288 exceeded"),
    _Status("PRIVATE", 413),
])
def test_oversized_request_is_request_too_large(error):
    failure = classify_run_error(error)
    assert failure.code == "request_too_large"
    assert failure.retryable is False
    message = failure.public_message
    assert "too large" in message and "will not help" in message
    assert "PRIVATE" not in message and "524288" not in message


def test_wrapped_provider_status_is_found_on_the_cause():
    outer = RuntimeError("agent run failed")
    outer.__cause__ = ModelRateLimitError("PRIVATE", status_code=402)
    assert classify_run_error(outer).code == "credit_exhausted"


def test_runtime_terminal_event_keeps_the_category():
    error = run_error_from_runtime("Insufficient balance PRIVATE", "ModelRateLimitError")
    assert error.code == "credit_exhausted"
    assert classify_run_error(error).code == "credit_exhausted"


def test_retry_advice_is_consistent_with_the_codes():
    assert NON_RETRYABLE_CODES <= set(PUBLIC_MESSAGES)
    for code in ("generic", "dns", "connection", "timeout", "rate_limit"):
        assert is_retryable(code)
    for code in NON_RETRYABLE_CODES:
        assert not is_retryable(code)
        assert "Please retry" not in PUBLIC_MESSAGES[code]
    assert is_retryable("not-a-code") is is_retryable(GENERIC)


@pytest.mark.asyncio
@pytest.mark.parametrize("error,code", [
    (ModelRateLimitError("Insufficient balance PRIVATE-KEY", status_code=402), "credit_exhausted"),
    (ModelProviderError("Maximum request body size exceeded PRIVATE-BODY", status_code=413), "request_too_large"),
])
async def test_durable_run_publishes_the_category_channel_bridges_render(tmp_path, error, code):
    """Channel bridges render ``PUBLIC_MESSAGES[record.output['error_code']]``."""

    class Policy:
        async def authorize(self, *args, **kwargs):
            return True

    class Failing:
        async def execute(self, *args):
            raise error

    principal = PrincipalRef("host", "tenant", "alice")
    context = ExecutionContext(principal, principal, principal, "session", "agent", (principal,))
    runtime = Runtime(
        RuntimeSettings("agent", tmp_path),
        RuntimeServices(SqliteRuntimeStore(tmp_path / "state.sqlite3"), Failing(), Policy()),
    )
    await runtime.start()
    try:
        await runtime.submit(RunRequest("run", "session", "request", "hello"), context)
        record = await runtime.wait("run", context)
    finally:
        await runtime.close()
    assert record.status == "failed"
    assert record.output["error_code"] == code
    assert record.output["error_public"] == PUBLIC_MESSAGES[code]
    assert "PRIVATE" not in str(record.output)
    reply = RunTurnError(PUBLIC_MESSAGES[record.output["error_code"]], code=record.output["error_code"])
    assert str(reply) != PUBLIC_MESSAGES[GENERIC]
