"""Stable classification for agent-run failures.

``Agent.run_stream`` catches every failure and reports it as a ``done``
frame instead of raising. Without a marker on that frame a failed turn is
shaped exactly like a successful one: ``StreamSession`` republishes the
text as ``OutTextDelta`` and closes the turn ``completed``, so the app, the
CLI and every bridge file the exception message as the model's answer. The
frame now carries ``errored`` plus a code from this module, and the turn
ends on ``TURN_END_ERROR`` through ``OutError`` — the vocabulary
``src/stream/events.py`` already defines for exactly this.

Codes are deliberately coarse. They exist so a client can react — "check
the provider key", "the network is down" — without parsing prose that
varies by provider, version and locale. Classification reads the exception
chain locally and only constants leave this module through
``public_message``; the real diagnostic still travels in the frame's
``text``, because a self-hosted agent is expected to diagnose itself from
what it actually saw (§14). A host that must not show provider detail to
the person in the chat renders ``public_message`` instead, keeping the
code.
"""

from __future__ import annotations

GENERIC = "generic"

PUBLIC_MESSAGES: dict[str, str] = {
    GENERIC: "The agent run failed. Please retry.",
    "dns": (
        "The model provider could not be reached because DNS resolution "
        "failed. Check the host's network and DNS, then retry."
    ),
    "connection": (
        "The model provider could not be reached. Check the network and the "
        "provider endpoint, then retry."
    ),
    "timeout": (
        "The model provider request timed out. Check the connection and the "
        "provider's status before retrying."
    ),
    "auth": (
        "AI account authentication is invalid or access was denied. Reconnect the "
        "account or check its API key and model permissions in Models and Providers."
    ),
    "auth_expired": (
        "AI account authentication has expired or was revoked. Reconnect the "
        "account in Models and Providers; retrying the message cannot renew it."
    ),
    "provider_not_configured": (
        "No AI provider account is configured. Open Models and Providers and "
        "connect an account or add an API key."
    ),
    "model_not_configured": (
        "No AI model is configured and enabled. Open Models and Providers, "
        "add a model under a connected AI account and enable it."
    ),
    "model_unavailable": (
        "The selected AI model is unavailable. Check its identifier and your "
        "account's access in Models and Providers, or select another enabled model."
    ),
    "quota_exhausted": (
        "The AI account's usage allowance or credits are exhausted. Check usage "
        "and billing; wait for the quota reset, add credits or select another account."
    ),
    "budget_exhausted": (
        "This agent's configured AI spending budget has been reached. Review "
        "Budgets or select a model that is within the budget."
    ),
    "context_length": (
        "This conversation exceeds the AI model's context limit. Start a new "
        "conversation or use a model with a larger context window."
    ),
    "rate_limit": (
        "The AI provider temporarily limited the request rate. Wait for the "
        "retry interval or quota reset shown by the provider, then retry."
    ),
}

_DNS_MARKERS = (
    "could not resolve host",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "nodename nor servname provided",
)

# Bound the walk: ``__cause__``/``__context__`` chains can be long, and a
# self-referential one would otherwise spin.
_MAX_CHAIN = 8


class RunTurnError(RuntimeError):
    """A run failure reconstructed from a ``done`` frame.

    ``run_stream`` already handled the original exception, so the stream
    layer has no exception object to record. This stands in for one, which
    is what ``StreamSession`` needs to report ``TURN_END_ERROR`` instead of
    inferring "answered" from the absence of a raise.
    """

    def __init__(self, message: str, *, code: str = GENERIC) -> None:
        self.code = code if code in PUBLIC_MESSAGES else GENERIC
        super().__init__(message)


class RunErrorClass:
    """A classified failure: a stable code and a diagnostic-free message."""

    __slots__ = ("code",)

    def __init__(self, code: str = GENERIC) -> None:
        self.code = code if code in PUBLIC_MESSAGES else GENERIC

    @property
    def public_message(self) -> str:
        return PUBLIC_MESSAGES[self.code]

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"RunErrorClass({self.code!r})"


def classify_run_error(error: BaseException | None) -> RunErrorClass:
    """Map a failure onto one of ``PUBLIC_MESSAGES``' codes.

    Walks the exception chain because the useful signal is usually on the
    cause: a provider wrapper carries the status code, the socket error
    underneath carries the DNS text. Unrecognised failures classify as
    ``generic`` rather than guessing.
    """
    seen: set[int] = set()
    for _ in range(_MAX_CHAIN):
        if error is None or id(error) in seen:
            break
        seen.add(id(error))

        if isinstance(error, RunTurnError) and error.code != GENERIC:
            return RunErrorClass(error.code)

        diagnostic = ""
        if isinstance(error, BaseException):
            diagnostic = str(error).lower()

        signal = _model_diagnostic_code(diagnostic)
        if signal is not None:
            return RunErrorClass(signal)

        if any(marker in diagnostic for marker in _DNS_MARKERS):
            return RunErrorClass("dns")
        if isinstance(error, TimeoutError) or "timed out" in diagnostic:
            return RunErrorClass("timeout")
        if "connection refused" in diagnostic or "failed to connect" in diagnostic:
            return RunErrorClass("connection")

        status = getattr(error, "status_code", None)
        if status is None:
            data = getattr(error, "additional_data", None)
            if isinstance(data, dict):
                status = data.get("status_code")
        if status == 402:
            return RunErrorClass("quota_exhausted")
        if status in (401, 403):
            return RunErrorClass("auth")
        if status == 429:
            return RunErrorClass("rate_limit")

        error = getattr(error, "__cause__", None) or getattr(error, "__context__", None)

    return RunErrorClass()


# Diagnostics are inspected locally; only an allowlisted code and constant
# message are published. Never return provider JSON, keys or request bodies.
def _model_diagnostic_code(text: str) -> str | None:
    patterns = (
        ("auth_expired", ("invalid_grant", "refresh token expired", "refresh_token expired", "token has expired", "token is expired", "token expired", "authentication expired", "authentication has expired", "credentials have expired", "credentials expired", "token was revoked")),
        ("provider_not_configured", ("provider_not_configured", "no ai account is configured", "no claude ai account", "no provider configured", "provider is not configured", "api key is required", "no api key", "api_key must be set", "missing api key", "missing api_key")),
        ("model_not_configured", ("no_enabled_model", "no models are enabled", "no model is currently enabled", "no models configured", "no model configured")),
        ("quota_exhausted", ("insufficient_quota", "quota_exhausted", "quota exceeded", "quota has been exceeded", "exceeded your current quota", "credit balance is too low", "insufficient credits", "out of credits", "credits exhausted", "usage allowance", "usage limit reached", "usage limit exceeded", "spending limit reached", "billing hard limit", "billing_hard_limit")),
        ("context_length", ("context_length_exceeded", "maximum context length", "context window exceeded", "prompt is too long", "input is too long")),
        ("budget_exhausted", ("budget exceeded", "budget exhausted", "budget has been reached", "excluded by its budget")),
        ("auth", ("authentication_error", "modelauthenticationerror", "invalid_api_key", "invalid api key", "incorrect api key", "invalid bearer token", "unauthorized", "authentication is invalid")),
        ("model_unavailable", ("model_not_found", "model not found", "model does not exist", "model is not available", "pinned model is unavailable", "pinned model is not configured")),
        ("rate_limit", ("rate_limit_error", "modelratelimiterror", "rate limit", "rate_limit_exceeded", "too many requests", "tokens per minute", "requests per minute")),
    )
    for code, markers in patterns:
        if any(marker in text for marker in markers):
            return code
    return None
