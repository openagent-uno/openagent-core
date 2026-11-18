"""Claude-Code-faithful Anthropic client — no official SDK.

Lifted near-verbatim from ``marf/hermes-agent-claude-sub`` (``agent/cc_anthropic.py``)
with the *complete* fingerprint profile folded in from that fork's
``agent/anthropic_adapter.py`` (``_CC_PROFILE_DEFAULT``): the full 11-beta list,
the JS/Bun stainless headers, and the billing header. The *complete* set is what
gets subscription (OAuth) traffic billed on the included quota — a partial set is
treated as paid extra-usage and 400s ("You're out of extra usage") once that's
exhausted.

Transport ladder:
  L2  httpx       -> defeats header gating
  L3  curl_cffi   -> also defeats TLS/JA3 fingerprinting (BoringSSL/Chromium,
                     closest to Claude Code's Bun stack). Auto-selected when
                     curl_cffi is importable, unless overridden.

This module deliberately has no dependency on the rest of the package so it
stays drop-in. The OAuth token is supplied by a provider callable so each
request can pick up a freshly refreshed token (see ``auth.TokenManager``).
"""

from __future__ import annotations

import json
import threading
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

ANTHROPIC_MESSAGES_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

# Identity string Anthropic validates for Claude-Code OAuth traffic.
CLAUDE_CODE_SYSTEM_PREFIX = "You are Claude Code, Anthropic's official CLI for Claude."

# The Claude Code version we emulate. PINNED (not auto-detected) so the spoofed
# user-agent stays consistent with BILLING_HEADER_TEXT's cc_version below —
# Hermes pins it the same way. Bump BOTH together when tracking a newer CLI;
# Anthropic rejects OAuth requests whose spoofed version is too stale.
CC_VERSION = "2.1.296"

# Beta set for the native-Anthropic SUBSCRIPTION (OAuth) path — matched exactly
# to the fork's verified-working native request: _COMMON_BETAS (interleaved-
# thinking, fine-grained-tool-streaming) + _OAUTH_ONLY_BETAS (claude-code,
# oauth). context-1m is intentionally NEVER sent: Opus 4.8 gets the 1M window
# natively on Max (a >200K request bills to the subscription at standard tier),
# and sending context-1m BREAKS that path; Sonnet 4.6 hits a usage-credit gate
# on long context regardless. 1M comes from the model + plan, not a beta.
BETAS: List[str] = [
    "claude-code-20250219",
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "fine-grained-tool-streaming-2025-05-14",
]

# Billing header, emitted as the FIRST system block — this is what routes usage
# onto the subscription's included quota. Version-tied; bump alongside the CLI.
BILLING_HEADER_TEXT = (
    f"x-anthropic-billing-header: cc_version={CC_VERSION}.b12; "
    "cc_entrypoint=sdk-cli; cch=c0c74;"
)

# JS/Bun runtime fingerprint. Must NOT say python — the official anthropic SDK
# leaks x-stainless-lang: python, which contradicts the claude-cli user-agent
# and gets subscription traffic rejected.
STAINLESS: Dict[str, str] = {
    "lang": "js",
    "packageVersion": "0.94.0",
    "os": "MacOS",
    "arch": "arm64",
    "runtime": "node",
    "runtimeVersion": "v24.3.0",
    "timeout": "600",
}

_cc_version_cache: Optional[str] = None
_session_id_cache: Optional[str] = None


def cc_session_id() -> str:
    """Stable per-process Claude Code session id (sent as x-claude-code-session-id).

    Matches the fork, which caches one uuid4 per process. Real Claude Code uses
    one per conversation; a per-process id is sufficient for the subscription
    fingerprint and what the verified-working client does."""
    global _session_id_cache
    if _session_id_cache is None:
        _session_id_cache = str(uuid.uuid4())
    return _session_id_cache


class AnthropicHTTPError(RuntimeError):
    """Raised on a non-2xx response from the Anthropic Messages endpoint."""

    def __init__(self, status: int, body: Any, headers=None):
        self.status = status
        self.body = body
        self.headers = headers or {}
        super().__init__(f"Anthropic HTTP {status}: {body}")






def detect_cc_version() -> str:
    """The version is part of the adapter contract, never read from another app."""
    return CC_VERSION


def claude_code_headers(
    token: str, *, stream: bool, cc_version: Optional[str] = None,
    extra_betas: Optional[List[str]] = None, session_id: Optional[str] = None,
) -> Dict[str, str]:
    """The exact header set real Claude Code sends. No SDK extras.

    ``extra_betas`` are appended to ``BETAS`` for this request only (used to
    attach the 1M-context beta lazily on large requests — see the server)."""
    ver = cc_version or CC_VERSION
    st = STAINLESS
    betas = BETAS + list(extra_betas or [])
    return {
        "content-type": "application/json",
        "accept": "text/event-stream" if stream else "application/json",
        "anthropic-version": ANTHROPIC_VERSION,
        "authorization": f"Bearer {token}",
        "anthropic-beta": ",".join(betas),
        "user-agent": f"claude-cli/{ver} (external, sdk-cli)",
        "x-app": "cli",
        "anthropic-dangerous-direct-browser-access": "true",
        "x-claude-code-session-id": session_id or cc_session_id(),
        "x-stainless-lang": st["lang"],
        "x-stainless-runtime": st["runtime"],
        "x-stainless-runtime-version": st["runtimeVersion"],
        "x-stainless-os": st["os"],
        "x-stainless-arch": st["arch"],
        "x-stainless-package-version": st["packageVersion"],
        "x-stainless-retry-count": "0",
        "x-stainless-timeout": st["timeout"],
    }


def billing_block() -> Dict[str, str]:
    """The billing header text as a system content block (first system block)."""
    return {"type": "text", "text": BILLING_HEADER_TEXT}


# ── transports ───────────────────────────────────────────────────────────────
def _close_on_cancel(response, cancel_event: Optional[threading.Event]):
    """Close a live response as soon as its downstream caller disconnects.

    Starlette runs this blocking subscription stream in a worker thread.
    Cancelling the awaiting coroutine alone does not stop that thread, so the
    response must be closed to interrupt the blocking read and release the
    proxy's upstream lease.
    """
    finished = threading.Event()
    if cancel_event is None:
        return finished

    def _watch() -> None:
        while not finished.wait(0.05):
            if cancel_event.is_set():
                try:
                    response.close()
                except Exception:
                    pass
                return

    threading.Thread(
        target=_watch,
        name="claude-sub-proxy-stream-cancel",
        daemon=True,
    ).start()
    return finished


def _post_httpx(url, headers, body, *, stream, timeout, cancel_event=None):
    import httpx

    client = httpx.Client(timeout=httpx.Timeout(timeout, connect=10.0))
    if not stream:
        resp = client.post(url, headers=headers, content=body)
        try:
            return resp.status_code, resp.json(), None
        finally:
            client.close()
    req = client.build_request("POST", url, headers=headers, content=body)
    resp = client.send(req, stream=True)
    finished = _close_on_cancel(resp, cancel_event)

    def _it():
        try:
            for line in resp.iter_lines():
                if cancel_event is not None and cancel_event.is_set():
                    break
                yield line
        finally:
            finished.set()
            resp.close()
            client.close()

    return resp.status_code, None, _it()


def _post_curl_cffi(
    url, headers, body, *, stream, timeout, impersonate="chrome", cancel_event=None, chunk_timeout=30.0,
):
    """L3 transport: TLS/JA3 impersonation via curl_cffi (a Chromium target is
    the closest public profile to Claude Code's Bun/BoringSSL stack)."""
    from curl_cffi import requests as creq

    if not stream:
        r = creq.post(url, headers=headers, data=body, impersonate=impersonate, timeout=timeout)
        return r.status_code, r.json(), None
    curl_options = None
    idle_timeout = float(chunk_timeout)
    if idle_timeout > 0:
        # curl_cffi does not expose a cancellable Response until headers arrive.
        # libcurl's native low-speed timer bounds that pre-header/no-byte window
        # without a watchdog thread; once headers exist, _close_on_cancel closes
        # the response immediately on downstream loss.
        from curl_cffi.const import CurlOpt

        curl_options = {
            CurlOpt.LOW_SPEED_LIMIT: 1,
            CurlOpt.LOW_SPEED_TIME: max(1, int(idle_timeout)),
        }
    r = creq.post(
        url, headers=headers, data=body, impersonate=impersonate,
        timeout=timeout, stream=True, curl_options=curl_options,
    )
    finished = _close_on_cancel(r, cancel_event)

    def _it():
        try:
            for raw in r.iter_lines():
                if cancel_event is not None and cancel_event.is_set():
                    break
                yield raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        finally:
            finished.set()
            r.close()

    return r.status_code, None, _it()


def _select_transport(transport: Optional[str]) -> str:
    if transport in ("httpx", "curl_cffi"):
        return transport
    try:
        import curl_cffi  # noqa: F401

        return "curl_cffi"
    except Exception:
        return "httpx"


# ── client ───────────────────────────────────────────────────────────────────
class ClaudeCodeClient:
    """Sends already-shaped Anthropic Messages payloads (see ``translate``).

    ``token_provider`` is a callable returning a fresh token (or a plain string).
    A callable lets each request pick up a token refreshed by ``TokenManager``.
    """

    def __init__(
        self,
        token_provider: Union[str, Callable[[], str]],
        *,
        transport: Optional[str] = None,
        impersonate: str = "chrome",
        cc_version: Optional[str] = None,
        timeout: float = 600.0,
        chunk_timeout: float = 30.0,
    ):
        self._token_provider = token_provider
        self.transport = _select_transport(transport)
        self.impersonate = impersonate
        self.cc_version = cc_version or CC_VERSION
        self.timeout = timeout
        self.chunk_timeout = chunk_timeout
        self.session_id = str(uuid.uuid4())

    def _token(self, override: Optional[str] = None) -> str:
        if override:
            return override
        tp = self._token_provider
        tok = tp() if callable(tp) else tp
        if not tok:
            raise RuntimeError("No Claude OAuth token available.")
        return tok

    def _send(
        self,
        payload: Dict[str, Any],
        *,
        stream: bool,
        extra_betas: Optional[List[str]] = None,
        token: Optional[str] = None,
        cancel_event: Optional[threading.Event] = None,
    ):
        headers = claude_code_headers(
            self._token(token), stream=stream, cc_version=self.cc_version, extra_betas=extra_betas, session_id=self.session_id,
        )
        body = json.dumps(payload).encode("utf-8")
        if self.transport == "curl_cffi":
            return _post_curl_cffi(
                ANTHROPIC_MESSAGES_URL, headers, body,
                stream=stream, timeout=self.timeout, impersonate=self.impersonate,
                cancel_event=cancel_event, chunk_timeout=self.chunk_timeout,
            )
        return _post_httpx(
            ANTHROPIC_MESSAGES_URL, headers, body,
            stream=stream, timeout=self.timeout, cancel_event=cancel_event,
        )

    def complete(
        self,
        payload: Dict[str, Any],
        *,
        extra_betas: Optional[List[str]] = None,
        token: Optional[str] = None,
    ) -> Dict[str, Any]:
        p = dict(payload)
        p.pop("stream", None)
        status, data, _ = self._send(p, stream=False, extra_betas=extra_betas, token=token)
        if status >= 400:
            raise AnthropicHTTPError(status, data)
        return data

    def stream(
        self,
        payload: Dict[str, Any],
        *,
        extra_betas: Optional[List[str]] = None,
        token: Optional[str] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Yield parsed Anthropic SSE events (dicts). Raises on non-2xx."""
        p = dict(payload)
        p["stream"] = True
        status, _, lines = self._send(
            p, stream=True, extra_betas=extra_betas, token=token,
            cancel_event=cancel_event,
        )
        if status >= 400:
            body = "".join(line for line in (lines or []))
            raise AnthropicHTTPError(status, body)
        for line in lines:
            if not line or not line.startswith("data:"):
                continue
            data = line[len("data:"):].strip()
            if not data or data == "[DONE]":
                continue
            try:
                yield json.loads(data)
            except json.JSONDecodeError:
                continue
