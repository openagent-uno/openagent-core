"""Codex-CLI-faithful ChatGPT client — no official SDK.

The subscription path is ``POST https://chatgpt.com/backend-api/codex/responses``:
a Responses-API endpoint that only exists for Codex, only speaks SSE, and only
accepts a ChatGPT OAuth access token together with the workspace id in
``chatgpt-account-id``.

Three things it will not tolerate (verified against a Pro account, 2026-08-22):

  1. ``temperature`` and ``max_output_tokens`` — 400 "Unsupported parameter".
     Reasoning ``effort`` is the only generation knob.
  2. Non-Codex model ids — 400 "not supported when using Codex with a ChatGPT
     account". ``gpt-5.6-codex`` and plain ``gpt-5.6`` are both rejected;
     ``gpt-5.6-sol``, ``gpt-5.6-luna`` and ``gpt-5.3-codex-spark`` work.
  3. ``stream: false`` — there is no non-streaming mode. A non-streamed caller
     is served by aggregating the SSE here.

Unlike the Anthropic subscription path there is no TLS/JA3 gate today, so plain
httpx is enough; curl_cffi stays available as a lever if that ever changes.

This module deliberately has no dependency on the rest of the package so it
stays drop-in.
"""

from __future__ import annotations

import json
import platform
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

CODEX_RESPONSES_URL = "https://chatgpt.com/backend-api/codex/responses"
# Realtime voice, the same path the ChatGPT app uses: POST an SDP offer,
# get an SDP answer, then the audio flows client-to-OpenAI over WebRTC —
# the proxy only brokers the handshake, never the media.
CODEX_REALTIME_CALLS_URL = "https://chatgpt.com/backend-api/codex/realtime/calls"
DEFAULT_REALTIME_MODEL = "gpt-realtime-1.5"

# The Codex CLI version we emulate, mirrored in the user-agent. Bump it when
# tracking a newer CLI; the backend has been observed to reject stale
# originators, and `originator` itself is what routes the request onto the
# subscription rather than the metered API.
CODEX_CLI_VERSION = "0.149.0"
# Ci presentiamo come la CLI di Codex perche' e' quello che il backend si
# aspetta sul percorso responses. Sul realtime pero' la CLI non fa telefonate:
# se il server tratta diversamente un originator che non dovrebbe chiamare,
# questo e' il posto per scoprirlo.
ORIGINATOR = "codex_cli_rs"

# Quota telemetry the edge returns on every response. Percentages are of the
# plan's window, so 100 means the next call 429s.
_QUOTA_HEADERS = {
    "x-codex-plan-type": "plan",
    "x-codex-active-limit": "active_limit",
    "x-codex-primary-used-percent": "primary_used_percent",
    "x-codex-secondary-used-percent": "secondary_used_percent",
    "x-codex-primary-window-minutes": "primary_window_minutes",
    "x-codex-secondary-window-minutes": "secondary_window_minutes",
    "x-codex-primary-reset-after-seconds": "primary_reset_after_s",
    "x-codex-secondary-reset-after-seconds": "secondary_reset_after_s",
    "x-codex-credits-balance": "credits_balance",
}

_session_id_cache: Optional[str] = None


def codex_session_id() -> str:
    """Stable per-process Codex session id (sent as ``session_id``).

    The real CLI mints one per conversation; a per-process id is enough for the
    subscription fingerprint and keeps the header stable across requests.
    """
    global _session_id_cache
    if _session_id_cache is None:
        _session_id_cache = str(uuid.uuid4())
    return _session_id_cache


class CodexHTTPError(RuntimeError):
    """Raised on a non-2xx response from the Codex Responses endpoint."""

    def __init__(self, status: int, body: Any, headers: Optional[Dict[str, str]] = None):
        self.status = status
        self.body = body
        self.headers = headers or {}
        super().__init__(f"Codex HTTP {status}: {body}")


class CodexStreamError(RuntimeError):
    """Raised when the stream itself carries a failure (``response.failed``).

    The transport returned 200 and then told us the turn died — a rate limit
    surfaces this way as often as it does via an HTTP status.
    """

    def __init__(self, error: Any, headers: Optional[Dict[str, str]] = None):
        self.error = error
        self.headers = headers or {}
        self.status = 200
        self.body = error
        super().__init__(f"Codex stream failed: {error}")


def quota_from_headers(headers: Dict[str, str]) -> Dict[str, Any]:
    """Project the ``x-codex-*`` usage headers into a small dict."""
    lowered = {str(k).lower(): v for k, v in (headers or {}).items()}
    quota: Dict[str, Any] = {}
    for header, key in _QUOTA_HEADERS.items():
        value = lowered.get(header)
        if value in (None, ""):
            continue
        try:
            quota[key] = int(value)
        except (TypeError, ValueError):
            quota[key] = value
    return quota


def reset_after_seconds(headers: Dict[str, str]) -> Optional[float]:
    """The upstream's own hint for how long the exhausted window lasts.

    Prefer whichever window is actually spent: a secondary (weekly) limit can
    be the one that bit even while the primary still has room.
    """
    lowered = {str(k).lower(): v for k, v in (headers or {}).items()}

    def _num(name: str) -> Optional[float]:
        try:
            value = float(lowered.get(name, ""))
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    candidates: List[float] = []
    for used, reset in (
        ("x-codex-primary-used-percent", "x-codex-primary-reset-after-seconds"),
        ("x-codex-secondary-used-percent", "x-codex-secondary-reset-after-seconds"),
    ):
        used_pct = _num(used)
        reset_s = _num(reset)
        if reset_s is not None and (used_pct is None or used_pct >= 95):
            candidates.append(reset_s)
    if candidates:
        return min(candidates)
    return None


def codex_headers(token: str, account_id: str, *, session_id: Optional[str] = None) -> Dict[str, str]:
    """The header set the real Codex CLI sends."""
    os_name = platform.system() or "Mac OS"
    return {
        "content-type": "application/json",
        "accept": "text/event-stream",
        "authorization": f"Bearer {token}",
        "chatgpt-account-id": account_id,
        "openai-beta": "responses=experimental",
        "originator": ORIGINATOR,
        "session_id": session_id or codex_session_id(),
        "user-agent": (
            f"{ORIGINATOR}/{CODEX_CLI_VERSION} "
            f"({os_name} {platform.release()}; {platform.machine()})"
        ),
    }


# ── transports ───────────────────────────────────────────────────────────────
def _post_httpx(url, headers, body, *, timeout):
    import httpx

    client = httpx.Client(timeout=httpx.Timeout(timeout, connect=15.0))
    req = client.build_request("POST", url, headers=headers, content=body)
    resp = client.send(req, stream=True)

    def _it():
        try:
            for line in resp.iter_lines():
                yield line
        finally:
            resp.close()
            client.close()

    def _close():
        resp.close()
        client.close()

    return resp.status_code, dict(resp.headers), _it(), _close


def _post_curl_cffi(url, headers, body, *, timeout, impersonate="chrome"):
    from curl_cffi import requests as creq

    r = creq.post(url, headers=headers, data=body, impersonate=impersonate, timeout=timeout, stream=True)

    def _it():
        try:
            for raw in r.iter_lines():
                yield raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        finally:
            r.close()

    return r.status_code, dict(r.headers), _it(), r.close


def _select_transport(transport: Optional[str]) -> str:
    if transport in ("httpx", "curl_cffi"):
        return transport
    return "httpx"


# ── client ───────────────────────────────────────────────────────────────────
class CodexClient:
    """Sends already-shaped Responses payloads (see ``translate``).

    ``token_provider`` is a callable returning a fresh token (or a plain
    string). A callable lets each request pick up a token refreshed by
    ``TokenManager``.
    """

    def __init__(
        self,
        token_provider: Union[str, Callable[[], str]],
        *,
        transport: Optional[str] = None,
        impersonate: str = "chrome",
        timeout: float = 1800.0,
    ):
        self._token_provider = token_provider
        self.transport = _select_transport(transport)
        self.impersonate = impersonate
        self.timeout = timeout
        self.session_id = str(uuid.uuid4())
        self.cli_version = CODEX_CLI_VERSION
        self.last_headers: Dict[str, str] = {}

    def _token(self, override: Optional[str] = None) -> str:
        if override:
            return override
        tp = self._token_provider
        tok = tp() if callable(tp) else tp
        if not tok:
            raise RuntimeError("No ChatGPT OAuth token available.")
        return tok

    def stream(
        self,
        payload: Dict[str, Any],
        *,
        token: Optional[str] = None,
        account_id: str = "",
        session_id: Optional[str] = None,
    ) -> Iterator[Tuple[Dict[str, Any], Dict[str, str]]]:
        """Yield ``(event, response_headers)`` for each parsed SSE event.

        Headers ride along with every event so the caller can record quota
        without a second channel. Raises ``CodexHTTPError`` on a non-2xx and
        ``CodexStreamError`` when the stream reports its own failure.
        """
        body = json.dumps(dict(payload, stream=True)).encode("utf-8")
        headers = codex_headers(self._token(token), account_id, session_id=session_id or self.session_id)
        if self.transport == "curl_cffi":
            status, resp_headers, lines, close = _post_curl_cffi(
                CODEX_RESPONSES_URL, headers, body, timeout=self.timeout, impersonate=self.impersonate
            )
        else:
            status, resp_headers, lines, close = _post_httpx(
                CODEX_RESPONSES_URL, headers, body, timeout=self.timeout
            )
        self.last_headers = resp_headers

        if status >= 400:
            raw = "".join(line for line in (lines or []))
            close()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw[:2000]
            raise CodexHTTPError(status, parsed, resp_headers)

        for line in lines:
            if not line or not line.startswith("data:"):
                continue
            data = line[len("data:"):].strip()
            if not data or data == "[DONE]":
                continue
            try:
                event = json.loads(data)
            except json.JSONDecodeError:
                continue
            if event.get("type") in ("response.failed", "error"):
                close()
                error = event.get("response", {}).get("error") or event.get("error") or event
                raise CodexStreamError(error, resp_headers)
            yield event, resp_headers


    def realtime_call(
        self,
        sdp_offer: str,
        session: Optional[Dict[str, Any]] = None,
        *,
        token: Optional[str] = None,
        account_id: str = "",
        session_id: Optional[str] = None,
    ) -> Tuple[str, Dict[str, str]]:
        """Broker one WebRTC handshake. Returns ``(sdp_answer, headers)``.

        The upstream answers 201 with the raw SDP. Media never touches this
        process: once the answer is relayed the client talks to OpenAI directly.
        """
        import httpx

        payload = {
            "sdp": sdp_offer,
            "session": dict(session or {}) or {"type": "realtime", "model": DEFAULT_REALTIME_MODEL},
        }
        headers = codex_headers(self._token(token), account_id, session_id=session_id or self.session_id)
        headers["content-type"] = "application/json"
        headers["accept"] = "application/sdp, application/json"
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0)) as client:
            resp = client.post(CODEX_REALTIME_CALLS_URL, headers=headers, json=payload)
        resp_headers = dict(resp.headers)
        self.last_headers = resp_headers
        if resp.status_code >= 300:
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:2000]
            raise CodexHTTPError(resp.status_code, body, resp_headers)
        text = resp.text
        if resp.headers.get("content-type", "").startswith("application/json"):
            try:
                text = str(resp.json().get("sdp") or text)
            except Exception:
                pass
        return text, resp_headers
