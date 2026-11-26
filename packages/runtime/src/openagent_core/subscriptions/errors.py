"""Bounded, sanitized subscription errors shared by embedded hosts."""
from __future__ import annotations
import json
import logging
import time
from datetime import datetime, timezone

ERROR_BODY_LIMIT = 64 << 10
log = logging.getLogger(__name__)

# Public relay codes. Each message carries the wording OpenAgent Core's
# public_errors classifier recognises, so the failed run is published with the
# right error_code (quota_exhausted, auth_expired, rate_limit, ...) and the
# app can say what to do. No upstream body, token or prompt text is included.
USAGE_LIMIT = "subscription_usage_limit"
RATE_LIMITED = "subscription_rate_limited"
RECONNECT = "subscription_reconnect_required"
REFUSED = "subscription_refused"
UNAVAILABLE = "subscription_upstream_unavailable"
NOT_CONNECTED = "subscription_account_not_connected"
MESSAGES = {
    USAGE_LIMIT: "Subscription usage limit reached for this account.",
    RATE_LIMITED: "Subscription rate limit: too many requests right now, retry shortly.",
    RECONNECT: "Subscription authentication has expired or was revoked. Reconnect the account in Providers and models.",
    REFUSED: "The subscription provider refused this request.",
    UNAVAILABLE: "The subscription provider is temporarily unavailable. Retry shortly.",
    NOT_CONNECTED: "No AI account is configured for this subscription. Connect it in Providers and models.",
}
# A 429 whose reset is further away than this is a usage window, not a burst.
USAGE_WINDOW_S = 15*60


class SubscriptionError(Exception):
    def __init__(self, status=502, code=UNAVAILABLE, *, message=None, resets_at_ms=0, retry_after_s=0):
        super().__init__(code)
        self.status, self.code = status, code
        self.message = message or MESSAGES.get(code) or "The subscription request could not complete."
        self.resets_at_ms, self.retry_after_s = int(resets_at_ms or 0), int(retry_after_s or 0)

    def public(self):
        message=self.message
        if self.code==USAGE_LIMIT and self.resets_at_ms:
            when=datetime.fromtimestamp(self.resets_at_ms/1000,timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
            message=f"{message} It resets at {when}."
        error={'message':message,'type':self.code,'code':self.code,'status':self.status}
        if self.resets_at_ms: error['resets_at_ms']=self.resets_at_ms
        return error

    def headers(self):
        headers={}
        # The OpenAI SDK in OpenAgent retries 429/5xx by default. Waiting will
        # not clear a usage window or a revoked login; say so explicitly.
        if self.code in {USAGE_LIMIT,RECONNECT,REFUSED,NOT_CONNECTED}: headers['x-should-retry']='false'
        # Standard Retry-After: OpenAgent's credential pool cools this account
        # down until then and moves the request to the next account.
        wait=self.retry_after_s
        if self.code==USAGE_LIMIT and self.resets_at_ms: wait=max(1,int(self.resets_at_ms/1000-time.time()))
        if wait: headers['retry-after']=str(int(wait))
        return headers


def _number(value):
    try:
        number=float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if 0<number<1e13 else 0.0


def _reset_ms(error, headers, now_s):
    """Latest reset hint the vendor gave, as epoch milliseconds (0 if none)."""
    candidates=[]
    for key in ('resets_at','reset_at'):
        value=_number(error.get(key))
        if value: candidates.append(value*1000 if value<1e11 else value)
    for key in ('resets_in_seconds','reset_in_seconds','retry_after_seconds'):
        value=_number(error.get(key))
        if value: candidates.append((now_s+value)*1000)
    for key in ('anthropic-ratelimit-unified-reset','anthropic-ratelimit-unified-7d-reset','anthropic-ratelimit-unified-5h-reset'):
        value=_number(headers.get(key))
        if value: candidates.append(value*1000)
    for key in ('x-codex-primary-reset-after-seconds','x-codex-secondary-reset-after-seconds'):
        value=_number(headers.get(key))
        if value and _number(headers.get(key.replace('reset-after-seconds','used-percent')))>=100:
            candidates.append((now_s+value)*1000)
    retry=_number(headers.get('retry-after'))
    if retry: candidates.append((now_s+retry)*1000)
    return int(max(candidates)) if candidates else 0


def upstream_failure(vendor, status, body, headers=None, *, now_s=None):
    """Classify a non-200 vendor answer from a bounded, parsed error body.

    Only the error's type/code and reset time are kept; the vendor's message
    is used solely to recognise a usage window and is never forwarded.
    """
    if isinstance(body, dict):
        body = json.dumps(body).encode()
    elif isinstance(body, str):
        body = body.encode()
    headers={str(k).lower():str(v) for k,v in (headers or {}).items()}
    now_s=time.time() if now_s is None else now_s
    data={}
    if isinstance(body,(bytes,bytearray)) and len(body)<=ERROR_BODY_LIMIT:
        try:
            data=json.loads(bytes(body).decode('utf-8','replace'))
        except ValueError:
            data={}
    if not isinstance(data,dict): data={}
    error=data.get('error') if isinstance(data.get('error'),dict) else data
    kind=str(error.get('type') or error.get('code') or (data.get('error') if isinstance(data.get('error'),str) else '') or '')[:64]
    text=str(error.get('message') or data.get('detail') or '').lower()[:500]
    resets=_reset_ms(error,headers,now_s)
    if status==429:
        usage=(kind in {'usage_limit_reached','usage_not_included','insufficient_quota'}
               or headers.get('anthropic-ratelimit-unified-status')=='rejected'
               or 'usage limit' in text or 'limit reached' in text
               or resets>(now_s+USAGE_WINDOW_S)*1000)
        failure=(SubscriptionError(429,USAGE_LIMIT,resets_at_ms=resets) if usage else
                 SubscriptionError(429,RATE_LIMITED,retry_after_s=max(0,int(resets/1000-now_s)) if resets else 0))
    elif status==401 or kind in {'authentication_error','invalid_grant','token_expired','invalid_token'}:
        failure=SubscriptionError(401,RECONNECT)
    elif status==403:
        failure=SubscriptionError(403,REFUSED,message="The subscription provider denied access for this account. Reconnect the account or check its plan.")
    elif status in {400,404,409,413,422}:
        failure=SubscriptionError(status,REFUSED)
    else:
        failure=SubscriptionError(503,UNAVAILABLE)
    log.warning("subscription upstream error vendor=%s status=%s type=%s public=%s resets_at_ms=%s request_id=%s",
                vendor,status,kind or '-',failure.code,failure.resets_at_ms or '-',
                '-')
    return failure


_STREAM_STATUS={'overloaded_error':503,'api_error':503,'server_error':503,'rate_limit_error':429,'rate_limit_exceeded':429,
                'usage_limit_reached':429,'insufficient_quota':429,'authentication_error':401,'permission_error':403,
                'invalid_request_error':400,'context_length_exceeded':400}


def stream_failure(vendor, event):
    """Classify an error delivered inside an SSE stream (no HTTP status)."""
    error=event.get('error')
    if not isinstance(error,dict):
        response=event.get('response') if isinstance(event.get('response'),dict) else {}
        error=response.get('error') if isinstance(response.get('error'),dict) else {}
    kind=str(error.get('type') or error.get('code') or '')
    if event.get('type')=='response.incomplete' or kind not in _STREAM_STATUS:
        return SubscriptionError(code="subscription_inference_failed",message="The subscription provider stopped this answer before it completed.")
    return upstream_failure(vendor,_STREAM_STATUS[kind],json.dumps({'error':error}).encode())



def validated_events(vendor, events, *, require_completed=False):
    """Never manufacture a completed answer after an error or truncated SSE.

    Native Codex length-limited answers remain valid unless a host explicitly
    requires response.completed. Closing this wrapper closes the upstream too.
    """
    terminal = False
    try:
        for item in events:
            event = item[0] if isinstance(item, tuple) else item
            kind = event.get('type')
            if kind in {'error', 'response.failed'} or (kind == 'response.incomplete' and require_completed):
                raise stream_failure(vendor, event)
            if kind == ('message_stop' if vendor == 'claude' else 'response.completed'):
                terminal = True
            if vendor == 'codex' and kind == 'response.incomplete' and not require_completed:
                terminal = True
            yield item
        if not terminal:
            raise SubscriptionError(code='subscription_inference_failed',
                                    message='The subscription provider stopped this answer before it completed.')
    finally:
        close = getattr(events, 'close', None)
        if close is not None:
            close()


def bounded_sse_lines(lines, *, max_line_bytes=4 << 20, max_total_bytes=32 << 20):
    """Bound raw SSE before parsing, including malformed or ignored lines."""
    total = 0
    try:
        for line in lines:
            size = len(line.encode('utf-8')) if isinstance(line, str) else len(line)
            total += size
            if size > max_line_bytes or total > max_total_bytes:
                raise SubscriptionError(code='subscription_inference_failed',
                                        message='The subscription provider exceeded the response limit.')
            yield line
    finally:
        close = getattr(lines, 'close', None)
        if close is not None:
            close()


_PUBLIC_ERROR_STATUSES = {
    USAGE_LIMIT: 429, RATE_LIMITED: 429, RECONNECT: 401,
    # Older relays used this code for several client errors. A generic refusal
    # alone cannot authorize retrying the request on another account.
    REFUSED: 400,
    UNAVAILABLE: 503, NOT_CONNECTED: 409, "subscription_inference_failed": 502,
}


def stream_error_status(error: dict) -> int | None:
    """Read structured error status when an SSE reply already sent its headers."""
    code = error.get("code")
    for value in (error.get("status"), code):
        if type(value) is int and 400 <= value <= 599:
            return value
    return _PUBLIC_ERROR_STATUSES.get(code) if isinstance(code, str) else None


def completion_failure(vendor, payload, status, headers=None):
    """Decode Core's public envelope without trusting an upstream message."""
    error = payload.get('error') if isinstance(payload, dict) else None
    code = error.get('code') if isinstance(error, dict) else None
    if status < 400:
        embedded_status = stream_error_status(error) if isinstance(error, dict) else None
        status = embedded_status or 502
    if isinstance(code, str) and code in _PUBLIC_ERROR_STATUSES:
        retry = _number((headers or {}).get('retry-after'))
        return SubscriptionError(status, code,
                                 resets_at_ms=_number(error.get('resets_at_ms')), retry_after_s=retry)
    return upstream_failure(vendor, status, payload, headers)
