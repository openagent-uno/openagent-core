"""OpenAI-compatible HTTP front. A caller registers this as a ``local`` model
via ``base_url=http://HOST:PORT/v1`` and talks to it like any OpenAI server.

Endpoints: ``POST /v1/chat/completions`` (stream + non-stream),
``GET /v1/models``, ``GET /health``, ``GET|POST|DELETE /admin/accounts…`` and
``POST /admin/oauth/{start,complete}`` — the login flow a UI drives when the
browser is not on the same host as the proxy.

The admission/pacing/rotation machinery is the same shape as
``claude_sub_proxy.server`` — a subscription is a shared, rate-limited resource
either way, and operating two proxies that behave differently under load is how
you end up debugging the proxy instead of the agent.
"""

from __future__ import annotations

import json
import random
import secrets
import uuid
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set, Tuple

from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.concurrency import run_in_threadpool

from . import translate
from .auth import (
    REDIRECT_URI,
    AccountToken,
    NoAvailableAccountError,
    PendingLogin,
    TokenManager,
    begin_login,
    exchange_code,
)
from .codex_backend import (
    CodexClient,
    CodexHTTPError,
    CodexStreamError,
    quota_from_headers,
    reset_after_seconds,
)
from .usage import UsageLedger



def create_app(settings, *, token_manager=None):
    app = FastAPI(title="codex-sub-proxy", version="0.5.8")

    _token_mgr = token_manager or TokenManager(settings.creds_file)
    _client = CodexClient(
        _token_mgr.get_token,
        transport=settings.transport,
        impersonate=settings.impersonate,
        timeout=settings.request_timeout,
    )
    _usage_ledger = UsageLedger(
        Path(settings.usage_file) if settings.usage_file else settings.creds_file.parent / "usage.json",
        retain_days=settings.usage_retain_days,
    ) if settings.usage_enabled else None
    _reasoning_cache = (
        translate.ReasoningCache(settings.reasoning_cache_size) if settings.reasoning_replay else None
    )
    from ..admission import StorePacer, StoreSemaphore
    _upstream_slots = StoreSemaphore(settings.creds_file, "active", settings.upstream_max_concurrency)
    _queue_slots = StoreSemaphore(settings.creds_file, "queue", settings.upstream_max_concurrency + settings.upstream_max_queue)
    _store_pacer = StorePacer(settings.creds_file)
    _pace_lock = threading.Lock()
    _next_upstream_at = 0.0
    _metrics_lock = threading.Lock()
    _metrics = {
        "accepted": 0,
        "completed": 0,
        "released": 0,
        "active": 0,
        "queued": 0,
        "rejected": 0,
        "rate_limited": 0,
        "upstream_errors": 0,
        "upstream_overloaded": 0,
        "queue_timeouts": 0,
        "account_rate_limited": 0,
        "account_switches": 0,
        "last_error": "",
        "last_error_at": 0.0,
        "max_queue_wait_s": 0.0,
    }

    _RATE_LIMIT_MARKERS = (
        "rate limit",
        "rate_limit",
        "rate-limit",
        "usage limit",
        "quota",
        "too many requests",
        "you've hit your",
        "please try again later",
    )


    class UpstreamOverloaded(Exception):
        pass


    def _set_last_error(message: str) -> None:
        with _metrics_lock:
            _metrics["last_error"] = message[:500]
            _metrics["last_error_at"] = time.time()


    def _metric_add(name: str, delta: int = 1) -> None:
        with _metrics_lock:
            _metrics[name] = int(_metrics.get(name, 0)) + delta


    def _metrics_snapshot() -> dict:
        with _metrics_lock:
            snapshot = dict(_metrics)
        snapshot["last_error_at"] = round(float(snapshot["last_error_at"]), 3)
        snapshot["max_queue_wait_s"] = round(float(snapshot["max_queue_wait_s"]), 3)
        return snapshot


    def _record_queue_wait(wait_s: float) -> None:
        with _metrics_lock:
            _metrics["max_queue_wait_s"] = max(float(_metrics["max_queue_wait_s"]), wait_s)


    @contextmanager
    def _upstream_lease():
        admission_timeout = max(0.0, settings.upstream_admission_timeout)
        queue_timeout = max(1.0, settings.upstream_queue_timeout)
        if not _queue_slots.acquire(timeout=admission_timeout):
            _metric_add("rejected")
            msg = (
                "upstream queue is full "
                f"(active={settings.upstream_max_concurrency}, queue={settings.upstream_max_queue})"
            )
            _set_last_error(msg)
            raise UpstreamOverloaded(msg)

        queue_registered = False
        upstream_acquired = False
        queued_at = time.monotonic()
        try:
            with _metrics_lock:
                _metrics["accepted"] += 1
                _metrics["queued"] += 1
            queue_registered = True

            if not _upstream_slots.acquire(timeout=queue_timeout):
                _metric_add("queue_timeouts")
                msg = f"upstream queue timed out after {queue_timeout:.0f}s"
                _set_last_error(msg)
                raise UpstreamOverloaded(msg)

            upstream_acquired = True
            wait_s = time.monotonic() - queued_at
            with _metrics_lock:
                _metrics["queued"] -= 1
                _metrics["active"] += 1
            queue_registered = False
            _record_queue_wait(wait_s)

            _pace_upstream()
            yield
            _metric_add("completed")
        finally:
            if upstream_acquired:
                with _metrics_lock:
                    _metrics["active"] -= 1
                    _metrics["released"] += 1
                _upstream_slots.release()
            elif queue_registered:
                with _metrics_lock:
                    _metrics["queued"] -= 1
            _queue_slots.release()


    def _check_auth(authorization: Optional[str]) -> None:
        if settings.api_key and not _bearer_matches(authorization, settings.api_key):
            raise HTTPException(status_code=401, detail="invalid api key")


    def _check_admin_auth(authorization: Optional[str]) -> None:
        """Authorize an /admin route.

        ``admin_api_key`` when the operator set one, otherwise the inference key —
        the posture the admin routes already had. An unkeyed proxy (localhost) stays
        unkeyed: there is nothing to check against, and refusing every admin call
        would only move the operator to editing credentials.json by hand.
        """
        expected = settings.admin_api_key or settings.api_key
        if expected and not _bearer_matches(authorization, expected):
            raise HTTPException(status_code=401, detail="invalid admin api key")


    def _bearer_matches(authorization: Optional[str], expected: str) -> bool:
        return bool(authorization) and secrets.compare_digest(authorization, f"Bearer {expected}")


    # A login started over HTTP is redeemed by a second, later call, so the PKCE
    # verifier has to outlive the first request. It stays in memory keyed by the
    # opaque state: the verifier is never published, and a process restart simply
    # invalidates the logins in flight. One process owns the credential file anyway
    # (see the README), so there is nothing for a second replica to share.
    _PENDING_LOGIN_TTL_MS = 10 * 60 * 1000
    _MAX_PENDING_LOGINS = 8
    _pending_logins: Dict[str, PendingLogin] = {}
    _pending_logins_lock = threading.Lock()


    def _remember_login(pending: PendingLogin) -> None:
        cutoff = int(time.time() * 1000) - _PENDING_LOGIN_TTL_MS
        with _pending_logins_lock:
            for state, login in list(_pending_logins.items()):
                if login.created_at_ms < cutoff:
                    del _pending_logins[state]
            while len(_pending_logins) >= _MAX_PENDING_LOGINS:
                oldest = min(_pending_logins, key=lambda s: _pending_logins[s].created_at_ms)
                del _pending_logins[oldest]
            _pending_logins[pending.state] = pending


    def _redeem_login(state: str) -> Optional[PendingLogin]:
        cutoff = int(time.time() * 1000) - _PENDING_LOGIN_TTL_MS
        with _pending_logins_lock:
            pending = _pending_logins.pop(state, None)
        if pending is None or pending.created_at_ms < cutoff:
            return None
        return pending


    def _account_view(account: Dict[str, Any]) -> Dict[str, Any]:
        """The publishable shape of a stored account — never its tokens.

        ``TokenManager.save_account`` hands back the row it wrote, access and
        refresh token included. Returning that over HTTP would hand the caller the
        subscription itself, so the response is the status shape
        ``/admin/accounts`` already publishes.
        """
        account_id = str(account.get("id") or "")
        for status in _token_mgr.account_statuses():
            if str(status.get("id")) == account_id:
                return status
        return {
            "id": account_id,
            "name": account.get("name"),
            "priority": account.get("priority"),
            "plan": account.get("plan") or "",
            "managed": True,
            "limited": False,
            "limited_until_ms": 0,
            "has_refresh_token": bool(account.get("refresh_token")),
            "expires_at_ms": account.get("expires_at_ms") or 0,
        }


    def _pace_upstream() -> None:
        nonlocal _next_upstream_at
        min_interval = max(0.0, settings.upstream_min_interval)
        if min_interval <= 0:
            return
        with _pace_lock:
            now = time.monotonic()
            sleep_s = _store_pacer.reserve(min_interval)
            _next_upstream_at = now + sleep_s + min_interval
        if sleep_s:
            time.sleep(sleep_s)


    def _cool_down_upstream(wait_s: float) -> None:
        nonlocal _next_upstream_at
        wait_s = max(0.0, wait_s)
        if wait_s <= 0:
            return
        with _pace_lock:
            _store_pacer.cool_down(wait_s)
            _next_upstream_at = max(_next_upstream_at, time.monotonic() + wait_s)


    def _contains_rate_limit_marker(value: object) -> bool:
        text = str(value).lower()
        return any(marker in text for marker in _RATE_LIMIT_MARKERS)


    def _is_rate_limit(exc: Exception) -> bool:
        if getattr(exc, "status", None) == 429:
            return True
        body = getattr(exc, "body", "") or getattr(exc, "error", "")
        if isinstance(body, dict):
            for key in ("detail", "message", "type", "code"):
                if _contains_rate_limit_marker(body.get(key, "")):
                    return True
            error = body.get("error")
            if isinstance(error, dict):
                for key in ("message", "type", "code"):
                    if _contains_rate_limit_marker(error.get(key, "")):
                        return True
        return _contains_rate_limit_marker(body)


    def _is_overloaded(exc: Exception) -> bool:
        """Transient upstream capacity — not an account problem.

        Rotating accounts is futile here: every account lands on the same edge, so
        the right move is to back off and retry the SAME one.
        """
        status = getattr(exc, "status", None)
        if isinstance(status, int) and status in (500, 502, 503, 504, 529):
            return True
        body = getattr(exc, "body", "") or getattr(exc, "error", "")
        return "overload" in str(body).lower()


    def _rate_limit_sleep(attempt: int) -> float:
        base = max(0.0, settings.rate_limit_backoff)
        cap = max(base, settings.rate_limit_max_backoff)
        return min(cap, base * (2 ** attempt)) + random.uniform(0.0, 2.0)


    def _account_cooldown(exc: Exception) -> float:
        """How long to park an account, preferring the upstream's own hint."""
        hint = reset_after_seconds(getattr(exc, "headers", {}) or {})
        cooldown = hint if hint else settings.account_rate_limit_cooldown
        return max(0.0, min(cooldown, settings.account_rate_limit_max_cooldown))


    def _mark_account_rate_limited(
        account: AccountToken, exc: Exception, model: Optional[str] = None
    ) -> None:
        cooldown_s = _account_cooldown(exc)
        body = getattr(exc, "body", "") or getattr(exc, "error", "")
        reason = json.dumps(body)[:500] if isinstance(body, dict) else str(body)[:500]
        _token_mgr.mark_rate_limited(account.id, cooldown_s, reason=reason, model=model)
        _metric_add("account_rate_limited")
        scope = f"model {model!r} on account {account.name!r}" if model else f"account {account.name!r}"
        print(f"upstream rate limited {scope}; cooling it for {cooldown_s:.0f}s", flush=True)


    def _no_available_account_payload(exc: NoAvailableAccountError) -> dict:
        message = str(exc)
        if exc.retry_after_ms:
            retry_after_s = max(1, int((exc.retry_after_ms + 999) // 1000))
            message = f"{message} Retry after about {retry_after_s}s."
        return {"error": {"message": message, "type": "no_available_account", "code": 429}}


    def _events(payload: dict, account: AccountToken) -> Iterator[Tuple[dict, Dict[str, str]]]:
        return _client.stream(payload, token=account.token, account_id=account.account_id)


    def _headers_recorder(account: AccountToken):
        def _record(headers: Dict[str, str]) -> None:
            _token_mgr.record_quota(account.id, quota_from_headers(headers))

        return _record


    class _RetryState:
        """Per-request retry bookkeeping.

        Passed explicitly rather than kept thread-local: Starlette pumps a
        streaming generator through a thread POOL, so successive chunks can land on
        different threads and thread-local state would silently vanish mid-stream.
        """

        def __init__(self):
            self.excluded: set[str] = set()
            self.retry: Optional[str] = None
            self.reason: str = ""
            self.last_rate_limit: Optional[Exception] = None
            self.last_overloaded: Optional[Exception] = None
            self.no_available: Optional[NoAvailableAccountError] = None


    # ── credenziali impegnate da una telefonata ──────────────────────────────────
    #
    # L'abbonamento regge UNA chiamata vocale per credenziale: quando ne arriva una
    # seconda sulla stessa, la prima muore dopo una quarantina di secondi senza un
    # errore — solo silenzio, con la linea che resta aperta. Misurato.
    #
    # Ma il limite e' per CREDENZIALE, non per account: due login distinti dello
    # stesso abbonamento reggono due telefonate insieme (verificato, 120 secondi,
    # zero verifiche perse). Quindi qui si tiene il conto di quali credenziali
    # stanno gia' parlando con qualcuno, e a una nuova chiamata se ne da' una
    # libera.
    #
    # La prenotazione ha una scadenza perche' il proxy NON e' nel percorso audio e
    # non puo' accorgersi da solo che una telefonata e' finita: se il chiamante
    # sparisce senza dirlo, la credenziale deve tornare libera da sola.
    _calls_lock = threading.Lock()
    _calls_in_corso: Dict[str, Dict[str, Any]] = {}


    def _scadute(now: float) -> None:
        for cid, info in list(_calls_in_corso.items()):
            if info["scade"] <= now:
                _calls_in_corso.pop(cid, None)


    def _impegna_credenziale(candidate: list, ttl: float) -> Tuple[str, Optional[str], Set[str]]:
        """Sceglie una credenziale libera e la marca occupata, atomicamente.

        ⚠️ La scelta va fatta QUI, non dopo la risposta del backend. Due telefonate
        che arrivano nello stesso secondo impiegano un paio di secondi ciascuna per
        completare l'handshake: se la credenziale si segnasse occupata solo alla
        fine, la seconda troverebbe il registro ancora vuoto e prenderebbe la
        stessa della prima. E' successo: entrambe su `marco-pro`, e una delle due
        e' ammutolita.
        """
        now = time.time()
        with _calls_lock:
            _scadute(now)
            occupate = {info["account"] for info in _calls_in_corso.values() if info.get("account")}
            scelta = next((a for a in candidate if a not in occupate), None)
            cid = str(uuid.uuid4())
            # Se sono tutte occupate si parte lo stesso senza prenotare nulla:
            # meglio una telefonata che rischia di disturbarne un'altra che una
            # telefonata che non parte.
            _calls_in_corso[cid] = {"account": scelta, "scade": now + ttl}
            return cid, scelta, occupate


    def _conferma_credenziale(cid: str, account_id: str) -> None:
        with _calls_lock:
            if cid in _calls_in_corso:
                _calls_in_corso[cid]["account"] = account_id


    def _libera_credenziale(cid: str) -> bool:
        with _calls_lock:
            return _calls_in_corso.pop(cid, None) is not None


    def _account_attempts(state: "_RetryState", model: Optional[str] = None, *, keep_excluded: bool = False):
        """Yield ``(account, attempt)``, rotating accounts and backing off.

        The loop body decides what happens next by calling ``_classify_and_raise``,
        which either re-raises (fatal) or records ``state.retry``.
        """
        attempts = max(0, settings.rate_limit_retries) + 1
        iniziali = set(state.excluded) if keep_excluded else set()
        for attempt in range(attempts):
            state.excluded = set(iniziali)
            while True:
                try:
                    account = _token_mgr.select_account(exclude_ids=state.excluded, model=model)
                except NoAvailableAccountError as exc:
                    state.no_available = exc
                    break
                if state.excluded:
                    _metric_add("account_switches")
                state.retry = None
                yield account, attempt
                if state.retry == "rotate":
                    continue
                if state.retry == "backoff":
                    break
                return

            # ``select_account`` has already inspected the complete credential
            # store.  Retrying here cannot make a quarantined/expired account
            # available and only adds another backoff window to the caller's
            # request timeout.  In particular, when every model-specific quota is
            # parked, sleeping 15s + 31s before returning the same 429 makes
            # OpenAgent's own fallback lane hit its 30s MCP deadline first.
            if state.no_available is not None:
                _raise_exhausted(state)

            if attempt >= attempts - 1:
                _raise_exhausted(state)
            wait_s = _rate_limit_sleep(attempt)
            _cool_down_upstream(wait_s)
            print(
                f"upstream busy ({state.reason or 'rate limited'}); "
                f"sleeping {wait_s:.1f}s (retry {attempt + 1}/{attempts - 1})",
                flush=True,
            )
            time.sleep(wait_s)
        _raise_exhausted(state)


    def _classify_and_raise(
        exc: Exception, account: AccountToken, state: "_RetryState", model: Optional[str] = None
    ) -> None:
        """Decide whether ``exc`` is retryable, and how. Re-raises if it is not."""
        if _is_rate_limit(exc):
            state.last_rate_limit = exc
            state.reason = "rate limited"
            _metric_add("rate_limited")
            _mark_account_rate_limited(account, exc, model)
            state.excluded.add(account.id)
            state.retry = "rotate"
            return
        if _is_overloaded(exc):
            # Global upstream capacity, not this account: back off and retry the
            # SAME credential — rotating would just burn the pool on one outage.
            state.last_overloaded = exc
            state.reason = "overloaded"
            _metric_add("upstream_overloaded")
            state.retry = "backoff"
            return
        _metric_add("upstream_errors")
        _set_last_error(str(exc))
        raise exc


    def _raise_exhausted(state: "_RetryState") -> None:
        wait_s = max(settings.rate_limit_backoff, settings.rate_limit_max_backoff)
        _cool_down_upstream(wait_s)
        if state.last_overloaded is not None:
            _set_last_error(str(state.last_overloaded))
            print(f"upstream overloaded; cooling down {wait_s:.0f}s (retries exhausted)", flush=True)
            raise UpstreamOverloaded(str(state.last_overloaded))
        if state.last_rate_limit is not None:
            _set_last_error(str(state.last_rate_limit))
            print(
                f"all available accounts rate limited; cooling down {wait_s:.0f}s (retries exhausted)",
                flush=True,
            )
            raise state.last_rate_limit
        if state.no_available is not None:
            _set_last_error(str(state.no_available))
            raise state.no_available
        raise NoAvailableAccountError("No available ChatGPT accounts.")



    def _record_usage(account: AccountToken, assembler: translate.TurnAssembler) -> None:
        if _usage_ledger is None:
            return
        _usage_ledger.record(
            account_id=account.id,
            account_name=account.name,
            model=assembler.model,
            usage=assembler.usage,
            quota=quota_from_headers(_client.last_headers),
        )


    def _run_turn(payload: dict, make_assembler) -> dict:
        """Non-streaming path: aggregate the SSE into one Chat Completions body."""
        assembler = make_assembler()
        _run_turn_into(payload, assembler)
        return assembler.completion()


    def _run_turn_into(payload: dict, assembler: translate.TurnAssembler) -> None:
        """Drive one upstream turn into ``assembler``, with rotation and retries."""
        model = str(payload.get("model") or "")
        state = _RetryState()
        for account, _attempt in _account_attempts(state, model):
            try:
                with _upstream_lease():
                    assembler.consume(_events(payload, account), on_headers=_headers_recorder(account))
                _token_mgr.mark_success(account.id, model=model)
                assembler.cache_reasoning(_reasoning_cache)
                _record_usage(account, assembler)
                return
            except (CodexHTTPError, CodexStreamError) as exc:
                _classify_and_raise(exc, account, state, model)
        raise RuntimeError("unreachable")


    def _stream_turn(payload: dict, make_assembler, *, include_usage: bool):
        """Streaming path.

        Failing over is only safe before the first byte reaches the caller — after
        that a retry would duplicate content mid-answer. ``TurnAssembler.chunks``
        reads the upstream's first event before emitting anything, so an HTTP 429
        or an immediate ``response.failed`` still rotates cleanly.
        """
        model = str(payload.get("model") or "")
        state = _RetryState()
        for account, _attempt in _account_attempts(state, model):
            assembler = make_assembler()
            yielded = False
            try:
                with _upstream_lease():
                    for chunk in assembler.chunks(
                        _events(payload, account),
                        include_usage=include_usage,
                        on_headers=_headers_recorder(account),
                    ):
                        yielded = True
                        yield chunk
                _token_mgr.mark_success(account.id, model=model)
                assembler.cache_reasoning(_reasoning_cache)
                _record_usage(account, assembler)
                return
            except (CodexHTTPError, CodexStreamError) as exc:
                if yielded:
                    _metric_add("upstream_errors")
                    _set_last_error(str(exc))
                    raise
                _classify_and_raise(exc, account, state, model)



    # What this subscription can actually do, verified against a Pro plan rather
    # than copied from OpenAI's public API docs — the two differ. Advertised on
    # /v1/models and /v1/capabilities so a caller (OpenAgent included) can route by
    # capability instead of hardcoding model names.
    _TEXT_CAPABILITIES = [
        "chat",
        "streaming",
        "tools",
        "vision",
        "reasoning_effort",
        "image_generation",
        "web_search",
    ]
    _MODEL_CAPABILITIES = {
        "gpt-5.6-sol": _TEXT_CAPABILITIES,
        "gpt-5.6-luna": _TEXT_CAPABILITIES,
        "gpt-5.3-codex-spark": _TEXT_CAPABILITIES,
    }
    # Not text models: they are what /v1/realtime/calls and /v1/images/generations
    # resolve to, and they are listed so a capability lookup finds them.
    _EXTRA_CAPABILITIES = {"realtime": ["realtime_voice"]}

    _UNSUPPORTED = {
        "temperature": "rejected upstream (400 Unsupported parameter) — dropped",
        "top_p": "rejected upstream — dropped",
        "max_tokens": "rejected upstream (max_output_tokens) — dropped",
        "image_quality": "not controllable; the tool reports quality=low whatever is asked",
        "image_size_square": "a square request stays at the backend native 1254x1254; other ratios are honoured",
        "realtime_websocket": "subscription realtime is WebRTC only; ws upgrades 403",
    }


    def _capabilities_for(model: str) -> list:
        base, _ = translate.split_model(model, model_map=settings.model_map, default_effort="medium")
        caps = list(_MODEL_CAPABILITIES.get(base, _TEXT_CAPABILITIES))
        if not settings.image_enabled and "image_generation" in caps:
            caps.remove("image_generation")
        return caps


    @app.get("/health")
    def health():
        """Public liveness only: never read credentials or expose diagnostics.

        The account list, usage ledger and metrics used to be served here without a
        key. They moved to ``/admin/diagnostics`` — identical payload, behind the
        admin bearer — because a probe endpoint is not a place to publish account
        names and plan burn. ``claude_sub_proxy.server`` made the same move.
        """
        return {"status": "ok"}


    @app.get("/admin/diagnostics")
    def admin_diagnostics(authorization: Optional[str] = Header(default=None)):
        _check_admin_auth(authorization)
        return {
            "status": "ok",
            "transport": _client.transport,
            "codex_cli_version": _client.cli_version,
            "settings": {
                "request_timeout": settings.request_timeout,
                "default_reasoning_effort": settings.default_reasoning_effort,
                "realtime_enabled": settings.realtime_enabled,
                "realtime_model": settings.realtime_model,
                "reasoning_replay": settings.reasoning_replay,
                "upstream_max_concurrency": settings.upstream_max_concurrency,
                "upstream_max_queue": settings.upstream_max_queue,
                "upstream_admission_timeout": settings.upstream_admission_timeout,
                "upstream_queue_timeout": settings.upstream_queue_timeout,
                "upstream_min_interval": settings.upstream_min_interval,
                "rate_limit_retries": settings.rate_limit_retries,
                "rate_limit_backoff": settings.rate_limit_backoff,
                "rate_limit_max_backoff": settings.rate_limit_max_backoff,
                "account_rate_limit_cooldown": settings.account_rate_limit_cooldown,
                # Quanto dura la PRIMA quarantena prima del risondaggio: senza
                # questo numero su /health non si distingue un modello appena
                # parcheggiato da uno fuori da ore.
                "account_rate_limit_probe": settings.account_rate_limit_probe_seconds,
                "account_rate_limit_max_cooldown": settings.account_rate_limit_max_cooldown,
            },
            "metrics": _metrics_snapshot(),
            "usage": _usage_ledger.report(days=7, include_days=False) if _usage_ledger is not None else None,
            "accounts": _token_mgr.account_statuses(),
        }


    @app.get("/admin/accounts")
    def admin_list_accounts(authorization: Optional[str] = Header(default=None)):
        _check_admin_auth(authorization)
        return {"accounts": _token_mgr.account_statuses()}


    @app.post("/admin/accounts/{account_id}/reset")
    def admin_reset_account(account_id: str, authorization: Optional[str] = Header(default=None)):
        """Clear an account's rate-limit cooldown, in memory and on disk."""
        _check_admin_auth(authorization)
        with _token_mgr._lock:
            store = _token_mgr._load_store_unlocked()
            account = _token_mgr._find_account_unlocked(account_id)
            if not account:
                raise HTTPException(status_code=404, detail=f"account not found: {account_id}")
            account["limited_until_ms"] = 0
            account["limited_models"] = {}
            account["last_error"] = ""
            _token_mgr._save_store_unlocked(store)
        return {"reset": True, "id": account.get("id"), "name": account.get("name"), "limited": False}


    @app.delete("/admin/accounts/{account_id}")
    def admin_delete_account(account_id: str, authorization: Optional[str] = Header(default=None)):
        """Unlink an account: drop the credential from the store for good.

        The upstream refresh token is not revoked — only this proxy forgets it.
        """
        _check_admin_auth(authorization)
        try:
            account = _token_mgr.delete_account(account_id)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"account not found: {account_id}") from None
        return {"deleted": True, "id": account.get("id"), "name": account.get("name")}


    @app.post("/admin/oauth/start")
    def admin_oauth_start(authorization: Optional[str] = Header(default=None)):
        """Begin a PKCE login that the caller's own browser will complete.

        ``login`` needs a browser and a listener on the proxy's host; a proxy in a
        cluster has neither. This returns the authorize URL plus an opaque state and
        keeps the verifier. ChatGPT redirects the browser to ``REDIRECT_URI``, which
        is nothing on the operator's machine: the page fails to load, and the
        ``code`` sits in the address bar ready to be posted to
        ``/admin/oauth/complete``.
        """
        _check_admin_auth(authorization)
        pending = begin_login()
        _remember_login(pending)
        return {
            "authorize_url": pending.authorize_url,
            "state": pending.state,
            "redirect_uri": REDIRECT_URI,
            "expires_in": _PENDING_LOGIN_TTL_MS // 1000,
        }


    @app.post("/admin/oauth/complete")
    def admin_oauth_complete(
        body: Dict[str, Any] = Body(default_factory=dict),
        authorization: Optional[str] = Header(default=None),
    ):
        """Redeem the code from an ``/admin/oauth/start`` login and store the account."""
        _check_admin_auth(authorization)
        state = str(body.get("state") or "").strip()
        code = str(body.get("code") or "").strip()
        if not state or not code:
            raise HTTPException(status_code=400, detail="state and code are required")
        pending = _redeem_login(state)
        if pending is None:
            # Expired, already redeemed, or never ours. All three mean one thing to
            # the caller: start the login again.
            raise HTTPException(status_code=400, detail="unknown or expired login state")
        try:
            cred = exchange_code(code, pending.verifier)
        except Exception as exc:  # noqa: BLE001 — the upstream detail is the caller's
            raise HTTPException(status_code=502, detail=f"token exchange failed: {exc}") from None
        name = str(body.get("name") or "").strip() or None
        raw_priority = body.get("priority")
        try:
            priority = int(raw_priority) if raw_priority is not None and str(raw_priority).strip() else None
        except (TypeError, ValueError):
            priority = None
        account = _token_mgr.save_account(cred, name=name, priority=priority)
        return {"account": _account_view(account)}


    @app.get("/admin/usage")
    def admin_usage(days: int = 7, authorization: Optional[str] = Header(default=None)):
        """Token totals per model plus the plan percentage burned per account.

        ``plan_percent_burned`` is the number that matters for a subscription: how
        much of the window the traffic actually consumed. Tokens are the input to
        that, not a substitute for it.
        """
        _check_admin_auth(authorization)
        if _usage_ledger is None:
            return JSONResponse(
                status_code=404,
                content={"error": {"message": "usage accounting is disabled", "type": "not_found"}},
            )
        _usage_ledger.flush()
        return _usage_ledger.report(days=max(1, min(int(days or 7), 90)))


    @app.get("/v1/models")
    def list_models():
        now = int(time.time())
        return {
            "object": "list",
            "data": [
                {
                    "id": m,
                    "object": "model",
                    "created": now,
                    "owned_by": "chatgpt-subscription",
                    "capabilities": _capabilities_for(m),
                    "input_modalities": ["text", "image"] if "vision" in _capabilities_for(m) else ["text"],
                }
                for m in settings.models
            ],
        }


    @app.get("/v1/capabilities")
    def list_capabilities():
        """What the plan behind this proxy can do, and what it refuses.

        A capability list is only useful if it is honest about the gaps, so the
        unsupported knobs are named here too: a caller that sends `temperature`
        should be able to discover that it is being dropped, not infer it from a
        result that ignored it.
        """
        models = {m: _capabilities_for(m) for m in settings.models}
        if settings.realtime_enabled:
            models[settings.realtime_model] = list(_EXTRA_CAPABILITIES["realtime"])
        return {
            "provider": "chatgpt-subscription",
            "endpoints": {
                "chat": "/v1/chat/completions",
                "images": "/v1/images/generations" if settings.image_enabled else None,
                "realtime": "/v1/realtime/calls" if settings.realtime_enabled else None,
            },
            "reasoning_efforts": ["none", "minimal", "low", "medium", "high", "xhigh"],
            "models": models,
            "unsupported": _UNSUPPORTED,
        }


    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request, authorization: Optional[str] = Header(default=None)):
        _check_auth(authorization)
        body = await request.json()
        model = body.get("model", "")
        stream = bool(body.get("stream"))
        payload, tool_name_map = translate.openai_to_responses(
            body,
            default_effort=settings.default_reasoning_effort,
            reasoning_summary=settings.reasoning_summary,
            model_map=settings.model_map,
            reasoning_cache=_reasoning_cache,
        )
        def make_assembler() -> translate.TurnAssembler:
            # One per attempt: a rotated retry must not inherit half-accumulated
            # state from the account that just 429'd.
            return translate.TurnAssembler(
                model,
                tool_name_map=tool_name_map,
                emit_reasoning_content=settings.emit_reasoning_content,
            )

        if stream:
            include_usage = bool((body.get("stream_options") or {}).get("include_usage"))

            def _gen():
                try:
                    for chunk in _stream_turn(payload, make_assembler, include_usage=include_usage):
                        yield chunk
                except UpstreamOverloaded as exc:
                    yield _error_chunk(str(exc), "upstream_overloaded", 503)
                except NoAvailableAccountError as exc:
                    yield "data: " + json.dumps(_no_available_account_payload(exc)) + "\n\n"
                    yield "data: [DONE]\n\n"
                except (CodexHTTPError, CodexStreamError) as exc:
                    yield _error_chunk(str(getattr(exc, "body", exc)), "upstream_error", getattr(exc, "status", 502))
                except Exception as exc:
                    yield _error_chunk(str(exc), "proxy_error", 502)

            return StreamingResponse(_gen(), media_type="text/event-stream")

        try:
            resp = await run_in_threadpool(_run_turn, payload, make_assembler)
        except UpstreamOverloaded as exc:
            return JSONResponse(
                status_code=503,
                content={"error": {"message": str(exc), "type": "upstream_overloaded", "code": 503}},
            )
        except NoAvailableAccountError as exc:
            return JSONResponse(status_code=429, content=_no_available_account_payload(exc))
        except (CodexHTTPError, CodexStreamError) as exc:
            status = exc.status if isinstance(getattr(exc, "status", None), int) and exc.status >= 400 else 502
            return JSONResponse(status_code=status, content=_error_payload(exc))
        except Exception as exc:
            return JSONResponse(status_code=502, content=_proxy_error_payload(exc))
        return JSONResponse(resp)


    @app.post("/v1/realtime/calls")
    async def realtime_calls(request: Request, authorization: Optional[str] = Header(default=None)):
        """Broker a WebRTC voice call against the subscription.

        Shaped after OpenAI's own ``/v1/realtime/calls``, and deliberately tolerant
        about how the offer arrives, because clients disagree:

          * ``application/sdp`` body — the offer as text, session knobs as query
            params (``model``, ``voice``, ``instructions``); the answer comes back
            as ``application/sdp``.
          * ``application/json`` body — ``{"sdp": …, "session": {…}}``; the answer
            comes back as ``{"sdp": …}``.

        Only the handshake passes through here. Once the client applies the answer,
        audio flows straight between it and OpenAI — the proxy is not in the media
        path and cannot become the bottleneck.
        """
        _check_auth(authorization)
        if not settings.realtime_enabled:
            return JSONResponse(
                status_code=404,
                content={"error": {"message": "realtime is disabled (CODEX_SUB_PROXY_REALTIME=0)", "type": "not_found"}},
            )

        content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        wants_json = content_type == "application/json"
        if wants_json:
            body = await request.json()
            sdp_offer = str(body.get("sdp") or "")
            session = dict(body.get("session") or {})
        else:
            sdp_offer = (await request.body()).decode("utf-8", "replace")
            session = {}

        params = request.query_params
        session.setdefault("type", "realtime")
        session.setdefault("model", params.get("model") or settings.realtime_model)
        voice = params.get("voice") or settings.realtime_voice
        if voice:
            audio = dict(session.get("audio") or {})
            output = dict(audio.get("output") or {})
            output.setdefault("voice", voice)
            audio["output"] = output
            session["audio"] = audio
        instructions = params.get("instructions") or settings.realtime_instructions
        if instructions:
            session.setdefault("instructions", instructions)

        if not sdp_offer.strip():
            return JSONResponse(
                status_code=400,
                content={"error": {"message": "an SDP offer is required", "type": "invalid_request_error"}},
            )

        # La prenotazione si apre PRIMA di parlare con il backend, cosi' due
        # chiamate che arrivano insieme non scelgono la stessa credenziale.
        call_ref, assegnata, occupate = _impegna_credenziale(
            _token_mgr.account_ids(), settings.realtime_hold_seconds
        )
        usata: Dict[str, Any] = {"account": assegnata}

        def _call() -> str:
            realtime_model = str(session.get("model") or settings.realtime_model)
            state = _RetryState()
            # Una telefonata puo' chiedere una credenziale PRECISA.
            #
            # Serve perche' l'abbonamento regge una sola chiamata vocale per
            # credenziale: quando arriva la seconda, la prima muore dopo una
            # quarantina di secondi senza un errore, solo silenzio. Con piu'
            # credenziali nel pool, mandare la seconda chiamata su un'altra e' il
            # modo per tenerle vive tutte e due — ma va deciso da chi conosce le
            # chiamate in corso, cioe' il bridge, non da qui.
            voluto = (request.headers.get("x-codex-account") or "").strip() or assegnata
            tutte = _token_mgr.account_ids()
            if voluto:
                state.excluded = {a for a in tutte if a != voluto}
            for account, _attempt in _account_attempts(state, realtime_model, keep_excluded=True):
                try:
                    with _upstream_lease():
                        # Un session_id NUOVO per ogni telefonata.
                        #
                        # Di default l'id e' stabile per processo: va benissimo per il
                        # testo, dove le richieste sono indipendenti, ma una chiamata
                        # vocale e' una conversazione che dura. Due chiamate in
                        # contemporanea che si presentano con lo stesso id sembrano la
                        # stessa conversazione, e a monte ne sopravvive una sola:
                        # misurato, la seconda che arriva fa cadere la prima dopo una
                        # quarantina di secondi. Con id distinti sono due telefonate
                        # diverse, che e' quello che sono.
                        answer, headers = _client.realtime_call(
                            sdp_offer, session, token=account.token, account_id=account.account_id,
                            session_id=str(uuid.uuid4()),
                        )
                    _conferma_credenziale(call_ref, account.id)
                    usata["account"] = account.id
                    quota = quota_from_headers(headers)
                    _token_mgr.record_quota(account.id, quota)
                    _token_mgr.mark_success(account.id, model=realtime_model)
                    if _usage_ledger is not None:
                        _usage_ledger.record(
                            account_id=account.id, account_name=account.name,
                            model=str(session.get("model") or settings.realtime_model),
                            usage=None, quota=quota, kind="realtime",
                        )
                    return answer
                except CodexHTTPError as exc:
                    _classify_and_raise(exc, account, state, realtime_model)
            raise RuntimeError("unreachable")

        try:
            answer = await run_in_threadpool(_call)
        except UpstreamOverloaded as exc:
            return JSONResponse(
                status_code=503,
                content={"error": {"message": str(exc), "type": "upstream_overloaded", "code": 503}},
            )
        except NoAvailableAccountError as exc:
            return JSONResponse(status_code=429, content=_no_available_account_payload(exc))
        except CodexHTTPError as exc:
            _libera_credenziale(call_ref)
            status = exc.status if isinstance(exc.status, int) and exc.status >= 400 else 502
            return JSONResponse(status_code=status, content=_error_payload(exc))
        except Exception as exc:
            _libera_credenziale(call_ref)
            return JSONResponse(status_code=502, content=_proxy_error_payload(exc))

        # Chi ha chiamato deve sapere due cose: su quale credenziale sta parlando
        # (utile nei log quando una telefonata va storta) e come dire "ho finito",
        # cosi' la credenziale torna libera prima della scadenza.
        testate = {"x-codex-account": usata["account"] or "", "x-codex-call-ref": call_ref}
        if wants_json:
            return JSONResponse(status_code=201, content={"sdp": answer}, headers=testate)
        return Response(status_code=201, content=answer, media_type="application/sdp", headers=testate)


    @app.delete("/v1/realtime/calls/{call_ref}")
    async def realtime_call_end(call_ref: str, authorization: Optional[str] = Header(default=None)):
        """La telefonata e' finita: la credenziale torna libera.

        Senza questa chiamata la prenotazione scade da sola (vedi
        ``realtime_hold_seconds``), ma nel frattempo una credenziale perfettamente
        libera resterebbe da parte — e con poche credenziali si sente."""
        _check_auth(authorization)
        return JSONResponse({"released": _libera_credenziale(call_ref)})


    @app.post("/v1/images/generations")
    async def images_generations(request: Request, authorization: Optional[str] = Header(default=None)):
        """OpenAI-shaped image generation, served by the subscription.

        There is no images endpoint upstream: generation happens through the
        ``image_generation`` server-side tool on the ordinary responses path, so
        this forces that tool and returns the base64 PNG in OpenAI's envelope.
        ``size`` works by being asked for in the prompt (the tool field is ignored
        upstream): ``1536x1024`` and ``1024x1536`` come back exactly, while square
        stays at the backend's native 1254x1254. ``quality`` is not controllable.
        Each entry reports the size actually produced.
        """
        _check_auth(authorization)
        if not settings.image_enabled:
            return JSONResponse(
                status_code=404,
                content={"error": {"message": "image generation is disabled (CODEX_SUB_PROXY_IMAGES=0)", "type": "not_found"}},
            )
        body = await request.json()
        if not str(body.get("prompt") or "").strip():
            return JSONResponse(
                status_code=400,
                content={"error": {"message": "prompt is required", "type": "invalid_request_error"}},
            )
        payload = translate.image_request_to_responses(
            body, default_model=settings.image_model, model_map=settings.model_map
        )
        requested = max(1, min(int(body.get("n") or 1), 4))

        def make_assembler() -> translate.TurnAssembler:
            return translate.TurnAssembler(
                str(body.get("model") or settings.image_model),
                emit_reasoning_content=False,
            )

        def _generate() -> list:
            images: list = []
            # The tool emits one image per turn, so `n` is n turns. Sequential on
            # purpose: parallel turns would multiply the plan burn with no warning.
            for _ in range(requested):
                assembler = make_assembler()
                _run_turn_into(payload, assembler)
                images.extend(assembler.images)
            return images

        try:
            images = await run_in_threadpool(_generate)
        except UpstreamOverloaded as exc:
            return JSONResponse(
                status_code=503,
                content={"error": {"message": str(exc), "type": "upstream_overloaded", "code": 503}},
            )
        except NoAvailableAccountError as exc:
            return JSONResponse(status_code=429, content=_no_available_account_payload(exc))
        except (CodexHTTPError, CodexStreamError) as exc:
            status = exc.status if isinstance(getattr(exc, "status", None), int) and exc.status >= 400 else 502
            return JSONResponse(status_code=status, content=_error_payload(exc))
        except Exception as exc:
            return JSONResponse(status_code=502, content=_proxy_error_payload(exc))

        if not images:
            return JSONResponse(
                status_code=502,
                content={"error": {"message": "the model returned no image", "type": "upstream_error"}},
            )
        return JSONResponse(
            {
                "created": int(time.time()),
                # `size` is what came back, not what was asked for: the backend
                # keeps its own square default, so echoing the request would lie.
                "data": [
                    {
                        "b64_json": img["b64_json"],
                        "revised_prompt": img.get("revised_prompt"),
                        "size": img.get("size"),
                    }
                    for img in images
                ],
            }
        )


    def _error_chunk(message: str, error_type: str, code: Any) -> str:
        return (
            "data: "
            + json.dumps({"error": {"message": message, "type": error_type, "code": code}})
            + "\n\ndata: [DONE]\n\n"
        )


    def _error_payload(exc: Exception) -> dict:
        body = getattr(exc, "body", None)
        return {
            "error": {
                "message": json.dumps(body) if isinstance(body, dict) else str(body or exc),
                "type": "upstream_error",
                "code": getattr(exc, "status", None),
            }
        }


    def _proxy_error_payload(exc: Exception) -> dict:
        return {"error": {"message": str(exc), "type": "proxy_error", "code": 502}}

    app.state.token_manager = _token_mgr
    app.state.upstream_client = _client
    return app
