"""Explicit PKCE OAuth exchange and locked credential rotation for Codex subscriptions.

The host owns the credential path; no browser, callback listener, CLI store or
keychain is discovered or imported. Legacy store adoption is a separate offline
operation. Every mutation reloads the private file under a process-safe lock.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .config import Settings
from ..credential_storage import write_credentials

# The Codex CLI's public OAuth client. Extracted from the shipped binary
# (`/Applications/ChatGPT.app/Contents/Resources/codex`), which is also where
# the endpoints below come from.
OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
AUTHORIZE_URL = "https://auth.openai.com/oauth/authorize"
TOKEN_URL = "https://auth.openai.com/oauth/token"
REDIRECT_PORT = 1455
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/auth/callback"
LOGIN_SCOPES = "openid profile email offline_access"
REFRESH_SCOPE = "openid profile email"
AUTH_CLAIM = "https://api.openai.com/auth"

_EXPIRY_SKEW_MS = 60_000
ACCOUNT_STORE_VERSION = 2
DEFAULT_ACCOUNT_PREFIX = "account"
DEFAULT_ACCOUNT_PRIORITY = 100



@dataclass(frozen=True)
class AccountToken:
    """A resolved access token plus the account identity that supplied it."""

    id: str
    name: str
    priority: int
    token: str
    account_id: str = ""
    plan: str = ""
    managed: bool = True


class NoAvailableAccountError(RuntimeError):
    """Raised when every configured account is currently unavailable."""

    def __init__(self, message: str, *, retry_after_ms: Optional[int] = None):
        self.retry_after_ms = retry_after_ms
        super().__init__(message)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _slugify_account_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return slug or "account"


def _unique_account_id(name: str, used: Set[str]) -> str:
    base = _slugify_account_id(name)
    if base not in used:
        return base
    idx = 2
    while f"{base}-{idx}" in used:
        idx += 1
    return f"{base}-{idx}"


def _next_account_name(accounts: List[Dict[str, Any]]) -> str:
    used = {str(account.get("name") or "") for account in accounts}
    idx = 1
    while f"{DEFAULT_ACCOUNT_PREFIX}-{idx}" in used:
        idx += 1
    return f"{DEFAULT_ACCOUNT_PREFIX}-{idx}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def jwt_claims(token: str) -> Dict[str, Any]:
    """Decode a JWT payload without verifying it.

    We never trust these claims for authorization — they are the token's own
    self-description, and we only read the bits we need to route the request
    (workspace id, plan) and to know when to refresh (``exp``). The upstream
    remains the only authority on whether a token is actually good.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


def credential_from_tokens(
    access_token: str,
    refresh_tok: str = "",
    id_token: str = "",
    *,
    account_id: str = "",
) -> Dict[str, Any]:
    """Build a store credential from a raw token triple.

    Expiry comes from the access token's own ``exp`` claim rather than
    ``expires_in``: the JWT is authoritative, survives a round-trip through a
    file, and is still right if a caller hands us a token out of band.
    """
    claims = jwt_claims(access_token)
    auth = claims.get(AUTH_CLAIM) or {}
    exp = _safe_int(claims.get("exp"), 0)
    return {
        "access_token": access_token,
        "refresh_token": refresh_tok,
        "id_token": id_token,
        "account_id": account_id or str(auth.get("chatgpt_account_id") or ""),
        "plan": str(auth.get("chatgpt_plan_type") or ""),
        "expires_at_ms": exp * 1000 if exp else 0,
    }


def _post_form(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """POST an OAuth token request as JSON (what the Codex CLI sends)."""
    data = json.dumps(body).encode()
    headers = {
        "content-type": "application/json",
        "accept": "application/json",
        "user-agent": "codex_cli_rs/0.149.0",
    }
    try:
        from curl_cffi import requests as creq

        r = creq.post(url, data=data, headers=headers, impersonate="chrome", timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        return r.json()
    except ImportError:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:  # surface the body, not just the code
            detail = exc.read().decode(errors="replace")[:300]
            raise RuntimeError(f"HTTP {exc.code}: {detail}") from None


def refresh_token(refresh_tok: str, scope: str = REFRESH_SCOPE) -> Dict[str, Any]:
    """Exchange a refresh token for a fresh (rotated) credential."""
    if not refresh_tok:
        raise ValueError("refresh_token is required")
    res = _post_form(
        TOKEN_URL,
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_tok,
            "client_id": OAUTH_CLIENT_ID,
            "scope": scope,
        },
    )
    access = res.get("access_token")
    if not access:
        raise RuntimeError(f"ChatGPT token refresh returned no access_token: {str(res)[:200]}")
    cred = credential_from_tokens(
        access,
        res.get("refresh_token") or refresh_tok,
        res.get("id_token", ""),
    )
    if not cred["expires_at_ms"]:
        cred["expires_at_ms"] = _now_ms() + _safe_int(res.get("expires_in"), 3600) * 1000
    return cred


def _generate_pkce() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


@dataclass(frozen=True)
class PendingLogin:
    """An authorize URL plus the PKCE secret that will redeem its code.

    ``login`` keeps this in a local variable; the HTTP admin surface has to hand
    the URL to somebody else's browser and be called back later, so the pair has
    to survive the round trip. The verifier NEVER leaves the process: only
    ``state`` is published, and it is the key the caller quotes to redeem it.
    """

    state: str
    verifier: str
    authorize_url: str
    created_at_ms: int


def begin_login() -> PendingLogin:
    """Mint a PKCE authorize URL without opening a browser or a listener.

    Same parameters the Codex CLI sends, ``REDIRECT_URI`` included: the
    authorization is bound to that redirect, so a remote operator lands on a
    dead ``localhost:1455`` page and copies ``code`` out of the address bar.
    The page failing to load is expected — the code is in the URL either way.
    """
    verifier, challenge = _generate_pkce()
    state = secrets.token_urlsafe(32)
    params = {
        "response_type": "code",
        "client_id": OAUTH_CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": LOGIN_SCOPES,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "id_token_add_organizations": "true",
        "codex_cli_simplified_flow": "true",
        "state": state,
    }
    return PendingLogin(
        state=state,
        verifier=verifier,
        authorize_url=f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}",
        created_at_ms=_now_ms(),
    )


def exchange_code(code: str, verifier: str) -> Dict[str, Any]:
    """Redeem an authorization code for a store credential."""
    if not code:
        raise ValueError("code is required")
    if not verifier:
        raise ValueError("code_verifier is required")
    res = _post_form(
        TOKEN_URL,
        {
            "grant_type": "authorization_code",
            "client_id": OAUTH_CLIENT_ID,
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": verifier,
        },
    )
    if not res.get("access_token"):
        raise RuntimeError(f"token exchange returned no access_token: {str(res)[:200]}")
    return credential_from_tokens(
        res["access_token"], res.get("refresh_token", ""), res.get("id_token", "")
    )


class TokenManager:
    """Resolves OAuth access tokens across a priority-sorted account pool.

    Thread-safe: the proxy may serve concurrent requests, and refresh must
    happen at most once across them.

    All reads and writes reload under a shared credential-file lock, including
    refresh-token exchanges performed by different Core worker processes.
    """

    def __init__(self, creds_file: Path, *, settings=None):
        self.settings = settings or Settings(creds_file=creds_file)
        self.creds_file = Path(creds_file)
        from ..store_lock import CredentialLock
        self._lock = CredentialLock(self)
        self._store: Optional[Dict[str, Any]] = None

    def _empty_store(self) -> Dict[str, Any]:
        return {"version": ACCOUNT_STORE_VERSION, "accounts": []}

    def _normalize_store(self, data: Dict[str, Any]) -> Dict[str, Any]:
        store = self._empty_store()
        used_ids: Set[str] = set()

        if isinstance(data.get("accounts"), list):
            raw_accounts = [a for a in data["accounts"] if isinstance(a, dict)]
        elif isinstance(data.get("tokens"), dict):
            # A raw ~/.codex/auth.json dropped in as-is.
            tokens = data["tokens"]
            raw_accounts = [
                credential_from_tokens(
                    str(tokens.get("access_token") or ""),
                    str(tokens.get("refresh_token") or ""),
                    str(tokens.get("id_token") or ""),
                    account_id=str(tokens.get("account_id") or ""),
                )
            ]
        elif data.get("access_token") or data.get("refresh_token"):
            raw_accounts = [data]
        else:
            raw_accounts = []

        for idx, raw in enumerate(raw_accounts):
            name = str(raw.get("name") or raw.get("id") or f"{DEFAULT_ACCOUNT_PREFIX}-{idx + 1}").strip()
            if not name:
                name = f"account-{idx + 1}"
            account_id = str(raw.get("id") or "").strip()
            if not account_id or account_id in used_ids:
                account_id = _unique_account_id(name, used_ids)
            used_ids.add(account_id)

            account = dict(raw)
            account.update(
                {
                    "id": account_id,
                    "name": name,
                    "priority": _safe_int(raw.get("priority"), DEFAULT_ACCOUNT_PRIORITY),
                    "access_token": str(raw.get("access_token") or ""),
                    "refresh_token": str(raw.get("refresh_token") or ""),
                    "account_id": str(raw.get("account_id") or ""),
                    "plan": str(raw.get("plan") or ""),
                    "expires_at_ms": _safe_int(raw.get("expires_at_ms"), 0),
                    "limited_until_ms": _safe_int(raw.get("limited_until_ms"), 0),
                    "limited_models": dict(raw.get("limited_models") or {}),
                    "last_rate_limited_at_ms": _safe_int(raw.get("last_rate_limited_at_ms"), 0),
                }
            )
            if account["access_token"] or account["refresh_token"]:
                store["accounts"].append(account)

        store["accounts"] = self._sorted_accounts(store["accounts"])
        return store

    def _load_store_unlocked(self) -> Dict[str, Any]:
        if self._store is not None:
            return self._store
        if not self.creds_file.exists():
            self._store = self._empty_store()
            return self._store
        try:
            data = json.loads(self.creds_file.read_text())
            if not isinstance(data, dict):
                raise ValueError("Credential store must contain an object")
        except (OSError, ValueError) as exc:
            raise ValueError("Subscription credential store cannot be read; existing data was preserved") from exc
        self._store = self._normalize_store(data)
        return self._store

    def _sorted_accounts(self, accounts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            accounts,
            key=lambda a: (
                _safe_int(a.get("priority"), DEFAULT_ACCOUNT_PRIORITY),
                str(a.get("name") or "").lower(),
                str(a.get("id") or ""),
            ),
        )

    def _save_store_unlocked(self, store: Dict[str, Any]) -> None:
        store["version"] = ACCOUNT_STORE_VERSION
        store["accounts"] = self._sorted_accounts(list(store.get("accounts") or []))
        try:
            write_credentials(self.creds_file, store)
        except Exception:
            self._store = None
            raise
        self._store = store

    def _find_account_unlocked(self, identifier: str) -> Optional[Dict[str, Any]]:
        ident = identifier.strip()
        ident_lower = ident.lower()
        for account in self._load_store_unlocked().get("accounts", []):
            if str(account.get("id")) == ident or str(account.get("name")) == ident:
                return account
            if str(account.get("id")).lower() == ident_lower:
                return account
            if str(account.get("name")).lower() == ident_lower:
                return account
        return None

    def _get_account_token_unlocked(self, account: Dict[str, Any], store: Dict[str, Any]) -> str:
        access_token = str(account.get("access_token") or "")
        exp = _safe_int(account.get("expires_at_ms"), 0)
        if access_token and (exp == 0 or _now_ms() < exp - _EXPIRY_SKEW_MS):
            return access_token

        refresh_tok = str(account.get("refresh_token") or "")
        if refresh_tok:
            refreshed = refresh_token(refresh_tok)
            # Keep the stored workspace id if the refreshed JWT omits it.
            if not refreshed.get("account_id"):
                refreshed["account_id"] = str(account.get("account_id") or "")
            account.update(refreshed)
            account["last_refresh_at_ms"] = _now_ms()
            account["last_error"] = ""
            self._save_store_unlocked(store)
            return str(refreshed["access_token"])

        if access_token:
            # Static token with no refresh material — return it; may 401.
            return access_token
        raise RuntimeError(f"account {account.get('name') or account.get('id')} has no access token")

    def save_account(
        self,
        creds: Dict[str, Any],
        *,
        name: Optional[str] = None,
        priority: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            store = self._load_store_unlocked()
            account_name = (name or "").strip()
            if not account_name:
                account_name = _next_account_name(store.get("accounts", []))
            existing = self._find_account_unlocked(account_name)
            used_ids = {str(a.get("id")) for a in store.get("accounts", []) if a is not existing}
            account_id = str(existing.get("id")) if existing else _unique_account_id(account_name, used_ids)
            account_priority = _safe_int(
                priority,
                _safe_int(existing.get("priority"), DEFAULT_ACCOUNT_PRIORITY) if existing else DEFAULT_ACCOUNT_PRIORITY,
            )
            account = dict(creds)
            account.update(
                {
                    "id": account_id,
                    "name": account_name,
                    "priority": account_priority,
                    "access_token": str(creds.get("access_token") or ""),
                    "refresh_token": str(creds.get("refresh_token") or ""),
                    "account_id": str(creds.get("account_id") or ""),
                    "plan": str(creds.get("plan") or ""),
                    "expires_at_ms": _safe_int(creds.get("expires_at_ms"), 0),
                    "limited_until_ms": 0,
                    "limited_models": {},
                    "last_rate_limited_at_ms": 0,
                    "last_rate_limit_reason": "",
                    "last_error": "",
                }
            )
            if existing:
                store["accounts"] = [
                    account if str(a.get("id")) == account_id else a
                    for a in store.get("accounts", [])
                ]
            else:
                store["accounts"].append(account)
            self._save_store_unlocked(store)
            return dict(account)

    def delete_account(self, identifier: str) -> Dict[str, Any]:
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(identifier)
            if not account:
                raise KeyError(f"account not found: {identifier}")
            store["accounts"] = [
                a for a in store.get("accounts", []) if str(a.get("id")) != str(account.get("id"))
            ]
            self._save_store_unlocked(store)
            return dict(account)

    def set_priority(self, identifier: str, priority: int) -> Dict[str, Any]:
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(identifier)
            if not account:
                raise KeyError(f"account not found: {identifier}")
            account["priority"] = int(priority)
            self._save_store_unlocked(store)
            return dict(account)

    def list_accounts(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(a) for a in self._sorted_accounts(self._load_store_unlocked().get("accounts", []))]

    def account_statuses(self) -> List[Dict[str, Any]]:
        now = _now_ms()
        statuses: List[Dict[str, Any]] = []
        with self._lock:
            for account in self._sorted_accounts(self._load_store_unlocked().get("accounts", [])):
                limited_until = _safe_int(account.get("limited_until_ms"), 0)
                status = {
                    "id": account.get("id"),
                    "enabled": account.get("enabled", True),
                    "name": account.get("name"),
                    "priority": _safe_int(account.get("priority"), DEFAULT_ACCOUNT_PRIORITY),
                    "plan": account.get("plan") or "",
                    "managed": True,
                    "limited": limited_until > now,
                    "limited_until_ms": limited_until,
                    "has_refresh_token": bool(account.get("refresh_token")),
                    "expires_at_ms": _safe_int(account.get("expires_at_ms"), 0),
                }
                if status["limited"]:
                    status["limited_for_s"] = max(0, (limited_until - now + 999) // 1000)
                limited_models = {
                    m: max(0, (_safe_int(until, 0) - now + 999) // 1000)
                    for m, until in (account.get("limited_models") or {}).items()
                    if _safe_int(until, 0) > now
                }
                if limited_models:
                    status["limited_models_for_s"] = limited_models
                if account.get("quota"):
                    status["quota"] = account["quota"]
                if account.get("last_error"):
                    status["last_error"] = str(account["last_error"])
                statuses.append(status)
        return statuses

    def mark_rate_limited(
        self,
        account_id: str,
        cooldown_s: float,
        *,
        reason: str = "",
        model: Optional[str] = None,
    ) -> None:
        """Park an account, by default only for the model that hit the wall.

        Codex meters each model family against its OWN window — `codex-spark`
        runs on a 5h window while `sol`/`luna` run on the weekly one. Parking
        the whole account because spark ran out would throw away a plan that is
        still 94% unspent. Only a limit we cannot attribute to a model parks
        everything.
        """
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(account_id)
            if not account:
                return
            # Quanto si sta fuori NON e' il suggerimento di upstream preso alla
            # lettera. Il 24-ago-2026 l'edge ha detto "riprova fra 5,6 giorni"
            # per un modello che al risondaggio rispondeva: obbedire significava
            # tenerlo fuori per il tetto pieno di 3 ore senza mai verificare.
            # Si parte corti e si raddoppia a ogni muro CONSECUTIVO, senza mai
            # superare il suggerimento onesto: un upstream davvero esaurito
            # arriva al tetto in pochi giri, uno tornato su rientra al primo
            # risondaggio. ``mark_success`` azzera il contatore.
            key = model or ""
            strikes = dict(account.get("rate_limit_strikes") or {})
            count = int(strikes.get(key, 0)) + 1
            strikes[key] = count
            account["rate_limit_strikes"] = strikes
            probe = max(1.0, self.settings.account_rate_limit_probe_seconds)
            backoff = min(probe * (2 ** (count - 1)), self.settings.account_rate_limit_max_cooldown)
            effective = min(max(0.0, cooldown_s), backoff) if cooldown_s > 0 else backoff
            until = _now_ms() + int(effective * 1000)
            if model:
                limited_models = dict(account.get("limited_models") or {})
                limited_models[model] = until
                account["limited_models"] = limited_models
            else:
                account["limited_until_ms"] = until
            account["last_rate_limited_at_ms"] = _now_ms()
            account["last_rate_limit_reason"] = reason[:500]
            account["last_rate_limit_wait_s"] = round(effective, 1)
            self._save_store_unlocked(store)

    def record_quota(self, account_id: str, quota: Dict[str, Any]) -> None:
        """Store the last quota snapshot the upstream reported for an account.

        Purely observational — the Codex edge returns its usage percentages on
        every response, and surfacing them on /health is the only way to see a
        limit coming before it lands.
        """
        if not quota:
            return
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(account_id)
            if not account:
                return
            if account.get("quota") == quota:
                return
            account["quota"] = quota
            self._save_store_unlocked(store)

    def _model_limited_until(self, account: Dict[str, Any], model: Optional[str]) -> int:
        if not model:
            return 0
        return _safe_int((account.get("limited_models") or {}).get(model), 0)

    def mark_success(self, account_id: str, *, model: Optional[str] = None) -> None:
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(account_id)
            if not account:
                return
            changed = False
            if account.get("limited_until_ms") and _safe_int(account.get("limited_until_ms"), 0) <= _now_ms():
                account["limited_until_ms"] = 0
                changed = True
            limited_models = dict(account.get("limited_models") or {})
            expired = [m for m, until in limited_models.items() if _safe_int(until, 0) <= _now_ms()]
            if expired:
                for m in expired:
                    limited_models.pop(m, None)
                account["limited_models"] = limited_models
                changed = True
            # Il modello che ha appena risposto riparte da zero: la prossima
            # quarantena torna a essere quella corta. Senza questo, il backoff
            # crescerebbe per sempre e un modello sano finirebbe trattato come
            # uno morto da giorni.
            strikes = dict(account.get("rate_limit_strikes") or {})
            if strikes.pop(model or "", None) is not None:
                account["rate_limit_strikes"] = strikes
                changed = True
            if account.get("last_error"):
                account["last_error"] = ""
                changed = True
            if changed:
                self._save_store_unlocked(store)

    def account_ids(self) -> list:
        """Gli id delle credenziali nel pool, in ordine di priorita'."""
        with self._lock:
            store = self._load_store_unlocked()
            return [str(a.get("id") or "") for a in self._sorted_accounts(store.get("accounts", []))]

    def select_account(
        self, *, exclude_ids: Optional[Set[str]] = None, model: Optional[str] = None
    ) -> AccountToken:
        exclude = set(exclude_ids or set())
        with self._lock:
            store = self._load_store_unlocked()
            now = _now_ms()
            last_error: Optional[Exception] = None

            for account in self._sorted_accounts(store.get("accounts", [])):
                account_id = str(account.get("id") or "")
                if account.get("enabled") is False:
                    continue
                if account_id in exclude:
                    continue
                if _safe_int(account.get("limited_until_ms"), 0) > now:
                    continue
                if self._model_limited_until(account, model) > now:
                    continue
                try:
                    token = self._get_account_token_unlocked(account, store)
                except Exception as exc:
                    last_error = exc
                    account["last_error"] = str(exc)
                    continue
                return AccountToken(
                    id=account_id,
                    name=str(account.get("name") or account_id),
                    priority=_safe_int(account.get("priority"), DEFAULT_ACCOUNT_PRIORITY),
                    token=token,
                    account_id=str(account.get("account_id") or ""),
                    plan=str(account.get("plan") or ""),
                )

            retry_after_ms = None
            future_limits = [
                until
                for a in store.get("accounts", [])
                for until in (
                    _safe_int(a.get("limited_until_ms"), 0),
                    self._model_limited_until(a, model),
                )
                if until > now
            ]
            if future_limits:
                retry_after_ms = max(0, min(future_limits) - now)
            msg = (
                "No available ChatGPT accounts. Run `codex-sub-proxy login --priority 10` "
                "(or `codex-sub-proxy import`) to add one."
            )
            if last_error:
                msg += f" Last account error: {last_error}"
            raise NoAvailableAccountError(msg, retry_after_ms=retry_after_ms)

    def get_token(self) -> str:
        return self.select_account().token

    def get_account_token(self, identifier: str) -> str:
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(identifier)
            if not account:
                raise KeyError(f"account not found: {identifier}")
            return self._get_account_token_unlocked(account, store)
