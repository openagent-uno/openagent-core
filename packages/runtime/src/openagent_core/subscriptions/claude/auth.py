"""Explicit PKCE OAuth exchange and locked credential rotation for Claude subscriptions.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlencode

from .cc_anthropic import detect_cc_version
from ..credential_storage import write_credentials

OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
AUTHORIZE_URL = "https://claude.ai/oauth/authorize"
TOKEN_ENDPOINTS = [
    "https://platform.claude.com/v1/oauth/token",
    "https://console.anthropic.com/v1/oauth/token",
]
REDIRECT_URI = "https://console.anthropic.com/oauth/code/callback"
LOGIN_SCOPES = "org:create_api_key user:profile user:inference"
REFRESH_SCOPE = (
    "user:inference user:profile user:mcp_servers "
    "user:file_upload user:sessions:claude_code"
)
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
    managed: bool = True


class AccountAuthenticationError(RuntimeError):
    """The provider rejected a credential; retrying cannot renew consent."""


class NoAvailableAccountError(RuntimeError):
    """Raised when every configured account is currently unavailable."""

    def __init__(self, message: str, *, retry_after_ms: Optional[int] = None, reason: str = "no_available_account"):
        self.reason = reason
        self.status_code = {"authentication_error": 401, "provider_not_configured": 503, "upstream_unavailable": 503}.get(reason, 429)
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


def _creds_from_claude_code_shape(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    o = data.get("claudeAiOauth")
    if not isinstance(o, dict):
        return None
    access_token = o.get("accessToken", "")
    refresh_token = o.get("refreshToken", "")
    if not access_token and not refresh_token:
        return None
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at_ms": _safe_int(o.get("expiresAt"), 0),
    }




def _post_token(endpoint: str, body: Dict[str, Any]) -> Dict[str, Any]:
    from ..oauth import post_token
    return post_token("claude", endpoint, body)


def refresh_token(refresh_tok: str, scope: str = REFRESH_SCOPE) -> Dict[str, Any]:
    """Exchange a refresh token for a fresh (rotated) credential."""
    if not refresh_tok:
        raise ValueError("refresh_token is required")
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_tok,
        "client_id": OAUTH_CLIENT_ID,
        "scope": scope,
    }
    last_error: Optional[Exception] = None
    for endpoint in TOKEN_ENDPOINTS:
        try:
            res = _post_token(endpoint, body)
        except Exception as exc:  # try the next endpoint only for endpoint/transport failures
            detail = str(exc).lower()
            if "invalid_grant" in detail or "refresh token expired" in detail or "refresh_token expired" in detail:
                raise AccountAuthenticationError("Claude authentication expired or was revoked. Reconnect the Claude account.") from exc
            last_error = exc
            continue
        if res.get("access_token"):
            return {
                "access_token": res["access_token"],
                "refresh_token": res.get("refresh_token", refresh_tok),
                "expires_at_ms": _now_ms() + int(res.get("expires_in", 3600)) * 1000,
                "scope": res.get("scope", scope),
            }
    raise last_error or RuntimeError("Anthropic token refresh failed")


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
    """Mint a PKCE authorize URL without prompting at a terminal.

    Same parameters ``login`` sends. Anthropic renders ``code#state`` on the
    redirect page, so a remote operator copies that pair off the page and posts
    it to ``/admin/oauth/complete``.
    """
    verifier, challenge = _generate_pkce()
    state = secrets.token_urlsafe(32)
    params = {
        "code": "true",
        "client_id": OAUTH_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": LOGIN_SCOPES,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    return PendingLogin(
        state=state,
        verifier=verifier,
        authorize_url=f"{AUTHORIZE_URL}?{urlencode(params)}",
        created_at_ms=_now_ms(),
    )


def exchange_code(code: str, verifier: str, state: str, *, redirect_uri: str = REDIRECT_URI) -> Dict[str, Any]:
    """Redeem an authorization code for a store credential.

    Both token endpoints are tried in order: the platform host is the current
    one and the console host the historical fallback, and which answers has
    changed before.
    """
    if not code:
        raise ValueError("code is required")
    if not verifier:
        raise ValueError("code_verifier is required")
    body = {
        "grant_type": "authorization_code",
        "client_id": OAUTH_CLIENT_ID,
        "code": code,
        "state": state,
        "redirect_uri": redirect_uri,
        "code_verifier": verifier,
    }
    last_error: Optional[Exception] = None
    for endpoint in TOKEN_ENDPOINTS:
        try:
            res = _post_token(endpoint, body)
        except Exception as exc:  # noqa: BLE001 — try the next endpoint
            if "invalid_grant" in str(exc):
                raise
            last_error = exc
            continue
        if res.get("access_token"):
            return {
                "access_token": res["access_token"],
                "refresh_token": res.get("refresh_token", ""),
                "expires_at_ms": _now_ms() + int(res.get("expires_in", 3600)) * 1000,
                "scope": res.get("scope", LOGIN_SCOPES),
                "account_id": str((res.get("account") or {}).get("uuid") or "")[:200],
                "email": str((res.get("account") or {}).get("email_address") or "")[:320],
            }
    raise last_error or RuntimeError("token exchange failed")




class TokenManager:
    """Resolves OAuth access tokens across a priority-sorted account pool.

    Thread-safe: the proxy may serve concurrent requests, and refresh must
    happen at most once across them.
    """

    def __init__(self, creds_file: Path):
        self.creds_file = Path(creds_file)
        from ..store_lock import CredentialLock
        self._lock = CredentialLock(self)
        self._store: Optional[Dict[str, Any]] = None
        self._unmanaged_limited_until_ms: Dict[str, int] = {}

    def _empty_store(self) -> Dict[str, Any]:
        return {"version": ACCOUNT_STORE_VERSION, "accounts": []}

    def _normalize_store(self, data: Dict[str, Any]) -> Dict[str, Any]:
        store = self._empty_store()
        used_ids: Set[str] = set()
        raw_accounts: List[Dict[str, Any]]

        if isinstance(data.get("accounts"), list):
            raw_accounts = [a for a in data["accounts"] if isinstance(a, dict)]
        else:
            migrated = _creds_from_claude_code_shape(data)
            if migrated is None and (data.get("access_token") or data.get("refresh_token")):
                migrated = data
            raw_accounts = [migrated] if migrated else []

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
                    "expires_at_ms": _safe_int(raw.get("expires_at_ms"), 0),
                    "limited_until_ms": _safe_int(raw.get("limited_until_ms"), 0),
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
            # Callers may have mutated the cached store before attempting a save.
            # Reload the last committed file after a failed replacement.
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
            account.update(refreshed)
            account["last_refresh_at_ms"] = _now_ms()
            account["last_error"] = ""
            self._save_store_unlocked(store)
            return str(refreshed["access_token"])

        if access_token:
            # Static token with no refresh material — return it; may 401.
            return access_token
        raise RuntimeError(f"account {account.get('name') or account.get('id')} has no access token")


    def save(self, creds: Dict[str, Any]) -> None:
        """Backward-compatible single-account save."""
        self.save_account(creds, name=f"{DEFAULT_ACCOUNT_PREFIX}-1")

    def save_account(
        self,
        creds: Dict[str, Any],
        *,
        name: Optional[str] = None,
        priority: Optional[int] = None,
        identifier: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            store = self._load_store_unlocked()
            existing = self._find_account_unlocked(identifier) if identifier else None
            if identifier and existing is None:
                raise KeyError("Subscription account is no longer linked")
            account_name = (name or "").strip() or (str(existing.get("name") or "") if existing else "")
            if not account_name:
                account_name = _next_account_name(store.get("accounts", []))
            if existing is None:
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
                    "enabled": existing.get("enabled", True) if existing else True,
                    "access_token": str(creds.get("access_token") or ""),
                    "refresh_token": str(creds.get("refresh_token") or ""),
                    "expires_at_ms": _safe_int(creds.get("expires_at_ms"), 0),
                    "limited_until_ms": 0,
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
            accounts = self._sorted_accounts(self._load_store_unlocked().get("accounts", []))
            for account in accounts:
                limited_until = _safe_int(account.get("limited_until_ms"), 0)
                status = {
                    "id": account.get("id"),
                    "enabled": account.get("enabled", True),
                    "name": account.get("name"),
                    "priority": _safe_int(account.get("priority"), DEFAULT_ACCOUNT_PRIORITY),
                    "managed": True,
                    "limited": limited_until > now,
                    "limited_until_ms": limited_until,
                    "has_refresh_token": bool(account.get("refresh_token")),
                    "expires_at_ms": _safe_int(account.get("expires_at_ms"), 0),
                }
                if account.get("quota"):
                    status["quota"] = dict(account["quota"])
                if status["limited"]:
                    status["limited_for_s"] = max(0, (limited_until - now + 999) // 1000)
                if account.get("last_error"):
                    status["last_error"] = str(account["last_error"])
                statuses.append(status)

        return statuses

    def record_quota(self, account_id: str, quota: Dict[str, Any]) -> None:
        if not quota:
            return
        with self._lock:
            account = self._find_account_unlocked(account_id)
            if account is None:
                return
            account['quota'] = dict(quota)
            self._save_store_unlocked(self._load_store_unlocked())

    def mark_rate_limited(self, account_id: str, cooldown_s: float, *, reason: str = "") -> None:
        until = _now_ms() + int(max(0.0, cooldown_s) * 1000)
        with self._lock:
            if account_id.startswith("env:") or account_id.startswith("keychain:"):
                self._unmanaged_limited_until_ms[account_id] = until
                return
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(account_id)
            if not account:
                return
            account["limited_until_ms"] = until
            account["last_rate_limited_at_ms"] = _now_ms()
            account["last_rate_limit_reason"] = reason[:500]
            self._save_store_unlocked(store)

    def mark_success(self, account_id: str) -> None:
        with self._lock:
            if account_id.startswith("env:") or account_id.startswith("keychain:"):
                self._unmanaged_limited_until_ms.pop(account_id, None)
                return
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(account_id)
            if not account:
                return
            changed = False
            if _safe_int(account.get("limited_until_ms"), 0) <= _now_ms() and account.get("limited_until_ms"):
                account["limited_until_ms"] = 0
                changed = True
            if account.get("last_error"):
                account["last_error"] = ""
                changed = True
            if changed:
                self._save_store_unlocked(store)

    def select_account(self, *, exclude_ids: Optional[Set[str]] = None) -> AccountToken:
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
                    managed=True,
                )

            retry_after_ms = None
            future_limits = [
                _safe_int(a.get("limited_until_ms"), 0)
                for a in store.get("accounts", [])
                if _safe_int(a.get("limited_until_ms"), 0) > now
            ]
            if future_limits:
                retry_after_ms = max(0, min(future_limits) - now)
            msg = "No available Claude OAuth accounts. Run `claude-sub-proxy login --priority 10` to add one."
            if last_error:
                msg += f" Last account error: {last_error}"
            reason = "no_available_account"
            if not future_limits:
                if isinstance(last_error, AccountAuthenticationError):
                    reason = "authentication_error"
                    msg = "Claude authentication expired or is invalid. Reconnect the Claude account in provider settings."
                elif not store.get("accounts"):
                    reason = "provider_not_configured"
                    msg = "No Claude AI account is configured. Connect an account in provider settings."
                elif last_error is not None:
                    reason = "upstream_unavailable"
                    msg = "Claude account renewal is unavailable. Check the provider connection and account settings."
            raise NoAvailableAccountError(msg, retry_after_ms=retry_after_ms, reason=reason)

    def get_token(self) -> str:
        return self.select_account().token

    def get_account_token(self, identifier: str) -> str:
        with self._lock:
            store = self._load_store_unlocked()
            account = self._find_account_unlocked(identifier)
            if not account:
                raise KeyError(f"account not found: {identifier}")
            return self._get_account_token_unlocked(account, store)
