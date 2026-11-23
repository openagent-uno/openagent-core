"""Per-request account selection and distributed admission for native providers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import math
from typing import Protocol
from urllib.parse import urlsplit

from .contracts import CredentialResolver, ExecutionContext, ResourceRef


def _identifier(value):
    return isinstance(value, str) and 1 <= len(value) <= 240 and all(c.isprintable() for c in value)


@dataclass(frozen=True, slots=True)
class ProviderAccount:
    account_id: str
    credential_ref: str
    capacity: int

    def __post_init__(self):
        if (
            not all(map(_identifier, (self.account_id, self.credential_ref)))
            or type(self.capacity) is not int
            or not 1 <= self.capacity <= 65536
        ):
            raise ValueError("Invalid provider account or capacity")


@dataclass(frozen=True, slots=True)
class ProviderRoute:
    provider_id: str
    capacity: int
    base_url: str
    accounts: tuple[ProviderAccount, ...]
    wait_seconds: float = 30
    tenant_capacity: int | None = None

    def __post_init__(self):
        if self.tenant_capacity is not None and (
            type(self.tenant_capacity) is not int or not 1 <= self.tenant_capacity <= self.capacity
        ):
            raise ValueError("Invalid tenant provider capacity")
        url = urlsplit(self.base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("Provider routes need an explicit host-owned HTTP endpoint")
        if (
            not _identifier(self.provider_id)
            or type(self.capacity) is not int
            or not 1 <= self.capacity <= 65536
        ):
            raise ValueError("Invalid provider or capacity")
        if (
            not isinstance(self.accounts, tuple)
            or not 1 <= len(self.accounts) <= 8
            or not all(isinstance(a, ProviderAccount) for a in self.accounts)
        ):
            raise ValueError("Expected up to eight explicitly authorized provider accounts")
        if (
            len({a.account_id for a in self.accounts}) != len(self.accounts)
            or not isinstance(self.wait_seconds, (float, int))
            or not math.isfinite(self.wait_seconds)
            or not 0 < self.wait_seconds <= 300
        ):
            raise ValueError("Invalid provider admission route")


class ProviderAdmission(Protocol):
    async def select(self, context: ExecutionContext, model_ref: str) -> ProviderRoute:
        """Choose exact eligible physical accounts and their non-secret credential references."""
        ...


def isolated_sdk_model(model, credential, base_url):
    """Preserve model behavior while detaching process-wide SDK state."""
    from copy import copy

    if any(
        getattr(model, key, None)
        for key in (
            "default_headers",
            "extra_headers",
            "client_params",
            "default_query",
            "extra_query",
            "request_params",
        )
    ):
        raise ValueError("Distributed provider credentials require an unmodified request transport")
    scoped = copy(model)
    scoped.api_key = credential
    scoped.base_url = base_url
    scoped.client = scoped.async_client = scoped.http_client = None
    scoped._openagent_account_routes = scoped._openagent_cred_pool = None
    scoped.max_retries = 0
    return scoped


def openai_account_model(model, credential, base_url):
    from openai import AsyncOpenAI

    scoped = isolated_sdk_model(model, credential, base_url)
    options = {"timeout": model.timeout} if model.timeout is not None else {}
    scoped.async_client = AsyncOpenAI(
        api_key=credential, base_url=base_url, organization="", project="", max_retries=0, **options
    )
    return scoped


def current_admission():
    from .runtime import current_runtime

    runtime = current_runtime()
    if runtime is None:
        return None
    policy = runtime.service(ProviderAdmission)
    if policy is None and getattr(runtime.settings, "require_provider_admission", False):
        raise PermissionError("The required provider admission policy is unavailable")
    return policy


def require_async_admission(model):
    from .runtime import has_current_publication_guard
    if has_current_publication_guard():
        raise PermissionError("Source-bound model requests require asynchronous reauthorization")
    if current_admission() is not None or getattr(model, "_openagent_requires_admission", False):
        raise RuntimeError("Distributed provider admission requires asynchronous inference")


@asynccontextmanager
async def provider_attempt(model):
    from .runtime import check_current_publication
    policy = current_admission()
    if policy is None:
        if getattr(model, "_openagent_requires_admission", False):
            raise PermissionError("Provider admission was revoked")
        await check_current_publication()
        yield model
        return
    from .runtime import current_runtime, current_execution_context, current_run_id

    runtime, context, run_id = current_runtime(), current_execution_context(), current_run_id()
    model_ref = getattr(model, "_openagent_runtime_id", None)
    if context is None or run_id is None or not _identifier(model_ref):
        raise PermissionError("Provider admission requires an authenticated model run")
    reserve = getattr(runtime.services.store, "provider_slot", None)
    credentials = runtime.service(CredentialResolver)
    build = getattr(model, "for_admitted_account", None)
    validate = getattr(runtime.services.store, "check_provider_slot", None)
    if (
        not callable(reserve)
        or not callable(validate)
        or credentials is None
        or not callable(build)
    ):
        raise RuntimeError(
            "This model needs an admission-capable store, credential resolver and scoped transport"
        )
    resource = ResourceRef("model", context.tenant_id, model_ref)
    await runtime.authorize(context, "model.use", resource, audience=context.audience)
    route = await policy.select(context, model_ref)
    if not isinstance(route, ProviderRoute):
        raise TypeError("Invalid provider admission policy")
    async with reserve(context, run_id, route) as account:
        # Grants and per-user account selection may change while waiting.
        await runtime.authorize(context, "model.use", resource, audience=context.audience)
        live = await policy.select(context, model_ref)
        if (
            current_admission() is not policy
            or not isinstance(live, ProviderRoute)
            or live.provider_id != route.provider_id
            or live.base_url != route.base_url
            or live.capacity != route.capacity
            or live.tenant_capacity != route.tenant_capacity
            or account not in live.accounts
        ):
            raise PermissionError("The selected provider account is no longer authorized")
        secret = await credentials.resolve(account.credential_ref, context)
        if not isinstance(secret, str) or not secret.strip() or any(ord(c) < 32 for c in secret):
            raise PermissionError("The selected provider credential is unavailable")
        candidate = build(secret, route.base_url)
        try:
            await runtime.authorize(context, "model.use", resource, audience=context.audience)
            if (
                current_admission() is not policy
                or runtime.service(CredentialResolver) is not credentials
            ):
                raise PermissionError("The provider account resolver was revoked")
            await validate(context, run_id, route.provider_id, account.account_id)
            latest = await policy.select(context, model_ref)
            if not isinstance(latest, ProviderRoute) or latest != live:
                raise PermissionError("Provider account changed while resolving credentials")
            try:
                await check_current_publication()
                yield candidate
            except Exception as error:
                if secret in str(error):
                    from .core.runtime_errors import ModelProviderError

                    raise ModelProviderError(
                        "The provider request failed",
                        status_code=getattr(error, "status_code", 502),
                    ) from None
                raise
        finally:
            from .models.runtime_db_lifecycle import close_runtime_clients

            try:
                await close_runtime_clients(candidate)
            finally:
                candidate.api_key = None
