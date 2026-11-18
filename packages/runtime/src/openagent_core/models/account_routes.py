"""Explicit same-model account failover at a single provider request boundary.

Never retries a tool loop or a stream that has yielded an upstream frame.
Ordinary connections have no policy and retain exact-account semantics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable
import contextvars

_USAGE = contextvars.ContextVar('openagent_account_usage', default=None)


def open_usage():
    value = {'models': {}, 'selected': {}}
    return value, _USAGE.set(value)


def close_usage(token):
    _USAGE.reset(token)


def effective_model(requested: str, previous: dict | None = None) -> str:
    value = _USAGE.get()
    return ((value or {}).get('selected') or previous or {}).get(requested, requested)


def record_response(requested: str, actual: str, response: Any):
    sink = _USAGE.get()
    if sink is None:
        return
    sink['selected'][requested] = actual
    metrics = getattr(response, 'response_usage', None)
    if metrics is None:
        return
    row = sink['models'].setdefault(actual, [0, 0, 0])
    for index, key in enumerate(('input_tokens', 'output_tokens', 'cache_read_tokens')):
        row[index] += max(0, int(getattr(metrics, key, 0) or 0))


def charge_rows(sink: dict, requested: str, total: tuple[int, int, int]):
    """Attribute reported requests, then the un-attributed run remainder once."""
    rows = {key: list(value) for key, value in sink['models'].items()}
    remainder = [max(0, int(total[i] or 0) - sum(v[i] for v in rows.values())) for i in range(3)]
    if any(remainder):
        previous = rows.setdefault(requested, [0, 0, 0])
        for i, value in enumerate(remainder):
            previous[i] += value
    return [(key, *value) for key, value in rows.items() if any(value[:2])]


@dataclass
class AccountRoutes:
    requested: str
    entries: tuple[Any, ...]
    build: Callable[[str], Any]
    guard: Any = None
    available: Any = None
    models: dict[str, Any] = field(default_factory=dict)

    def candidates(self, primary):
        blocked = self.guard.blocked_runtime_ids(self.entries) if self.guard is not None else set()
        for entry in self.entries:
            if entry.runtime_id in blocked:
                continue
            if self.available is not None and not self.available(entry.runtime_id):
                continue
            if entry.runtime_id == self.requested:
                yield entry.runtime_id, primary
            else:
                if entry.runtime_id not in self.models:
                    self.models[entry.runtime_id] = self.build(entry.runtime_id)
                yield entry.runtime_id, self.models[entry.runtime_id]


    async def acandidates(self, primary):
        from openagent_core.persistence import run_sync
        blocked = self.guard.blocked_runtime_ids(self.entries) if self.guard is not None else set()
        for entry in self.entries:
            if entry.runtime_id in blocked:
                continue
            if self.available is not None and not await run_sync(self.available, entry.runtime_id):
                continue
            if entry.runtime_id == self.requested:
                yield entry.runtime_id, primary
            else:
                if entry.runtime_id not in self.models:
                    self.models[entry.runtime_id] = self.build(entry.runtime_id)
                yield entry.runtime_id, self.models[entry.runtime_id]


def _can_retry(error):
    from .credential_pool import rotates_account
    return rotates_account(error)


def account_call(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        policy = getattr(self, '_openagent_account_routes', None)
        if policy is None:
            return fn(self, *args, **kwargs)
        last = None
        for rid, candidate in policy.candidates(self):
            try:
                response = fn(candidate, *args, **kwargs)
            except Exception as error:
                if not _can_retry(error):
                    raise
                last = error
                continue
            record_response(policy.requested, rid, response)
            return response
        if last is not None:
            raise last
        raise PermissionError('No enabled subscription account is eligible for this route')
    return wrapped


def account_acall(fn):
    @wraps(fn)
    async def wrapped(self, *args, **kwargs):
        policy = getattr(self, '_openagent_account_routes', None)
        if policy is None:
            return await fn(self, *args, **kwargs)
        last = None
        async for rid, candidate in policy.acandidates(self):
            try:
                response = await fn(candidate, *args, **kwargs)
            except Exception as error:
                if not _can_retry(error):
                    raise
                last = error
                continue
            record_response(policy.requested, rid, response)
            return response
        if last is not None:
            raise last
        raise PermissionError('No enabled subscription account is eligible for this route')
    return wrapped


def account_stream(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        policy = getattr(self, '_openagent_account_routes', None)
        if policy is None:
            yield from fn(self, *args, **kwargs)
            return
        last = None
        for rid, candidate in policy.candidates(self):
            emitted = False
            iterator = fn(candidate, *args, **kwargs)
            try:
                for response in iterator:
                    emitted = True
                    record_response(policy.requested, rid, response)
                    yield response
                return
            except Exception as error:
                if emitted or not _can_retry(error):
                    raise
                last = error
            finally:
                iterator.close()
        if last is not None:
            raise last
        raise PermissionError('No enabled subscription account is eligible for this route')
    return wrapped


def account_astream(fn):
    @wraps(fn)
    async def wrapped(self, *args, **kwargs):
        policy = getattr(self, '_openagent_account_routes', None)
        if policy is None:
            iterator = fn(self, *args, **kwargs)
            try:
                async for response in iterator:
                    yield response
            finally:
                await iterator.aclose()
            return
        last = None
        async for rid, candidate in policy.acandidates(self):
            emitted = False
            iterator = fn(candidate, *args, **kwargs)
            try:
                async for response in iterator:
                    emitted = True
                    record_response(policy.requested, rid, response)
                    yield response
                return
            except Exception as error:
                if emitted or not _can_retry(error):
                    raise
                last = error
            finally:
                await iterator.aclose()
        if last is not None:
            raise last
        raise PermissionError('No enabled subscription account is eligible for this route')
    return wrapped
