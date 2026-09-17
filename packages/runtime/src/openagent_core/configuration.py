"""Instance configuration for inherited algorithms and explicit subprocesses.

Products interpret their environment before constructing RuntimeSettings. Once
bound to a runtime there is no fallback to another agent's process environment.
The unbound fallback supports standalone module entrypoints during migration.
"""
from __future__ import annotations
from collections.abc import Mapping
from types import MappingProxyType
import os


_DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 60_000


def runtime_environment() -> Mapping[str, str]:
    from .runtime import current_runtime
    runtime = current_runtime()
    if runtime is not None:
        return MappingProxyType(dict(runtime.settings.environment))
    return MappingProxyType(dict(os.environ))


def getenv(name: str, default=None):
    return runtime_environment().get(name, default)


def sqlite_busy_timeout_ms() -> int:
    """Shared SQLite writer budget for every core and module connection."""
    raw = runtime_environment().get("OPENAGENT_SQLITE_BUSY_TIMEOUT_MS")
    if raw is None:
        return _DEFAULT_SQLITE_BUSY_TIMEOUT_MS
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return _DEFAULT_SQLITE_BUSY_TIMEOUT_MS
    return value if value > 0 else _DEFAULT_SQLITE_BUSY_TIMEOUT_MS


def sqlite_busy_timeout_s() -> float:
    """The shared writer budget in seconds for ``sqlite3.connect``."""
    return sqlite_busy_timeout_ms() / 1000.0
