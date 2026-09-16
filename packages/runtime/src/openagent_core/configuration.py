"""Instance configuration for inherited algorithms and explicit subprocesses.

Products interpret their environment before constructing RuntimeSettings. Once
bound to a runtime there is no fallback to another agent's process environment.
The unbound fallback supports standalone module entrypoints during migration.
"""
from __future__ import annotations
from collections.abc import Mapping
from types import MappingProxyType
import os


def runtime_environment() -> Mapping[str, str]:
    from .runtime import current_runtime
    runtime = current_runtime()
    if runtime is not None:
        return MappingProxyType(dict(runtime.settings.environment))
    return MappingProxyType(dict(os.environ))


def getenv(name: str, default=None):
    return runtime_environment().get(name, default)
