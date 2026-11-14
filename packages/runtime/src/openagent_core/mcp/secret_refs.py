"""Host secret references in MCP source configuration.

A source's HTTP header values and stdio environment values may contain
``${secret:NAME}``. When the source connects, each reference is replaced by the
value of ``NAME`` in the server process environment. Only names that match
``OPENAGENT_MCP_SECRET_[A-Z0-9_]+`` can be referenced: the operator opts a value
in by naming it so, and no other process variable (model keys, database
credentials, ...) can be sent to a URL chosen by whoever configures a source.

The stored configuration keeps the reference, never the value. Resolution
happens only when the transport is built, so the value does not reach the
catalog, status snapshots, logs or model context. Errors name the reference,
never a value.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping

SECRET_REFERENCE = re.compile(r"\$\{secret:([^}]*)\}")
ALLOWED_SECRET_NAME = re.compile(r"^OPENAGENT_MCP_SECRET_[A-Z0-9_]+$")


class SecretReferenceError(ValueError):
    """A ``${secret:NAME}`` reference that cannot be resolved.

    ``reason`` is ``"not_allowed"`` (the name is outside the reserved prefix) or
    ``"missing"`` (the variable is absent or empty in the server environment).
    """

    def __init__(self, name: str, reason: str):
        self.secret_name = name
        self.reason = reason
        if reason == "not_allowed":
            message = (
                f"secret reference ${{secret:{name}}} is not allowed: referenced names "
                "must match OPENAGENT_MCP_SECRET_[A-Z0-9_]+"
            )
        else:
            message = (
                f"secret reference ${{secret:{name}}} is not set in the server "
                "process environment"
            )
        super().__init__(message)


def has_secret_reference(value: str) -> bool:
    return bool(SECRET_REFERENCE.search(value))


def resolve_value(value: str, environ: Mapping[str, str] | None = None) -> str:
    """Return ``value`` with every ``${secret:NAME}`` replaced, or raise."""
    env = os.environ if environ is None else environ

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if not ALLOWED_SECRET_NAME.fullmatch(name):
            raise SecretReferenceError(name, "not_allowed")
        resolved = env.get(name)
        if not resolved:
            raise SecretReferenceError(name, "missing")
        return resolved

    return SECRET_REFERENCE.sub(substitute, value)


def resolve_mapping(
    values: Mapping[str, str] | None, environ: Mapping[str, str] | None = None
) -> dict[str, str] | None:
    """Resolve references in every value of a header or environment mapping."""
    if values is None:
        return None
    return {key: resolve_value(value, environ) if isinstance(value, str) else value
            for key, value in values.items()}


_AUTH_SCHEME = re.compile(r"^(?:bearer|basic|token|bot)?$", re.IGNORECASE)


def is_reference_only(value: object) -> bool:
    """True for values that carry no secret of their own, e.g. ``Bearer ${secret:X}``."""
    if not isinstance(value, str) or not has_secret_reference(value):
        return False
    return bool(_AUTH_SCHEME.fullmatch(SECRET_REFERENCE.sub("", value).strip()))


def redact_mapping(values: Mapping[str, str] | None) -> dict[str, str] | None:
    """Mask stored values for display; reference-only values are kept (they hold no secret)."""
    if values is None:
        return None
    return {key: value if is_reference_only(value) else "***" for key, value in values.items()}
