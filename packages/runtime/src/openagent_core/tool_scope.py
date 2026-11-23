"""Turn-local, narrowing capability source scopes for embedding and delegation."""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import FrozenSet, Iterable, Optional


def normalize_family(name: object) -> str:
    """Coerce a server/family name into the runtime's tool-name prefix form.

    Mirrors ``openagent_core.mcp.pool._safe_prefix``: every char that is not alphanumeric
    or ``_`` becomes ``_`` (so ``computer-control`` → ``computer_control``).
    Idempotent — normalising an already-normalised name returns it unchanged.
    """
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in str(name))


# The per-run tool allowlist. ``None`` (the default) means "no restriction" and
# is what every unrestricted delegation / chat / automation run leaves in place.
# A frozenset of normalised family names restricts the run to those families.
#
# A ContextVar propagates into ``asyncio.create_task`` children (the context is
# COPIED at task-creation time) and into the coroutines a provider fans tool
# calls out through — the same propagation ``child_session._depth_var`` relies
# on — so a value set around one child's run is seen by every generate/tool call
# that run makes while remaining invisible to its siblings and to its parent.
_allowlist_var: contextvars.ContextVar[Optional[FrozenSet[str]]] = contextvars.ContextVar(
    "openagent_child_tool_allowlist", default=None,
)


def current_tool_allowlist() -> Optional[FrozenSet[str]]:
    """The tool-family allowlist for the run executing on this context, or
    ``None`` when the run is unrestricted (the default)."""
    requested = _allowlist_var.get()
    exact = current_capability_sources()
    if exact is None:
        return requested
    ceiling = frozenset(normalize_family(name) for name in exact)
    return ceiling if requested is None else ceiling & requested


def set_tool_allowlist(allow: Optional[Iterable[str]]) -> contextvars.Token:
    """Install an allowlist for the current context; returns a reset token.

    ``None`` clears any restriction (explicitly unrestricted). An iterable is
    normalised and frozen. Pass the returned token to :func:`reset_tool_allowlist`
    in the same context to restore the prior value.
    """
    value: Optional[FrozenSet[str]] = (
        None if allow is None else frozenset(normalize_family(a) for a in allow)
    )
    return _allowlist_var.set(value)


def reset_tool_allowlist(token: contextvars.Token) -> None:
    """Restore the allowlist to what it was before the matching
    :func:`set_tool_allowlist`."""
    _allowlist_var.reset(token)

# Keep the public source identity verbatim. Native provider family names use
# underscores for punctuation, so two distinct catalog sources can normalize
# to the same provider family. The live authorization boundary uses exact IDs.
_source_scope: contextvars.ContextVar[Optional[FrozenSet[str]]] = contextvars.ContextVar(
    "openagent_turn_capability_sources", default=None,
)


def current_capability_sources() -> Optional[FrozenSet[str]]:
    return _source_scope.get()


@contextmanager
def capability_scope(sources: Optional[Iterable[str]]):
    """Narrow the current turn to these exact catalog sources.

    None inherits the enclosing scope. An empty iterable removes all model
    tools. The host's authorizer still controls every underlying operation.
    """
    if sources is None:
        yield
        return
    if isinstance(sources, (str, bytes)):
        raise TypeError("Expected an iterable of exact capability source IDs")
    requested = frozenset(sources)
    if any(not isinstance(name, str) or not name or name.strip() != name for name in requested):
        raise ValueError("Capability source IDs must be nonempty strings")
    parent = current_capability_sources()
    exact = requested if parent is None else requested & parent
    inherited = current_tool_allowlist()
    if inherited is not None:
        exact = frozenset(name for name in exact if normalize_family(name) in inherited)
    source_token = _source_scope.set(exact)
    family_token = set_tool_allowlist(exact)
    try:
        yield
    finally:
        reset_tool_allowlist(family_token)
        _source_scope.reset(source_token)
