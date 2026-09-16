"""Explicit product extensions; importing the engine never selects a product."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


@dataclass(frozen=True, slots=True)
class EngineExtensions:
    # A handler returns None when it does not own this operation. Handlers
    # run inside the same authorized execution scope as the domain module.
    event_handler: Callable[..., Awaitable[Any | None]] | None = None
    scheduled_handler: Callable[..., Awaitable[Mapping[str, Any] | None]] | None = None
    output_retainer: Callable[[str, int], str] | None = None
    content_validator: Callable[..., Awaitable[Mapping[str, Any] | None]] | None = None
    execution_profile_selector: Callable[..., Awaitable[bool]] | None = None
