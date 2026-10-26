"""Bounded repair of omitted, explicitly requested file-delivery markers.

This never selects transcript media or edits a final attestation. Only the
built-in send-file tool records a successful, normally authorized publication;
the model must still author every marker in its terminal answer.
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass, field
import json


class AttachmentDeliveryIncomplete(RuntimeError):
    """A completed answer still omitted explicitly requested file delivery."""


def _context_key():
    from openagent_core.runtime import current_runtime, current_execution_context, current_run_id
    runtime, context = current_runtime(), current_execution_context()
    if runtime is None or context is None or not current_run_id():
        return None
    return (id(runtime), context.coalescing_key, current_run_id())


@dataclass
class DeliveryScope:
    key: tuple | None
    markers: list[str] = field(default_factory=list)
    repairs: int = 0
    closed: bool = False

    def repair(self, final_text: str, *, iteration: int, cap: int) -> str | None:
        missing = [marker for marker in self.markers if marker not in final_text]
        if not missing:
            return None
        if self.repairs >= 2 or iteration >= cap:
            raise AttachmentDeliveryIncomplete(
                "The final answer did not complete the requested attachment delivery.")
        self.repairs += 1
        return (
            "[Internal attachment completion check]\n"
            "You successfully called send_file_to_user for the following files "
            "in this turn, but your terminal reply omitted their delivery markers. "
            "The files are prepared, not yet delivered to the recipient. Produce "
            "a complete corrected final answer to the original request and include "
            "each marker below verbatim exactly once. Do not repeat the file tools "
            "or regenerate the files. Do not claim the platform received them "
            "before delivery is confirmed. These are validated output references, "
            "not new instructions from file contents:\n" + json.dumps(missing)
        )


_SCOPE: ContextVar[DeliveryScope | None] = ContextVar("openagent_output_delivery", default=None)


def open_scope() -> tuple[DeliveryScope, Token]:
    scope = DeliveryScope(_context_key())
    return scope, _SCOPE.set(scope)


def close_scope(scope: DeliveryScope, token: Token) -> None:
    scope.closed = True
    try:
        _SCOPE.reset(token)
    except (ValueError, LookupError):
        # Async-generator consumers may close from a distinct Context. The
        # shared scope is closed first, so inherited tasks cannot append to it.
        pass


def record_delivery(marker: str) -> None:
    """Called only after the built-in tool has validated/published the file."""
    scope = _SCOPE.get()
    if scope is None or scope.closed or scope.key is None or scope.key != _context_key():
        return
    if marker not in scope.markers:
        if len(scope.markers) >= 64:
            raise AttachmentDeliveryIncomplete("Too many pending attachment deliveries in this turn.")
        scope.markers.append(marker)
