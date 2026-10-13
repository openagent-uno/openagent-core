"""Host-controlled provenance and selective privacy processing.

No model-facing tool is installed. Hosts authenticate provider requests and bind
scope/generation/subjects before invoking this API. Storage adapters own durable
metadata; sinks own canonical content and must implement idempotent exact-revision
operations. A delivery ACK is never a completion receipt.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol
import re


class PrivacyFence(PermissionError):
    """A revoked origin cannot publish a new derived revision."""


class PrivacyConflict(ValueError):
    """An immutable receipt or revision was reused with different metadata."""


def _nonempty(value: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 1024:
        raise ValueError("Privacy identifiers must be bounded nonempty strings")


@dataclass(frozen=True, slots=True)
class ContentOrigin:
    tenant_id: str
    source_id: str
    group_id: str
    generation: int
    subjects: tuple[str, ...] = ()

    def __post_init__(self):
        for value in (self.tenant_id, self.source_id, self.group_id, *self.subjects):
            _nonempty(value)
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("Origin generation must be host-issued")
        if not isinstance(self.subjects, tuple) or len(self.subjects) > 4096 or len(set(self.subjects)) != len(self.subjects):
            raise ValueError("Origin subjects must be bounded, immutable and unique")


@dataclass(frozen=True, slots=True)
class ContentRevision:
    tenant_id: str
    artifact_id: str
    revision_id: str
    sink_id: str
    content_sha256: str

    def __post_init__(self):
        for value in (self.tenant_id, self.artifact_id, self.revision_id, self.sink_id):
            _nonempty(value)
        if not re.fullmatch(r"[a-f0-9]{64}", self.content_sha256):
            raise ValueError("An exact canonical revision digest is required")


@dataclass(frozen=True, slots=True)
class PrivacyRequest:
    request_id: str
    tenant_id: str
    source_id: str
    group_id: str
    through_generation: int
    operation: Literal["redact", "export"]
    subjects: tuple[str, ...] = ()

    def __post_init__(self):
        _nonempty(self.request_id)
        ContentOrigin(self.tenant_id, self.source_id, self.group_id, self.through_generation, self.subjects)
        if self.operation not in {"redact", "export"}:
            raise ValueError("Unsupported privacy operation")

    def matches(self, origin: ContentOrigin) -> bool:
        return (self.tenant_id == origin.tenant_id and self.source_id == origin.source_id
                and self.group_id == origin.group_id and origin.generation <= self.through_generation
                and (not self.subjects or not origin.subjects or bool(set(self.subjects).intersection(origin.subjects))))


@dataclass(frozen=True, slots=True)
class PrivacyProgress:
    request_id: str
    status: Literal["pending", "processing", "blocked", "complete"]
    completed_revisions: int
    remaining_revisions: int
    reason: str | None = None


class ContentPrivacyStore(Protocol):
    """Trusted host metadata API. Never accepts model-supplied provenance."""
    async def record(self, revision: ContentRevision, *, origins: tuple[ContentOrigin, ...] = (),
                     parents: tuple[tuple[str, str], ...] = ()) -> None: ...
    async def enqueue(self, request: PrivacyRequest) -> PrivacyProgress: ...
    async def progress(self, request_id: str) -> PrivacyProgress: ...
    async def process(self, request_id: str, sinks: tuple[ContentPrivacySink, ...], *,
                      covered_surfaces: frozenset[str]) -> PrivacyProgress: ...


class ContentPrivacySink(Protocol):
    """Canonical-content adapter with idempotent compare-and-swap semantics.

    ``complete`` means the exact revision is erased/exported (including replicas
    and caches owned by the sink), or already absent. A human modification or
    untracked mixed revision returns ``conflict`` without deleting its content.
    ``export`` must deliver only to the host-authorized destination; the ledger
    never archives exported content. Crashes may repeat this operation.
    """
    sink_id: str
    async def apply(self, request: PrivacyRequest, revision: ContentRevision) -> Literal["complete", "conflict"]: ...


class ToolResultPrivacyPublisher(Protocol):
    """Backend-owned atomic tool publication and trusted provenance write.

    Registered explicitly by a host in Runtime's service registry. A capability
    requiring provenance cannot execute without this service. Callback origins
    identify provider-verified scope; the publisher serializes publication with
    durable revocation fences in its canonical storage transaction.
    """
    async def origins_for_execution(self, context, run_id: str) -> tuple[ContentOrigin, ...]: ...
    async def validate_tool_admission(self, context, run_id: str) -> None: ...
    async def publish_tool_result(self, context, run_id: str, call_id: str,
                                  result, origins: tuple[ContentOrigin, ...]) -> None: ...


class EngineContentPrivacyWriter(Protocol):
    """Trusted storage-owner hook in canonical engine serialization transactions.

    The storage adapter supplies its transaction handle, actual persisted source
    row and framework-owned run/context. Models cannot choose those envelopes.
    This hook must raise on a revocation fence; projection fallback must never
    swallow privacy failure or commit the legacy private source independently.
    """
    def record_engine_session(self, transaction, session_id: str, source_row,
                              context, run_id: str | None) -> None: ...
    def validate_compaction(self, session_id: str, folded_run_ids: tuple[str, ...]) -> None: ...
    def record_compaction(self, transaction, session_id: str, runs,
                          folded_run_ids: tuple[str, ...], recap_id: str) -> None: ...
