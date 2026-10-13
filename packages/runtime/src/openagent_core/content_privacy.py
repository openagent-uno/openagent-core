"""Host-controlled provenance and selective privacy processing.

No model-facing tool is installed. Hosts authenticate provider requests and bind
scope/generation/subjects before invoking this API. Storage adapters own durable
metadata; sinks own canonical content and must implement idempotent exact-revision
operations. A delivery ACK is never a completion receipt.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Protocol
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
    nonpersonal: bool = False

    def __post_init__(self):
        for value in (self.tenant_id, self.source_id, self.group_id, *self.subjects):
            _nonempty(value)
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("Origin generation must be host-issued")
        if not isinstance(self.subjects, tuple) or len(self.subjects) > 4096 or len(set(self.subjects)) != len(self.subjects):
            raise ValueError("Origin subjects must be bounded, immutable and unique")
        if type(self.nonpersonal) is not bool or self.nonpersonal and self.subjects:
            raise ValueError('Nonpersonal source scope cannot claim customer subjects')


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
                and (not self.subjects or (not origin.nonpersonal and
                     (not origin.subjects or bool(set(self.subjects).intersection(origin.subjects))))))


@dataclass(frozen=True, slots=True)
class PrivacyProgress:
    request_id: str
    status: Literal["pending", "processing", "blocked", "complete"]
    completed_revisions: int
    remaining_revisions: int
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class JsonStringBoundary:
    """Explicit transition from a JSON string into its encoded JSON value."""
    decode_json: Literal[True] = True

    def __post_init__(self):
        if self.decode_json is not True:
            raise ValueError('JSON string boundary is a fixed typed marker')


def decode_fragment_json(value):
    """Reject ambiguous duplicate object keys at a trusted encoded boundary."""
    import json
    if not isinstance(value,str) or len(value.encode('utf-8'))>8*1024*1024:
        raise PrivacyConflict('Fragment JSON boundary must be a bounded string')
    def object_pairs(pairs):
        result={}
        for key,item in pairs:
            if key in result:
                raise PrivacyConflict('Encoded JSON contains duplicate keys')
            result[key]=item
        return result
    try:
        return json.loads(value,object_pairs_hook=object_pairs,parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError,TypeError,RecursionError):
        raise PrivacyConflict('Fragment boundary is not unambiguous JSON') from None


@dataclass(frozen=True, slots=True)
class ContentFragment:
    """Host-classified exact JSON subtree; empty origins mean no provider source.

    The path is into the normalized provider result, not into arbitrary storage.
    A trusted schema adapter must classify every leaf and cannot overlap paths.
    """
    path: tuple[str | int | JsonStringBoundary, ...]
    origins: tuple[ContentOrigin, ...]

    def __post_init__(self):
        if (not isinstance(self.path,tuple) or len(self.path)>64
                or any(not (type(value) is int and value>=0 or isinstance(value,str) and len(value)<=1024
                            or isinstance(value,JsonStringBoundary)) for value in self.path)
                or not isinstance(self.origins,tuple) or len(self.origins)>4096
                or any(not isinstance(origin,ContentOrigin) for origin in self.origins)):
            raise ValueError('Typed bounded JSON fragments are required')


@dataclass(frozen=True, slots=True)
class ToolResultProvenance:
    """Complete trusted classification for one normalized provider JSON result."""
    fragments: tuple[ContentFragment, ...]

    def __post_init__(self):
        if (not isinstance(self.fragments,tuple) or not self.fragments or len(self.fragments)>4096
                or any(not isinstance(fragment,ContentFragment) for fragment in self.fragments)):
            raise ValueError('A bounded immutable fragment partition is required')
        for index,fragment in enumerate(self.fragments):
            for other in self.fragments[:index]:
                if fragment.path[:len(other.path)]==other.path or other.path[:len(fragment.path)]==fragment.path:
                    raise ValueError('Overlapping provenance fragments are forbidden')

    @property
    def origins(self) -> tuple[ContentOrigin, ...]:
        return tuple(dict.fromkeys(origin for fragment in self.fragments for origin in fragment.origins))

    def validate_result(self,result,tenant_id:str) -> None:
        paths={fragment.path for fragment in self.fragments}
        def walk(value,path=()):
            if path in paths:
                return
            encoded_path=path+(JsonStringBoundary(),)
            if isinstance(value,str) and any(candidate[:len(encoded_path)]==encoded_path for candidate in paths):
                walk(decode_fragment_json(value),encoded_path)
                return
            if isinstance(value,dict) and value:
                for key,child in value.items(): walk(child,path+(key,))
            elif isinstance(value,list) and value:
                for index,child in enumerate(value): walk(child,path+(index,))
            else:
                raise PrivacyConflict('Provider JSON has an unclassified fragment')
        for fragment in self.fragments:
            value=result
            for component in fragment.path:
                if isinstance(component,JsonStringBoundary):
                    value=decode_fragment_json(value)
                    continue
                if ((isinstance(value,dict) and not isinstance(component,str))
                        or (isinstance(value,list) and type(component) is not int)):
                    raise PrivacyConflict('Fragment selector does not match provider JSON')
                try: value=value[component]
                except (KeyError,IndexError,TypeError):
                    raise PrivacyConflict('Fragment selector does not match provider JSON') from None
            if any(origin.tenant_id!=tenant_id for origin in fragment.origins):
                raise PrivacyConflict('Provider fragment crossed tenant authority')
        walk(result)


class ContentPrivacyStore(Protocol):
    """Trusted host metadata API. Never accepts model-supplied provenance."""
    async def record(self, revision: ContentRevision, *, origins: tuple[ContentOrigin, ...] = (),
                     parents: tuple[tuple[str, str], ...] = ()) -> None: ...
    async def publish(self, revision: ContentRevision, *, publish: Callable[[], None],
                      origins: tuple[ContentOrigin, ...] = (),
                      parents: tuple[tuple[str, str], ...] = ()) -> None:
        """Run a synchronous canonical writer while holding the revocation fence.

        External writers must first persist a recovery intent and ``record`` the
        revision before changing content. The callback may not reenter this store,
        await, or assume external writes roll back with SQLite. Its writer/sink
        lock and durable intent must recover a crash after publication. This API
        serializes publication with enqueue; it is not a filesystem transaction.
        """
        ...
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


class ContentSearchPrivacy(Protocol):
    """Host-bound canonical index refresh through a durable committed outbox seq."""
    async def refresh(self, through_seq: int) -> bool: ...


class ToolResultPrivacyPublisher(Protocol):
    """Backend-owned atomic tool publication and trusted provenance write.

    Registered explicitly by a host in Runtime's service registry. A capability
    requiring provenance cannot execute without this service. Callback origins
    identify provider-verified scope; the publisher serializes publication with
    durable revocation fences in its canonical storage transaction.
    """
    async def origins_for_execution(self, context, run_id: str) -> tuple[ContentOrigin, ...]: ...
    async def validate_tool_admission(self, context, run_id: str, *, privacy_fenced: bool = False) -> None: ...
    async def publish_tool_result(self, context, run_id: str, call_id: str,
                                  result, origins: tuple[ContentOrigin, ...] | ToolResultProvenance) -> None: ...


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


class SessionJournalPrivacyWriter(Protocol):
    """Canonical journal write owner, bound before a protected run is admitted."""
    async def append_session_event(self, database_path, session_id: str, event_type: str,
                                   data, context, run_id: str) -> int: ...
