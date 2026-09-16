"""Transport-independent contracts. Only authenticated host code creates contexts.

Wire adapters must never accept principals, authority or capability leases from
caller JSON. Credentials do not belong in contexts or persisted run snapshots.
"""
from __future__ import annotations
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping, Protocol
import hashlib
import json


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


@dataclass(frozen=True, slots=True)
class PrincipalRef:
    authority: str
    tenant_id: str
    subject_id: str
    kind: str = "user"

    def __post_init__(self) -> None:
        if not all(isinstance(v, str) and v.strip() for v in (self.authority, self.tenant_id, self.subject_id, self.kind)):
            raise ValueError("A principal requires authority, tenant, subject and kind")

    @property
    def key(self) -> str:
        return canonical_json(asdict(self))


@dataclass(frozen=True, slots=True)
class ResourceRef:
    kind: str
    tenant_id: str
    resource_id: str


@dataclass(frozen=True, slots=True)
class CapabilityLease:
    source_id: str
    instance_id: str
    generation: str


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    author: PrincipalRef
    initiator: PrincipalRef
    authority: PrincipalRef
    session_id: str
    agent_id: str
    audience: tuple[PrincipalRef, ...]
    scopes: tuple[str, ...] = ()
    delegation_id: str | None = None
    capabilities: tuple[CapabilityLease, ...] = ()
    ingress_id: str | None = None
    parent_run_id: str | None = None
    deferred: bool = False

    def __post_init__(self) -> None:
        if not self.session_id or not self.agent_id or not self.audience:
            raise ValueError("An execution requires session, agent and explicit result audience")
        if not all(isinstance(v, tuple) for v in (self.audience, self.scopes, self.capabilities)):
            raise TypeError("Execution context collections must be immutable tuples")
        if any(p.tenant_id != self.tenant_id for p in (self.author, self.initiator, *self.audience)):
            raise ValueError("Cross-tenant work requires a separate host-mediated operation")
        if self.deferred and self.capabilities:
            raise ValueError("Deferred work cannot retain temporary capabilities")
        if self.deferred and not self.delegation_id:
            raise ValueError("Deferred work requires an explicit delegation")

    @property
    def tenant_id(self) -> str:
        return self.authority.tenant_id

    @property
    def coalescing_key(self) -> str:
        return hashlib.sha256(canonical_json(asdict(self)).encode()).hexdigest()

    def child(self, *, session_id: str, run_id: str, agent: PrincipalRef, deferred: bool = False) -> ExecutionContext:
        if agent.kind != "agent":
            raise ValueError("Child messages must retain agent authorship")
        return replace(self, author=agent, session_id=session_id, parent_run_id=run_id,
                       capabilities=() if deferred else self.capabilities,
                       ingress_id=None if deferred else self.ingress_id, deferred=deferred)

    def snapshot(self) -> dict[str, Any]:
        return asdict(self)


class AuthorizationDenied(PermissionError):
    pass


class IdempotencyConflict(ValueError):
    pass


class Authorizer(Protocol):
    async def authorize(self, context: ExecutionContext, action: str, resource: ResourceRef,
                        *, audience: tuple[PrincipalRef, ...] = ()) -> bool: ...


async def require_authorized(authorizer: Authorizer, context: ExecutionContext, action: str,
                             resource: ResourceRef, *, audience: tuple[PrincipalRef, ...] = ()) -> None:
    if resource.tenant_id != context.tenant_id or not await authorizer.authorize(context, action, resource, audience=audience):
        raise AuthorizationDenied(f"Not authorized for {action}")


class IdentityDirectory(Protocol):
    async def resolve(self, principal: PrincipalRef) -> Mapping[str, Any] | None: ...


class CredentialResolver(Protocol):
    async def resolve(self, credential_ref: str, context: ExecutionContext) -> Any: ...


class ModelCatalog(Protocol):
    async def list_models(self, context: ExecutionContext) -> tuple[Mapping[str, Any], ...]: ...
    async def resolve(self, model_ref: str, context: ExecutionContext) -> Any: ...


class DelegationService(Protocol):
    async def validate(self, context: ExecutionContext) -> bool: ...


@dataclass(frozen=True, slots=True)
class RunRequest:
    run_id: str
    session_id: str
    idempotency_key: str
    input: str
    deadline_seconds: float | None = None
    attachments: tuple[Mapping[str, Any], ...] = ()
    model_ref: str | None = None
    steer_run_id: str | None = None

    def __post_init__(self) -> None:
        if not all(isinstance(v, str) and v.strip() for v in (self.run_id, self.session_id, self.idempotency_key)):
            raise ValueError("Stable run, session and idempotency IDs are required")
        if not isinstance(self.input, str):
            raise TypeError("Run input must be text")
        if self.steer_run_id is not None and (not isinstance(self.steer_run_id, str) or not self.steer_run_id or self.steer_run_id == self.run_id):
            raise ValueError("Steering requires a different, exact target run ID")
        if not isinstance(self.attachments, tuple):
            raise TypeError("Attachments must be an immutable tuple of verified references")
        if self.deadline_seconds is not None and self.deadline_seconds <= 0:
            raise ValueError("The run deadline must be positive")

    def fingerprint(self, context: ExecutionContext) -> str:
        return hashlib.sha256(canonical_json({"request": asdict(self), "context": context.snapshot()}).encode()).hexdigest()


TERMINAL_STATUSES = frozenset({"success", "failed", "cancelled", "rejected", "interrupted", "skipped", "timed_out"})


@dataclass(frozen=True, slots=True)
class AcceptedRunRequest:
    """Immutable admission evidence, without reusable credentials or leases."""
    request: RunRequest
    author: PrincipalRef
    initiator: PrincipalRef
    authority: PrincipalRef
    audience: tuple[PrincipalRef, ...] = ()
    scopes: tuple[str, ...] = ()
    delegation_id: str | None = None
    ingress_id: str | None = None
    parent_run_id: str | None = None
    deferred: bool = False


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    session_id: str
    tenant_id: str
    status: str
    request_digest: str
    output: Any = None
    cancel_requested: bool = False

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


@dataclass(frozen=True, slots=True)
class RunEvent:
    cursor: int
    run_id: str
    kind: str
    payload: Mapping[str, Any]


class RuntimeStore(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def accept(self, request: RunRequest, context: ExecutionContext) -> tuple[RunRecord, bool]: ...
    async def get(self, run_id: str) -> RunRecord | None: ...
    async def accepted_request(self, run_id: str) -> AcceptedRunRequest: ...
    async def transition(self, run_id: str, status: str, *, output: Any = None) -> RunRecord: ...
    async def request_cancel(self, run_id: str) -> RunRecord: ...
    async def reserve_cancel(self, run_id: str, context: ExecutionContext) -> RunRecord: ...
    async def append_event(self, run_id: str, kind: str, payload: Mapping[str, Any]) -> RunEvent: ...
    async def events(self, run_id: str, after: int = 0) -> tuple[RunEvent, ...]: ...
    async def recover(self) -> None: ...
    async def children(self, run_id: str) -> tuple[RunRecord, ...]: ...
    async def begin_tool(self, run_id: str, call_id: str, binding: Mapping[str, Any], arguments: Mapping[str, Any]) -> None: ...
    async def finish_tool(self, run_id: str, call_id: str, *, result: Any = None, error: Mapping[str, Any] | None = None) -> None: ...


class AgentExecutor(Protocol):
    async def execute(self, request: RunRequest, context: ExecutionContext, runtime: Any) -> Any: ...
