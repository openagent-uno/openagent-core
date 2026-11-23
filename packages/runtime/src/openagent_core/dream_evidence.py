"""Bounded canonical activity for one host-mapped document dream delegation."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from .contracts import (
    ExecutionContext,
    ResourceRef,
    SessionRef,
    canonical_json,
    require_authorized,
)

DREAM_SCOPE_PREFIX = "openagent.document-dream:"
REVIEW_TOOL = "vault_review_activity"
MESSAGE_CHARS = 256
MESSAGES_PER_RUN = 4
EVENTS_PER_RUN = 8
# Inspect a bounded suffix even when older turns have different audiences.
CANDIDATE_RUNS = 16
EVENT_KINDS = frozenset(
    {
        "run.accepted",
        "run.running",
        "run.success",
        "run.failed",
        "run.interrupted",
        "run.timed_out",
        "run.cancelled",
        "run.cancel_requested",
        "tool.invoking",
        "tool.completed",
    }
)


class DreamVaultSource:
    """Trusted native vault source eligible for a document maintenance catalog."""


def is_document_dream(context):
    return isinstance(context, ExecutionContext) and any(
        isinstance(s, str) and s.startswith(DREAM_SCOPE_PREFIX) for s in context.scopes
    )


@runtime_checkable
class DreamActivityStore(Protocol):
    async def dream_evidence_receipt(self, run_id: str) -> dict | None: ...
    async def check_dream_session(self, ref: SessionRef, context: ExecutionContext) -> None: ...
    async def dream_session_activity(
        self,
        ref: SessionRef,
        context: ExecutionContext,
        *,
        since_ms: int,
        limit: int,
        run_ids: tuple[str, ...] | None = None,
    ) -> list[dict]: ...


@dataclass(frozen=True)
class DocumentDreamEvidence:
    """Host opt-in. Each resolution must consult current consent for the exact scope and audience."""

    resolve_sessions: Callable[
        [ExecutionContext, Mapping[str, str]], Awaitable[tuple[SessionRef, ...] | None]
    ]
    max_sessions: int = 4
    runs_per_session: int = 4
    max_bytes: int = 16384
    lookback_seconds: int = 86400

    def __post_init__(self):
        for value, low, high in (
            (self.max_sessions, 1, 8),
            (self.runs_per_session, 1, 8),
            (self.max_bytes, 256, 65536),
            (self.lookback_seconds, 1, 604800),
        ):
            if type(value) is not int or not low <= value <= high:
                raise ValueError("Invalid dream evidence budget")

    async def _selection(self, vault, context):
        from .memory.vault.document_dream import check_dream_space

        if not context.deferred or not context.delegation_id or not is_document_dream(context):
            raise PermissionError("A document dream delegation is required")
        space = await vault.resolve_space(context)
        scope = {"tenant": context.tenant_id, "agent": context.agent_id, "space": space}
        check_dream_space(context, scope)
        refs = await self.resolve_sessions(context, MappingProxyType(scope))
        if refs is None:
            return scope, None
        if not isinstance(refs, tuple) or len(refs) > self.max_sessions or len(context.audience) > 16:
            raise PermissionError("Dream evidence selection exceeds its bound")
        for ref in refs:
            if (
                not isinstance(ref, SessionRef)
                or ref.tenant_id != context.tenant_id
                or ref.agent_id != context.agent_id
                or ref.authority != context.authority.authority
                or len(ref.session_id.encode()) > 200
                or ref.session_id.startswith("dream-session-")
            ):
                raise PermissionError("Dream evidence source is unavailable")
        if len(set(refs)) != len(refs):
            raise PermissionError("Ambiguous dream evidence selection")
        return scope, tuple(sorted(refs, key=lambda ref: ref.token))

    async def enabled(self, vault, context):
        if not is_document_dream(context) or not context.deferred:
            return False
        scope, refs = await self._selection(vault, context)
        return refs is not None and await vault.authorizer.authorize(
            context,
            "memory.dream.review",
            ResourceRef("vault-space", scope["tenant"], scope["space"]),
            audience=context.audience,
        )

    @staticmethod
    def _binding(refs, scope, original):
        return sha256(
            canonical_json([scope, sorted(ref.token for ref in refs), original.coalescing_key]).encode()
        ).hexdigest()

    async def authorize_sources(self, runtime, vault, original, reader, digest, samples=None):
        async def check_grants():
            scope, refs = await self._selection(vault, original)
            if refs is None or self._binding(refs, scope, original) != digest:
                raise PermissionError("Dream evidence selection changed")
            allowed = {original.authority, original.initiator, *original.audience}
            if not {reader.authority, reader.initiator, *reader.audience}.issubset(allowed):
                raise PermissionError("Dream result has a different audience")
            target = ResourceRef("vault-space", scope["tenant"], scope["space"])
            for action in ("memory.dream", "memory.dream.review", "memory.publish"):
                await runtime.authorize(original, action, target, audience=original.audience)
                await require_authorized(
                    vault.authorizer, original, action, target, audience=original.audience
                )
            for ctx in (original,) if original == reader else (original, reader):
                for ref in refs:
                    resource = ResourceRef("session", ref.tenant_id, ref.session_id)
                    for action in ("session.read", "run.replay"):
                        # Authorize the exact admitted sample below; private
                        # unrelated turns in a shared room are never selected.
                        await runtime._authorize_policy(ctx, action, resource, audience=ctx.audience)
                    await runtime.services.store.check_dream_session(ref, ctx)
            return refs

        refs = await check_grants()
        if samples is not None:
            if not isinstance(samples, list) or len(samples) > len(refs):
                raise PermissionError("Invalid dream activity receipt")
            for ref, sample in zip(refs, samples):
                ids = sample["runs"]
                if (
                    not isinstance(ids, list)
                    or len(ids) > self.runs_per_session
                    or any(not isinstance(i, str) or len(i.encode()) > 200 for i in ids)
                ):
                    raise PermissionError("Invalid dream activity references")
                from .memory.document_exposure import authorize_document_history
                for ident in ids:
                    await authorize_document_history(runtime, original, run_id=ident)
                    if reader != original:
                        await authorize_document_history(runtime, reader, run_id=ident)
                turns = await runtime.services.store.dream_session_activity(
                    ref,
                    original,
                    since_ms=0,
                    limit=self.runs_per_session,
                    run_ids=tuple(ids),
                )
                if sha256(canonical_json(turns).encode()).hexdigest() != sample["digest"]:
                    raise PermissionError("Dream activity was removed or changed")
            await check_grants()

    async def read(self, vault, context):
        from .runtime import (
            current_execution_context,
            current_run_id,
            current_runtime,
            guard_current_publication,
        )

        runtime, run_id = current_runtime(), current_run_id()
        if (
            runtime is None
            or current_execution_context() != context
            or not run_id
            or context.session_id != "dream-session-" + run_id
            or vault.dream_queue is None
        ):
            raise PermissionError("A running document dream is required")
        store = runtime.services.store
        if not isinstance(store, DreamActivityStore):
            raise PermissionError("The canonical store does not support authorized dream evidence")
        scope, refs = await self._selection(vault, context)
        if refs is None:
            raise PermissionError("Dream evidence is disabled for this scope")

        from .memory.vault.document_dream import DOCUMENT_DREAM_REVIEW_PROMPT

        admitted = await store.accepted_request(run_id)
        if (
            admitted.request.session_id != context.session_id
            or admitted.request.input != DOCUMENT_DREAM_REVIEW_PROMPT
        ):
            raise PermissionError("The second mission was not admitted for this dream")
        digest = self._binding(refs, scope, context)

        samples = None

        async def recheck():
            await self.authorize_sources(runtime, vault, context, context, digest, samples)

        await recheck()
        await vault.dream_queue.begin_evidence(scope, run_id)
        since_ms = int((time.time() - self.lookback_seconds) * 1000)
        result = {"since_ms": since_ms, "sampled": True, "truncated": False, "sessions": []}
        for ref in refs:
            runs = await store.dream_session_activity(
                ref,
                context,
                since_ms=since_ms,
                limit=self.runs_per_session,
            )
            session = {"session_id": ref.session_id, "runs": []}
            result["sessions"].append(session)
            if len(canonical_json(result).encode()) > self.max_bytes:
                result["sessions"].pop()
                result["truncated"] = True
                break
            for run in runs:
                session["runs"].append(run)
                if len(canonical_json(result).encode()) > self.max_bytes:
                    session["runs"].pop()
                    result["truncated"] = True
                    break
        samples = [
            {
                "runs": [r["run_id"] for r in session["runs"]],
                "digest": sha256(canonical_json(session["runs"]).encode()).hexdigest(),
            }
            for session in result["sessions"]
        ]
        await recheck()
        # The receipt becomes durable before the catalog or model receives
        # source material. The hash pins just the excerpts actually shown.
        await store.append_event(run_id, "run.dream.evidence", {"binding": digest, "samples": samples})
        guard_current_publication(recheck)
        return result


def dream_run_id(session_id):
    if isinstance(session_id, str) and session_id.startswith("dream-session-"):
        run_id = session_id.removeprefix("dream-session-")
        if len(run_id) == 32 and all(c in "0123456789abcdef" for c in run_id):
            return run_id
    return None


async def authorize_dream_history(runtime, viewer, session_id):
    run_id = dream_run_id(session_id)
    store = runtime.services.store
    lookup = getattr(store, "dream_evidence_receipt", None)
    if not callable(lookup):
        # A custom store can still serve the original notes-only mission.
        # A review written by another adapter requires receipt-aware reads.
        from .memory.vault.document_dream import DOCUMENT_DREAM_PROMPT

        admitted = await store.accepted_request(run_id)
        if admitted.request.session_id == session_id and admitted.request.input == DOCUMENT_DREAM_PROMPT:
            return
        raise PermissionError("Dream evidence history is unavailable")
    receipt = await lookup(run_id)
    if receipt is None:
        return
    from .memory.vault.documents import SqlDocumentVault

    service = runtime.service(DocumentDreamEvidence)
    vault = runtime.service(SqlDocumentVault)
    if service is None or vault is None or not isinstance(store, DreamActivityStore):
        raise PermissionError("Dream evidence authorization is unavailable")
    accepted = await store.accepted_request(run_id)
    if accepted.request.session_id != session_id or not accepted.deferred or not accepted.delegation_id:
        raise PermissionError("Invalid dream evidence history")
    original = ExecutionContext(
        accepted.author,
        accepted.initiator,
        accepted.authority,
        session_id,
        viewer.agent_id,
        accepted.audience,
        scopes=accepted.scopes,
        delegation_id=accepted.delegation_id,
        ingress_id=accepted.ingress_id,
        parent_run_id=accepted.parent_run_id,
        deferred=True,
    )
    await service.authorize_sources(runtime, vault, original, viewer, receipt["binding"], receipt["samples"])
