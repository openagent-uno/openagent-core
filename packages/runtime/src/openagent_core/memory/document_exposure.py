"""Track the immutable note revisions actually exposed to an admitted turn."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from ..contracts import ResourceRef, require_authorized

_MAX_SOURCES = 2048
_active = ContextVar("openagent_document_exposure", default=None)


@dataclass
class _Turn:
    runtime: object
    context: object
    run_id: str
    seen: set = field(default_factory=set)
    tokens: set = field(default_factory=set)
    guarded: bool = False


@contextmanager
def document_turn(runtime, context, run_id):
    parent = _active.get()
    if parent and (parent.runtime, parent.context, parent.run_id) == (
        runtime,
        context,
        run_id,
    ):
        yield
        return
    token = _active.set(_Turn(runtime, context, run_id) if run_id is not None else None)
    try:
        yield
    finally:
        _active.reset(token)


def note_sources(scope):
    turn = _active.get()
    if not turn:
        return ()
    if any(
        row[:3] != (scope["tenant"], scope["agent"], scope["space"])
        for row in turn.seen
    ):
        raise PermissionError(
            "The memory write exceeds the authorized provenance boundary"
        )
    return tuple((path, revision) for _, _, _, path, revision in turn.seen)


async def _verify(vault, runtime, context, tokens):
    if not tokens:
        return ()
    from .vault.documents import SqlDocumentVault

    if (
        len(tokens) > _MAX_SOURCES
        or vault is None
        or runtime.service(SqlDocumentVault) is not vault
    ):
        raise PermissionError("Document history authorization is unavailable")
    space = await vault.resolve_space(context)
    scope = {"tenant": context.tenant_id, "agent": context.agent_id, "space": space}
    target = ResourceRef("vault-space", context.tenant_id, space)
    for action, audience in (
        ("memory.read", (context.initiator,)),
        ("memory.publish", context.audience),
    ):
        await runtime.authorize(context, action, target, audience=audience)
        await require_authorized(
            vault.authorizer, context, action, target, audience=audience
        )
    rows = await vault.resolve_note_receipts(scope, tokens)
    if len(rows) != len(tokens) or await vault.resolve_space(context) != space:
        raise PermissionError("A source note revision is no longer available")
    return tuple(
        (context.tenant_id, context.agent_id, space, path, revision)
        for path, revision in rows
    )


async def _inherit(vault, turn, tokens):
    from ..runtime import guard_current_publication

    tokens = set(tokens)
    if len(tokens | turn.tokens) > _MAX_SOURCES:
        raise PermissionError("Document provenance exceeds the admitted turn budget")
    unseen = tokens - turn.tokens
    if unseen:
        persist = getattr(turn.runtime.services.store, "bind_document_sources", None)
        if not callable(persist):
            raise PermissionError("The run store cannot bind document provenance")
        # No source bytes may reach the catalog, model or stream before this
        # independently committed runtime marker. A failed receipt is harmless.
        await persist(turn.context, turn.run_id, tuple(sorted(unseen)))
        turn.tokens.update(unseen)
    if not turn.guarded:

        async def recheck():
            await _verify(vault, turn.runtime, turn.context, turn.tokens)

        guard_current_publication(recheck)
        turn.guarded = True
    turn.seen.update(await _verify(vault, turn.runtime, turn.context, tokens))


async def saw_notes(vault, context, scope, rows):
    turn = _active.get()
    if turn is None or not rows:
        return
    if turn.context != context:
        raise PermissionError("The document result belongs to another admitted context")
    keys = {(row["path"], row["revision"]) for row in rows}
    if len(keys) > _MAX_SOURCES:
        raise PermissionError("Document provenance exceeds the admitted turn budget")
    tokens = await vault.note_receipts(scope, keys)
    await _inherit(vault, turn, tokens)


def require_admitted_audience(accepted, viewer):
    allowed = {accepted.authority, accepted.initiator, *accepted.audience}
    if not {viewer.authority, viewer.initiator, *viewer.audience}.issubset(allowed):
        raise PermissionError("The admitted document audience changed")


async def authorize_source_audience(runtime, accepted, viewer):
    require_admitted_audience(accepted, viewer)
    if accepted.delegation_id:
        from ..contracts import DelegationService, ExecutionContext

        delegated = ExecutionContext(
            accepted.author, accepted.initiator, accepted.authority,
            accepted.request.session_id, viewer.agent_id, accepted.audience,
            accepted.scopes, accepted.delegation_id, (), accepted.ingress_id,
            accepted.parent_run_id, accepted.deferred,
        )
        service = runtime.service(DelegationService)
        if service is None or not await service.validate(delegated):
            raise PermissionError("The document source delegation is no longer active")


async def authorize_document_history(runtime, viewer, *, run_id=None, session_id=None):
    """Revalidate exact revision tokens on a cold reader, and propagate into a consuming turn."""
    lookup = getattr(runtime.services.store, "document_sources", None)
    if lookup is None:
        return  # Unsupported stores cannot begin a protected read.
    tokens = await lookup(viewer, run_id=run_id, session_id=session_id)
    if not tokens:
        return
    from .vault.documents import SqlDocumentVault

    vault = runtime.service(SqlDocumentVault)
    protected = (
        (run_id,)
        if run_id is not None
        else await runtime.services.store.document_source_runs(viewer, session_id)
    )
    if len(protected) > _MAX_SOURCES:
        raise PermissionError("Document history exceeds its authorization budget")
    for ident in protected:
        accepted = await runtime.services.store.accepted_request(ident)
        await authorize_source_audience(runtime, accepted, viewer)
    await _verify(vault, runtime, viewer, tokens)
    turn = _active.get()
    if turn is not None and turn.runtime is runtime:
        await _inherit(vault, turn, tokens)


def has_document_exposure():
    turn = _active.get()
    return bool(turn and turn.tokens)


def require_unexposed_turn(surface):
    if has_document_exposure():
        raise PermissionError(f"{surface} cannot retain document-derived content")
