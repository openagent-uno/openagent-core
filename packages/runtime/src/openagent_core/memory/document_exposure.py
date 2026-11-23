"""The note revisions presented to this admitted turn, shared by its tool tasks."""
from contextlib import contextmanager
from contextvars import ContextVar

_active = ContextVar("openagent_document_exposure", default=None)
_MAX_SOURCES = 2048


@contextmanager
def document_turn(runtime, context, run_id):
    parent = _active.get()
    key = (id(runtime), run_id, context)
    if run_id is not None and parent and parent[0] == key:
        yield
        return
    token = _active.set((key, set()) if run_id is not None else None)
    try:
        yield
    finally:
        _active.reset(token)


def saw_notes(scope, rows):
    active = _active.get()
    if active:
        seen = active[1]
        for row in rows:
            if len(seen) > _MAX_SOURCES:
                break
            seen.add((scope["tenant"], scope["agent"], scope["space"], row["path"], row["revision"]))


def note_sources(scope):
    active = _active.get()
    if not active:
        return ()
    seen = active[1]
    if len(seen) > _MAX_SOURCES or any(
        row[:3] != (scope["tenant"], scope["agent"], scope["space"]) for row in seen
    ):
        raise PermissionError("The memory write exceeds the authorized provenance boundary")
    return tuple((path, revision) for _, _, _, path, revision in seen)
