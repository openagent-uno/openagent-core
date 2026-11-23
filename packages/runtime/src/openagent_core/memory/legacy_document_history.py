"""Visibility of document-dependent sessions in standalone SQLite projections."""
from __future__ import annotations

import json
from contextlib import closing

from openagent_core.dream_evidence import dream_run_id

LEDGER_EXISTS = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='oa_document_run_sources'"


async def has_ledger(conn):
    return await (await conn.execute(LEDGER_EXISTS)).fetchone() is not None


async def marked_session(conn, session_id):
    if not await has_ledger(conn):
        return False
    row = await (await conn.execute(
        "SELECT 1 FROM oa_document_run_sources WHERE session_id=? LIMIT 1", (session_id,),
    )).fetchone()
    return row is not None


def withheld_sessions(source, session_ids=None):
    """FTS and vectors have no authenticated viewer to resolve note receipts."""
    if not source.execute(LEDGER_EXISTS).fetchone():
        return set()
    if session_ids is None:
        rows = source.execute("SELECT DISTINCT session_id FROM oa_document_run_sources")
    else:
        rows = source.execute(
            "SELECT value FROM json_each(?) AS candidate WHERE EXISTS "
            "(SELECT 1 FROM oa_document_run_sources WHERE session_id=candidate.value)",
            (json.dumps(list(session_ids)),),
        )
    return {row[0] for row in rows}


def visible_cache_hits(open_source, hits):
    candidates = {hit["session_id"] for hit in hits if hit.get("session_id")}
    if not candidates:
        return hits
    source = open_source()
    if source is None:
        withheld = candidates
    else:
        with closing(source):
            withheld = withheld_sessions(source, candidates)
    return [hit for hit in hits if not dream_run_id(hit.get("session_id"))
            and hit.get("session_id") not in withheld]


class SessionProjectionGuard:
    """Bind legacy HTTP projections to the same viewer until response publication."""

    def __init__(self, authorize=None):
        self.authorize = authorize
        self._read = {}

    async def visible(self, conn, session_ids, *, remember=True):
        ids = set(session_ids)
        if not ids:
            return set()
        protected = {sid for sid in ids if dream_run_id(sid)}
        if await has_ledger(conn):
            cursor = await conn.execute(
                "SELECT value FROM json_each(?) AS candidate WHERE EXISTS "
                "(SELECT 1 FROM oa_document_run_sources WHERE session_id=candidate.value)",
                (json.dumps(list(ids)),),
            )
            protected.update(row[0] for row in await cursor.fetchall())
        denied = set()
        for sid in protected:
            if self.authorize is None:
                denied.add(sid)
                continue
            try:
                await self.authorize(sid)
            except (PermissionError, LookupError):
                denied.add(sid)
        allowed = ids - denied
        if remember and allowed:
            self._read.setdefault(conn, set()).update(allowed)
        return allowed

    async def recheck(self, *, resolve_connection=None):
        if self._read and resolve_connection is not None:
            # Individual handlers may use dedicated SQLite transactions that
            # close before HTTP publication. Reopen the same canonical source.
            conn = await resolve_connection()
            session_ids = set().union(*self._read.values())
            if await self.visible(conn, session_ids, remember=False) != session_ids:
                raise PermissionError("Session sources changed before publication")
            return
        for conn, session_ids in self._read.items():
            if await self.visible(conn, session_ids, remember=False) != session_ids:
                raise PermissionError("Session sources changed before publication")
