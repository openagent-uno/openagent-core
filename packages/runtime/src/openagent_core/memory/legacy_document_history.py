"""Live authorization of source-dependent and host-scoped SQLite session projections."""
from __future__ import annotations

import json
from contextlib import closing

from openagent_core.dream_evidence import dream_run_id

LEDGER_EXISTS = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='oa_document_run_sources'"


HOST_TABLE = "oa_host_scoped_sessions"
SOURCE_TABLE = "oa_document_run_sources"
PROJECTION_TABLES = (
    "SELECT name FROM sqlite_master WHERE type='table' "
    "AND name IN ('oa_document_run_sources', 'oa_host_scoped_sessions')"
)


def _select_marked(table, ids=None):
    # Only the two fixed schema names obtained from PROJECTION_TABLES enter SQL.
    if table not in (HOST_TABLE, SOURCE_TABLE):
        raise ValueError("Unknown projection registry")
    if ids is None:
        return f"SELECT DISTINCT session_id FROM {table}", ()
    return (
        f"SELECT value FROM json_each(?) AS candidate WHERE EXISTS "
        f"(SELECT 1 FROM {table} WHERE session_id=candidate.value)",
        (json.dumps(list(ids)),),
    )


async def projection_tables(conn):
    rows = await (await conn.execute(PROJECTION_TABLES)).fetchall()
    return tuple(row[0] for row in rows)


def passive_session_filter(tables, column):
    # These are canonical SQL aliases from the two read-index implementations.
    if column not in {"s.id", "h.canonical_resource_id"} or not set(tables) <= {SOURCE_TABLE, HOST_TABLE}:
        raise ValueError("Unknown canonical projection")
    return "".join(f" AND NOT EXISTS (SELECT 1 FROM {table} d WHERE d.session_id={column})" for table in tables)


async def protected_projection_sessions(conn, ids, *, host_only=False, tables=None):
    if not ids:
        return set()
    if tables is None:
        tables = await projection_tables(conn)
    result = set() if host_only else {sid for sid in ids if dream_run_id(sid)}
    for table in tables:
        if host_only and table != HOST_TABLE:
            continue
        query, args = _select_marked(table, ids)
        rows = await (await conn.execute(query, args)).fetchall()
        result.update(row[0] for row in rows)
    return result


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
    """FTS and vectors have no live viewer for host grants or document receipts."""
    result = set()
    for table, in source.execute(PROJECTION_TABLES):
        query, args = _select_marked(table, session_ids)
        result.update(row[0] for row in source.execute(query, args))
    return result


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
        self._read = {False: {}, True: {}}

    async def visible(self, conn, session_ids, *, remember=True, host_only=False):
        ids = set(session_ids)
        if not ids:
            return set()
        protected = await protected_projection_sessions(conn, ids, host_only=host_only)
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
            self._read[host_only].setdefault(conn, set()).update(allowed)
        return allowed

    async def recheck(self, *, resolve_connection=None):
        for host_only, groups in self._read.items():
            if not groups:
                continue
            if resolve_connection is not None:
                # Individual handlers may close a dedicated read transaction before publication.
                conn = await resolve_connection()
                groups = {conn: set().union(*groups.values())}
            for conn, ids in groups.items():
                allowed = await self.visible(conn, ids, remember=False, host_only=host_only)
                if allowed != ids:
                    raise PermissionError("Session authorization changed")
