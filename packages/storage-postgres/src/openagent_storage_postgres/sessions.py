"""Cursor-paged, principal-scoped session operations."""

from base64 import urlsafe_b64decode, urlsafe_b64encode
import json
from dataclasses import asdict
from psycopg.types.json import Jsonb

from openagent_core.contracts import SessionRef, IdempotencyConflict, canonical_json


def _limit(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("Session page limit must be between 1 and 100")
    return value


def _cursor(value, length):
    if value is None:
        return (None,) * length
    try:
        decoded = json.loads(urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if isinstance(decoded, list) and len(decoded) == length:
            return tuple(decoded)
    except (TypeError, ValueError):
        pass
    raise ValueError("Invalid session cursor")


def _encode(*values):
    return urlsafe_b64encode(json.dumps(values).encode()).decode().rstrip("=")


def _ms(stamp):
    return int(stamp.timestamp() * 1000)


def _wire(row):
    reference = SessionRef(row["authority"], row["tenant"], row["agent"], row["id"])
    return dict(
        session_ref=reference.token,
        session_id=row["id"],
        agent_id=row["agent"],
        title=row["title"],
        archived=row["archived"],
        status="archived" if row["archived"] else "active",
        kind="chat",
        origin=row["origin"],
        parent_session_id=row["parent_id"],
        root_session_id=row["root_id"],
        created_at_ms=_ms(row["created_at"]),
        updated_at_ms=_ms(row["updated_at"]),
        last_activity_at_ms=_ms(row["last_activity_at"]),
    )


def _members(ctx):
    return sorted({p.key for p in (ctx.authority, ctx.initiator, *ctx.audience)})


# Run contexts are immutable. A room participant may have individual turns in
# the same room, so a room-wide grant alone cannot expose those messages.
RUN_AUDIENCE = (
    "((r.context->'audience') || (r.context->'authority') || (r.context->'initiator')) @> %s"
)


def _audience(ctx):
    return Jsonb([asdict(p) for p in (ctx.authority, ctx.initiator, *ctx.audience)])


class Sessions:
    def _boundary(self, ref, ctx):
        if (ref.authority, ref.tenant_id, ref.agent_id) != (
            ctx.authority.authority,
            ctx.tenant_id,
            self.agent_id,
        ) or ctx.agent_id != self.agent_id:
            raise PermissionError("Foreign session reference")

    async def check_session_access(self, context, ident):
        # The host already checked the live authorization policy. Constrain
        # reads and result publication to the admitted durable audience too.
        async with self._db() as db:
            try:
                await self._session(db, ident, context, lock=False)
            except LookupError:
                pass  # Run submission and pre-admission cancellation can precede the session.

    async def _session(self, db, ident, ctx, *, write=False, lock=True):
        row = await (
            await db.execute(
                "SELECT * FROM sessions WHERE agent=%s AND id=%s" + (" FOR UPDATE" if lock else ""),
                (self.agent_id, ident),
            )
        ).fetchone()
        if row is None:
            raise LookupError(ident)
        if (
            row["tenant"] != ctx.tenant_id
            or row["authority"] != ctx.authority.authority
            or ctx.agent_id != self.agent_id
            or not set(_members(ctx)).issubset(row["participants"])
            or write
            and row["owner_id"] != ctx.authority.key
        ):
            raise PermissionError("Session unavailable")
        return row

    async def _ensure_session(self, db, ident, ctx, title=None, parent=None, strict=False):
        if ctx.agent_id != self.agent_id:
            raise PermissionError("Foreign agent")
        root = None
        if parent:
            self._boundary(parent, ctx)
            ancestor = await self._session(db, parent.session_id, ctx)
            root = ancestor["root_id"] or ancestor["id"]
        cur = await db.execute(
            """INSERT INTO sessions(agent,id,tenant,authority,owner_id,participants,title,parent_id,root_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                self.agent_id,
                ident,
                ctx.tenant_id,
                ctx.authority.authority,
                ctx.authority.key,
                _members(ctx),
                title,
                parent.session_id if parent else None,
                root,
            ),
        )
        row = await self._session(db, ident, ctx)
        if strict and cur.rowcount != 1:
            raise IdempotencyConflict("Session ID already exists")
        return row

    async def create_session(self, reference, context, *, title=None, parent=None):
        self._boundary(reference, context)
        async with self._db() as db:
            return _wire(
                await self._ensure_session(db, reference.session_id, context, title, parent, True)
            )

    async def _list(self, context, archived, limit, cursor, agent_id, query=None):
        if context.agent_id != self.agent_id or agent_id not in (None, self.agent_id):
            raise PermissionError("Foreign agent")
        _limit(limit)
        stamp, ident = _cursor(cursor, 2)
        sql = """SELECT s.* FROM sessions s WHERE agent=%s AND tenant=%s AND authority=%s
                 AND %s::text[] <@ participants AND (%s OR NOT archived)"""
        args = [
            self.agent_id,
            context.tenant_id,
            context.authority.authority,
            _members(context),
            archived,
        ]
        if stamp is not None:
            sql += " AND (last_activity_at,id)<(%s::timestamptz,%s)"
            args.extend((stamp, ident))
        if query is not None:
            sql += f""" AND (strpos(lower(coalesce(title,'')),lower(%s))>0 OR EXISTS(
                SELECT 1 FROM messages m JOIN runs r ON m.agent=r.agent AND m.run_id=r.id
                WHERE {RUN_AUDIENCE} AND m.agent=s.agent AND m.session_id=s.id
                AND m.role IN ('user','assistant') AND strpos(lower(m.body#>>'{{}}'),lower(%s))>0))"""
            args.extend((query, _audience(context), query))
        sql += " ORDER BY last_activity_at DESC,id DESC LIMIT %s"
        async with self._db() as db:
            rows = await (await db.execute(sql, (*args, limit + 1))).fetchall()
        page = rows[:limit]
        return dict(
            sessions=[_wire(row) for row in page],
            next_cursor=(
                _encode(page[-1]["last_activity_at"].isoformat(), page[-1]["id"])
                if len(rows) > limit
                else None
            ),
        )

    async def list_sessions(self, context, *, include_archived, limit, cursor=None, agent_id=None):
        return await self._list(context, include_archived, limit, cursor, agent_id)

    async def search_sessions(self, context, *, query, limit, cursor=None, agent_id=None):
        if not query.strip() or len(query) > 512:
            raise ValueError("Invalid session search")
        page = await self._list(context, False, limit, cursor, agent_id, query.strip())
        return dict(page, index={"kind": "canonical", "complete": True})

    async def read_session(self, reference, context, *, limit, cursor=None):
        self._boundary(reference, context)
        (after,) = _cursor(cursor, 1)
        async with self._db() as db:
            row = await self._session(db, reference.session_id, context)
            records = await (
                await db.execute(
                    f"""SELECT m.* FROM messages m JOIN runs r ON m.agent=r.agent AND m.run_id=r.id
                WHERE m.agent=%s AND m.session_id=%s AND {RUN_AUDIENCE}
                AND m.session_seq>%s ORDER BY m.session_seq LIMIT %s""",
                    (
                        self.agent_id,
                        reference.session_id,
                        _audience(context),
                        int(after) if after is not None else -1,
                        _limit(limit) + 1,
                    ),
                )
            ).fetchall()
        page = records[:limit]

        def message(m):
            author = json.loads(m["author"])
            return dict(
                id=m["id"],
                run_id=m["run_id"],
                sequence=m["session_seq"],
                role=m["role"],
                status=m["status"],
                author_kind=author["kind"],
                author_principal_id=m["author"],
                created_at_ms=_ms(m["created_at"]),
                text=m["body"] if isinstance(m["body"], str) else canonical_json(m["body"]),
                content_json=None,
                tool_call_id=m["call_id"],
            )

        return dict(
            session=_wire(row),
            messages=[message(m) for m in page],
            next_cursor=_encode(page[-1]["session_seq"]) if len(records) > limit else None,
        )

    async def check_dream_session(self, ref, context):
        self._boundary(ref, context)
        async with self._db() as db:
            await self._session(db, ref.session_id, context, lock=False)

    async def dream_evidence_receipt(self, ident):
        async with self._db() as db:
            row = await (await db.execute(
                "SELECT payload FROM events "
                "WHERE agent=%s AND run_id=%s AND kind='run.dream.evidence' LIMIT 1",
                (self.agent_id, ident),
            )).fetchone()
            return row["payload"] if row else None

    async def dream_session_activity(self, ref, context, *, since_ms, limit, run_ids=None):
        from openagent_core.dream_evidence import (
            CANDIDATE_RUNS, EVENT_KINDS, EVENTS_PER_RUN, MESSAGE_CHARS, MESSAGES_PER_RUN,
        )
        if type(limit) is not int or not 1 <= limit <= 8 or type(since_ms) is not int:
            raise ValueError("Invalid activity query")
        self._boundary(ref, context)
        async with self._db() as db:
            await self._session(db, ref.session_id, context, lock=False)
            params = [ref.agent_id, ref.tenant_id, ref.session_id]
            selected = ""
            if run_ids is not None:
                if not isinstance(run_ids, tuple) or len(run_ids) > limit:
                    raise ValueError("Invalid activity selection")
                if not run_ids:
                    return []
                selected = " AND id=ANY(%s)"
                params.append(list(run_ids))
            # Fetch all selected runs and both bounded suffixes in a single
            # snapshot/round trip. The inner limits apply before payload filtering.
            cursor = await db.execute(
                f"""
                WITH recent AS MATERIALIZED (
                    SELECT agent,id,status,ordinal,finished_at,context FROM runs
                    WHERE agent=%s AND tenant=%s AND session_id=%s {selected}
                    ORDER BY ordinal DESC LIMIT %s
                ), visible AS (
                    SELECT agent,id,status,ordinal,finished_at FROM recent r WHERE {RUN_AUDIENCE}
                    AND finished_at >= to_timestamp(%s::double precision / 1000)
                    AND octet_length(id)<=200 ORDER BY ordinal DESC LIMIT %s
                )
                SELECT r.id AS run_id,r.status,r.finished_at,
                    (SELECT coalesce(jsonb_agg(jsonb_build_object(
                        'role',m.role,'excerpt',left(m.body#>>'{{}}',%s)
                    ) ORDER BY m.session_seq),'[]'::jsonb)
                     FROM (SELECT role,status,body,session_seq FROM messages
                           WHERE agent=r.agent AND run_id=r.id ORDER BY session_seq DESC LIMIT %s) m
                     WHERE m.role IN ('user','assistant') AND m.status='complete'
                       AND jsonb_typeof(m.body)='string') AS messages,
                    (SELECT coalesce(jsonb_agg(jsonb_build_object(
                        'kind',e.kind,'outcome',CASE WHEN e.kind='tool.completed'
                          AND e.payload->>'status' IN ('success','error') THEN e.payload->>'status' END
                    ) ORDER BY e.seq),'[]'::jsonb)
                     FROM (SELECT kind,payload,seq FROM events
                           WHERE agent=r.agent AND run_id=r.id ORDER BY seq DESC LIMIT %s) e
                     WHERE e.kind=ANY(%s)) AS events
                FROM visible r ORDER BY r.ordinal DESC
                """,
                (*params, CANDIDATE_RUNS, _audience(context), since_ms, limit, MESSAGE_CHARS,
                 MESSAGES_PER_RUN, EVENTS_PER_RUN, list(EVENT_KINDS)),
            )
            rows = await cursor.fetchall()
            for row in rows:
                row["finished_at_ms"] = _ms(row.pop("finished_at"))
            return rows

    async def provider_history(self, context, run_id, *, limit, max_bytes):
        from openagent_core.provider_history import ProviderHistoryMessage

        if (
            type(limit) is not int
            or not 1 <= limit <= 128
            or type(max_bytes) is not int
            or not 1 <= max_bytes <= 65536
        ):
            raise ValueError("Invalid provider history budget")
        async with self._db() as db:
            await self._session(db, context.session_id, context, lock=False)
            current = await self._owned(db, run_id)
            if (
                current["status"] != "running"
                or current["session_id"] != context.session_id
                or current["tenant"] != context.tenant_id
            ):
                raise PermissionError("Provider history requires the current admitted turn")
            # Order by turn admission before ordering messages inside each turn.
            # An earlier assistant response may commit after this turn's user input.
            # A later queued user's input may already be present in the transcript.
            rows = await (
                await db.execute(
                    f"""
                WITH recent AS MATERIALIZED (
                    SELECT r.ordinal, m.session_seq, m.role, m.body #>> '{{}}' AS content
                    FROM runs r JOIN messages m ON m.agent=r.agent AND m.run_id=r.id
                    WHERE r.agent=%s AND r.session_id=%s AND r.ordinal<%s AND {RUN_AUDIENCE}
                      AND m.role IN ('user','assistant') AND m.status='complete'
                      AND jsonb_typeof(m.body)='string'
                    ORDER BY r.ordinal DESC,m.session_seq DESC LIMIT %s
                ), bounded AS (
                    SELECT *,sum(octet_length(content)) OVER (ORDER BY ordinal DESC,session_seq DESC) AS total
                    FROM recent
                )
                SELECT role, content FROM bounded WHERE total<=%s ORDER BY ordinal,session_seq
                """,
                    (
                        self.agent_id,
                        context.session_id,
                        current["ordinal"],
                        _audience(context),
                        limit,
                        max_bytes,
                    ),
                )
            ).fetchall()
            return tuple(ProviderHistoryMessage(row["role"], row["content"]) for row in rows)

    async def _edit(self, reference, context, field, value):
        self._boundary(reference, context)
        async with self._db() as db:
            await self._session(db, reference.session_id, context, write=True)
            # Column names come only from these three public methods.
            cur = await db.execute(
                f"UPDATE sessions SET {field}=%s,updated_at=clock_timestamp() WHERE agent=%s AND id=%s RETURNING *",
                (value, self.agent_id, reference.session_id),
            )
            return _wire(await cur.fetchone())

    async def rename_session(self, reference, context, *, title):
        if not title.strip() or len(title) > 240:
            raise ValueError("Invalid session title")
        return await self._edit(reference, context, "title", title.strip())

    async def archive_session(self, reference, context):
        return await self._edit(reference, context, "archived", True)

    async def restore_session(self, reference, context):
        return await self._edit(reference, context, "archived", False)

    async def purge_session(self, reference, context):
        self._boundary(reference, context)
        async with self._db() as db:
            row = await self._session(db, reference.session_id, context, write=True)
            if not row["archived"]:
                raise ValueError("Archive session before purging")
            if await (
                await db.execute(
                    "SELECT 1 FROM runs WHERE agent=%s AND session_id=%s AND status IN ('queued','running') LIMIT 1",
                    (self.agent_id, reference.session_id),
                )
            ).fetchone():
                raise ValueError("Active runs prevent purging")
            await db.execute(
                "DELETE FROM reservations WHERE agent=%s AND session_id=%s",
                (self.agent_id, reference.session_id),
            )
            await db.execute(
                "DELETE FROM sessions WHERE agent=%s AND id=%s",
                (self.agent_id, reference.session_id),
            )
