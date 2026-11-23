"""Transactional origin and segment bookkeeping for OpenAgent's durable runner."""

from openagent_core.contracts import IdempotencyConflict, RunEvent, canonical_json
from openagent_core.operations import CHECKPOINT_EVENTS, STARTED


class Operations:
    async def _admit_segment(self, db, request, context, snapshot, reserved):
        descriptor = request.continuation
        if not context.deferred or not context.delegation_id:
            raise PermissionError("Missing operation delegation")
        origin = descriptor.origin_run_id
        if descriptor.segment == 0:
            # Serialize capacity and handoff admission for the small active-operation budget.
            await db.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,3))",
                (str((self.schema, self.agent_id)),),
            )
            count = await (
                await db.execute(
                    "SELECT count(*) AS n FROM operations WHERE agent=%s AND state='active'",
                    (self.agent_id,),
                )
            ).fetchone()
            if count["n"] >= 100:
                raise ValueError("Durable operation admission limit exceeded")
            if descriptor.handoff_run_id:
                await self._guard_checkpoint(db, descriptor.handoff_run_id)
                source = await self._run(db, descriptor.handoff_run_id)
                identity = (
                    "author",
                    "initiator",
                    "authority",
                    "session_id",
                    "agent_id",
                    "audience",
                    "scopes",
                    "parent_run_id",
                )
                old = source["context"]
                resumed = await (
                    await db.execute(
                        """SELECT 1 FROM events WHERE agent=%s AND kind=%s
                    AND payload->>'resume_run_id'=%s""",
                        (self.agent_id, STARTED, descriptor.handoff_run_id),
                    )
                ).fetchone()
                if (
                    source["status"] not in {"timed_out", "interrupted"}
                    or source["request"].get("continuation")
                    or old.get("deferred")
                    or old.get("delegation_id")
                    or resumed
                    or any(canonical_json(old[k]) != canonical_json(snapshot[k]) for k in identity)
                ):
                    raise IdempotencyConflict("Source checkpoint unavailable")
            await db.execute(
                "INSERT INTO operations VALUES (%s,%s,%s,%s,%s)",
                (self.agent_id, origin, request.run_id, 0, "cancelled" if reserved else "active"),
            )
            if descriptor.handoff_run_id:
                cur = await db.execute(
                    "INSERT INTO handoffs VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                    (self.agent_id, descriptor.handoff_run_id, origin),
                )
                if not cur.rowcount:
                    raise IdempotencyConflict("Source checkpoint already claimed")
            return
        previous = await self._run(db, descriptor.previous_run_id)
        old = previous["request"]
        immutable = (
            "origin_run_id",
            "name",
            "version",
            "expires_at",
            "segment_seconds",
            "max_segments",
            "requested_deadline_seconds",
            "handoff_run_id",
            "checkpoint_origin_run_id",
        )
        prior = old.get("continuation") or {}
        if (
            previous["status"] not in {"timed_out", "interrupted"}
            or previous["session_id"] != request.session_id
            or prior.get("segment") != descriptor.segment - 1
            or old["input"] != request.input
            or old["attachments"] != list(request.attachments)
            or old["model_ref"] != request.model_ref
            or canonical_json(previous["context"]) != canonical_json(snapshot)
            or any(prior.get(k) != getattr(descriptor, k) for k in immutable)
        ):
            raise IdempotencyConflict("Operation continuation changed its admission")
        cur = await db.execute(
            """UPDATE operations SET current_run_id=%s,segment=%s WHERE agent=%s AND
            origin_run_id=%s AND current_run_id=%s AND segment=%s AND state='active'""",
            (
                request.run_id,
                descriptor.segment,
                self.agent_id,
                origin,
                descriptor.previous_run_id,
                descriptor.segment - 1,
            ),
        )
        if not cur.rowcount:
            raise IdempotencyConflict("Operation continuation is no longer active")

    async def _guard_checkpoint(self, db, ident):
        await db.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,4))",
            (str((self.schema, self.agent_id, ident)),),
        )

    async def start_operation(self, run_id, payload):
        async with self._db() as db:
            row = await self._owned(db, run_id)
            if row["status"] != "running":
                raise ValueError("Checkpoint requires an active turn")
            previous = payload.get("resume_run_id")
            if previous:
                await self._guard_checkpoint(db, previous)
                claim = await (
                    await db.execute(
                        "SELECT origin_run_id FROM handoffs WHERE agent=%s AND source_id=%s",
                        (self.agent_id, previous),
                    )
                ).fetchone()
                descriptor = row["request"].get("continuation") or {}
                if claim and claim["origin_run_id"] != descriptor.get("origin_run_id"):
                    raise IdempotencyConflict("Checkpoint owned by another handoff")
            conflict = await (
                await db.execute(
                    """SELECT 1 FROM events WHERE agent=%s AND kind=%s
                AND (run_id=%s OR payload->>'resume_run_id'=%s) LIMIT 1""",
                    (self.agent_id, STARTED, run_id, previous),
                )
            ).fetchone()
            if conflict:
                raise IdempotencyConflict("Checkpoint already has a continuation")
            return await self._event(db, run_id, STARTED, dict(payload))

    async def operation_events(self, run_id):
        async with self._db() as db:
            rows = await (
                await db.execute(
                    """SELECT * FROM events WHERE agent=%s AND run_id=%s AND kind=ANY(%s)
                ORDER BY seq""",
                    (self.agent_id, run_id, list(CHECKPOINT_EVENTS)),
                )
            ).fetchall()
        return tuple(RunEvent(r["seq"], run_id, r["kind"], r["payload"]) for r in rows)

    async def durable_operation(self, ident):
        async with self._db() as db:
            return await (
                await db.execute(
                    """SELECT d.* FROM operations d JOIN runs r ON r.agent=d.agent
                AND d.origin_run_id=coalesce(r.request->'continuation'->>'origin_run_id',r.id)
                WHERE r.agent=%s AND r.id=%s""",
                    (self.agent_id, ident),
                )
            ).fetchone()

    async def durable_operations(self):
        async with self._db() as db:
            rows = await (
                await db.execute(
                    """SELECT d.*,extract(epoch from r.finished_at)*1000 AS segment_finished_at_ms
                FROM operations d JOIN runs r ON r.agent=d.agent AND r.id=d.current_run_id WHERE d.agent=%s
                AND d.state='active' ORDER BY r.ordinal LIMIT 100""",
                    (self.agent_id,),
                )
            ).fetchall()
            return tuple(
                dict(row, segment_finished_at_ms=int(row["segment_finished_at_ms"] or 0))
                for row in rows
            )

    async def stop_durable_operation(self, ident, state):
        if state not in {
            "success",
            "failed",
            "cancelled",
            "rejected",
            "skipped",
            "expired",
            "exhausted",
            "revoked",
        }:
            raise ValueError("Unsupported operation outcome")
        async with self._db() as db:
            cur = await db.execute(
                """UPDATE operations SET state=%s WHERE agent=%s AND origin_run_id=%s
                AND state='active' RETURNING current_run_id""",
                (state, self.agent_id, ident),
            )
            op = await cur.fetchone()
            if op and state in {"cancelled", "expired", "exhausted", "revoked"}:
                if state == "cancelled":
                    await self._cancel_in(db, op["current_run_id"])
                await self._event(
                    db,
                    ident,
                    "run.operation.finished",
                    dict(
                        status="cancelled" if state == "cancelled" else "failed",
                        reason="operation_" + state,
                        retry_allowed=False,
                    ),
                )

    async def durable_operation_events(self, origin, after):
        return await self.durable_operation_event_window(origin, after, 1001)

    async def durable_operation_event_window(self, origin, after, limit):
        async with self._db() as db:
            rows = await (
                await db.execute(
                    """SELECT e.* FROM events e JOIN runs r ON r.agent=e.agent AND r.id=e.run_id
                WHERE r.agent=%s AND r.request->'continuation'->>'origin_run_id'=%s AND e.seq>%s
                ORDER BY e.seq LIMIT %s""",
                    (self.agent_id, origin, after, limit),
                )
            ).fetchall()
        return tuple(RunEvent(r["seq"], r["run_id"], r["kind"], r["payload"]) for r in rows)
