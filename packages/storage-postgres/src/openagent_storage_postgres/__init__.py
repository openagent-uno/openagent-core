"""PostgreSQL-backed run ledger for an authenticated host's agent."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from importlib.resources import files
import re
from uuid import NAMESPACE_URL, uuid4, uuid5

from psycopg import OperationalError, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from openagent_core.contracts import (
    AcceptedRunRequest,
    IdempotencyConflict,
    PrincipalRef,
    RunEvent,
    RunRecord,
    RunRequest,
    SessionRef,
    TERMINAL_STATUSES,
    canonical_json,
)
from .sessions import RUN_AUDIENCE, Sessions, _audience, _members
from .operations import Operations
from .notifications import RunNotifications
from .providers import ProviderSlots
from openagent_core.durable_operations import (
    OperationRunRequest,
    OperationContinuation,
    DurableRunRecord,
)


def _record(row):
    cls = RunRecord
    extra = ()
    descriptor = row["request"].get("continuation")
    if descriptor:
        cls = DurableRunRecord
        extra = descriptor["origin_run_id"], row["id"], descriptor["segment"]
    return cls(
        row["id"],
        row["session_id"],
        row["tenant"],
        row["status"],
        row["digest"],
        row["output"],
        row["cancel_requested"],
        row["parent_run_id"],
        *extra,
    )


def _principal(key):
    return PrincipalRef(**key)


class PostgresRuntimeStore(Sessions, Operations, ProviderSlots):
    """One agent namespace, multiple tenant sessions and independently fenced workers.

    A new worker identity is minted on each start. Only the worker that admitted a
    request can execute it. Loss of its lease interrupts ambiguous work and never
    grants another process reusable user credentials.
    """

    def __init__(
        self, dsn: str, *, agent_id: str, schema="openagent_runtime", pool_size=8, lease_seconds=30
    ):
        if not agent_id or not re.fullmatch("[a-z_][a-z0-9_]{0,62}", schema):
            raise ValueError("Invalid agent or schema")
        if type(pool_size) is not int or pool_size < 2 or not 3 <= lease_seconds <= 3600:
            raise ValueError("Invalid pool or worker lease")
        self.agent_id, self.schema, self.lease_seconds = agent_id, schema, lease_seconds
        self._dsn, self._pool_size = dsn, pool_size
        self._pool = self._heartbeat = self._worker = None
        self._executing = {}
        self._notices = RunNotifications(dsn, schema, agent_id)

    @asynccontextmanager
    async def _db(self):
        if self._pool is None:
            raise RuntimeError("Store has not started")
        async with self._pool.connection() as db:
            async with db.transaction():
                await db.execute(
                    sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                        sql.Identifier(self.schema)
                    )
                )
                yield db

    async def start(self):
        if self._pool is not None:
            return
        self._worker = uuid4()
        self._pool = AsyncConnectionPool(
            self._dsn,
            min_size=1,
            max_size=self._pool_size,
            kwargs={"row_factory": dict_row},
            open=False,
        )
        try:
            await self._pool.open(wait=True)
            async with self._db() as db:
                # Serialize first-time DDL in this namespace across worker starts.
                await db.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    ("openagent:" + self.schema,),
                )
                await db.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema))
                )
                await db.execute(files(__package__).joinpath("schema.sql").read_text())
                self._heartbeat_ack = asyncio.get_running_loop().time()
                await db.execute(
                    """INSERT INTO workers(id,agent,expires_at)
                    VALUES (%s,%s,clock_timestamp()+make_interval(secs=>%s))""",
                    (self._worker, self.agent_id, self.lease_seconds),
                )
            self._notices.start()
            self._heartbeat = asyncio.create_task(self._maintain(), name="openagent-pg-lease")
        except BaseException:
            await self._pool.close()
            self._pool = None
            raise

    async def close(self):
        await self._notices.close()
        if self._heartbeat is not None:
            self._heartbeat.cancel()
            await asyncio.gather(self._heartbeat, return_exceptions=True)
            self._heartbeat = None
        if self._pool is not None:
            try:
                async with self._db() as db:
                    await db.execute(
                        "UPDATE workers SET expires_at=clock_timestamp() WHERE id=%s",
                        (self._worker,),
                    )
            finally:
                await self._pool.close()
                self._pool = None

    async def _maintain(self):
        while True:
            await asyncio.sleep(min(1, self.lease_seconds / 3))
            try:
                async with self._db() as db:
                    sent_at = asyncio.get_running_loop().time()
                    cur = await db.execute(
                        """UPDATE workers SET expires_at=clock_timestamp()+make_interval(secs=>%s)
                        WHERE id=%s AND expires_at>clock_timestamp()""",
                        (self.lease_seconds, self._worker),
                    )
                    if not cur.rowcount:
                        for task in tuple(self._executing.values()):
                            task.cancel()
                        return
                    cancellations = await (
                        await db.execute(
                            """SELECT id FROM runs WHERE worker=%s AND status='running'
                        AND cancel_requested""",
                            (self._worker,),
                        )
                    ).fetchall()
                self._heartbeat_ack = sent_at
                for row in cancellations:
                    task = self._executing.pop(row["id"], None)
                    if task:
                        task.cancel()
                await self.recover()
            except (OSError, OperationalError, TimeoutError):
                # Every write checks the database clock independently. A lost or
                # expired identity cannot be resurrected by a late heartbeat.
                continue

    async def _fence(self, db):
        existing = await (
            await db.execute(
                """SELECT id FROM workers WHERE id=%s
            AND expires_at>clock_timestamp() FOR SHARE""",
                (self._worker,),
            )
        ).fetchone()
        if existing is None:
            raise RuntimeError("Worker lease expired")

    async def recover(self):
        async with self._db() as db:
            victims = await (
                await db.execute(
                    """SELECT r.* FROM runs r JOIN workers w ON r.worker=w.id
                WHERE r.agent=%s AND w.expires_at<=clock_timestamp() AND r.status IN ('queued','running')
                ORDER BY r.ordinal LIMIT 128 FOR UPDATE OF r SKIP LOCKED""",
                    (self.agent_id,),
                )
            ).fetchall()
            for row in victims:
                error = dict(reason="worker_lease_expired", effects="unknown", retry_allowed=False)
                calls = await (
                    await db.execute(
                        """SELECT id,intent FROM tool_calls WHERE agent=%s AND run_id=%s
                    AND NOT finished""",
                        (self.agent_id, row["id"]),
                    )
                ).fetchall()
                for call in calls:
                    await self._finish(db, row, call, error=error)
                await self._terminal(db, row, "interrupted", error)
            # Expired worker rows remain until every affected run was reconciled.
            await db.execute(
                """DELETE FROM workers w WHERE agent=%s AND expires_at<=clock_timestamp()
                AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.worker=w.id AND r.status IN ('queued','running'))""",
                (self.agent_id,),
            )

    async def _event(self, db, ident, kind, payload):
        cur = await db.execute(
            """INSERT INTO events(agent,run_id,kind,payload) VALUES (%s,%s,%s,%s) RETURNING seq""",
            (self.agent_id, ident, kind, Jsonb(payload)),
        )
        return RunEvent((await cur.fetchone())["seq"], ident, kind, payload)

    async def _run(self, db, ident, *, locked=False):
        query = "SELECT * FROM runs WHERE agent=%s AND id=%s" + (" FOR UPDATE" if locked else "")
        row = await (await db.execute(query, (self.agent_id, ident))).fetchone()
        if row is None:
            raise LookupError(ident)
        return row

    async def _owned(self, db, ident):
        await self._fence(db)
        row = await self._run(db, ident, locked=True)
        if row["worker"] != self._worker or row["status"] in TERMINAL_STATUSES:
            raise RuntimeError("This execution has lost write authority")
        return row

    async def _message(self, db, row, role, author, body, *, call_id=None, status="complete"):
        # The row lock serializes public sequence allocation for this session only.
        await db.execute(
            """UPDATE sessions SET updated_at=statement_timestamp(),last_activity_at=statement_timestamp()
                         WHERE agent=%s AND id=%s""",
            (self.agent_id, row["session_id"]),
        )
        await db.execute(
            """INSERT INTO messages(id,agent,session_id,run_id,role,author,body,call_id,status,session_seq)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,coalesce(max(session_seq),-1)+1
            FROM messages WHERE agent=%s AND session_id=%s""",
            (
                str(uuid4()),
                self.agent_id,
                row["session_id"],
                row["id"],
                role,
                author.key,
                Jsonb(body),
                call_id,
                status,
                self.agent_id,
                row["session_id"],
            ),
        )

    def _agent(self, row):
        return PrincipalRef(
            row["context"]["authority"]["authority"], row["tenant"], self.agent_id, "agent"
        )

    async def _guard_id(self, db, ident):
        await db.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,1))",
            (str((self.schema, self.agent_id, ident)),),
        )

    async def accept(self, request, context):
        return await self.accept_with_profile(request, context, None)

    async def accept_with_profile(self, request, context, profile):
        if type(request) not in (RunRequest, OperationRunRequest):
            raise ValueError("Operation continuation requires an operation-capable store")
        if request.session_id != context.session_id or context.agent_id != self.agent_id:
            raise ValueError("Mismatched execution context")
        async with self._db() as db:
            await self._fence(db)
            await self._guard_id(db, request.run_id)
            await self._guard_session(db, request.session_id)
            existing = await (
                await db.execute(
                    """SELECT * FROM runs WHERE agent=%s AND
                (id=%s OR (session_id=%s AND request_key=%s))""",
                    (self.agent_id, request.run_id, request.session_id, request.idempotency_key),
                )
            ).fetchall()
            digest = request.fingerprint(context)
            parent_ref = None
            if context.parent_run_id is not None:
                parent = await self._run(db, context.parent_run_id)
                if (
                    parent["tenant"] != context.tenant_id
                    or parent["session_id"] == request.session_id
                    or (not existing and parent["status"] != "running")
                ):
                    raise PermissionError("Parent is unavailable")
                parent_ref = SessionRef(
                    context.authority.authority,
                    context.tenant_id,
                    self.agent_id,
                    parent["session_id"],
                )
            session = await self._ensure_session(db, request.session_id, context, parent=parent_ref)
            if parent_ref and session["parent_id"] != parent_ref.session_id:
                raise IdempotencyConflict("Session belongs to a different parent")
            if existing:
                if (
                    len(existing) != 1
                    or existing[0]["id"] != request.run_id
                    or existing[0]["digest"] != digest
                ):
                    raise IdempotencyConflict("Different request or authority already admitted")
                return _record(existing[0]), False
            if session["archived"]:
                raise ValueError("Restore the archived session before starting a run")
            reservation = await (
                await db.execute(
                    "SELECT * FROM reservations WHERE agent=%s AND id=%s",
                    (self.agent_id, request.run_id),
                )
            ).fetchone()
            if reservation and (
                (reservation["tenant"], reservation["session_id"])
                != (context.tenant_id, context.session_id)
                or not set(_members(context)).issubset(reservation["participants"])
            ):
                raise IdempotencyConflict("Foreign cancellation reservation")
            status = "cancelled" if reservation else "queued"
            snapshot = context.snapshot()
            snapshot.pop("capabilities", None)
            cur = await db.execute(
                """INSERT INTO runs(agent,id,session_id,tenant,request_key,digest,request,context,
                worker,status,cancel_requested,parent_run_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (
                    self.agent_id,
                    request.run_id,
                    request.session_id,
                    context.tenant_id,
                    request.idempotency_key,
                    digest,
                    Jsonb(asdict(request)),
                    Jsonb(snapshot),
                    self._worker,
                    status,
                    bool(reservation),
                    context.parent_run_id,
                ),
            )
            row = await cur.fetchone()
            if isinstance(request, OperationRunRequest):
                await self._admit_segment(db, request, context, snapshot, bool(reservation))
            continuation = getattr(request, "continuation", None)
            ancestors = [v for v in (context.parent_run_id, getattr(continuation,"previous_run_id",None),
                                     getattr(continuation,"handoff_run_id",None)) if v]
            if ancestors:
                await db.execute("""INSERT INTO oa_document_run_sources(agent,run_id,session_id,source_id)
                    SELECT d.agent,%s,%s,d.source_id FROM oa_document_run_sources d
                    JOIN runs r ON r.id=d.run_id AND r.agent=d.agent WHERE r.tenant=%s AND d.agent=%s
                    AND d.run_id=ANY(%s) ON CONFLICT DO NOTHING""",
                    (request.run_id,request.session_id,context.tenant_id,self.agent_id,ancestors))
            await self._message(db, row, "user", context.author, request.input)
            await self._event(
                db, request.run_id, "run.accepted", dict(status=status, request_digest=digest)
            )
            if profile is not None and reservation is None:
                await self._event(db, request.run_id, "run.profile", profile)
            return _record(row), reservation is None

    async def get(self, ident):
        async with self._db() as db:
            row = await (
                await db.execute(
                    "SELECT * FROM runs WHERE agent=%s AND id=%s", (self.agent_id, ident)
                )
            ).fetchone()
            if row:
                return _record(row)
            reservation = await (
                await db.execute(
                    "SELECT * FROM reservations WHERE agent=%s AND id=%s", (self.agent_id, ident)
                )
            ).fetchone()
            if reservation:
                return RunRecord(
                    ident,
                    reservation["session_id"],
                    reservation["tenant"],
                    "cancelled",
                    "",
                    cancel_requested=True,
                )
            return None

    async def check_run_access(self, context, ident):
        async with self._db() as db:
            admitted = await (
                await db.execute(
                    f"SELECT {RUN_AUDIENCE} AS visible FROM runs r WHERE r.agent=%s AND r.id=%s",
                    (_audience(context), self.agent_id, ident),
                )
            ).fetchone()
            if admitted:
                if not admitted["visible"]:
                    raise PermissionError("Run unavailable")
                return
            row = await (
                await db.execute(
                    "SELECT participants FROM reservations WHERE agent=%s AND id=%s",
                    (self.agent_id, ident),
                )
            ).fetchone()
            if row and not set(_members(context)).issubset(row["participants"]):
                raise PermissionError("Foreign cancellation reservation")

    async def accepted_request(self, ident):
        async with self._db() as db:
            row = await self._run(db, ident)
        data, ctx = row["request"], row["context"]
        data["attachments"] = tuple(data["attachments"])
        if data.get("continuation"):
            data["continuation"] = OperationContinuation(**data["continuation"])
            request = OperationRunRequest(**data)
        else:
            request = RunRequest(**data)
        return AcceptedRunRequest(
            request,
            _principal(ctx["author"]),
            _principal(ctx["initiator"]),
            _principal(ctx["authority"]),
            tuple(map(_principal, ctx["audience"])),
            tuple(ctx["scopes"]),
            ctx["delegation_id"],
            ctx["ingress_id"],
            ctx["parent_run_id"],
            ctx["deferred"],
        )

    async def _terminal(self, db, row, status, output):
        cur = await db.execute(
            "UPDATE runs SET status=%s,output=%s,finished_at=clock_timestamp() WHERE agent=%s AND id=%s RETURNING *",
            (status, Jsonb(output), self.agent_id, row["id"]),
        )
        updated = await cur.fetchone()
        if status == "success" and output is not None:
            await self._message(db, row, "assistant", self._agent(row), output)
        await self._event(db, row["id"], "run." + status, dict(status=status, output=output))
        continuation = row["request"].get("continuation")
        if continuation:
            delay = 0
            if status in {"interrupted", "timed_out"} and isinstance(output, dict) and output.get("reason") == "operation_pending":
                delay = output.get("retry_after_seconds", 0)
            finished = updated["finished_at"].timestamp()
            due = min(finished + delay, continuation["expires_at"]) if delay else 0
            await db.execute(
                "UPDATE operations SET due_at=%s WHERE agent=%s AND current_run_id=%s AND state='active'",
                (due, self.agent_id, row["id"]),
            )
            await self._notices.publish(db, "operations", self.agent_id)
        await self._notices.publish(db, "session", row["session_id"])
        await self._notices.publish(db, "run", row["id"])
        return _record(updated)

    async def _guard_session(self, db, session_id):
        await db.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,2))",
            (str((self.schema, self.agent_id, session_id)),),
        )

    def observe_run(self, ident):
        return self._notices.observe("run", ident)

    @staticmethod
    async def _wait_for_notice(event):
        try:
            async with asyncio.timeout(5):
                await event.wait()
        except TimeoutError:
            pass

    async def _predecessor(self, ident):
        # This is an observation only. All writes still acquire and verify the
        # worker lease and run lock in their own transaction.
        async with self._db() as db:
            row = await (await db.execute(
                """SELECT r.cancel_requested, (
                       SELECT prev.id FROM runs prev
                       WHERE prev.agent=r.agent AND prev.session_id=r.session_id
                         AND prev.ordinal<r.ordinal AND prev.status IN ('queued','running')
                       ORDER BY prev.ordinal DESC LIMIT 1
                   ) AS predecessor
                   FROM runs r JOIN workers w ON w.id=r.worker
                   WHERE r.agent=%s AND r.id=%s AND r.worker=%s
                     AND r.status IN ('queued','running') AND w.expires_at>clock_timestamp()""",
                (self.agent_id, ident, self._worker),
            )).fetchone()
        if row is None:
            raise RuntimeError("This execution has lost write authority")
        if row["cancel_requested"]:
            raise asyncio.CancelledError()
        return row["predecessor"]

    async def wait_session_turn(self, ident):
        with self._notices.observe("run", ident) as changed:
            while (predecessor := await self._predecessor(ident)) is not None:
                # The immediately preceding live turn (or our own cancellation)
                # can unblock us. Other completions in this session needn't wake
                # its entire queue. Verify again after attaching the listener.
                with self._notices.observe("run", predecessor, event=changed):
                    changed.clear()
                    if await self._predecessor(ident) == predecessor:
                        await self._wait_for_notice(changed)

    @asynccontextmanager
    async def provider_release_scope(self, context):
        # Hold the session lock while releasing a provider that is no longer active.
        with self._notices.observe("session", context.session_id) as changed:
            while True:
                changed.clear()
                async with self._db() as db:
                    await self._guard_session(db, context.session_id)
                    active = await (await db.execute(
                        """SELECT 1 FROM runs WHERE agent=%s AND tenant=%s
                           AND session_id=%s AND status='running' LIMIT 1""",
                        (self.agent_id, context.tenant_id, context.session_id),
                    )).fetchone()
                    if not active:
                        yield
                        return
                await self._wait_for_notice(changed)

    async def transition(self, ident, status, *, output=None):
        if status != "running" and status not in TERMINAL_STATUSES:
            raise ValueError("Unsupported transition")
        async with self._db() as db:
            await self._fence(db)
            row = await self._run(db, ident, locked=True)
            if row["status"] in TERMINAL_STATUSES:
                return _record(row)
            if row["worker"] != self._worker:
                raise RuntimeError("Foreign worker cannot advance a run")
            if status != "running":
                if status == "success" and row["cancel_requested"]:
                    status, output = "cancelled", None
                return await self._terminal(db, row, status, output)
            if row["status"] != "queued":
                raise ValueError("Run already started")
            await self._guard_session(db, row["session_id"])
            before = await (
                await db.execute(
                    """SELECT 1 FROM runs WHERE agent=%s AND session_id=%s
                AND ordinal<%s AND status IN ('queued','running') LIMIT 1""",
                    (self.agent_id, row["session_id"], row["ordinal"]),
                )
            ).fetchone()
            if before:
                raise RuntimeError("An earlier admitted turn still owns this session")
            if row["cancel_requested"]:
                raise asyncio.CancelledError()
            cur = await db.execute(
                "UPDATE runs SET status='running' WHERE agent=%s AND id=%s RETURNING *",
                (self.agent_id, ident),
            )
            await self._event(db, ident, "run.running", {"status": "running", "output": None})
            task = asyncio.current_task()
            self._executing[ident] = task
            task.add_done_callback(lambda completed: self._executing.pop(ident, None))
            return _record(await cur.fetchone())

    async def request_cancel(self, ident):
        async with self._db() as db:
            return await self._cancel_in(db, ident)

    async def _cancel_in(self, db, ident):
        row = await self._run(db, ident, locked=True)
        if row["status"] in TERMINAL_STATUSES or row["cancel_requested"]:
            return _record(row)
        cur = await db.execute(
            "UPDATE runs SET cancel_requested=true WHERE agent=%s AND id=%s RETURNING *",
            (self.agent_id, ident),
        )
        await self._event(db, ident, "run.cancel_requested", {})
        await self._notices.publish(db, "session", row["session_id"])
        await self._notices.publish(db, "run", ident)
        return _record(await cur.fetchone())

    async def reserve_cancel(self, ident, context):
        if context.agent_id != self.agent_id:
            raise PermissionError("Foreign agent")
        async with self._db() as db:
            await self._guard_id(db, ident)
            actual = await (
                await db.execute(
                    "SELECT * FROM runs WHERE agent=%s AND id=%s", (self.agent_id, ident)
                )
            ).fetchone()
            if actual:
                if (actual["tenant"], actual["session_id"]) != (
                    context.tenant_id,
                    context.session_id,
                ):
                    raise PermissionError("Foreign run")
                return _record(actual)
            await db.execute(
                """INSERT INTO reservations(agent,id,tenant,session_id,participants) VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (self.agent_id, ident, context.tenant_id, context.session_id, _members(context)),
            )
            row = await (
                await db.execute(
                    "SELECT * FROM reservations WHERE agent=%s AND id=%s", (self.agent_id, ident)
                )
            ).fetchone()
            if (row["tenant"], row["session_id"]) != (
                context.tenant_id,
                context.session_id,
            ) or not set(_members(context)).issubset(row["participants"]):
                raise PermissionError("Foreign reservation")
            return RunRecord(
                ident, context.session_id, context.tenant_id, "cancelled", "", cancel_requested=True
            )

    async def append_event(self, ident, kind, payload):
        async with self._db() as db:
            await self._owned(db, ident)
            return await self._event(db, ident, kind, dict(payload))

    async def _events(self, ident, after, limit=None):
        if (
            type(after) is not int
            or after < 0
            or limit is not None
            and (type(limit) is not int or limit < 1)
        ):
            raise ValueError("Invalid event window")
        query = "SELECT * FROM events WHERE agent=%s AND run_id=%s AND seq>%s ORDER BY seq"
        args = (self.agent_id, ident, after)
        if limit is not None:
            query += " LIMIT %s"
            args += (limit,)
        async with self._db() as db:
            rows = await (await db.execute(query, args)).fetchall()
        return tuple(
            RunEvent(row["seq"], row["run_id"], row["kind"], row["payload"]) for row in rows
        )

    async def events(self, run_id, after=0):
        return await self._events(run_id, after)

    async def event_window(self, run_id, after, limit):
        return await self._events(run_id, after, limit)

    async def final_answer_for_run(self, ident):
        async with self._db() as db:
            row = await (
                await db.execute(
                    """SELECT payload FROM events WHERE agent=%s AND run_id=%s
                AND kind='run.stream' AND (payload->>'kind'='done' OR payload @> '{"privacy_redacted":true}')
                ORDER BY seq DESC LIMIT 1""",
                    (self.agent_id, ident),
                )
            ).fetchone()
        if row:
            payload = row["payload"]
            answer = payload.get("final_answer")
            if not payload.get("privacy_redacted") and isinstance(answer, dict):
                return answer
        return None

    async def children(self, ident):
        async with self._db() as db:
            rows = await (
                await db.execute(
                    "SELECT * FROM runs WHERE agent=%s AND parent_run_id=%s ORDER BY ordinal",
                    (self.agent_id, ident),
                )
            ).fetchall()
        return tuple(map(_record, rows))

    async def begin_tool(self, ident, call_id, binding, arguments):
        async with self._db() as db:
            row = await self._owned(db, ident)
            if row["status"] != "running" or row["cancel_requested"]:
                raise RuntimeError("Tool execution is unavailable")
            intent = dict(call_id=call_id, binding=dict(binding), arguments=dict(arguments))
            cur = await db.execute(
                """INSERT INTO tool_calls(agent,run_id,id,intent,invocation_id) VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
                (self.agent_id, ident, call_id, Jsonb(intent), uuid5(NAMESPACE_URL, canonical_json([ident, call_id]))),
            )
            if not cur.rowcount:
                raise IdempotencyConflict("This tool invocation already has a durable intent")
            await self._message(
                db, row, "tool", self._agent(row), "", call_id=call_id, status="streaming"
            )
            await self._event(db, ident, "tool.invoking", intent)

    async def _finish(self, db, row, call, *, result=None, error=None):
        cur = await db.execute(
            """UPDATE tool_calls SET result=%s,error=%s,finished=true
            WHERE agent=%s AND run_id=%s AND id=%s AND NOT finished""",
            (Jsonb(result), Jsonb(error), self.agent_id, row["id"], call["id"]),
        )
        if not cur.rowcount:
            raise IdempotencyConflict("Tool invocation already finished")
        failed = error is not None or isinstance(result, dict) and result.get("isError")
        event = dict(call["intent"], status="error" if failed else "success")
        event["error" if error is not None else "result"] = error if error is not None else result
        await db.execute(
            """UPDATE messages SET status='complete',body=%s
            WHERE agent=%s AND run_id=%s AND call_id=%s""",
            (Jsonb(error if error is not None else result), self.agent_id, row["id"], call["id"]),
        )
        await self._event(db, row["id"], "tool.completed", event)

    async def finish_tool(self, ident, call_id, *, result=None, error=None):
        async with self._db() as db:
            row = await self._owned(db, ident)
            call = await (
                await db.execute(
                    "SELECT id,intent FROM tool_calls WHERE agent=%s AND run_id=%s AND id=%s",
                    (self.agent_id, ident, call_id),
                )
            ).fetchone()
            if call is None:
                raise LookupError("Missing invocation intent")
            await self._finish(db, row, call, result=result, error=error)
