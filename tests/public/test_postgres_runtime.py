"""Real PostgreSQL checks. The DSN must point to a disposable test database."""

import asyncio
from contextlib import asynccontextmanager
import os
from uuid import uuid4

import pytest
from aiohttp.test_utils import TestClient, TestServer

pytestmark = pytest.mark.asyncio


@pytest.fixture
def pg():
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("OPENAGENT_TEST_POSTGRES_DSN is not configured")
    from openagent_storage_postgres import PostgresRuntimeStore

    schema = "oa_test_" + uuid4().hex
    return lambda: PostgresRuntimeStore(dsn, agent_id="agent", schema=schema, lease_seconds=6)


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied and context.tenant_id == resource.tenant_id


class Executor:
    def __init__(self):
        self.started = asyncio.Queue()
        self.release = asyncio.Event()

    async def execute(self, request, context, runtime):
        await self.started.put(request.run_id)
        if request.input == "hold":
            await self.release.wait()
        return request.input


def make_context(user="alice", session="session", tenant="tenant"):
    from openagent_core import PrincipalRef, ExecutionContext

    p = PrincipalRef("local", tenant, user)
    return ExecutionContext(p, p, p, session, "agent", (p,))


def worker(store, executor, policy, tmp_path):
    from openagent_core import Runtime, RuntimeServices, RuntimeSettings

    return Runtime(RuntimeSettings("agent", tmp_path), RuntimeServices(store, executor, policy))


async def test_two_gateways_admission_cancellation_and_revocation(pg, tmp_path):
    from openagent_core.contracts import SessionRef
    from openagent_gateway import create_app

    a, b = pg(), pg()
    first, second = Executor(), Executor()
    policy = Policy()
    ctx = make_context()

    @asynccontextmanager
    async def resolve(request, action, run_id, session_id):
        yield make_context(
            user=request.headers.get("X-Test-Principal", "alice"),
            session=session_id or ctx.session_id,
        )

    async with TestClient(
        TestServer(create_app(worker(a, first, policy, tmp_path), resolve))
    ) as left:
        async with TestClient(
            TestServer(create_app(worker(b, second, policy, tmp_path), resolve))
        ) as right:
            body = dict(run_id="one", session_id="session", idempotency_key="one", input="hold")
            assert (await left.post("/api/v1/runs", json=body)).status == 202
            assert await asyncio.wait_for(first.started.get(), 2) == "one"
            assert (await right.post("/api/v1/runs", json=body)).status == 202
            assert (
                await right.post("/api/v1/runs", json={**body, "input": "changed"})
            ).status == 409
            assert (
                await right.post(
                    "/api/v1/runs",
                    json={**body, "idempotency_key": "two", "run_id": "two", "input": "after"},
                )
            ).status == 202
            assert (await b.get("one")).status == "running"
            await b.recover()
            assert (await b.get("one")).status == "running"
            assert (await b.get("two")).status == "queued"
            for path in ("/api/v1/runs/one", "/api/v1/runs/one/events"):
                assert (await right.get(path, headers={"X-Test-Principal": "bob"})).status == 403
            denied = await a.list_sessions(make_context("bob"), include_archived=True, limit=10)
            assert denied["sessions"] == []
            with pytest.raises(PermissionError):
                await a.read_session(
                    SessionRef("local", "tenant", "agent", "session"),
                    make_context("bob"),
                    limit=100,
                )
            assert (await right.post("/api/v1/runs/one/cancel", json={})).status == 200
            async with asyncio.timeout(5):
                while (await a.get("one")).status != "cancelled":
                    await asyncio.sleep(0.025)
                while (await a.get("two")).status != "success":
                    await asyncio.sleep(0.025)
            assert await second.started.get() == "two"
            assert second.started.empty()
            page = await a.read_session(
                SessionRef("local", "tenant", "agent", "session"), ctx, limit=10
            )
            assert [m["text"] for m in page["messages"]] == ["hold", "after", "after"]
            assert len({m["id"] for m in page["messages"]}) == 3
            events = await b.event_window("two", 0, 2)
            assert len(events) == 2
            assert (await b.event_window("two", events[-1].cursor, 2))[0].cursor > events[-1].cursor
            # Revoke between provider start and publication, with both gateways live.
            assert (
                await left.post(
                    "/api/v1/runs", json={**body, "idempotency_key": "three", "run_id": "three"}
                )
            ).status == 202
            assert await asyncio.wait_for(first.started.get(), 2) == "three"
            policy.denied.add("run.publish")
            first.release.set()
            async with asyncio.timeout(4):
                while (await a.get("three")).status != "failed":
                    await asyncio.sleep(0.025)
            assert not any(
                m["role"] == "assistant" and m["run_id"] == "three"
                for m in (
                    await a.read_session(
                        SessionRef("local", "tenant", "agent", "session"), ctx, limit=10
                    )
                )["messages"]
            )
            policy.denied.add("run.replay")
            assert (await right.get("/api/v1/runs/two/events")).status == 403


async def test_expired_owner_preserves_uncertain_effect_and_rejects_stale_writes(pg):
    from openagent_core import RunRequest

    a, b = pg(), pg()
    await a.start()
    await b.start()
    try:
        context = make_context()
        req = RunRequest("r", "session", "key", "hello")
        assert (await a.accept(req, context))[1]
        await a.transition("r", "running")
        await a.begin_tool("r", "tool", {"name": "operation", "source_id": "host"}, {"value": 1})
        # Simulate a paused worker at the authority boundary using the database clock.
        async with b._db() as db:
            await db.execute(
                "UPDATE workers SET expires_at=clock_timestamp() WHERE id=%s", (a._worker,)
            )
        await b.recover()
        observed = await b.get("r")
        assert observed.status == "interrupted" and observed.output["retry_allowed"] is False
        assert (await b.events("r"))[-2].payload["error"]["effects"] == "unknown"
        with pytest.raises(RuntimeError):
            await a.finish_tool("r", "tool", result="late success")
        assert not (await b.accept(req, context))[1]
    finally:
        await a.close()
        await b.close()


async def test_terminated_worker_recovery_unblocks_session(pg, tmp_path):
    import sys
    from openagent_core import RunRequest

    survivor = pg()
    child_code = """
import asyncio, os, sys
from openagent_core import ExecutionContext, PrincipalRef, RunRequest
from openagent_storage_postgres import PostgresRuntimeStore
async def hold():
    store = PostgresRuntimeStore(os.environ['OPENAGENT_TEST_POSTGRES_DSN'], agent_id='agent',
                                schema=sys.argv[1], lease_seconds=3)
    await store.start()
    principal = PrincipalRef('local', 'tenant', 'alice')
    context = ExecutionContext(principal, principal, principal, 'session', 'agent', (principal,))
    await store.accept(RunRequest('gone', 'session', 'gone', 'first'), context)
    await store.transition('gone','running')
    await store.begin_tool('gone', 'effect', {'name':'effect','source_id':'host'}, {})
    print("intent committed", flush=True)
    await asyncio.Event().wait()
asyncio.run(hold())
"""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        child_code,
        survivor.schema,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(child.stdout.readline(), 10) == b"intent committed\n"
        next_executor = Executor()
        runtime = worker(survivor, next_executor, Policy(), tmp_path)
        await runtime.start()
        try:
            await runtime.submit(RunRequest("next", "session", "next", "second"), make_context())
            assert (await survivor.get("gone")).status == "running"
            assert (await survivor.get("next")).status == "queued"
            child.kill()
            await child.wait()
            observed = await asyncio.wait_for(runtime.wait("next", make_context()), 10)
            assert observed.output == "second"
            assert (await runtime.get_run("gone", make_context())).status == "interrupted"
            assert (await survivor.events("gone"))[-2].payload["error"]["effects"] == "unknown"
            assert await next_executor.started.get() == "next"
            assert next_executor.started.empty()
        finally:
            await runtime.close()
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


async def test_pre_admission_cancellation_is_principal_scoped(pg, tmp_path):
    from openagent_core import RunRequest
    from openagent_core.contracts import IdempotencyConflict

    store = pg()
    runtime = worker(store, Executor(), Policy(), tmp_path)
    await runtime.start()
    try:
        assert (await runtime.cancel("reserved", make_context(), reserve=True)).cancel_requested
        with pytest.raises(PermissionError):
            await runtime.get_run("reserved", make_context("bob"))
        request = RunRequest("reserved", "session", "key", "hello")
        with pytest.raises(IdempotencyConflict):
            await runtime.submit(request, make_context("bob"))
        assert (await runtime.submit(request, make_context())).status == "cancelled"
    finally:
        await runtime.close()
