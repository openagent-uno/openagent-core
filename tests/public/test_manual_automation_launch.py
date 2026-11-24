"""Manual host requests retain their grants and fire at most once across workers."""
import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from openagent_core import ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.automation_runtime import AutomationRuntime
from openagent_core.engine import MemoryDB
from openagent_storage_sqlite import SqliteRuntimeStore

pytestmark = pytest.mark.asyncio


class Host:
    def __init__(self, runtime):
        self.runtime = runtime
        self.effects = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def authorized_definition_ids(self, kind):
        return ()

    async def run_task(self, scheduler, row, *, trigger, request_id, payload, execute):
        assert request_id == row["_manual_request_id"]
        run_id = row["_preclaimed_run_id"]
        user = PrincipalRef("issuer", "tenant", "caller")
        context = ExecutionContext(user, user, user, run_id, "agent", (user,), deferred=True,
                                   delegation_id=request_id)

        async def effect(request, verified, runtime):
            assert verified.delegation_id == request_id
            self.effects.append(request.run_id)
            self.started.set()
            await self.release.wait()
            await scheduler.db.update_task_run(run_id, status="success", output="done")
            return "done"

        await self.runtime.execute_operation(
            RunRequest(run_id, run_id, request_id, "test"), context, SimpleNamespace(execute=effect))
        return (await self.runtime.wait(run_id, context)).output


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return context.tenant_id == resource.tenant_id


class Delegations:
    allowed = True

    async def validate(self, context):
        return self.allowed and context.delegation_id == "request"

    @asynccontextmanager
    async def execution_scope(self, context):
        yield


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
async def test_exact_launch_cross_worker_receipts_and_current_authority(tmp_path, backend):
    if backend == "postgres":
        dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
        if not dsn:
            pytest.skip("Set OPENAGENT_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
        from openagent_storage_postgres import PostgresRuntimeStore
        store = PostgresRuntimeStore(dsn, agent_id="agent", schema="test_manual_" + uuid4().hex)
    else:
        store = SqliteRuntimeStore(tmp_path / "canonical.db")
    runtime = Runtime(RuntimeSettings("agent", tmp_path), RuntimeServices(store, object(), Policy(), delegations=Delegations()))
    await runtime.start()
    a, b = (MemoryDB(str(tmp_path / "definitions.db")) for _ in range(2))
    host = Host(runtime)
    first, other = (AutomationRuntime(db, object(), host, enabled_domains=("scheduler",)) for db in (a, b))
    try:
        await first.start(runtime)
        await other.start(runtime)
        task = await a.add_task("One", "0 9 * * *", "test")
        second = await a.add_task("Two", "0 9 * * *", "test")
        row = await a.get_task(task)
        key = "request"
        run_id = str(uuid5(NAMESPACE_URL, f"task:{task}:{key}"))
        primary = first.launch("scheduled_task", row, request_id=key)
        await asyncio.wait_for(host.started.wait(), 5)
        duplicate = other.launch("scheduled_task", row, request_id=key)
        assert await asyncio.wait_for(duplicate, 5) is None
        for definition, inputs in [(row, {"changed": True}), (await a.get_task(second), {})]:
            with pytest.raises(ValueError, match="already bound"):
                await other.launch("scheduled_task", definition, request_id=key, inputs=inputs)
        host.release.set()
        assert await asyncio.wait_for(primary, 5) == "done"
        assert await other.launch("scheduled_task", row, request_id=key) is None
        runtime.services.delegations.allowed = False
        with pytest.raises(PermissionError, match="delegation"):
            await other.launch("scheduled_task", row, request_id="revoked")
        assert (await store.get(run_id)).status == "success"
        assert (await b.get_task_run(run_id))["status"] == "success"
        assert host.effects == [run_id]
    finally:
        host.release.set()
        await first.close()
        await other.close()
        await a.close()
        await b.close()
        await runtime.close()


async def test_recovery_cancel_and_unbound_legacy_projection(tmp_path):
    db = MemoryDB(str(tmp_path / "definitions.db"))
    await db.connect()
    try:
        task = await db.add_task("One", "0 9 * * *", "test")
        key = "recover"
        run_id = await db.enqueue_manual_automation("scheduled_task", task, key, {})
        [old] = await db.claim_pending_task_requests(only_id=run_id, reserve_runs=True)
        await db.close()
        await db.connect()
        assert await db.reap_orphan_task_runs() == 0
        [new] = await db.claim_pending_task_requests(only_id=run_id, reserve_runs=True)
        assert (new["id"], new["host_request_id"]) == (run_id, key)
        assert not await db.begin_requested_task_run(old, session_id="old")
        assert await db.begin_requested_task_run(new, session_id="session")
        await db.close()
        await db.connect()
        await db.reap_orphan_task_runs()
        assert not await db.claim_pending_task_requests(only_id=run_id, reserve_runs=True)
        stop = await db.enqueue_manual_automation("scheduled_task", task, "stop", {})
        assert await db.flag_task_runs_cancelling(task) == [stop]
        assert (await db.get_task_run(stop))["status"] == "cancelled"
        assert not await db.claim_pending_task_requests(only_id=stop, reserve_runs=True)
        await db.add_task_run(task_id=task, run_id="legacy")
        with pytest.raises(ValueError, match="no matching manual request"):
            await db.enqueue_manual_automation("scheduled_task", task, "legacy-key", {}, run_id="legacy")
    finally:
        await db.close()


async def test_close_during_detached_admission(tmp_path):
    who = PrincipalRef("issuer", "tenant", "caller")
    context = ExecutionContext(who, who, who, "session", "agent", (who,))
    store = SqliteRuntimeStore(tmp_path / "canonical.db")
    attempted = asyncio.Event()

    class Pending:
        async def start(self, runtime):
            self.runtime = runtime

        async def close(self):
            with pytest.raises(RuntimeError, match="not accepting"):
                await self.runtime.submit(RunRequest("run", "session", "key", "test"), context)
            attempted.set()

    runtime = Runtime(RuntimeSettings("agent", tmp_path), RuntimeServices(store, object(), Policy()), modules=(Pending(),))
    await runtime.start()
    await asyncio.wait_for(runtime.close(), 3)
    assert attempted.is_set()
