"""A session loop's firings are turns of its conversation, never child sessions."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core import ExecutionContext, PrincipalRef
from openagent_core.contracts import RunRecord
from openagent_core.runtime import execution_scope


class FakeRuntime(SimpleNamespace):
    def __init__(self, status="success"):
        super().__init__(submitted=[], status=status, settings=SimpleNamespace(environment=(), workspace=Path(TemporaryDirectory().name)))

    async def submit(self, request, context):
        self.submitted.append((request, context))
        return RunRecord(request.run_id, request.session_id, context.tenant_id, "queued", "digest")

    async def wait(self, run_id, context):
        request, _ = self.submitted[-1]
        return RunRecord(run_id, request.session_id, context.tenant_id, self.status, "digest",
                         output="checked: all green")


class Agent(SimpleNamespace):
    def __init__(self):
        super().__init__(name="agent", extensions=None, _mcp=None, turns=[])

    async def refresh_registries(self):
        pass

    async def run(self, message, user_id=None, session_id=None, author=None, **kwargs):
        self.turns.append((message, session_id, author))
        return "legacy turn done"


class SessionLoop(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from openagent_core.core.scheduler import Scheduler
        from openagent_core.engine import MemoryDB

        self.temp = TemporaryDirectory()
        self.db = MemoryDB(str(Path(self.temp.name) / "state.sqlite"))
        await self.db.connect()
        self.agent = Agent()
        self.scheduler = Scheduler(self.db, self.agent)
        task_id = await self.db.add_task("check hourly", "0 * * * *", "Check the queue and report.",
                                         target_session_id="session-chat-1")
        self.task = await self.db.get_task(task_id)

    async def asyncTearDown(self):
        await self.db.close()
        self.temp.cleanup()

    def context(self, *, deferred=True, delegation="grant-1"):
        user = PrincipalRef("openagent", "tenant", "alice")
        agent = PrincipalRef("openagent", "tenant", "agent", "agent")
        return ExecutionContext(agent, user, user, "automation:task:x:1", "agent", (user,),
                                delegation_id=delegation, deferred=deferred, parent_run_id="automation-run")

    async def test_firing_is_a_top_level_turn_of_the_target_session_under_the_task_grant(self):
        runtime = FakeRuntime()
        with execution_scope(runtime, self.context(), "automation-run"):
            await self.scheduler._execute_task(self.task)

        (request, context), = runtime.submitted
        self.assertEqual("session-chat-1", request.session_id)
        self.assertEqual("Check the queue and report.", request.input)
        self.assertEqual(("session-chat-1", None, "grant-1", True),
                         (context.session_id, context.parent_run_id, context.delegation_id, context.deferred))
        runs = await self.db.list_task_runs(self.task["id"])
        self.assertEqual([("success", "session-chat-1", "checked: all green")],
                         [(r["status"], r["session_id"], r["output"]) for r in runs])

    async def test_an_unsuccessful_turn_is_recorded_as_such(self):
        runtime = FakeRuntime(status="failed")
        with execution_scope(runtime, self.context(), "automation-run"):
            await self.scheduler._execute_task(self.task)

        runs = await self.db.list_task_runs(self.task["id"])
        self.assertEqual("failed", runs[0]["status"])

    async def test_without_a_captured_delegation_nothing_runs(self):
        runtime = FakeRuntime()
        with execution_scope(runtime, self.context(deferred=False, delegation=None), "automation-run"):
            await self.scheduler._execute_task(self.task)

        self.assertEqual([], runtime.submitted)
        runs = await self.db.list_task_runs(self.task["id"])
        self.assertEqual("failed", runs[0]["status"])

    async def test_hosts_without_the_public_runtime_run_a_plain_turn_in_the_session(self):
        await self.scheduler._execute_task(self.task)

        (message, session_id, author), = self.agent.turns
        self.assertEqual(("Check the queue and report.", "session-chat-1"), (message, session_id))
        self.assertEqual("check hourly", author.get("display") or author.get("name") or author.get("label"))

    async def test_clients_with_the_conversation_open_are_told_the_turn_ended(self):
        from openagent_core.stream.child_stream import set_child_broadcast_sink

        frames = []

        async def sink(frame):
            frames.append(frame)

        for runtime in (FakeRuntime(), FakeRuntime(status="failed")):
            with execution_scope(runtime, self.context(), "automation-run"):
                set_child_broadcast_sink(sink)
                await self.scheduler._execute_task(self.task)

        self.assertEqual(
            [("turn_complete", "session-chat-1", "completed"), ("turn_complete", "session-chat-1", "error")],
            [(f["kind"], f["session_id"], f["reason"]) for f in frames],
        )
        # The frame carries no content: clients reload through the history API.
        self.assertNotIn("text", frames[0])

    async def test_a_failing_sink_does_not_fail_the_firing(self):
        from openagent_core.stream.child_stream import set_child_broadcast_sink

        async def broken(frame):
            raise RuntimeError("socket gone")

        with execution_scope(FakeRuntime(), self.context(), "automation-run"):
            set_child_broadcast_sink(broken)
            await self.scheduler._execute_task(self.task)

        runs = await self.db.list_task_runs(self.task["id"])
        self.assertEqual("success", runs[0]["status"])

    async def test_loop_firings_stay_out_of_the_activity_feed(self):
        from openagent_core.memory.operational.automation import project_automation_pending

        plain_id = await self.db.add_task("plain", "0 * * * *", "Do it.")
        loop_run = await self.db.add_task_run(task_id=self.task["id"])
        plain_run = await self.db.add_task_run(task_id=plain_id, session_id="scheduler:plain:1")
        conn = await self.db._ensure_connected()
        while not (await project_automation_pending(conn))[1]:
            pass
        await conn.commit()
        await self.db.update_task_run(loop_run, status="success", session_id="session-chat-1")
        while not (await project_automation_pending(conn))[1]:
            pass
        await conn.commit()

        rows = await (await conn.execute(
            "SELECT resource_id FROM activity_items WHERE kind='scheduled_run' AND deleted_at_ms IS NULL"
        )).fetchall()
        self.assertEqual([plain_run], [row[0] for row in rows])

    async def test_classic_tasks_are_unchanged(self):
        task_id = await self.db.add_task("plain", "0 * * * *", "Do it.")
        task = await self.db.get_task(task_id)

        self.assertIsNone(task["target_session_id"])


def test_the_agent_can_create_a_loop_of_its_current_conversation(tmp_path):
    import asyncio

    from openagent_core.automation import AutomationRepository, build_automation_toolkit
    from openagent_core.memory.db import SCHEMA_SQL

    repository = AutomationRepository(tmp_path / "agent.db")
    toolkit = build_automation_toolkit("scheduled_task")
    user = PrincipalRef("openagent", "network", "alice")

    class Management:
        async def call(self, kind, operation, arguments, scope):
            return await repository.call(kind, operation, arguments, scope)

    async def exercise():
        async with repository.session() as connection:
            await connection.executescript(SCHEMA_SQL)
            await connection.commit()
        runtime = SimpleNamespace(services=SimpleNamespace(automation_management=Management()),
                                  settings=SimpleNamespace(environment=()))
        create = toolkit.async_functions["create_scheduled_task"].entrypoint
        chat = ExecutionContext(user, user, user, "session-chat-7", "agent", (user,), ingress_id="verified")
        with execution_scope(runtime, chat, "run"):
            loop = await create(name="hourly here", cron_expression="0 * * * *", prompt="Check.",
                                in_this_conversation=True)
            plain = await create(name="standalone", cron_expression="0 * * * *", prompt="Check.")
        automation = ExecutionContext(user, user, user, "automation:task:x:1", "agent", (user,),
                                      ingress_id="verified")
        with execution_scope(runtime, automation, "run2"):
            try:
                await create(name="nested", cron_expression="0 * * * *", prompt="x", in_this_conversation=True)
                refused = False
            except ValueError:
                refused = True
        return loop, plain, refused

    loop, plain, refused = asyncio.run(exercise())

    def target(result):
        task = result.get("task", result)
        return task.get("target_session_id")

    assert target(loop) == "session-chat-7"
    assert target(plain) is None
    assert refused
