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

    async def test_classic_tasks_are_unchanged(self):
        task_id = await self.db.add_task("plain", "0 * * * *", "Do it.")
        task = await self.db.get_task(task_id)

        self.assertIsNone(task["target_session_id"])
