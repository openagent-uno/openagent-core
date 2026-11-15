"""A conversation goal keeps the agent working until it is achieved, blocked or out of budget."""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core import session_goal
from openagent_core.session_goal import GoalStore, after_reply, parse_marker


class GoalFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from openagent_core.engine import MemoryDB

        self._temp = TemporaryDirectory()
        self.db = MemoryDB(str(Path(self._temp.name) / "state.sqlite"))
        await self.db.connect()
        self.store = GoalStore(self.db)

    async def asyncTearDown(self):
        await self.db.close()
        self._temp.cleanup()


class GoalStoreBehaviour(GoalFixture):
    async def test_budget_runs_out_and_pauses_the_goal(self):
        await self.store.set("s", "ship the report", max_continuations=2)

        first = await self.store.consume_continuation("s")
        second = await self.store.consume_continuation("s")
        third = await self.store.consume_continuation("s")

        self.assertEqual((1, 2), (first.continuations, second.continuations))
        self.assertEqual(session_goal.PAUSED, third.status)
        self.assertIn("limit of 2", third.paused_reason)
        self.assertIsNone(await self.store.consume_continuation("s"))

    async def test_resume_restores_the_budget_and_set_replaces_the_goal(self):
        await self.store.set("s", "first", max_continuations=1)
        await self.store.consume_continuation("s")
        await self.store.pause("s", "waiting")

        resumed = await self.store.resume("s")
        replaced = await self.store.set("s", "second")

        self.assertEqual((session_goal.ACTIVE, 0), (resumed.status, resumed.continuations))
        self.assertEqual(("second", session_goal.DEFAULT_MAX_CONTINUATIONS), (replaced.objective, replaced.max_continuations))

    async def test_goals_belong_to_one_session(self):
        await self.store.set("a", "only here")

        self.assertIsNone(await self.store.get("b"))
        self.assertIsNone(await after_reply(self.db, "b", "done"))


class Markers(unittest.TestCase):
    def test_last_marker_line_wins_and_prose_mentions_do_not_count(self):
        reply = ("I will write [goal: achieved] only when done.\n"
                 "[goal: blocked] need the API key\n"
                 "**[goal: achieved] report published**")

        self.assertEqual(("achieved", "report published"), parse_marker(reply))
        self.assertIsNone(parse_marker("I will write [goal: achieved] only when done."))


class AfterReply(GoalFixture):
    async def test_open_goal_continues_with_a_reminder(self):
        await self.store.set("s", "fix the bug")

        reminder = await after_reply(self.db, "s", "I found the cause, fixing next.")

        self.assertIn("fix the bug", reminder)
        self.assertEqual(1, (await self.store.get("s")).continuations)

    async def test_achieved_and_blocked_markers_stop(self):
        await self.store.set("s", "fix the bug")
        self.assertIsNone(await after_reply(self.db, "s", "Done.\n[goal: achieved] tests green"))
        achieved = await self.store.get("s")

        await self.store.set("t", "deploy")
        self.assertIsNone(await after_reply(self.db, "t", "[goal: blocked] need prod access"))
        blocked = await self.store.get("t")

        self.assertEqual((session_goal.ACHIEVED, "tests green"), (achieved.status, achieved.outcome))
        self.assertEqual((session_goal.PAUSED, "need prod access"), (blocked.status, blocked.paused_reason))

    async def test_paused_goal_does_not_continue(self):
        await self.store.set("s", "fix the bug")
        await self.store.pause("s")

        self.assertIsNone(await after_reply(self.db, "s", "still working"))


class GoalCommand(GoalFixture):
    async def run_command(self, argument):
        from openagent_core.chat_commands import (ChatCommandHost, ChatCommandRequest,
                                                  ChatCommandService, SessionRef)

        agent = SimpleNamespace(memory_db=self.db)
        service = ChatCommandService(agent, ChatCommandHost())
        return await service.execute(ChatCommandRequest("goal", SessionRef("s"), argument))

    async def test_set_status_pause_resume_done_clear(self):
        set_result = await self.run_command("publish the weekly report")
        status = await self.run_command("status")
        paused = await self.run_command("pause")
        resumed = await self.run_command("resume")
        capped = await self.run_command("max 5")
        done = await self.run_command("done")
        cleared = await self.run_command("clear")

        self.assertIn("Goal set: publish the weekly report", set_result.text)
        self.assertIn("Goal (active)", status.text)
        self.assertIn("Goal (paused)", paused.text)
        self.assertIn("Goal resumed", resumed.text)
        self.assertIn("0 of 5", capped.text)
        self.assertIn("Goal (achieved)", done.text)
        self.assertEqual("Goal cleared.", cleared.text)
        self.assertIsNone(await self.store.get("s"))


class AgentLoop(GoalFixture):
    async def exercise(self, mode, replies, *, max_continuations=20):
        from openagent_core.core.agent import Agent
        from openagent_core.models.base import ModelResponse

        await self.store.set("goal-session", "make the tests pass", max_continuations=max_continuations)
        inputs = []

        class Model:
            history_mode = "caller"
            model = "fixture"

            def effective_model_id(self, _sid=None):
                return self.model

            async def close_session(self, _sid):
                pass

            def next_reply(self, messages, system):
                inputs.append((messages[0]["content"], system))
                return replies[min(len(inputs), len(replies)) - 1]

            async def stream(self, messages, system=None, session_id=None, on_status=None, on_final_answer=None):
                text = self.next_reply(messages, system)
                if on_final_answer is not None:
                    await on_final_answer({"text": text})
                yield text

            async def generate(self, messages, system=None, **kwargs):
                text = self.next_reply(messages, system)
                return ModelResponse(content=text, final_answer={"text": text})

        agent = Agent(name="fixture", model=Model(), memory=self.db, system_prompt="fixture")
        try:
            if mode == "stream":
                events = [e async for e in agent.run_stream("start", session_id="goal-session")]
                breaks = sum(e.get("kind") == "iteration_break" for e in events)
            else:
                await agent.run("start", session_id="goal-session")
                breaks = None
        finally:
            await agent.shutdown()
        return inputs, breaks

    async def assert_runs_until_achieved(self, mode):
        inputs, breaks = await self.exercise(mode, ["step one", "step two", "all green\n[goal: achieved] 12 tests pass"])

        goal = await self.store.get("goal-session")
        self.assertEqual(3, len(inputs))
        self.assertIn("<session-goal>", inputs[0][1])
        self.assertIn("make the tests pass", inputs[1][0])
        self.assertEqual((session_goal.ACHIEVED, "12 tests pass", 2), (goal.status, goal.outcome, goal.continuations))
        if breaks is not None:
            self.assertEqual(2, breaks)

    async def test_generate_runs_until_achieved(self):
        await self.assert_runs_until_achieved("generate")

    async def test_stream_runs_until_achieved(self):
        await self.assert_runs_until_achieved("stream")

    async def test_budget_stops_an_unreachable_goal(self):
        inputs, _ = await self.exercise("stream", ["still working"], max_continuations=2)

        goal = await self.store.get("goal-session")
        self.assertEqual(3, len(inputs))
        self.assertEqual(session_goal.PAUSED, goal.status)

    async def test_session_without_goal_runs_one_turn(self):
        from openagent_core.core.agent import Agent
        from openagent_core.models.base import ModelResponse

        calls = []

        class Model:
            history_mode = "caller"
            model = "fixture"

            def effective_model_id(self, _sid=None):
                return self.model

            async def close_session(self, _sid):
                pass

            async def generate(self, messages, system=None, **kwargs):
                calls.append(system)
                return ModelResponse(content="hi", final_answer={"text": "hi"})

        agent = Agent(name="fixture", model=Model(), memory=self.db, system_prompt="fixture")
        try:
            await agent.run("hello", session_id="plain-session")
        finally:
            await agent.shutdown()
        self.assertEqual(1, len(calls))
        self.assertNotIn("<session-goal>", calls[0])
