"""A goal a runtime restart interrupted starts again, or says why it stopped.

After a restart (for example the process ran out of memory) the interrupted
turn is over and nothing continued the goal, which still read as active. A
host with the owner's durable delegation resumes it once; otherwise, and on a
second restart in a row, the goal pauses with the reason.
"""
import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from openagent_core import session_goal
from openagent_core.session_goal import GoalStore, resume_after_restart


def _insert_run(path, run_id, session_id, ordinal, status, *, reason=None, key=None):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=OFF")
    values = {"id": run_id, "session_id": session_id, "ordinal": ordinal, "status": status,
              "status_raw": status, "completeness": "complete",
              "idempotency_key": key or run_id,
              "output_json": json.dumps({"reason": reason}) if reason else None}
    for _cid, name, kind, notnull, default, _pk in conn.execute("PRAGMA table_info(session_runs)"):
        if name not in values and notnull and default is None:
            values[name] = 1 if "INT" in kind.upper() else "{}" if name.endswith("_json") else "x"
    if "runner_kind" in values and values["runner_kind"] == "x":
        values["runner_kind"] = "agent"
    columns = ", ".join(values)
    conn.execute(f"INSERT INTO session_runs ({columns}) VALUES ({', '.join('?' for _ in values)})",
                 tuple(values.values()))
    conn.commit()
    conn.close()


class GoalRestart(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from openagent_core.engine import MemoryDB

        self._temp = TemporaryDirectory()
        self.path = str(Path(self._temp.name) / "state.sqlite")
        self.db = MemoryDB(self.path)
        await self.db.connect()
        self.store = GoalStore(self.db)
        self.submitted = []
        self.delegation = object()

    async def asyncTearDown(self):
        await self.db.close()
        self._temp.cleanup()

    async def _authority(self, session_id):
        return self.delegation

    async def _submit(self, request, context):
        self.submitted.append((request, context))

    async def _resume(self):
        return await resume_after_restart(self.db, authority=self._authority, submit=self._submit)

    async def test_an_interrupted_goal_resumes_once_under_its_delegation(self):
        await self.store.set("s", "triage every report")
        _insert_run(self.path, "run-1", "s", 0, "interrupted", reason="runtime_restarted")
        results = await self._resume()
        self.assertEqual(["resumed"], [r["action"] for r in results])
        request, context = self.submitted[0]
        self.assertIs(self.delegation, context)
        self.assertEqual(("s", "goal-restart:run-1"), (request.session_id, request.idempotency_key))
        self.assertIn("runtime restarted", request.input)
        self.assertIn("triage every report", request.input)
        goal = await self.store.get("s")
        self.assertEqual((session_goal.ACTIVE, 1), (goal.status, goal.continuations))

    async def test_without_a_delegation_the_goal_pauses_with_the_reason(self):
        await self.store.set("s", "triage every report")
        _insert_run(self.path, "run-1", "s", 0, "interrupted", reason="runtime_restarted")
        self.delegation = None
        results = await self._resume()
        self.assertEqual([("paused", "no_delegation")], [(r["action"], r["reason"]) for r in results])
        goal = await self.store.get("s")
        self.assertEqual(session_goal.PAUSED, goal.status)
        self.assertIn("runtime restarted", goal.paused_reason)
        self.assertEqual([], self.submitted)

    async def test_a_second_restart_in_a_row_pauses_instead_of_looping(self):
        await self.store.set("s", "triage every report")
        _insert_run(self.path, "run-1", "s", 0, "interrupted", reason="runtime_restarted")
        _insert_run(self.path, "run-2", "s", 1, "interrupted", reason="runtime_restarted",
                    key="goal-restart:run-1")
        results = await self._resume()
        self.assertEqual([("paused", "repeated_restart")], [(r["action"], r["reason"]) for r in results])
        self.assertIn("twice", (await self.store.get("s")).paused_reason)
        self.assertEqual([], self.submitted)

    async def test_only_active_goals_whose_last_run_a_restart_interrupted_are_touched(self):
        await self.store.set("finished", "a")
        _insert_run(self.path, "a-1", "finished", 0, "interrupted", reason="runtime_restarted")
        _insert_run(self.path, "a-2", "finished", 1, "success")
        await self.store.set("cancelled", "b")
        _insert_run(self.path, "b-1", "cancelled", 0, "cancelled")
        await self.store.set("paused", "c")
        await self.store.pause("paused", "Paused by the user.")
        _insert_run(self.path, "c-1", "paused", 0, "interrupted", reason="runtime_restarted")
        await self.store.set("idle", "d")
        self.assertEqual([], await self._resume())
        self.assertEqual([], self.submitted)
        self.assertEqual("Paused by the user.", (await self.store.get("paused")).paused_reason)

    async def test_a_spent_budget_pauses_and_a_failing_submit_does_not_stop_the_others(self):
        await self.store.set("spent", "a", max_continuations=0)
        _insert_run(self.path, "a-1", "spent", 0, "interrupted", reason="runtime_restarted")
        await self.store.set("broken", "b")
        _insert_run(self.path, "b-1", "broken", 0, "interrupted", reason="runtime_restarted")
        await self.store.set("fine", "c")
        _insert_run(self.path, "c-1", "fine", 0, "interrupted", reason="runtime_restarted")

        async def submit(request, context):
            if request.session_id == "broken":
                raise RuntimeError("refused")
            self.submitted.append((request, context))

        results = {r["session_id"]: r["action"] for r in
                   await resume_after_restart(self.db, authority=self._authority, submit=submit)}
        self.assertEqual({"spent": "paused", "broken": "failed", "fine": "resumed"}, results)
        self.assertEqual(["fine"], [request.session_id for request, _ in self.submitted])


class GoalChangesReachTheHost(unittest.IsolatedAsyncioTestCase):
    """The host hears every change a person makes, to capture or withdraw a delegation."""

    async def asyncSetUp(self):
        from openagent_core.engine import MemoryDB

        self._temp = TemporaryDirectory()
        self.db = MemoryDB(str(Path(self._temp.name) / "state.sqlite"))
        await self.db.connect()

    async def asyncTearDown(self):
        await self.db.close()
        self._temp.cleanup()

    async def test_changes_notify_and_reads_or_a_failing_host_do_not(self):
        from types import SimpleNamespace

        from openagent_core.chat_commands import (ChatCommandHost, ChatCommandRequest,
                                                  ChatCommandService, SessionRef)

        seen = []

        class Host(ChatCommandHost):
            async def goal_changed(self, session):
                seen.append(session.session_id)
                raise RuntimeError("the command already succeeded")

        service = ChatCommandService(SimpleNamespace(memory_db=self.db), Host())
        for argument in ("ship the report", "status", "pause", "resume", "max 5", "clear"):
            result = await service.execute(ChatCommandRequest("goal", SessionRef("s"), argument))
            self.assertTrue(result.text)
        self.assertEqual(["s"] * 5, seen)


if __name__ == "__main__":
    unittest.main()
