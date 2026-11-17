"""Full context reporting yields to the host and retains publication checks."""
import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core.core import context_report
from openagent_core.runtime import (
    current_execution_context, current_run_id, current_runtime, execution_scope,
)
from openagent_core.workspace_administration import SessionInspection


class AsyncContextReportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "context.sqlite3"
        self.runs = [{"run_id": f"run-{index}", "model": "fixture-model", "model_provider": "fixture",
                      "messages": [{"role": "user", "content": f"Historical question {index}"},
                                   {"role": "assistant", "content": f"Complete answer {index}"}],
                      "metrics": {"input_tokens": index + 100}} for index in range(50)]
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TABLE sessions(session_id TEXT PRIMARY KEY,runs TEXT,session_data TEXT,summary TEXT)")
            db.execute("INSERT INTO sessions VALUES(?,?,?,?)", ("session", json.dumps(self.runs), json.dumps({
                "session_metrics": {"cost": 4.25, "input_tokens": 12345, "output_tokens": 678}}), "Rolling summary"))
        self.agent = SimpleNamespace(_db=SimpleNamespace(db_path=str(self.path)), model=SimpleNamespace(), _mcp=None,
                                     _combined_system_prompt=lambda sid: "Neutral complete framework prompt")
        for value in (
            patch("openagent_core.models.catalog.get_model_context_window", return_value=(200000, "fixture")),
            patch("openagent_core.models.catalog.get_model_pricing", return_value={"input_cost_per_million": 0, "output_cost_per_million": 0}),
            patch("openagent_core.core._runner.utils.tokens._select_tokenizer", return_value=("none", None)),
        ):
            value.start()
            self.addCleanup(value.stop)

    async def test_complete_report_matches_sync_and_refreshes_after_a_new_turn(self):
        expected = context_report.build_context_report(self.agent, "session")
        result = await context_report.build_context_report_async(self.agent, "session")
        self.assertEqual(result, expected)
        self.assertEqual((result["turns"], result["cost_usd"], result["measured_input_tokens"]), (50, 4.25, 149))
        self.runs.append({"messages": [{"role": "user", "content": "New uncached history " * 100}]})
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sessions SET runs=? WHERE session_id='session'", (json.dumps(self.runs),))
        fresh = await context_report.build_context_report_async(self.agent, "session")
        self.assertEqual(fresh, context_report.build_context_report(self.agent, "session"))
        self.assertEqual(fresh["turns"], 51)
        self.assertGreater(fresh["used_tokens"], result["used_tokens"])

    async def test_runtime_execution_context_reaches_the_worker(self):
        seen = []
        host_thread = threading.get_ident()
        runtime, context = SimpleNamespace(module_registries={}), object()
        def prompt(_sid):
            seen.append((threading.get_ident(), current_runtime(), current_execution_context(), current_run_id()))
            return "Context-bound prompt"
        self.agent._combined_system_prompt = prompt
        with execution_scope(runtime, context, "run-context"):
            result = await context_report.build_context_report_async(self.agent, "session")
        self.assertEqual(result["turns"], 50)
        self.assertEqual(seen[0][1:], (runtime, context, "run-context"))
        self.assertNotEqual(seen[0][0], host_thread)

    async def test_blocked_report_keeps_the_event_loop_available(self):
        started, release = threading.Event(), threading.Event()
        original = context_report._load_runs
        def blocked(*args):
            started.set()
            release.wait(2)
            return original(*args)
        with patch.object(context_report, "_load_runs", side_effect=blocked):
            task = asyncio.create_task(context_report.build_context_report_async(self.agent, "session"))
            try:
                async with asyncio.timeout(1):
                    while not started.is_set():
                        await asyncio.sleep(0)
                await asyncio.sleep(0)
                self.assertFalse(task.done(), "Report work occupied the event loop")
                release.set()
                self.assertEqual((await task)["turns"], 50)
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)

    async def test_cancellation_drains_worker_before_returning_to_the_owner(self):
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        def blocked(*args):
            started.set()
            release.wait(2)
            finished.set()
            return None
        with patch.object(context_report, "build_context_report", side_effect=blocked):
            task = asyncio.create_task(context_report.build_context_report_async(self.agent, "session"))
            try:
                async with asyncio.timeout(1):
                    while not started.is_set():
                        await asyncio.sleep(0)
                task.cancel()
                await asyncio.sleep(0)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertTrue(finished.is_set())
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)

    async def test_empty_and_error_fallbacks_remain_unchanged(self):
        self.assertIsNone(await context_report.build_context_report_async(self.agent, None))
        with patch.object(context_report, "build_context_report", side_effect=ValueError("controlled")):
            with self.assertRaisesRegex(ValueError, "controlled"):
                await context_report.build_context_report_async(self.agent, "session")

    async def test_lazy_catalog_refresh_is_scheduled_on_the_host_thread(self):
        report = {"window_source": "fallback"}
        seen = []
        with patch.object(context_report, "build_context_report", return_value=report), patch(
            "openagent_core.models.catalog._maybe_prime_openrouter_cache", side_effect=lambda: seen.append(threading.get_ident()),
        ):
            self.assertIs(await context_report.build_context_report_async(self.agent, "session"), report)
        self.assertEqual(seen, [threading.get_ident()])

    async def test_inspection_rechecks_authority_before_publication(self):
        inspection = SessionInspection(None, None, agent=self.agent)
        inspection.authorize = AsyncMock(side_effect=[None, PermissionError("revoked during report")])
        with self.assertRaisesRegex(PermissionError, "revoked"):
            await inspection.context_report(object(), "session")
        self.assertEqual(inspection.authorize.await_count, 2)


if __name__ == "__main__":
    unittest.main()
