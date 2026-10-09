import os
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from openagent_core.core.scheduler import Scheduler
from openagent_core.memory.db import MemoryDB


class ScheduledTaskRetryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = MemoryDB(str(Path(self.temp.name) / "openagent.db"))
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.task_id = await self.db.add_task(
            "Morning work", "0 9 * * *", "do the work",
        )
        self.task = await self.db.get_task(self.task_id)
        self.scheduler = Scheduler(
            db=self.db,
            agent=SimpleNamespace(name="test", model=None),
        )
        self.previous_delays = os.environ.get(
            "OPENAGENT_SCHEDULED_TASK_RETRY_DELAYS_SECONDS",
        )
        os.environ["OPENAGENT_SCHEDULED_TASK_RETRY_DELAYS_SECONDS"] = "60,120"
        self.addCleanup(self._restore_delays)

    def _restore_delays(self):
        if self.previous_delays is None:
            os.environ.pop(
                "OPENAGENT_SCHEDULED_TASK_RETRY_DELAYS_SECONDS", None,
            )
        else:
            os.environ[
                "OPENAGENT_SCHEDULED_TASK_RETRY_DELAYS_SECONDS"
            ] = self.previous_delays

    async def _failed_run(self, error, *, trigger="schedule"):
        run_id = await self.db.add_task_run(
            task_id=self.task_id,
            trigger=trigger,
        )
        await self.db.update_task_run(
            run_id,
            status="failed",
            finished_at=time.time(),
            error=error,
        )
        return await self.db.get_task_run(run_id)

    async def _request_count(self):
        conn = await self.db._ensure_connected()
        row = await (
            await conn.execute("SELECT COUNT(*) FROM task_run_requests")
        ).fetchone()
        return int(row[0])

    async def test_delayed_request_is_not_claimed_early_and_is_deduplicated(self):
        trigger = "automatic-retry:1:root-run"
        request_id = await self.db.enqueue_task_run_request(
            task_id=self.task_id,
            trigger=trigger,
            not_before=time.time() + 60,
            deduplicate=True,
        )
        duplicate_id = await self.db.enqueue_task_run_request(
            task_id=self.task_id,
            trigger=trigger,
            not_before=time.time() + 60,
            deduplicate=True,
        )
        self.assertEqual(request_id, duplicate_id)
        self.assertEqual([], await self.db.claim_pending_task_requests())

        conn = await self.db._ensure_connected()
        await conn.execute(
            "UPDATE task_run_requests SET created_at = ? WHERE id = ?",
            (time.time() - 1, request_id),
        )
        await conn.commit()
        claimed = await self.db.claim_pending_task_requests()
        self.assertEqual([request_id], [row["id"] for row in claimed])

    async def test_transient_failure_queues_retry_and_preserves_root_error(self):
        run = await self._failed_run(
            "ModelRateLimitError: upstream returned 429 Too Many Requests",
        )
        request_id = await self.scheduler._maybe_schedule_task_retry(
            self.task, run,
        )
        self.assertIsNotNone(request_id)
        request = await self.db.get_task_run_request(request_id)
        self.assertEqual(
            f"automatic-retry:1:{run['id']}", request["trigger"],
        )
        self.assertGreater(request["created_at"], time.time())
        self.assertEqual([], await self.db.claim_pending_task_requests())
        updated = await self.db.get_task_run(run["id"])
        self.assertIn("upstream returned 429", updated["error"])
        self.assertIn("automatic retry 1/2 queued", updated["error"])

    async def test_retry_stops_after_tool_activity_permanent_error_or_budget(self):
        tool_run = await self._failed_run("rate limit 429")
        original_check = self.db.task_run_has_tool_activity

        async def has_activity(_run_id):
            return True

        self.db.task_run_has_tool_activity = has_activity
        self.assertIsNone(
            await self.scheduler._maybe_schedule_task_retry(self.task, tool_run),
        )
        self.db.task_run_has_tool_activity = original_check

        permanent = await self._failed_run("invalid request: bad template")
        self.assertIsNone(
            await self.scheduler._maybe_schedule_task_retry(self.task, permanent),
        )
        exhausted = await self._failed_run(
            "rate limit 429",
            trigger="automatic-retry:2:root-run",
        )
        self.assertIsNone(
            await self.scheduler._maybe_schedule_task_retry(self.task, exhausted),
        )
        self.assertEqual(0, await self._request_count())

    async def test_preclaimed_wrapper_does_not_erase_provider_detail(self):
        run = await self._failed_run(
            "ModelRateLimitError: account pool exhausted",
        )

        async def generic_wrapper_failure(_task):
            raise RuntimeError("Scheduled task did not complete successfully")

        self.scheduler.run_task = generic_wrapper_failure
        await self.scheduler._run_preclaimed_task(
            {**self.task, "_preclaimed_run_id": run["id"]},
        )
        updated = await self.db.get_task_run(run["id"])
        self.assertIn("account pool exhausted", updated["error"])
        self.assertNotIn("did not complete successfully", updated["error"])
        self.assertIn("automatic retry 1/2 queued", updated["error"])


if __name__ == "__main__":
    unittest.main()
