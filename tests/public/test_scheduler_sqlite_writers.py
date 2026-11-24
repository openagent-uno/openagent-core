"""Exercise scheduler and chat writers against the same WAL database."""

import asyncio
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import aiosqlite
from openagent_core.contracts import RunAdmissionPending
from openagent_core.core import compaction
from openagent_core.core.scheduler import Scheduler
from openagent_core.memory.db import AutomationOutcomeWriteError, MemoryDB
from openagent_core.workflow.executor import WorkflowExecutor


class SchedulerWriters(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.path = Path(scratch.name) / "agent.db"
        self.db = MemoryDB(str(self.path))
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.task = await self.db.add_task("Report", "0 0 * * *", "Report progress")
        self.host = SimpleNamespace(
            authorized_definition_ids=AsyncMock(return_value=(self.task,)),
            definition_authorized=AsyncMock(return_value=True),
        )
        self.scheduler = Scheduler(
            self.db, SimpleNamespace(), execution_service=self.host
        )

    async def test_claim_stop_and_reopen_keep_one_manual_occurrence(self):
        request = await self.db.enqueue_task_run_request(task_id=self.task)
        self.assertFalse(
            await self.db.claim_pending_task_requests(
                definition_ids=(), reserve_runs=True
            )
        )
        results = await asyncio.gather(
            *(
                self.db.claim_pending_task_requests(
                    definition_ids=(self.task,), reserve_runs=True
                )
                for _ in range(4)
            )
        )
        [first] = [row for batch in results for row in batch]
        self.assertEqual(first["run_id"], request)
        self.assertEqual((await self.db.get_task_run(request))["status"], "queued")
        # Crash before the dispatch CAS. The new writer can resume exactly this identity.
        await self.db.close()
        await self.db.connect()
        self.assertEqual(await self.db.reap_orphan_task_runs(), 0)
        [second] = await self.db.claim_pending_task_requests(reserve_runs=True)
        self.assertEqual(second["run_id"], request)
        self.assertFalse(
            await self.db.begin_requested_task_run(first, session_id="old")
        )
        self.assertTrue(
            await self.db.begin_requested_task_run(second, session_id="chat")
        )
        self.assertFalse(
            await self.db.begin_requested_task_run(second, session_id="chat")
        )
        # Crash after dispatch, where external effects are unknown. Another
        # identity must never fire this action as generic startup recovery.
        await self.db.close()
        await self.db.connect()
        self.assertEqual(await self.db.reap_orphan_task_runs(), 1)
        self.assertFalse(await self.db.requeue_interrupted_task_runs())
        self.assertFalse(await self.db.claim_pending_task_requests(reserve_runs=True))
        self.assertEqual((await self.db.get_task_run(request))["status"], "failed")
        self.assertEqual(
            (await self.db.get_task_run_request(request))["run_id"], request
        )

    async def test_claim_colliding_with_existing_history_rolls_back_batch(self):
        one = await self.db.enqueue_task_run_request(task_id=self.task)
        two = await self.db.enqueue_task_run_request(task_id=self.task)
        await self.db.add_task_run(task_id=self.task, run_id=two)
        with self.assertRaises(sqlite3.IntegrityError):
            await self.db.claim_pending_task_requests(reserve_runs=True)
        self.assertIsNone(await self.db.get_task_run(one))
        self.assertIsNone((await self.db.get_task_run_request(one))["claimed_at"])
        self.assertIsNone((await self.db.get_task_run_request(two))["claimed_at"])

    async def test_shared_chat_snapshot_cannot_hide_completed_finalizer_or_stop(self):
        run = await self.db.add_task_run(task_id=self.task)
        reader = await self.db._ensure_connected()
        await reader.execute("BEGIN")
        self.addAsyncCleanup(reader.rollback)
        await reader.execute_fetchall("SELECT * FROM task_runs")
        async with aiosqlite.connect(self.path) as chat:
            await chat.execute("BEGIN IMMEDIATE")
            finish = asyncio.create_task(
                self.scheduler._record_task_finish(
                    run, {"id": self.task}, status="success", output="done"
                )
            )
            await asyncio.sleep(0.05)
            self.assertFalse(finish.done())
            await chat.commit()
            await asyncio.wait_for(finish, 2)
        self.assertTrue(reader.in_transaction)
        self.assertEqual((await self.db.get_task_run(run))["output"], "done")
        await self.scheduler._record_task_finish(
            run, {"id": self.task}, status="failed", error="late"
        )
        row = await self.db.get_task_run(run)
        self.assertEqual(row["status"], "success")
        self.assertIsNone(row["error"])

    async def test_shared_snapshot_cannot_hide_cross_process_stop(self):
        run = await self.db.add_task_run(task_id=self.task)
        conn = await self.db._ensure_connected()
        await conn.execute("BEGIN")
        self.addAsyncCleanup(conn.rollback)
        await conn.execute_fetchall("SELECT status FROM task_runs")
        self.assertEqual(await self.db.flag_task_runs_cancelling(self.task), [run])
        [flagged] = await self.db.get_task_runs_by_status("cancelling")
        self.assertEqual(flagged["id"], run)
        await self.db.update_task_run(run, status="success")
        self.assertEqual((await self.db.get_task_run(run))["status"], "cancelling")
        await self.db.update_task_run(run, status="cancelled")
        self.assertEqual((await self.db.get_task_run(run))["status"], "cancelled")

    async def test_workflow_claim_resumes_before_admission_and_opens_only_once(self):
        workflow = await self.db.add_workflow(name="Summary")
        request = await self.db.enqueue_workflow_run_request(
            workflow_id=workflow, trigger="manual", inputs={"topic": "state"}
        )
        [first] = await self.db.claim_pending_workflow_requests(reserve_runs=True)
        self.assertEqual(first["run_id"], request)
        self.assertEqual(
            (await self.db.get_workflow_run(request))["inputs"], {"topic": "state"}
        )
        self.assertEqual(await self.db.reap_orphan_workflow_runs(), 0)
        [second] = await self.db.claim_pending_workflow_requests(reserve_runs=True)
        self.assertFalse(await self.db.begin_requested_workflow_run(first))
        self.assertTrue(await self.db.begin_requested_workflow_run(second))
        args = dict(
            workflow_id=workflow,
            trigger="manual",
            inputs=second["inputs"],
            run_id=request,
            request_id=request,
        )
        self.assertEqual(await self.db.add_workflow_run(**args), request)
        with self.assertRaises(sqlite3.IntegrityError):
            await self.db.add_workflow_run(**args)
        await self.db.update_workflow_run(
            request, status="success", outputs={"result": "ok"}
        )
        self.assertEqual(await self.db.reap_orphan_workflow_runs(), 0)
        await self.db.update_workflow_run(request, status="failed", error="late")
        self.assertEqual(
            (await self.db.get_workflow_run(request))["outputs"], {"result": "ok"}
        )
        self.assertEqual((await self.db.get_workflow_run(request))["status"], "success")

    async def test_cron_task_survives_crash_between_schedule_advance_and_dispatch(self):
        task = await self.db.get_task(self.task)
        now = time.time()
        reserved = await self.db.claim_due_task_run(
            task_id=self.task,
            expected_next_run=task["next_run"],
            fired_at=now,
            next_run=now + 60,
            disable=False,
            run_id="cron-occurrence",
            session_id="scheduler-child",
            reserve=True,
        )
        self.assertTrue(reserved)
        await self.db.close()
        await self.db.connect()
        self.assertEqual(await self.db.reap_orphan_task_runs(), 0)
        [claim] = await self.db.claim_pending_task_requests(reserve_runs=True)
        self.host.run_task = AsyncMock()
        await self.scheduler._run_requested_task(claim, task)
        self.assertEqual(self.host.run_task.await_count, 1)
        self.assertEqual(
            self.host.run_task.await_args.args[1]["_preclaimed_run_id"],
            "cron-occurrence",
        )
        self.assertEqual(
            (await self.db.get_task_run("cron-occurrence"))["status"], "success"
        )
        self.assertFalse(await self.db.claim_pending_task_requests(reserve_runs=True))

    async def test_competing_cron_claims_recover_exact_workflow_entry_after_restart(
        self,
    ):
        workflow = await self.db.add_workflow(name="With two entries")
        now = time.time()
        await self.db.upsert_schedule(
            workflow_id=workflow,
            node_id="selected-entry",
            cron_expression="0 0 * * *",
            next_run_at=now - 1,
            timezone="UTC",
        )
        [schedule] = await self.db.list_schedules(workflow_id=workflow)
        results = await asyncio.gather(
            *(
                self.db.enqueue_due_workflow(schedule, now=now, next_run=now + 100)
                for _ in range(2)
            )
        )
        self.assertEqual(sorted(results), [False, True])
        await self.db.close()
        await self.db.connect()
        [request] = await self.db.claim_pending_workflow_requests(reserve_runs=True)
        self.assertEqual(request["schedule"], schedule)
        self.host.schedule_authorized = AsyncMock(return_value=True)
        self.host.run_workflow = AsyncMock(return_value=request["id"])
        await self.scheduler._run_requested_workflow(
            request, await self.db.get_workflow(workflow)
        )
        options = self.host.run_workflow.await_args.kwargs
        self.assertEqual(options["entry_node_id"], "selected-entry")
        self.assertEqual(options["run_id"], request["id"])
        self.assertEqual(options["request_id"], request["id"])
        self.assertFalse(
            await self.db.claim_pending_workflow_requests(reserve_runs=True)
        )

    async def test_admission_delay_keeps_manual_and_cron_occurrences_under_the_same_identity(
        self,
    ):
        manual = await self.db.enqueue_task_run_request(task_id=self.task)
        [claim] = await self.db.claim_pending_task_requests(reserve_runs=True)
        task = await self.db.get_task(self.task)
        cron = await self.db.add_task_run(task_id=self.task, trigger="schedule")
        self.host.run_task = AsyncMock(side_effect=RunAdmissionPending(20))
        await self.scheduler._run_requested_task(claim, task)
        await self.scheduler._run_preclaimed_task({**task, "_preclaimed_run_id": cron})
        self.assertFalse(await self.db.claim_pending_task_requests(reserve_runs=True))
        for occurrence in (manual, cron):
            row = await self.db.get_task_run_request(occurrence)
            self.assertEqual(row["run_id"], occurrence)
            self.assertIsNone(row["claimed_at"])
            self.assertGreater(row["created_at"], time.time())
            self.assertEqual(
                (await self.db.get_task_run(occurrence))["status"], "queued"
            )
        async with aiosqlite.connect(self.path) as clock:
            await clock.execute(
                "UPDATE task_run_requests SET created_at=?", (time.time() - 1,)
            )
            await clock.commit()
        retry = await self.db.claim_pending_task_requests(reserve_runs=True)
        self.assertEqual({item["run_id"] for item in retry}, {manual, cron})
        self.assertTrue(
            await self.db.begin_requested_task_run(retry[0], session_id="allowed")
        )
        self.assertFalse(
            await self.db.begin_requested_task_run(retry[0], session_id="allowed")
        )

    async def test_existing_workflow_request_survives_additive_upgrade(self):
        workflow = await self.db.add_workflow(name="Previous version")
        request = await self.db.enqueue_workflow_run_request(
            workflow_id=workflow, trigger="manual", inputs={"n": 4}
        )
        await self.db.close()
        with closing(sqlite3.connect(self.path)) as legacy:
            legacy.execute(
                "ALTER TABLE workflow_run_requests DROP COLUMN schedule_json"
            )
            legacy.commit()
        await self.db.connect()
        [claim] = await self.db.claim_pending_workflow_requests(reserve_runs=True)
        self.assertEqual(claim["run_id"], request)
        self.assertEqual(claim["inputs"], {"n": 4})
        self.assertIsNone(claim.get("schedule"))

    async def test_permanent_finalizer_failure_is_not_reported_as_hook_success(self):
        run = await self.db.add_task_run(task_id=self.task)
        async with aiosqlite.connect(self.path) as writer:
            await writer.execute(
                "CREATE TRIGGER stop_finish BEFORE UPDATE ON task_runs BEGIN SELECT RAISE(ABORT,'test'); END"
            )
            await writer.commit()
        with self.assertRaises(AutomationOutcomeWriteError):
            await self.scheduler._record_task_finish(
                run, {"id": self.task}, status="success"
            )
        self.assertEqual((await self.db.get_task_run(run))["status"], "running")

    async def test_workflow_finalizer_exhaustion_surfaces_without_broadcasting_a_result(
        self,
    ):
        workflow = await self.db.add_workflow(name="Needs durable final")
        run = await self.db.add_workflow_run(workflow_id=workflow, trigger="manual")
        ctx = SimpleNamespace(run_id=run, workflow_id=workflow, trace=[])
        async with aiosqlite.connect(self.path) as writer:
            await writer.execute("BEGIN IMMEDIATE")
            with patch.dict("os.environ", {"OPENAGENT_SQLITE_BUSY_TIMEOUT_MS": "50"}):
                with self.assertRaises(AutomationOutcomeWriteError):
                    await asyncio.wait_for(
                        WorkflowExecutor(None, self.db)._finalize_run(
                            ctx, status="success"
                        ),
                        1,
                    )
        self.assertEqual((await self.db.get_workflow_run(run))["status"], "running")
        await WorkflowExecutor(None, self.db)._finalize_run(ctx, status="success")
        self.assertEqual((await self.db.get_workflow_run(run))["status"], "success")

    async def test_idle_claims_and_lease_tick_do_not_queue_behind_chat_writer(self):
        async with aiosqlite.connect(self.path) as writer:
            await writer.execute("BEGIN IMMEDIATE")
            done = await asyncio.wait_for(
                asyncio.gather(
                    self.db.claim_pending_task_requests(reserve_runs=True),
                    self.db.claim_pending_workflow_requests(),
                    self.db.claim_pending_event_deliveries(),
                    self.db.reap_expired_event_leases(),
                    self.db.reap_stale_event_deliveries(min_claim_age_seconds=60),
                ),
                1,
            )
            self.assertEqual(done, [[], [], [], 0, 0])

    async def test_age_sweep_cannot_reclaim_an_owned_live_lease(self):
        event = await self.db.add_event(
            name="Sample",
            action_kind="task",
            slug="sample",
            secret_enc="",
            action_ref=self.task,
        )
        item = await self.db.add_event_delivery(
            event_id=event, payload={}, claimed=True
        )
        async with aiosqlite.connect(self.path) as writer:
            await writer.execute(
                "UPDATE event_deliveries SET claimed_at=?,claim_expires=? WHERE id=?",
                (time.time() - 2000, time.time() + 100, item),
            )
            await writer.commit()
        self.assertEqual(
            await self.db.reap_stale_event_deliveries(min_claim_age_seconds=60), 0
        )
        self.assertEqual(
            await self.db.heartbeat_event_delivery(item, self.db.worker_id), 1
        )
        self.assertFalse(await self.db.claim_pending_event_deliveries())

    async def test_due_and_manual_drains_keep_excess_work_unclaimed(self):
        ready, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def run(scheduler, task, *, trigger, request_id, payload, execute):
            calls.append(task["_preclaimed_run_id"])
            ready.set()
            await release.wait()
            await self.db.update_task_run(task["_preclaimed_run_id"], status="success")

        self.host.run_task = run
        await self.db.update_task(self.task, next_run=time.time() - 2)
        requests = [
            await self.db.enqueue_task_run_request(task_id=self.task) for _ in range(2)
        ]
        with patch.dict("os.environ", {"OPENAGENT_BACKGROUND_RUN_CONCURRENCY": "1"}):
            await asyncio.gather(
                self.scheduler._drain_task_run_requests(),
                self.scheduler._check_and_run(),
            )
            await asyncio.wait_for(ready.wait(), 1)
            self.assertEqual(calls, [requests[0]])
            self.assertIsNone(
                (await self.db.get_task_run_request(requests[1]))["claimed_at"]
            )
            self.assertLess(
                (await self.db.get_task(self.task))["next_run"], time.time()
            )
            release.set()
            await self.scheduler.stop()
            await self.scheduler._check_and_run()
            await self.scheduler.stop()
            await self.scheduler._drain_task_run_requests()
            await self.scheduler.stop()
        self.assertEqual(len(set(calls)), 3)
        for run in calls:
            self.assertEqual((await self.db.get_task_run(run))["status"], "success")


class CompactionWriters(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.path = Path(self.scratch.name) / "state.db"
        self.runs = [
            {"run_id": str(i), "content": "synthetic " + str(i)} for i in range(4)
        ]
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE sessions(session_id TEXT PRIMARY KEY,runs TEXT,updated_at INTEGER)"
            )
            db.execute(
                "INSERT INTO sessions VALUES ('history',?,1)", (json.dumps(self.runs),)
            )
            db.commit()
        self.agent = SimpleNamespace(_db=SimpleNamespace(db_path=str(self.path)))
        self.ready = asyncio.Event()

        async def summary(*args):
            self.ready.set()
            return "recap"

        self.summary = patch.object(compaction, "_summarize_runs", side_effect=summary)
        self.summary.start()
        self.addCleanup(self.summary.stop)
        self.other = await aiosqlite.connect(self.path)
        self.addAsyncCleanup(self.other.close)

    async def test_compaction_waits_off_loop_and_preserves_concurrent_chat(self):
        await self.other.execute("BEGIN IMMEDIATE")
        job = asyncio.create_task(
            compaction.compact("history", None, self.agent, keep=1)
        )
        await asyncio.wait_for(self.ready.wait(), 1)
        await asyncio.sleep(0.05)
        self.assertFalse(job.done())
        updated = [
            *self.runs,
            {"run_id": "new", "content": "message received while folding"},
        ]
        await self.other.execute("UPDATE sessions SET runs=?", (json.dumps(updated),))
        await self.other.commit()
        self.assertIsNone(await job)
        self.assertEqual(compaction._load_runs(str(self.path), "history"), updated)

    async def test_preempted_writer_drains_without_waiting_for_contending_chat(self):
        await self.other.execute("BEGIN IMMEDIATE")
        job = asyncio.create_task(
            compaction.compact("history", None, self.agent, keep=1)
        )
        await asyncio.wait_for(self.ready.wait(), 1)
        await asyncio.sleep(0.05)
        job.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(job, 1)
        await self.other.commit()
        self.assertEqual(compaction._load_runs(str(self.path), "history"), self.runs)
        self.assertIsNotNone(
            await compaction.compact("history", None, self.agent, keep=1)
        )
        self.assertEqual(len(compaction._load_runs(str(self.path), "history")), 2)
