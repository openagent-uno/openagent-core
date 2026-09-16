from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from openagent_core.engine import MemoryDB
from openagent_core.automation_definitions import sync_workflow_schedules, next_run_for_expression


class ScheduleTimezoneTests(unittest.IsolatedAsyncioTestCase):
    async def test_additive_legacy_column_preserves_ids_and_deadlines(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"state.sqlite"
            db=MemoryDB(str(path));await db.connect()
            workflow=await db.add_workflow(name="Historical workflow")
            schedule=await db.upsert_schedule(workflow_id=workflow,node_id="timer",cron_expression="0 9 * * *",next_run_at=123456789)
            await db.close()
            with sqlite3.connect(path) as connection:
                connection.execute("ALTER TABLE workflow_schedules DROP COLUMN timezone")
            db=MemoryDB(str(path));await db.connect()
            row=(await db.list_schedules(workflow_id=workflow))[0]
            self.assertEqual((schedule,123456789,None),(row["id"],row["next_run_at"],row["timezone"]))
            await db.close()

    async def test_zone_change_recomputes_but_metadata_save_preserves_schedule(self):
        with tempfile.TemporaryDirectory() as directory:
            db=MemoryDB(str(Path(directory)/"state.sqlite"));await db.connect()
            workflow=await db.add_workflow(name="Workflow")
            graph={"nodes":[{"id":"timer","type":"trigger-schedule","config":{"cron_expression":"0 9 * * *","timezone":"Europe/Rome","enabled":False}}]}
            await sync_workflow_schedules(db,workflow,graph)
            first=(await db.list_schedules(workflow_id=workflow))[0]
            self.assertFalse(first["enabled"]);self.assertEqual("Europe/Rome",first["timezone"])
            await sync_workflow_schedules(db,workflow,graph)
            self.assertEqual(first["next_run_at"],(await db.list_schedules(workflow_id=workflow))[0]["next_run_at"])
            graph["nodes"][0]["config"]["timezone"]="America/New_York"
            await sync_workflow_schedules(db,workflow,graph)
            second=(await db.list_schedules(workflow_id=workflow))[0]
            self.assertEqual(first["id"],second["id"]);self.assertNotEqual(first["next_run_at"],second["next_run_at"])
            await db.close()

    async def test_zone_rules_follow_dst_at_next_occurrence(self):
        base=datetime(2026,3,28,12,tzinfo=timezone.utc).timestamp()
        result=next_run_for_expression("0 9 * * *",base,"Europe/Rome")
        self.assertEqual(datetime(2026,3,29,7,tzinfo=timezone.utc).timestamp(),result)
