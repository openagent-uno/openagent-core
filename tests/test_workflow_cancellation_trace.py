import asyncio
import tempfile
import unittest
from pathlib import Path

from openagent_core.memory.db import MemoryDB
from openagent_core.workflow.executor import WorkflowExecutor


class _Agent:
    _mcp = None
    model = None

    async def refresh_registries(self):
        return None


class WorkflowCancellationTraceTest(unittest.IsolatedAsyncioTestCase):
    async def test_active_node_is_terminal_when_workflow_is_cancelled(self):
        with tempfile.TemporaryDirectory() as root:
            db = MemoryDB(str(Path(root) / "state.sqlite3"))
            await db.connect()
            workflow_id = await db.add_workflow(
                name="cancel trace",
                graph={
                    "version": 1,
                    "nodes": [
                        {"id": "manual", "type": "trigger-manual", "config": {}},
                        {"id": "pause", "type": "wait", "config": {"mode": "duration", "seconds": 30}},
                    ],
                    "edges": [{"id": "edge", "source": "manual", "target": "pause"}],
                    "variables": {},
                },
            )
            workflow = await db.get_workflow(workflow_id)
            executor = WorkflowExecutor(_Agent(), db)
            task = asyncio.create_task(executor.run(workflow, run_id="cancelled-run"))
            for _ in range(200):
                row = await db.get_workflow_run("cancelled-run")
                if row and any(entry["node_id"] == "pause" and entry["status"] == "running" for entry in row["trace"]):
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("wait node never entered the running state")
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            row = await db.get_workflow_run("cancelled-run")
            self.assertEqual("cancelled", row["status"])
            pause = next(entry for entry in row["trace"] if entry["node_id"] == "pause")
            self.assertEqual("cancelled", pause["status"])
            self.assertIsNotNone(pause["finished_at"])
            self.assertEqual("Stopped by user", pause["error"])
            await db.close()


if __name__ == "__main__":
    unittest.main()
