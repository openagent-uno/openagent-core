import asyncio
from types import SimpleNamespace
import threading
import time
import unittest

from openagent_core.core._runner.agent._session import asave_session


class _BlockingSyncStore:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()

    def upsert_session(self, *, session):
        self.started.set()
        self.release.wait(2)
        return session


class AsyncSessionPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_session_writer_never_blocks_the_event_loop(self):
        store = _BlockingSyncStore()
        agent = SimpleNamespace(db=store, team_id=None, workflow_id=None)
        session = SimpleNamespace(
            session_id="session",
            session_data={"session_state": {"current_run_id": "run"}},
        )
        timer = threading.Timer(0.3, store.release.set)
        timer.start()
        started = time.monotonic()
        task = asyncio.create_task(asave_session(agent, session))
        try:
            while not store.started.is_set():
                await asyncio.sleep(0)
            await asyncio.sleep(0.02)
            self.assertLess(time.monotonic() - started, 0.15)
            store.release.set()
            await asyncio.wait_for(task, 1)
        finally:
            store.release.set()
            timer.cancel()


if __name__ == "__main__":
    unittest.main()
