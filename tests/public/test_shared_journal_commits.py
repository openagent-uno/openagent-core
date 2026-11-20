"""Ordinary state saves must survive concurrent journal writes on one database."""
import asyncio

import pytest

from openagent_core.memory.db import MemoryDB


@pytest.mark.asyncio
async def test_journal_and_state_writes_share_connection_without_failed_commits(tmp_path):
    # Value: independent public writes retain their successful outcome and data;
    # an unfinished RETURNING cursor used to make unrelated state commits fail.
    # Real SQLite and public DB methods; no provider or production data involved.
    db = MemoryDB(str(tmp_path / "agent.sqlite"))
    try:
        await db.connect()
        operations = []
        for index in range(100):
            operations.extend((
                db.append_session_event("fixture", "tool/status", {"index": index}),
                db.set_state(f"fixture:{index}", "ready"),
            ))
        results = await asyncio.gather(*operations, return_exceptions=True)
        assert not [result for result in results if isinstance(result, BaseException)]
        assert sorted(results[::2]) == list(range(1, 101))
        await db.close()
        db = MemoryDB(str(tmp_path / "agent.sqlite"))
        await db.connect()
        states = await asyncio.gather(*(db.get_state(f"fixture:{index}") for index in range(100)))
        assert states == ["ready"] * 100
        events = await db.list_session_events("fixture", limit=200)
        assert len(events) == 100
        assert {event["data"]["index"] for event in events} == set(range(100))
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_in_memory_revision_results_do_not_block_shared_state_commits():
    # In-memory operational maintenance uses the same connection as state saves.
    from openagent_core.memory.operational.automation import _allocate_revision

    db = MemoryDB(":memory:")
    try:
        await db.connect()
        conn = await db._ensure_connected()
        operations = []
        for index in range(100):
            operations.extend((
                _allocate_revision(conn, index),
                db.set_state(f"fixture:{index}", "ready"),
            ))
        results = await asyncio.gather(*operations, return_exceptions=True)
        assert not [result for result in results if isinstance(result, BaseException)]
        assert sorted(results[::2]) == list(range(1, 101))
        await conn.commit()
        states = await asyncio.gather(*(db.get_state(f"fixture:{index}") for index in range(100)))
        assert states == ["ready"] * 100
    finally:
        await db.close()
