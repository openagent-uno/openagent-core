import asyncio
import sqlite3

import pytest

from openagent_core.memory.db import MemoryDB


@pytest.mark.asyncio
async def test_checkpoint_does_not_pin_the_scheduler_snapshot(tmp_path):
    db = MemoryDB(str(tmp_path / "agent.db"))
    await db.connect()
    try:
        conn = await db._ensure_connected()
        async with conn.execute("SELECT 1") as cursor:
            await cursor.fetchall()
        writer = sqlite3.connect(db.db_path)
        try:
            writer.execute("INSERT INTO scheduled_tasks (id,name,cron_expression,prompt,enabled,next_run,created_at,updated_at) VALUES ('task','probe','* * * * *','probe',1,1,1,1)")
            writer.commit()
        finally:
            writer.close()
        assert [task["id"] for task in await db.get_due_tasks(2)] == ["task"]
        reader = await conn.execute("SELECT * FROM scheduled_tasks UNION ALL SELECT * FROM scheduled_tasks")
        await reader.fetchone()
        assert await db.claim_due_task_run(task_id="task", expected_next_run=1,
            fired_at=2, next_run=3, disable=False, run_id="run", session_id=None)
        assert await db.get_due_tasks(2) == []
        await reader.close()
        await asyncio.wait_for(db.upsert_session("chat", title="probe"), 2)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_metadata_write_survives_an_active_shared_reader(tmp_path):
    db = MemoryDB(str(tmp_path / "agent.db"))
    await db.connect()
    cursor = None
    try:
        conn = await db._ensure_connected()
        await conn.execute("INSERT INTO sessions (session_id,session_type,metadata,created_at,updated_at) VALUES ('a','agent','{}',1,1),('b','agent','{}',1,1)")
        await conn.commit()
        cursor = await conn.execute("SELECT * FROM sessions")
        await cursor.fetchone()
        writer = sqlite3.connect(db.db_path)
        try:
            writer.execute("UPDATE sessions SET metadata='{}' WHERE session_id='b'")
            writer.commit()
        finally:
            writer.close()
        await asyncio.wait_for(db.upsert_session("new", title="new chat"), 2)
        check = sqlite3.connect(db.db_path)
        try:
            assert check.execute("SELECT metadata FROM sessions WHERE session_id='new'").fetchone()
            assert check.execute("SELECT id FROM sessions_v2 WHERE id='new'").fetchone()
        finally:
            check.close()
    finally:
        if cursor is not None:
            await cursor.close()
        await db.close()


def test_background_model_selection_can_resolve_local_model():
    from unittest.mock import patch
    from types import SimpleNamespace
    from openagent_core.models.catalog import cheapest_enabled_model

    entry = SimpleNamespace(runtime_id="local:local-model", disabled=False, framework="api-based")
    with patch("openagent_core.models.catalog.iter_configured_models", return_value=[entry]), patch("openagent_core.models.catalog.get_model_pricing", return_value={}):
        assert cheapest_enabled_model({}) is entry


def test_compaction_resolves_a_plain_summarizer_instead_of_falling_back():
    from unittest.mock import patch
    from types import SimpleNamespace
    from openagent_core.core.compaction import _pick_summary_model

    entry = SimpleNamespace(runtime_id="local:local-model", disabled=False, framework="api-based")
    fallback = object()
    plain = object()
    with patch("openagent_core.core.compaction._is_router", return_value=True), patch("openagent_core.models.catalog.iter_configured_models", return_value=[entry]), patch("openagent_core.models.catalog.get_model_pricing", return_value={}), patch("openagent_core.models.native_provider.NativeProvider", return_value=plain):
        assert _pick_summary_model(SimpleNamespace(_providers_config=[]), fallback=fallback) is plain
