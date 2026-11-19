"""Ready storage must not reinstall journals or leak a failed bootstrap writer."""
import asyncio
import sqlite3

import aiosqlite
import pytest

from openagent_core.engine import MemoryDB
from openagent_core.memory.operational.schema import ensure_operational_storage


@pytest.mark.asyncio
async def test_ready_journals_are_read_only_while_another_writer_is_active(tmp_path):
    path = tmp_path / 'host.db'
    db = MemoryDB(str(path)); await db.connect(); await db.close()
    holder = sqlite3.connect(path, isolation_level=None)
    async with aiosqlite.connect(path, timeout=.02) as conn:
        holder.execute('BEGIN IMMEDIATE')
        try:
            async with asyncio.timeout(1):
                assert await ensure_operational_storage(conn, str(path), app_version='fixture') is None
            assert not conn.in_transaction
            assert holder.in_transaction, 'The bootstrap must not interfere with another owner'
        finally:
            holder.rollback(); holder.close()


@pytest.mark.asyncio
async def test_missing_automation_trigger_still_reseeds_and_restores_the_journal(tmp_path):
    path = tmp_path / 'host.db'
    db = MemoryDB(str(path)); await db.connect(); await db.close()
    with sqlite3.connect(path) as conn:
        conn.execute('DROP TRIGGER trg_operational_scheduled_tasks_insert')
        conn.execute("INSERT INTO scheduled_tasks(id,name,cron_expression,prompt,created_at,updated_at) VALUES('missed','Fixture','0 9 * * *','fixture',0,0)")
    async with aiosqlite.connect(path) as conn:
        await ensure_operational_storage(conn, str(path), app_version='fixture')
        assert await (await conn.execute("SELECT 1 FROM sqlite_schema WHERE name='trg_operational_scheduled_tasks_insert'")).fetchone()
        row = await (await conn.execute("SELECT count(*) FROM operational_automation_changes WHERE resource_type='scheduled_definition' AND resource_id='missed'")).fetchone()
        assert row[0] == 1
        await ensure_operational_storage(conn, str(path), app_version='fixture')
        assert (await (await conn.execute("SELECT count(*) FROM operational_automation_changes WHERE resource_type='scheduled_definition' AND resource_id='missed'")).fetchone())[0] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_failed_or_cancelled_open_releases_its_uncommitted_writer(tmp_path, cancel):
    path = tmp_path / 'host.db'; started = asyncio.Event()
    db = MemoryDB(str(path))
    async def migration(conn):
        await conn.execute("INSERT INTO vault_save_reminders(session_id,turn_count,created_at,updated_at) VALUES('not-committed',1,0,0)")
        started.set()
        if cancel: await asyncio.Event().wait()
        raise RuntimeError('Fixture failed module migration')
    db.add_migration(migration)
    try:
        task = asyncio.create_task(db.connect())
        await asyncio.wait_for(started.wait(), 5)
        if cancel: task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else RuntimeError): await task
        with sqlite3.connect(path, timeout=.02, isolation_level=None) as other:
            other.execute('BEGIN IMMEDIATE')
            assert other.execute("SELECT count(*) FROM vault_save_reminders WHERE session_id='not-committed'").fetchone()[0] == 0
            other.rollback()
    finally:
        await db.close()
