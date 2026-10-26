"""A cancelled index check never closes SQLite underneath its worker."""
import asyncio
import sqlite3
import threading
import pytest
from openagent_core.memory.operational.search import _index_thread_call

@pytest.mark.asyncio
async def test_repeated_cancellation_drains_worker_before_connection_close():
    started=threading.Event();release=threading.Event();finished=threading.Event()
    conn=sqlite3.connect(':memory:',check_same_thread=False)
    def worker(connection):
        started.set();assert release.wait(5)
        connection.execute('SELECT 1').fetchone();finished.set()
    async def owner():
        try:await _index_thread_call(worker,conn)
        finally:
            assert finished.is_set(), 'connection closed while worker was using it'
            conn.close()
    task=asyncio.create_task(owner())
    try:
        assert await asyncio.to_thread(started.wait,5)
        task.cancel();await asyncio.sleep(0)
        task.cancel();await asyncio.sleep(0)
        assert not task.done()
    finally:release.set()
    with pytest.raises(asyncio.CancelledError):await task

@pytest.mark.asyncio
async def test_cancelled_open_closes_result_after_worker_finishes():
    started=threading.Event();release=threading.Event();connections=[]
    def opening():
        started.set();assert release.wait(5)
        conn=sqlite3.connect(':memory:',check_same_thread=False);connections.append(conn);return conn
    task=asyncio.create_task(_index_thread_call(opening,_close_result_on_cancel=True))
    assert await asyncio.to_thread(started.wait,5)
    task.cancel();await asyncio.sleep(0);release.set()
    with pytest.raises(asyncio.CancelledError):await task
    with pytest.raises(sqlite3.ProgrammingError):connections[0].execute('SELECT 1')

@pytest.mark.asyncio
async def test_cancel_interrupts_long_running_sql_and_preserves_database():
    started=threading.Event();conn=sqlite3.connect(':memory:',check_same_thread=False)
    def query(connection):
        started.set()
        return connection.execute('WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n').fetchone()
    task=asyncio.create_task(_index_thread_call(query,conn))
    assert await asyncio.to_thread(started.wait,5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):await asyncio.wait_for(task,2)
    assert conn.execute('PRAGMA quick_check').fetchone()[0]=='ok';conn.close()

@pytest.mark.asyncio
async def test_public_status_cancellation_does_not_close_active_integrity_connection(tmp_path, monkeypatch):
    from openagent_core.memory.db import MemoryDB
    from openagent_core.memory.operational import search
    db=MemoryDB(str(tmp_path/'state.db'));await db.connect()
    started=threading.Event();release=threading.Event();connections=[]
    real_open=search._open_index
    def opening(path):
        conn=real_open(path);connections.append(conn);return conn
    def integrity(connection):
        started.set();assert release.wait(5)
        return False
    try:
        await search.sync_operational_search(db)
        monkeypatch.setattr(search,'_open_index',opening)
        monkeypatch.setattr(search,'_index_has_rowid_mismatch',integrity)
        task=asyncio.create_task(search.operational_search_status(db))
        assert await asyncio.to_thread(started.wait,5)
        try:
            task.cancel()
            for _ in range(3):await asyncio.sleep(0)
            assert not task.done()
        finally:release.set()
        with pytest.raises(asyncio.CancelledError):await task
        with pytest.raises(sqlite3.ProgrammingError):connections[-1].execute('SELECT 1')
    finally:await db.close()
