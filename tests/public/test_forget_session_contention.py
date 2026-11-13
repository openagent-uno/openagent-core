"""Reset must let the loop's existing writer finish and report storage failures."""
import asyncio
import sqlite3
from types import SimpleNamespace

import pytest
from openagent_core.models.dispatcher import TeamRouterProvider, ModelDispatcher
from openagent_core.memory.store.sqlite import SqliteDb


@pytest.mark.asyncio
async def test_router_forget_yields_until_same_loop_writer_commits(tmp_path, monkeypatch):
    monkeypatch.setenv('OPENAGENT_SESSION_STORE_BUSY_TIMEOUT_SECONDS', '0.3')
    path = tmp_path / 'history.db'
    store = SqliteDb(db_file=str(path))
    store._get_table(table_type='sessions', create_table_if_not_found=True)
    store.close()
    writer = sqlite3.connect(path)
    writer.execute("INSERT INTO sessions(session_id,session_type,created_at,updated_at) VALUES('chat-a','agent',1,1)")
    writer.commit()
    writer.execute("UPDATE sessions SET updated_at=2 WHERE session_id='chat-a'")
    committed = asyncio.Event()
    async def commit():
        await asyncio.sleep(0.05)
        writer.commit()
        committed.set()
    task = asyncio.create_task(commit())
    router = TeamRouterProvider('provider:model', [])
    router._db = SimpleNamespace(db_path=str(path))
    try:
        await router.forget_session('chat-a')
        assert committed.is_set(), 'the synchronous reset blocked its own writer'
        assert writer.execute("SELECT count(*) FROM sessions WHERE session_id='chat-a'").fetchone()[0] == 0
    finally:
        writer.rollback()
        await task
        writer.close()
        await router.shutdown()


@pytest.mark.asyncio
async def test_router_storage_failure_is_not_reported_as_success(tmp_path, monkeypatch):
    router = TeamRouterProvider('provider:model', [])
    router._db = SimpleNamespace(db_path=str(tmp_path / 'history.db'))
    def fail(*args, **kwargs):
        raise RuntimeError('storage unavailable')
    monkeypatch.setattr(SqliteDb, 'delete_session', fail)
    try:
        with pytest.raises(RuntimeError, match='storage unavailable'):
            await router.forget_session('chat-a')
    finally:
        await router.shutdown()


@pytest.mark.asyncio
async def test_dispatcher_forget_awaits_all_providers_and_propagates_failure():
    finished = []
    class Provider:
        def __init__(self, broken): self.broken = broken
        async def forget_session(self, sid):
            await asyncio.sleep(0)
            finished.append(self.broken)
            if self.broken: raise RuntimeError('storage unavailable')
    dispatcher = object.__new__(ModelDispatcher)
    dispatcher._db = None
    dispatcher._last_pick_by_session = {}
    dispatcher._team_providers = {'one': Provider(True), 'two': Provider(False)}
    with pytest.raises(RuntimeError, match='storage unavailable'):
        await dispatcher.forget_session('chat-a')
    assert sorted(finished) == [False, True]


@pytest.mark.asyncio
async def test_dispatcher_reset_after_restart_forgets_durable_history_without_cached_models(tmp_path):
    path = tmp_path / 'cold-history.db'
    store = SqliteDb(db_file=str(path))
    store._get_table('sessions', create_table_if_not_found=True)
    store.close()
    with sqlite3.connect(path) as writer:
        writer.execute("INSERT INTO sessions(session_id,session_type,created_at,updated_at) VALUES('chat-a','agent',1,1)")
    class Memory:
        db_path = str(path)
        async def delete_sdk_session(self, sid): pass
    dispatcher = ModelDispatcher([])
    dispatcher._db = Memory()
    await dispatcher.forget_session('chat-a')
    with sqlite3.connect(path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM sessions WHERE session_id='chat-a'").fetchone()[0] == 0
