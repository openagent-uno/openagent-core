import asyncio
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import aiosqlite
from openagent_core import PrincipalRef,ExecutionContext,RunRequest
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_core.configuration import sqlite_busy_timeout_ms
from openagent_core.memory.store.sqlite.sqlite import SqliteDb
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.core._runner.agent._storage import aupsert_session


class SqliteConcurrency(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'state.sqlite3'
        self.store=SqliteRuntimeStore(self.path);await self.store.start();self.addAsyncCleanup(self.store.close)
        self.assertEqual(
            self.store.connection.execute("PRAGMA busy_timeout").fetchone()[0],
            sqlite_busy_timeout_ms(),
        )
        self.other=await aiosqlite.connect(self.path);self.addAsyncCleanup(self.other.close)
        principal=PrincipalRef('host','tenant','alice')
        self.context=ExecutionContext(principal,principal,principal,'session','agent',(principal,))

    async def test_acceptance_wait_does_not_block_async_commit(self):
        await self.other.execute('BEGIN IMMEDIATE')
        async def commit():
            await asyncio.sleep(.03);await self.other.commit()
        release=asyncio.create_task(commit())
        record,created=await asyncio.wait_for(self.store.accept(RunRequest('run','session','key','hello'),self.context),1)
        await release
        self.assertTrue(created);self.assertEqual(record.status,'queued')

    async def test_cancel_drains_started_transaction_before_closing(self):
        await self.other.execute('BEGIN IMMEDIATE')
        request=asyncio.create_task(self.store.accept(RunRequest('run','session','key','hello'),self.context))
        await asyncio.sleep(.03);request.cancel();await asyncio.sleep(.01)
        self.assertFalse(request.done())
        await self.other.commit()
        with self.assertRaises(asyncio.CancelledError):await request
        self.assertEqual((await self.store.get('run')).status,'queued')
        await self.store.close()

    async def test_disconnected_submitter_does_not_strand_accepted_work(self):
        from openagent_core import Runtime,RuntimeServices,RuntimeSettings
        calls=[]
        class Policy:
            async def authorize(self,*args,**kwargs):return True
        async def execute(request,context,runtime):
            calls.append(request.run_id);return 'done'
        runtime=Runtime(RuntimeSettings('agent',self.path.parent),RuntimeServices(
            self.store,SimpleNamespace(execute=execute),Policy(),owns_store=False))
        await runtime.start()
        try:
            await self.other.execute('BEGIN IMMEDIATE')
            admission=asyncio.create_task(runtime.submit(RunRequest('run','session','key','hello'),self.context))
            await asyncio.sleep(.03);admission.cancel()
            await self.other.commit()
            with self.assertRaises(asyncio.CancelledError):await admission
            result=await runtime.wait('run',self.context)
            self.assertEqual(result.status,'success');self.assertEqual(calls,['run'])
        finally:await runtime.close()

    async def test_provider_upsert_allows_async_writer_to_commit(self):
        db=SqliteDb(db_file=str(self.path))
        try:
            db.upsert_session(AgentSession(session_id='history',user_id='alice',runs=[],created_at=1,updated_at=1))
            await self.other.execute('BEGIN IMMEDIATE')
            async def commit():
                await asyncio.sleep(.03);await self.other.commit()
            release=asyncio.create_task(commit())
            result=await asyncio.wait_for(aupsert_session(SimpleNamespace(db=db),
                AgentSession(session_id='history',user_id='alice',runs=[],created_at=1,updated_at=2)),1)
            await release
            self.assertIsNotNone(result)
        finally:db.close()


if __name__=='__main__':unittest.main()
