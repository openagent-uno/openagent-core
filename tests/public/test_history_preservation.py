"""Rehearse storage adoption with real legacy envelopes, never live user data."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from openagent_core import PrincipalRef,ExecutionContext,Runtime,RuntimeSettings,RuntimeServices,RunRequest
from openagent_core.engine import MemoryDB
from openagent_core.memory.store.sqlite.sqlite import SqliteDb
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.core._run_state.agent import RunOutput,RunInput
from openagent_core.core._run_state.base import RunStatus
from openagent_storage_sqlite import SqliteRuntimeStore


def decoded(value):
    for _ in range(3):
        if not isinstance(value,str):return value
        value=json.loads(value)
    return value


class HistoryPreservation(unittest.IsolatedAsyncioTestCase):
    async def test_adoption_retains_historical_user_and_author_ids_and_full_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder=Path(temporary);path=folder/'state.sqlite3'
            provider_store=SqliteDb(db_file=str(path),session_table='sessions')
            old_run=RunOutput(run_id='historical-run',agent_id='agent',session_id='shared',user_id='unresolved-original',
                input=RunInput(input_content='original question'),content='original answer',status=RunStatus.completed)
            old_session=AgentSession(session_id='shared',agent_id='agent',user_id='provider-era-owner',
                metadata={'client_id':'historic-owner','network_id':'tenant'},runs=[old_run],created_at=1,updated_at=2)
            provider_store.upsert_session(old_session)
            provider_store.close()
            memory=MemoryDB(str(path))
            self.addAsyncCleanup(memory.close)
            await memory.connect()
            row=await (await memory._conn.execute("SELECT user_id,runs,metadata FROM sessions WHERE session_id='shared'")).fetchone()
            self.assertEqual(row[0],'provider-era-owner')
            self.assertEqual(decoded(row[1])[0]['user_id'],'unresolved-original')
            self.assertEqual(decoded(row[2])['client_id'],'historic-owner')
            tenant=(await (await memory._conn.execute("SELECT tenant_id FROM sessions_v2 WHERE id='shared'")).fetchone())[0]
            await memory.close()
            # Repeated migration is harmless and never assigns today's user to
            # an unresolved historic author.
            await memory.connect();await memory.close()
            store=SqliteRuntimeStore(path)
            principal=PrincipalRef('host',tenant,'current-user')
            context=ExecutionContext(principal,principal,principal,'shared','agent',(principal,))
            class Policy:
                async def authorize(self,*args,**kwargs):return True
            async def execute(request,ctx,runtime):
                db=SqliteDb(db_file=str(path),session_table='sessions')
                try:
                    previous=db.get_session('shared',user_id='openagent')
                    self.assertEqual(previous.user_id,'provider-era-owner')
                    self.assertEqual(previous.runs[0].user_id,'unresolved-original')
                    previous.user_id='openagent'
                    previous.runs.append(RunOutput(run_id=request.run_id,agent_id='agent',session_id='shared',
                        user_id=ctx.author.key,input=RunInput(input_content=request.input),content='new answer',status=RunStatus.completed))
                    db.upsert_session(previous)
                finally:db.close()
                return 'new answer'
            runtime=Runtime(RuntimeSettings('agent',folder),RuntimeServices(store,SimpleNamespace(execute=execute),Policy()))
            await runtime.start()
            try:
                await runtime.submit(RunRequest('new','shared','new','new question'),context)
                record=await runtime.wait('new',context)
                self.assertEqual(record.status,'success',record.output)
                row=store.connection.execute("SELECT user_id,runs FROM sessions WHERE session_id='shared'").fetchone()
                self.assertEqual(row[0],'provider-era-owner')
                self.assertEqual([item['run_id'] for item in decoded(row[1])],['historical-run','new'])
                self.assertEqual(decoded(row[1])[0]['user_id'],'unresolved-original')
                self.assertEqual(store.connection.execute("SELECT status FROM session_runs WHERE id='new'").fetchone()[0],'success')
            finally:await runtime.close()


if __name__=='__main__':unittest.main()
