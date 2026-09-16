from pathlib import Path
import tempfile
import unittest
from openagent_core import Runtime,RuntimeServices,RuntimeSettings,PrincipalRef,ExecutionContext,RunRequest
from openagent_core.engine import Agent,AgentExecutor,module_pool
from openagent_core.models.base import BaseModel
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    async def authorize(self,*args,**kwargs):return True


class PromptAdmission(unittest.IsolatedAsyncioTestCase):
    async def test_prompt_revisions_are_durable_before_failed_inference(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);store=SqliteRuntimeStore(directory/'state.sqlite3')
            observed=[]
            class FailingModel(BaseModel):
                model='fixture'
                async def stream(self,messages,system=None,**kwargs):
                    events=await store.events('run')
                    observed.extend(events)
                    raise RuntimeError('fixture provider failure')
                    yield ''
                async def generate(self,*args,**kwargs):raise RuntimeError('fixture provider failure')
            agent=Agent(model=FailingModel(),mcp_pool=module_pool((),db_path=str(store.path)),
                system_prompt='Host instructions',config={'_enabled_prompt_modules':[]})
            executor=AgentExecutor(agent)
            runtime=Runtime(RuntimeSettings('agent',directory),RuntimeServices(store,executor,Policy()),modules=(executor,))
            principal=PrincipalRef('host','tenant','alice')
            context=ExecutionContext(principal,principal,principal,'session','agent',(principal,))
            await runtime.start()
            try:
                await runtime.submit(RunRequest('run','session','key','hello'),context)
                result=await runtime.wait('run',context)
                self.assertEqual(result.status,'failed')
                receipts=[event for event in observed if event.kind=='run.prompt']
                self.assertEqual(len(receipts),1)
                self.assertTrue(receipts[0].payload['blocks'])
                self.assertTrue(any(row['id']=='host.system' for row in receipts[0].payload['blocks']))
            finally:await runtime.close()


if __name__=='__main__':unittest.main()
