"""PTC scripts exercise the real process, RPC and authorization boundaries."""
import os
from pathlib import Path
import sys
import tempfile
import shutil
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from openagent_core.capabilities import CapabilityCatalog, ToolDefinition
from openagent_core.contracts import ExecutionContext, PrincipalRef, require_authorized
from openagent_core.core.dry_run import is_dry_run
from openagent_core.runtime import execution_scope
from openagent_core.mcp.servers.ptc.handlers import run_python_impl
from openagent_core.mcp.pool import MCPPool
from openagent_execution import LocalBackend, ProcessCodeExecutor


class PtcExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.catalog=CapabilityCatalog(self)
        p=PrincipalRef('host','tenant','alice')
        self.context=ExecutionContext(p,p,p,'session','agent',(p,))
        self.calls=[]
        self.denied=False
        async def echo(arguments,context):
            self.calls.append((arguments,context,is_dry_run()))
            return {'content':[{'type':'text','text':'complete'}], 'structuredContent':arguments, '_meta':{'full':True}}
        class Source:
            async def discover(self,context): return [ToolDefinition('echo','Echo',{'type':'object'})]
            async def call_tool(self,name,arguments,context): return await echo(arguments,context)
        source=Source()
        self.catalog.register('fixture',source,source,target_label='fixture environment')
        self.executor=ProcessCodeExecutor(backend=LocalBackend(environment={}),
            python_executable=sys.executable,bridge_transport='unix',isolated=False,environment={'PATH':'/usr/bin:/bin'})
        self.pool=MCPPool([])
        self.pool.bind_capability_catalog(self.catalog)
        self.runtime=SimpleNamespace(capabilities=self.catalog,authorize=self.require,
            services=SimpleNamespace(code_executor=self.executor,store=self),settings=SimpleNamespace(workspace=Path('/tmp')))
        self.settings=SimpleNamespace(require_sandbox=False,timeout_s=2,max_tool_calls=2,allowed_tools=None)

    async def authorize(self,context,action,resource,*,audience=()): return not self.denied
    async def require(self,*args,**kwargs): return await require_authorized(self,*args,**kwargs)
    async def begin_tool(self,*args,**kwargs): pass
    async def finish_tool(self,*args,**kwargs): pass
    async def asyncTearDown(self): await self.executor.close()

    async def run_code(self,code,**settings):
        with execution_scope(self.runtime,self.context,'run'):
            return await run_python_impl(code,pool=self.pool,
                settings=SimpleNamespace(**{**vars(self.settings),**settings}),dry_run=True)

    async def test_real_script_preserves_envelope_context_and_dry_run_without_ambient_env(self):
        reference=(await self.catalog.discover(self.context))[0].tool_ref
        with patch.dict(os.environ,{'OPENAGENT_TEST_SECRET':'private','UNRELATED_ENV_CANARY':'also-private'}):
            result=await self.run_code(f'import os\nprint(os.environ.get("OPENAGENT_TEST_SECRET"))\n'
                f'print(os.environ.get("UNRELATED_ENV_CANARY"))\nprint(call_tool({reference!r},{{"value":3}}))')
        self.assertEqual(result['status'],'ok',result)
        self.assertTrue(result['output'].startswith('None\nNone\n'))
        self.assertIn("'_meta': {'full': True}",result['output'])
        self.assertEqual(self.calls,[({'value':3},self.context,True)])

    async def test_missing_executor_or_isolation_never_executes(self):
        self.runtime.services.code_executor=None
        result=await self.run_code('raise AssertionError("must not execute")')
        self.assertEqual(result['status'],'refused')
        self.runtime.services.code_executor=self.executor
        result=await self.run_code('raise AssertionError("must not execute")',require_sandbox=True)
        self.assertEqual(result['status'],'refused')

    async def test_budget_allowlist_and_current_revocation_use_same_dispatcher(self):
        ref=(await self.catalog.discover(self.context))[0].tool_ref
        result=await self.run_code(f'call_tool({ref!r},{{}})\ncall_tool({ref!r},{{}})',max_tool_calls=1)
        self.assertEqual(result['status'],'error',result)
        self.assertEqual(len(self.calls),1)
        self.assertIn('max_tool_calls',result['stderr'])
        result=await self.run_code(f'call_tool({ref!r},{{}})',allowed_tools=['different'])
        self.assertEqual(result['status'],'error')
        self.assertEqual(len(self.calls),1)
        self.denied=True
        result=await self.run_code(f'call_tool({ref!r},{{}})')
        self.assertEqual(result['status'],'error')
        self.assertEqual(len(self.calls),1)

    async def test_timeout_stops_real_child_process(self):
        result=await self.run_code('import time\ntime.sleep(30)',timeout_s=0.1)
        self.assertEqual(result['status'],'error')
        self.assertTrue(result['timed_out'])
        self.assertLess(result['duration_s'],3)

    async def test_file_rpc_runs_real_script_with_same_catalog_and_budget(self):
        # Exercise the remote-style file protocol with a local test filesystem;
        # this validates transport, not Docker or pod isolation.
        class FileBackend(LocalBackend):
            async def container_mkdir(self,path): Path(path).mkdir()
            async def container_listdir(self,path): return [p.name for p in Path(path).iterdir()]
            async def container_read(self,path):
                try: return Path(path).read_bytes()
                except FileNotFoundError: return None
            async def container_write(self,path,data):
                temporary=Path(path+'.tmp')
                temporary.write_bytes(data)
                temporary.replace(path)
            async def container_rmtree(self,path): shutil.rmtree(path)
        with tempfile.TemporaryDirectory() as directory:
            executor=ProcessCodeExecutor(backend=FileBackend(environment={}),
                python_executable=sys.executable, bridge_transport='files',isolated=False,
                environment={},workdir=directory)
            self.runtime.services.code_executor=executor
            try:
                ref=(await self.catalog.discover(self.context))[0].tool_ref
                result=await self.run_code(f'print(call_tool({ref!r},{{"value":"files"}}))')
                self.assertEqual(result['status'],'ok',result)
                self.assertEqual(result['tool_calls_made'],1)
                self.assertIn("'value': 'files'",result['output'])
                self.assertEqual(list(Path(directory).iterdir()),[])
            finally:
                await executor.close()


if __name__=='__main__': unittest.main()
