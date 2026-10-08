from types import SimpleNamespace
import unittest

from openagent_core.capabilities import CapabilityUnavailable, ToolDescriptor
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.runtime import execution_scope
from openagent_core.workflow.executor import WorkflowExecutor


class _DB:
    def __init__(self):
        self.runs = {}

    async def add_workflow_run(self, *, workflow_id, trigger, inputs, run_id=None):
        run_id = run_id or "workflow-run"
        self.runs[run_id] = {
            "id": run_id,
            "workflow_id": workflow_id,
            "status": "running",
            "trigger": trigger,
            "inputs": inputs,
            "trace": [],
        }
        return run_id

    async def update_workflow_run(self, run_id, **updates):
        self.runs[run_id].update(updates)

    async def update_workflow(self, workflow_id, **updates):
        return None

    async def get_workflow_run(self, run_id):
        return self.runs.get(run_id)


class _Agent:
    def __init__(self, pool):
        self._mcp = pool
        self.model = None

    async def release_session(self, session_id):
        return None


class _LegacyPool:
    """Standalone keeps external MCPs out of this deferred pool."""

    _toolkit_by_name = {"tool-search": SimpleNamespace(functions={})}

    def list_mcp_tools(self):
        return [{"mcp_name": "tool-search", "tools": []}]

    def toolkit_by_name(self, name):
        return self._toolkit_by_name.get(name)


class _Catalog:
    def __init__(self, *, expose_tool=True):
        self.expose_tool = expose_tool
        self.calls = []
        self.discoveries = 0

    async def discover(self, context):
        self.discoveries += 1
        if not self.expose_tool:
            return ()
        return (
            ToolDescriptor(
                "web-ref",
                "web_search_get_results",
                "Search the web",
                {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
                "web-search",
                "Agent workspace",
            ),
        )

    async def call_tool(self, tool_ref, arguments, context):
        self.calls.append((tool_ref, arguments, context))
        return {"content": [{"type": "text", "text": "https://example.test"}]}


class WorkflowCatalogDispatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        principal = PrincipalRef("test", "tenant", "alice")
        self.context = ExecutionContext(
            principal,
            principal,
            principal,
            "session",
            "agent",
            (principal,),
        )

    async def test_runtime_workflow_uses_catalog_not_deferred_agent_pool(self):
        catalog = _Catalog()
        runtime = SimpleNamespace(capabilities=catalog)
        executor = WorkflowExecutor(_Agent(_LegacyPool()), _DB())
        workflow = {
            "id": "wf-runtime-catalog",
            "name": "Web search",
            "graph": {
                "version": 1,
                "nodes": [
                    {
                        "id": "search",
                        "type": "mcp-tool",
                        "config": {
                            "mcp_name": "web-search",
                            "tool_name": "web_search_get_results",
                            "args": {"query": "OpenAgent"},
                        },
                    }
                ],
                "edges": [],
                "variables": {},
            },
        }

        with execution_scope(runtime, self.context, "run"):
            final = await executor.run(workflow)

        self.assertEqual(final["status"], "success", final)
        self.assertEqual(catalog.discoveries, 2)
        self.assertEqual(
            catalog.calls,
            [("web-ref", {"query": "OpenAgent"}, self.context)],
        )
        self.assertEqual(
            final["outputs"]["value"]["result"]["content"][0]["text"],
            "https://example.test",
        )

    async def test_catalog_absence_never_falls_through_to_agent_pool(self):
        legacy_called = False

        async def legacy_tool(**arguments):
            nonlocal legacy_called
            legacy_called = True
            return arguments

        pool = _LegacyPool()
        pool._toolkit_by_name = {
            "web-search": SimpleNamespace(
                functions={},
                async_functions={"web_search_get_results": legacy_tool},
            )
        }
        catalog = _Catalog(expose_tool=False)
        runtime = SimpleNamespace(capabilities=catalog)
        executor = WorkflowExecutor(_Agent(pool), _DB())
        from openagent_core.workflow.executor import _RunCtx, _h_mcp_tool

        with execution_scope(runtime, self.context, "run"):
            with self.assertRaises(CapabilityUnavailable):
                await _h_mcp_tool(
                    executor,
                    {"id": "search", "type": "mcp-tool"},
                    {
                        "mcp_name": "web-search",
                        "tool_name": "web_search_get_results",
                        "args": {"query": "OpenAgent"},
                    },
                    _RunCtx("run", "workflow", {}, {}),
                )

        self.assertFalse(legacy_called)


if __name__ == "__main__":
    unittest.main()
