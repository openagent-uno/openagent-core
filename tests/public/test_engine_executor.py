"""The public engine adapter preserves terminal stream failure semantics."""
from types import SimpleNamespace
import unittest

from openagent_core.engine import AgentExecutor


class _Agent:
    def __init__(self, events):
        self.events = events

    async def run_stream(self, *_args, **_kwargs):
        for event in self.events:
            yield event


class _Policy:
    async def authorize(self, *_args, **_kwargs):
        return True


class _Store:
    def __init__(self):
        self.events = []

    async def append_event(self, run_id, kind, payload):
        self.events.append((run_id, kind, payload))


class EngineExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = _Store()
        self.runtime = SimpleNamespace(
            services=SimpleNamespace(authorizer=_Policy(), store=self.store)
        )
        self.context = SimpleNamespace(
            tenant_id="tenant", session_id="session", agent_id="agent",
            audience=(), author=SimpleNamespace(kind="user", key="alice"),
        )
        self.request = SimpleNamespace(
            input="hello", session_id="session", run_id="run",
            model_ref=None, attachments=(),
        )

    async def test_errored_done_is_a_failed_run_and_is_not_published(self):
        executor = AgentExecutor(_Agent([
            {"kind": "delta", "text": "partial"},
            {"kind": "done", "text": "internal detail", "errored": True,
             "error_public": "PRIVATE arbitrary detail", "error_code":"model_unavailable"},
        ]))
        with self.assertRaisesRegex(RuntimeError, "selected AI model is unavailable"):
            await executor.execute(self.request, self.context, self.runtime)
        self.assertEqual(
            ["delta"],
            [payload["kind"] for _, kind, payload in self.store.events if kind == "run.stream"],
        )

    async def test_successful_done_remains_a_success(self):
        executor = AgentExecutor(_Agent([
            {"kind": "delta", "text": "hello"},
            {"kind": "done", "text": "hello"},
        ]))
        self.assertEqual("hello", await executor.execute(self.request, self.context, self.runtime))

    async def test_output_marker_without_publisher_never_opens_arbitrary_file(self):
        self.assertIn("unavailable",await AgentExecutor(_Agent([
            {"kind":"done","text":"Result [FILE:/private/not-authorized.csv]"}
        ])).execute(self.request,self.context,self.runtime))
        frame=self.store.events[-1][2]
        self.assertEqual([],frame["attachments"])
        self.assertNotIn("/private",str(frame))

    async def test_runtime_tool_without_publisher_fails_without_filesystem_inspection(self):
        from unittest.mock import patch
        from openagent_core.runtime import execution_scope
        from openagent_core.mcp.servers.attachments.adapters import build_runtime_toolkit
        self.runtime.service=lambda _key:None
        with execution_scope(self.runtime,self.context,"run"), patch("pathlib.Path.resolve",side_effect=AssertionError("implicit filesystem lookup")):
            reply=await build_runtime_toolkit().tools[0]("/private/sentinel.csv")
        self.assertFalse(reply["ok"])
        self.assertIn("capability is unavailable",reply["error"])

    async def test_host_file_delivery_errors_preserve_answer_but_revocation_never_publishes(self):
        for error in (FileNotFoundError, PermissionError, ValueError):
            with self.subTest(error=error.__name__):
                async def publish(*_args):raise error("PRIVATE PATH")
                self.runtime.service=lambda _key:SimpleNamespace(publish=publish)
                agent=_Agent([{"kind":"done","text":"Valid answer. [FILE:/not-an-authorized-file.csv]"}])
                agent.memory_db=SimpleNamespace(db_path="/tmp/attachment-test/state.sqlite")
                result=await AgentExecutor(agent).execute(self.request,self.context,self.runtime)
                self.assertIn("Valid answer.",result)
                self.assertIn("attachment is unavailable",result)
                self.assertNotIn("PRIVATE",result)
        allowed=True
        async def authorize(*_args,**_kwargs):return allowed
        async def revoke(*_args):
            nonlocal allowed
            allowed=False
            raise PermissionError("PRIVATE PATH")
        self.runtime.services.authorizer=SimpleNamespace(authorize=authorize)
        self.runtime.service=lambda _key:SimpleNamespace(publish=revoke)
        self.store.events.clear()
        with self.assertRaises(PermissionError):
            await AgentExecutor(agent).execute(self.request,self.context,self.runtime)
        self.assertEqual([],self.store.events)


if __name__ == "__main__":
    unittest.main()
