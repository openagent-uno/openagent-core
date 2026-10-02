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
             "error_public": "The model is unavailable."},
        ]))
        with self.assertRaisesRegex(RuntimeError, "model is unavailable"):
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


if __name__ == "__main__":
    unittest.main()
