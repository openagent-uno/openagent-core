"""Exercise the native runner; script only the external model response boundary."""
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from openagent_core.core.execution_policy import set_execution_policy, reset_execution_policy
from openagent_core.models.native_provider import NativeProvider, _max_tool_calls_per_run
from openagent_core.models.providers.base import Model
from openagent_core.models.providers.response import ModelResponse


class LongToolRuns(unittest.IsolatedAsyncioTestCase):
    async def run_document_work(self, environment, policy=None):
        completed = []
        def prepare_page() -> str:
            """Perform the next deterministic document preparation step."""
            completed.append("prepare")
            return "prepared"
        def deliver_document() -> str:
            """Deliver the prepared document to its authorized recipient."""
            completed.append("deliver")
            return "delivered"
        class ProviderIO(Model):
            def __init__(self):
                super().__init__(id="fixture", provider="fixture")
                self.previous_completed = -1
            def invoke(self, *a, **k): raise AssertionError("no network")
            def invoke_stream(self, *a, **k): raise AssertionError("no network")
            async def ainvoke(self, *a, **k):
                return ModelResponse(role="assistant", content='{"summary":"Document prepared.","topics":["document"]}')
            async def ainvoke_stream(self, *a, **k):
                if "deliver" in completed:
                    yield ModelResponse(role="assistant", content="Document delivered.")
                elif not k.get("tools") or len(completed) == self.previous_completed:
                    yield ModelResponse(role="assistant", content="Stopped by explicit budget.")
                else:
                    self.previous_completed = len(completed)
                    name = "prepare_page" if len(completed) < 61 else "deliver_document"
                    yield ModelResponse(role="assistant", tool_calls=[{"id": f"call-{len(completed)}",
                        "type": "function", "function": {"name": name, "arguments": "{}"}}])
            def _parse_provider_response(self, response, **k): return response
            def _parse_provider_response_delta(self, response): return response
        token = set_execution_policy(policy or {})
        try:
            with tempfile.TemporaryDirectory() as directory, patch(
                "openagent_core.models.native_provider.runtime_environment", return_value=environment):
                native = NativeProvider("openai:fixture", api_key="fixture-only",
                    db_path=str(Path(directory) / "runtime.sqlite"))
                try:
                    with patch.object(native, "_build_runtime_model", return_value=ProviderIO()), patch.object(
                        native, "_compatible_mcp_toolkits", return_value=([prepare_page, deliver_document], [])):
                        text = "".join([chunk async for chunk in native.stream(
                            [{"role": "user", "content": "Prepare and deliver the document."}],
                            session_id="document-work")])
                finally:
                    await native.shutdown()
        finally:
            reset_execution_policy(token)
        return completed, text

    async def test_normal_document_work_can_deliver_after_sixty_tools(self):
        completed, text = await self.run_document_work({})
        self.assertEqual(completed, ["prepare"] * 61 + ["deliver"])
        self.assertEqual(text, "Document delivered.")

    async def test_explicit_operator_limit_still_stops_execution(self):
        for environment, policy in [({"OPENAGENT_MAX_TOOL_CALLS_PER_RUN": "3"}, None),
                                    ({}, {"max_tool_calls": 3})]:
            with self.subTest(environment=environment, policy=policy):
                completed, text = await self.run_document_work(environment, policy)
                self.assertEqual(completed, ["prepare"] * 3)
                self.assertEqual(text, "Stopped by explicit budget.")

    async def test_bad_operator_setting_keeps_the_fail_safe(self):
        with patch("openagent_core.models.native_provider.runtime_environment",
                   return_value={"OPENAGENT_MAX_TOOL_CALLS_PER_RUN": "misspelled"}):
            self.assertEqual(_max_tool_calls_per_run(), 60)

    async def test_lean_background_profiles_keep_their_limits(self):
        with patch("openagent_core.models.native_provider.runtime_environment", return_value={}), patch(
            "openagent_core.core.execution_profile.lean_local_event_active", return_value=True):
            for task, expected in [(False, 10), (True, 40)]:
                with self.subTest(task=task), patch(
                    "openagent_core.core.execution_profile.lean_local_task_active", return_value=task):
                    self.assertEqual(_max_tool_calls_per_run(), expected)
