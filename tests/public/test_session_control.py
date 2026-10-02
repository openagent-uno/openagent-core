"""Public session controls preserve the engine's model and compaction rules."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from openagent_core.session_control import SessionControl
from openagent_core.stream.presentation import assistant_text


class SessionControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = SimpleNamespace(
            get_model_by_runtime_id=AsyncMock(return_value={
                "kind": "llm", "enabled": True, "provider_enabled": True,
                "display_name": "Example",
            }),
            pin_session_model=AsyncMock(),
            unpin_session_model=AsyncMock(),
            get_session_pin=AsyncMock(return_value="api-based:example"),
        )
        self.agent = SimpleNamespace(memory_db=self.db, model=object())
        self.control = SessionControl(self.agent)

    async def test_pin_requires_enabled_llm_and_provider(self):
        for invalid in (None, {"kind": "tts", "enabled": True, "provider_enabled": True},
                        {"kind": "llm", "enabled": False, "provider_enabled": True},
                        {"kind": "llm", "enabled": True, "provider_enabled": False}):
            self.db.get_model_by_runtime_id.return_value = invalid
            with self.assertRaises(LookupError):
                await self.control.pin_model("session", "api-based:example")
        self.db.pin_session_model.assert_not_awaited()
        self.db.get_model_by_runtime_id.return_value = {
            "kind": "llm", "enabled": True, "provider_enabled": True,
        }
        await self.control.pin_model("session", "api-based:example")
        self.db.pin_session_model.assert_awaited_once_with("session", "api-based:example")

    async def test_clear_and_read_pin(self):
        self.assertEqual("api-based:example", await self.control.model_pin("session"))
        await self.control.clear_model_pin("session")
        self.db.unpin_session_model.assert_awaited_once_with("session")

    async def test_compaction_uses_core_session_lock(self):
        from openagent_core.core import compaction
        with patch.object(compaction, "compact", new_callable=AsyncMock) as compact:
            compact.return_value = {"folded_runs": 2}
            self.assertEqual({"folded_runs": 2}, await self.control.compact("session"))
            compact.assert_awaited_once_with(
                "session", self.agent.model, self.agent, on_status=None, keep=0,
            )


class StreamPresentationTests(unittest.TestCase):
    def test_tool_progress_is_removed_without_dropping_answer(self):
        self.assertEqual("Answer", assistant_text(
            "shell_exec(command='ls') completed in 0.0012s. Answer"
        ))
        self.assertEqual("Answer", assistant_text("Answer"))
