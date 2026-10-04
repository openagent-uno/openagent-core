"""Visual MCP results remain visible to models behind tool-search."""

import base64
import unittest

from openagent_core.mcp._runtime.function import ToolResult
from openagent_core.mcp.servers.tool_search.adapters import _model_visible_result
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.chat import OpenAIChat


class ToolSearchMediaTests(unittest.TestCase):
    def test_screenshot_is_model_image_and_wire_envelope_is_preserved(self):
        png = b"\x89PNG\r\n\x1a\nfixture"
        encoded = base64.b64encode(png).decode("ascii")
        envelope = {
            "content": [
                {"type": "text", "text": '{"image_width":1280,"image_height":920}'},
                {"type": "image", "data": encoded, "mimeType": "image/png"},
            ],
            "structuredContent": {"display": ":101"},
            "_meta": {"source": "computer-control"},
            "isError": False,
        }

        result = _model_visible_result(envelope)

        self.assertIsInstance(result, ToolResult)
        self.assertEqual(result.mcp_result, envelope)
        self.assertEqual(result.images[0].content, png)
        self.assertNotIn(encoded, result.content)
        self.assertIn("inspect the pixels", result.content)

        message = Message(role="tool", tool_call_id="call_1", content=result.content,
                          images=result.images)
        formatted = OpenAIChat(id="test", api_key="unused")._format_message(message)
        self.assertEqual(formatted["role"], "tool")
        self.assertEqual(formatted["content"][1]["type"], "image_url")
        self.assertTrue(formatted["content"][1]["image_url"]["url"].startswith(
            "data:image/png;base64,"))

    def test_text_only_result_keeps_existing_contract(self):
        envelope = {"content": [{"type": "text", "text": "ok"}], "isError": False}
        self.assertIs(_model_visible_result(envelope), envelope)

    def test_invalid_image_does_not_claim_pixels_are_visible(self):
        result = _model_visible_result({"content": [{"type": "image", "data": "not-base64"}]})
        self.assertIsInstance(result, ToolResult)
        self.assertIsNone(result.images)
        self.assertIn("Rejected MCP image", result.content)
