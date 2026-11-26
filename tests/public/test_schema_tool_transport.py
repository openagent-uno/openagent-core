"""The public provider config drives an actual SDK request and the ordinary tool wire contract."""
import json
import unittest

import httpx
from openai import AsyncOpenAI, OpenAI

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.native_provider import NativeProvider
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.like import OpenAILike


TOOL = {"type": "function", "function": {
    "name": "save_note", "description": "Save the user's note",
    "parameters": {"type": "object", "properties": {"title": {"$ref": "#/$defs/title"}},
                   "$defs": {"title": {"type": "string"}}, "required": ["title"], "additionalProperties": False},
}}
ARGUMENTS = {"title": "blue notebook"}


class StructuredTools(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.wire = []
        self.replies = []

        def reply(request):
            body = json.loads(request.content)
            self.wire.append(body)
            return httpx.Response(200, json={"id": "real-sdk-envelope", "created": 1, "model": "small",
                "object": "chat.completion", "choices": [{"index": 0, "message": {"role": "assistant",
                "content": self.replies.pop(0)}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 13, "completion_tokens": 5, "total_tokens": 18}})

        entry = {"name": "example", "framework": "api-based", "kind": "llm", "base_url": "https://example.invalid/v1"}
        native = NativeProvider("example:small", providers_config=[entry])
        constructor, _ = native._load_runtime_model_class("example")
        self.assertIs(constructor, OpenAILike)
        configured = NativeProvider("example:small", providers_config=[{**entry, "tool_transport": "json_schema"}])
        constructor, _ = configured._load_runtime_model_class("example")
        transport = httpx.MockTransport(reply)
        self.model = constructor(id="small", api_key="fixture", client=OpenAI(api_key="fixture",
                http_client=httpx.Client(transport=transport)), async_client=AsyncOpenAI(api_key="fixture",
                http_client=httpx.AsyncClient(transport=transport)))
        self.addCleanup(self.model.client.close)
        self.addAsyncCleanup(self.model.async_client.close)

    def ask(self, **kwargs):
        return {"messages": [Message(role="system", content="Use authorized notes"), Message(role="user", content="Save it")],
                "assistant_message": Message(role="assistant"), "tools": [TOOL], **kwargs}

    async def test_standing_operations_reach_the_wire_with_the_declaration_in_the_user_role(self):
        instruction = "Record lasting preferences the user shares in conversation."
        declaration = "I always prefer a blue notebook."
        context = [Message(role="system", content=instruction), Message(role="user", content=declaration)]
        self.replies = [json.dumps({"next": {"action": "save_note", "arguments": ARGUMENTS}})]
        result = await self.model.ainvoke(**self.ask(messages=context))

        self.assertEqual(result.tool_calls[0]["function"]["name"], "save_note")
        system, user = self.wire[-1]["messages"]
        self.assertEqual(context[0].content, instruction)
        self.assertEqual(system["role"], "system")
        self.assertIn(instruction, system["content"])
        self.assertIn("Standing instructions can require an authorized operation", system["content"])
        self.assertIn(TOOL["function"]["description"], system["content"])
        self.assertIn(TOOL["function"]["name"], system["content"])
        self.assertNotIn("FUNCTION_NAME", system["content"])
        self.assertEqual((user["role"], user["content"]), ("user", declaration))
        self.assertNotIn(declaration, system["content"])

    async def test_stream_returns_one_validated_tool_then_final_with_receipt_and_usage(self):
        self.replies = [json.dumps({"next": {"action": "save_note", "arguments": ARGUMENTS}}),
                        json.dumps({"next": {"action":"reply", "answer": "Saved"}})]
        args = self.ask()
        frames = [frame async for frame in self.model.ainvoke_stream(**args)]
        self.assertEqual(len(frames), 1)
        self.assertIsNone(frames[0].content)
        self.assertEqual(frames[0].response_usage.input_tokens, 13)
        call, = self.model.parse_tool_calls(frames[0].tool_calls)
        self.assertEqual(call["function"]["name"], "save_note")
        self.assertEqual(json.loads(call["function"]["arguments"]), ARGUMENTS)
        assistant = Message(role="assistant", tool_calls=[call])
        result = Message(role="tool", tool_call_id=call["id"], content="receipt 74")
        args["messages"].extend((assistant, result))
        frame, = [frame async for frame in self.model.ainvoke_stream(**args)]
        self.assertEqual(frame.content, "Saved")
        self.assertEqual(frame.provider_data["id"], "real-sdk-envelope")
        wire = self.wire[-1]
        self.assertNotIn("tools", wire)
        self.assertNotIn("tool_choice", wire)
        self.assertEqual(wire["response_format"]["type"], "json_schema")
        self.assertEqual(json.loads(wire["messages"][-1]["content"])["tool_result"]["id"], call["id"])
        self.assertEqual(result.role, "tool")
        self.assertIn("previous_tool_calls", wire["messages"][-2]["content"])

    async def test_requested_json_example_in_final_reply_is_returned_verbatim(self):
        example = json.dumps({"next": {"action": "save_note", "arguments": ARGUMENTS}}, indent=2)
        self.replies = [json.dumps({"next": {"action": "reply", "answer": example}})]
        response = await self.model.ainvoke(**self.ask(messages=[
            Message(role="user", content="Show the tool decision as a JSON example.")
        ]))
        self.assertEqual(response.content, example)
        self.assertFalse(response.tool_calls)

    async def test_malformed_unadvertised_arguments_and_ignored_choice_never_call(self):
        forced = {"type": "function", "function": {"name": "save_note"}}
        for invalid in ['{"next":{"action":"reply", "answer":"skip"}}', '{"next":{"action":"unknown","arguments":{}}}',
                        '{"next":{"action":"save_note","arguments":{"title":7}}}',
                        '{"next":{"action":"save_note","action":"save_note","arguments":{"title":"ok"}}}']:
            with self.subTest(invalid=invalid):
                self.replies = [invalid]
                with self.assertRaises(ModelProviderError):
                    await self.model.ainvoke(**self.ask(tool_choice=forced))
        with self.assertRaises(ValueError):
            await self.model.ainvoke(**self.ask(tool_choice={"type": "function", "function": {"name": "missing"}}))
        self.assertEqual(len(self.wire), 4)

    def test_sync_and_structured_final(self):
        self.replies = [json.dumps({"next":{"action":"save_note","arguments":ARGUMENTS}}), '{"next":{"action":"reply", "answer":{"n":1}}}']
        response = self.model.invoke(**self.ask(tool_choice="required"))
        self.assertEqual(response.tool_calls[0]["function"]["name"], "save_note")
        final, = list(self.model.invoke_stream(**self.ask(tool_choice="none", response_format={"type": "json_object"})))
        self.assertEqual(json.loads(final.content), {"n": 1})

    async def test_hot_tool_requires_an_offered_dispatcher_and_current_unambiguous_descriptor(self):
        from openagent_core.capabilities import ToolDescriptor
        descriptor = ToolDescriptor("scoped-reference", "save_note", "Save note",
                                    TOOL["function"]["parameters"], "vault", "Private")
        current = [descriptor]
        async def visible():
            return current
        self.model.authorized_hot_tools = visible
        dispatcher = {"type": "function", "function": {"name": "tool_search_call_tool",
                      "parameters": {"type": "object"}}}
        self.replies = [json.dumps({"next": {"action": "save_note", "arguments": ARGUMENTS}})] * 3
        args = self.ask(tools=[dispatcher])
        call, = (await self.model.ainvoke(**args)).tool_calls
        self.assertEqual(call["function"]["name"], "tool_search_call_tool")
        self.assertEqual(json.loads(call["function"]["arguments"]), {"tool_ref": "scoped-reference", "args": ARGUMENTS})
        current.append(descriptor)
        with self.assertRaises(ModelProviderError):
            await self.model.ainvoke(**args)
        current.clear()
        with self.assertRaises(ModelProviderError):
            await self.model.ainvoke(**args)

    def test_schema_rejects_remote_refs_before_request_and_preserves_property_names(self):
        forbidden = {"type":"function","function":{"name":"example","parameters":{"$ref":"https://example.invalid/untrusted"}}}
        with self.assertRaises(ValueError):
            self.model.invoke(**self.ask(tools=[forbidden]))
        self.assertFalse(self.wire)
        unusual = {"type":"function","function":{"name":"example","parameters":{"type":"object",
                   "properties":{"$id":{"enum":["data"]}},"required":["$id"],"additionalProperties":False}}}
        self.replies = ['{"next":{"action":"example","arguments":{"$id":"data"}}}']
        self.assertEqual(self.model.invoke(**self.ask(tools=[unusual])).tool_calls[0]["function"]["name"],"example")
