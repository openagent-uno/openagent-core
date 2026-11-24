"""Exercise native and structured provider loops with actual published vault receipts."""

import asyncio
from contextvars import Context
import json
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

import httpx
from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import ExecutionContext, ModuleCatalog, ModuleConfig, PrincipalRef, RunRequest, Runtime, RuntimeProfile, RuntimeServices, RuntimeSettings
from openagent_core.mcp._runtime.function import Function
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.response import ModelResponseEvent
from openagent_core.models.providers.openai.like import OpenAILike
from openagent_core.models.providers.openai.schema_tools import SchemaToolChat
from openagent_core.modules import ServiceRegistry
from openagent_module_vault import descriptor
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    revoked_tool = False

    async def authorize(self, context, action, resource, *, audience=()):
        return not (self.revoked_tool and resource.kind == "tool")


class PublishedReads(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.engine = create_async_engine("sqlite+aiosqlite:///" + str(root / "vault.db"))
        self.addAsyncCleanup(self.engine.dispose)
        self.policy = Policy()

        async def space(context):
            return context.author.key

        self.vault = SqlDocumentVault(self.engine, self.policy, space)
        await self.vault.initialize()
        self.runtime = Runtime(RuntimeSettings("agent"), RuntimeServices(SqliteRuntimeStore(root/"runs.db"),
            self, self.policy, registry=ServiceRegistry({SqlDocumentVault: self.vault})),
            profile=RuntimeProfile(1, {"vault": ModuleConfig(frozenset({"service", "agent_tools"}), {"backend": "documents"})}),
            module_catalog=ModuleCatalog((descriptor,)))
        await self.runtime.start()
        self.addAsyncCleanup(self.runtime.close)
        actor = PrincipalRef("test", "tenant", "owner")
        self.ctx = ExecutionContext(actor, actor, actor, "session", "agent", (actor,))
        await self.vault.write(self.ctx, "example.md", "verified-value", expected_revision=0)
        self.requests = []
        self.reads = 0

    async def execute(self, request, context, runtime):
        tools = {t.name: t for t in await runtime.capabilities.discover(context)}

        async def vault_read_note(path: str):
            value = await runtime.capabilities.call_tool(tools["vault_read_note"].tool_ref, {"path": path}, context)
            self.reads += 1
            return json.dumps(value)

        async def vault_write_note(path: str):
            receipt = await runtime.capabilities.call_tool(tools["vault_write_note"].tool_ref, {
                "path": path, "content": "changed-value", "expected_revision": 1}, context)
            return json.dumps(receipt)

        functions = [Function.from_callable(vault_read_note), Function.from_callable(vault_write_note)]
        messages = [Message(role="system", content="Answer using the document vault."),
                    Message(role="user", content="What is the verified value?")]
        if self.streaming:
            chunks = []
            async for frame in self.model.aresponse_stream(messages, tools=functions, tool_call_limit=9):
                if frame.event == ModelResponseEvent.assistant_response.value and isinstance(frame.content, str):
                    chunks.append(frame.content)
            return "".join(chunks)
        response = await self.model.aresponse(messages, tools=functions, tool_call_limit=9)
        return response.content

    async def exercise(self, schema, *, change_revision=False, revoke_before_publication=False, alternate_write=False, streaming=False, violate_final=False):
        self.streaming = streaming
        expected = "changed-value" if change_revision else "verified-value"
        if change_revision:
            check = self.vault.is_current_revision

            async def changed(context, path, revision):
                if self.reads == 3:
                    await asyncio.create_task(self.vault.write(
                        context, path, expected, expected_revision=1), context=Context())
                return await check(context, path, revision)
            self.vault.is_current_revision = changed


        def fixture(request):
            body = json.loads(request.content)
            self.requests.append(body)
            if len(self.requests) > 5:
                raise AssertionError("Unbounded read loop")
            if schema:
                options = body["response_format"]["json_schema"]["schema"]["properties"]["next"]["anyOf"]
                offered = any(o["properties"]["action"]["const"] == "vault_read_note" for o in options)
            else:
                offered = bool(body.get("tools"))
            message = {"role": "assistant"}
            if offered and not (change_revision and self.reads == 4) and not (alternate_write and len(self.requests) >= 4):
                selected = "vault_write_note" if alternate_write and len(self.requests) == 3 else "vault_read_note"
                call = {"name": selected, "arguments": json.dumps({"path": "example.md"})}
                if schema:
                    message["content"] = json.dumps({"next":{"action":call["name"],"arguments":json.loads(call["arguments"])}})
                else:
                    message["tool_calls"] = [{"type":"function", "id":"call_" + str(len(self.requests)), "function":call}]
            else:
                if alternate_write:
                    self.assertIn("revision", str(body["messages"][-1]["content"]))
                else:
                    self.assertIn(expected, str(body["messages"][-1]["content"]))
                message["content"] = json.dumps({"next":{"action":"reply","answer":expected}}) if schema else expected
                if violate_final:
                    message = {"role":"assistant", "tool_calls":[{"type":"function","id":"invalid-final",
                        "function":{"name":"vault_write_note","arguments":json.dumps({"path":"example.md"})}}]}
                if revoke_before_publication:
                    self.policy.revoked_tool = True
            if body.get("stream"):
                delta = dict(message)
                if "tool_calls" in delta:
                    delta["tool_calls"] = [dict(delta["tool_calls"][0], index=0)]
                chunk = {"id":"answer","model":"fixture","created":1,"object":"chat.completion.chunk",
                         "choices":[{"index":0,"delta":delta,"finish_reason":None}]}
                return httpx.Response(200, headers={"Content-Type":"text/event-stream"},
                    content=("data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n").encode())
            return httpx.Response(200, json={"id":"answer","model":"fixture","created":1,"object":"chat.completion",
                "choices":[{"index":0,"message":message,"finish_reason":"tool_calls" if "tool_calls" in message else "stop"}]})

        client = AsyncOpenAI(api_key="synthetic",http_client=httpx.AsyncClient(transport=httpx.MockTransport(fixture)))
        self.addAsyncCleanup(client.close)
        self.model = (SchemaToolChat if schema else OpenAILike)(id="fixture",api_key="synthetic",async_client=client)
        rid = uuid4().hex
        await self.runtime.submit(RunRequest(rid,self.ctx.session_id,rid,"question"),self.ctx)
        result = await self.runtime.wait(rid,self.ctx)
        if violate_final:
            self.assertEqual(result.status, "failed")
            self.assertEqual(self.reads, 3)
            self.assertEqual((await self.vault.read(self.ctx, "example.md"))["revision"], 1)
            return
        if revoke_before_publication:
            self.assertEqual(result.status, "failed")
            self.assertNotIn(expected, str(result.output))
            return
        self.assertEqual((result.status,result.output), ("success",expected))
        if alternate_write:
            self.assertEqual((len(self.requests),self.reads), (4,2))
            saved = await self.vault.read(self.ctx, "example.md")
            self.assertEqual((saved["content"], saved["revision"]), ("changed-value", 2))
            self.assertNotIn("three times",self.requests[-1]["messages"][0]["content"])
            return
        if change_revision:
            self.assertEqual((len(self.requests),self.reads), (5,4))
            self.assertNotIn("three times", self.requests[3]["messages"][0]["content"])
            return
        self.assertEqual((len(self.requests),self.reads), (4,3))
        self.assertIn("last two reads",self.requests[2]["messages"][0]["content"])
        self.assertIn("three times",self.requests[3]["messages"][0]["content"])

    async def test_native_model_finishes_after_three_published_reads(self):
        await self.exercise(False)

    async def test_structured_model_finishes_after_three_published_reads(self):
        await self.exercise(True)


    async def test_changed_head_before_synthesis_reopens_the_normal_tool_decision(self):
        await self.exercise(True, change_revision=True)

    async def test_revocation_during_synthesis_prevents_publication(self):
        await self.exercise(False, revoke_before_publication=True)


    async def test_native_stream_finishes_from_the_published_document(self):
        await self.exercise(False, streaming=True)

    async def test_structured_stream_finishes_from_the_published_document(self):
        await self.exercise(True, streaming=True)


    async def test_unsolicited_effect_on_answer_only_pass_is_never_executed(self):
        await self.exercise(False, violate_final=True)

    async def test_requested_write_after_two_reads_executes_once(self):
        await self.exercise(False, alternate_write=True)


class SeparatePhases(unittest.IsolatedAsyncioTestCase):
    async def test_intervening_work_errors_and_other_turns_end_the_chain(self):
        from openagent_core.read_convergence import VerifiedToolRead, decision_after_reads, published_read
        from openagent_core.runtime import execution_scope
        async def current():
            return True
        async def authorize():
            pass
        actor = object()
        evidence = VerifiedToolRead(("example.md", None, 1), current)
        tools = [{"type":"function","function":{"name":"read"}}]
        def pair(call_id, name="read", error=False):
            return [Message(role="assistant",tool_calls=[{"id":call_id,"function":{"name":name}}]),
                    Message(role="tool",tool_call_id=call_id,tool_call_error=error,content="receipt")]
        trail = [*pair("a"),*pair("b"),*pair("c")]
        async def decision(messages):
            return await decision_after_reads({"messages":messages,"tools":tools})
        with execution_scope(self, actor, "one"):
            for call_id in ["a","b","c"]:
                published_read(call_id, "ref", "read", evidence, authorize)
            self.assertTrue((await decision(trail))[1])
            for middle in [pair("other","write"),pair("b",error=True),[Message(role="user",content="New phase")]]:
                options, final = await decision([*pair("a"),*pair("b"),*middle,*pair("c")])
                self.assertFalse(final)
                self.assertIs(options["tools"],tools)
            options, final = await decision([*pair("a"),*pair("b"),*pair("c",name="custom_batch")])
            self.assertFalse(final)
            with execution_scope(self, object(), "two"):
                self.assertFalse((await decision(trail))[1])
            self.assertTrue((await decision(trail))[1])
