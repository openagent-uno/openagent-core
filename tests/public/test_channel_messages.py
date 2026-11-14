from pathlib import Path
import unittest

from openagent_core import (
    CapabilityCatalog, ChannelDestination, ChannelMessageCapability, ExecutionContext, PrincipalRef,
    Runtime, RuntimeServices, RuntimeSettings,
)
from openagent_core.runtime import execution_scope


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return True


class InvocationStore:
    async def begin_tool(self, run_id, call_id, binding, arguments):
        pass

    async def finish_tool(self, run_id, call_id, *, result=None, error=None):
        pass


class Sink:
    def __init__(self):
        self.available = [ChannelDestination("slack:CTEAM", "#team", "slack")]
        self.sent, self.records = [], {}

    async def destinations(self, context):
        return self.available

    async def send(self, request, context):
        self.sent.append(request)
        return self.records.setdefault(request.idempotency_key, f"message-{len(self.records)}")


class ChannelMessageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        principal = PrincipalRef("host-a", "tenant-a", "user-a")
        audience = PrincipalRef("host-a", "tenant-a", "session-a", "session")
        self.context = ExecutionContext(principal, principal, principal, "session-a", "agent-a", (audience,))
        self.catalog = CapabilityCatalog(Policy())
        self.sink = Sink()
        capability = ChannelMessageCapability(self.sink)
        self.catalog.register("channel_messages", capability, capability, target_label="Channels")
        self.runtime = Runtime(RuntimeSettings("agent-a", Path.cwd()), RuntimeServices(InvocationStore(), None, Policy(), self.catalog))

    async def refs(self):
        return {tool.name: tool.tool_ref for tool in await self.catalog.discover(self.context)}

    async def test_lists_destinations_and_sends_text_with_files_once(self):
        refs = await self.refs()
        with execution_scope(self.runtime, self.context, "run-a"):
            listed = await self.catalog.call_tool(refs["list_channel_destinations"], {}, self.context)
            arguments = {"destination": "slack:CTEAM", "text": " Report ", "files": ["cas/report.pdf"]}
            first = await self.catalog.call_tool(refs["send_channel_message"], arguments, self.context)
            second = await self.catalog.call_tool(refs["send_channel_message"], arguments, self.context)
        self.assertEqual(listed, {"destinations": [{"id": "slack:CTEAM", "label": "#team", "provider": "slack"}]})
        self.assertEqual(first, second)
        self.assertEqual(self.sink.sent[0].text, "Report")
        self.assertEqual(self.sink.sent[0].files, ("cas/report.pdf",))
        self.assertEqual(self.sink.sent[0].idempotency_key, self.sink.sent[1].idempotency_key)

    async def test_rejects_unlisted_destinations_empty_messages_and_missing_runs(self):
        refs = await self.refs()
        send = refs["send_channel_message"]
        with execution_scope(self.runtime, self.context, "run-a"):
            for arguments in ({"destination": "slack:COTHER", "text": "x"}, {"destination": "slack:CTEAM"},
                              {"destination": "slack:CTEAM", "files": ["a", "a"]},
                              {"destination": "slack:CTEAM", "text": "x", "channel": "C1"}):
                with self.assertRaises((PermissionError, ValueError)):
                    await self.catalog.call_tool(send, arguments, self.context)
        with self.assertRaises(PermissionError):
            await self.catalog.call_tool(send, {"destination": "slack:CTEAM", "text": "x"}, self.context)
        self.assertEqual(self.sink.sent, [])
