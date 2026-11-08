from dataclasses import replace
from pathlib import Path
import unittest

from openagent_core import (
    CapabilityCatalog,
    ExecutionContext,
    NotificationCapability,
    PrincipalRef,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_core.runtime import execution_scope


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied


class InvocationStore:
    async def begin_tool(self, run_id, call_id, binding, arguments):
        pass

    async def finish_tool(self, run_id, call_id, *, result=None, error=None):
        pass


class NotificationSink:
    def __init__(self):
        self.calls = []
        self.records = {}

    async def send(self, request, context):
        self.calls.append((request, context))
        return self.records.setdefault(request.idempotency_key, f"notice-{len(self.records)}")


class NotificationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        principal = PrincipalRef("host-a", "tenant-a", "user-a")
        audience = PrincipalRef("host-a", "tenant-a", "session-a", "session")
        self.context = ExecutionContext(principal, principal, principal, "session-a", "agent-a", (audience,))
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        self.sink = NotificationSink()
        self.capability = NotificationCapability(self.sink)
        self.catalog.register("notifications", self.capability, self.capability, target_label="Notifications")
        services = RuntimeServices(InvocationStore(), None, self.policy, self.catalog)
        self.runtime = Runtime(RuntimeSettings("agent-a", Path.cwd()), services)

    async def tool_ref(self):
        (tool,) = await self.catalog.discover(self.context)
        self.assertEqual(tool.name, "send_notification")
        self.assertEqual(set(tool.input_schema["properties"]), {"title", "body"})
        self.assertFalse(tool.input_schema["additionalProperties"])
        return tool.tool_ref

    async def test_opt_in_discovery_and_revoked_call_authority(self):
        self.assertEqual(await CapabilityCatalog(self.policy).discover(self.context), ())
        ref = await self.tool_ref()
        self.policy.denied.update({"tool.discover", "tool.call"})
        self.assertEqual(await self.catalog.discover(self.context), ())
        with execution_scope(self.runtime, self.context, "run-a"):
            with self.assertRaises(PermissionError):
                await self.catalog.call_tool(ref, {"title": "Done", "body": "Finished"}, self.context)
        self.assertEqual(self.sink.calls, [])

    async def test_userless_and_mismatched_authorities_have_no_recipient(self):
        ref = await self.tool_ref()
        service = PrincipalRef("host-a", "tenant-a", "automation-a", "service")
        other_user = PrincipalRef("host-a", "tenant-a", "user-b")
        for context in (
            replace(self.context, initiator=service, authority=service),
            replace(self.context, initiator=other_user),
            replace(self.context, authority=service),
        ):
            with self.subTest(context=context):
                self.assertEqual(await self.catalog.discover(context), ())
                with execution_scope(self.runtime, context, "run-a"):
                    with self.assertRaises(LookupError):
                        await self.catalog.call_tool(ref, {"title": "Done", "body": "Finished"}, context)
        self.assertEqual(self.sink.calls, [])

    async def test_sink_gets_bound_user_text_and_stable_idempotency(self):
        ref = await self.tool_ref()
        args = {"title": " Ready ", "body": " See the results.\nGrazie! "}
        with execution_scope(self.runtime, self.context, "run-a"):
            first = await self.catalog.call_tool(ref, args, self.context, call_id="call-a")
            repeat = await self.catalog.call_tool(ref, args, self.context, call_id="call-b")
        with execution_scope(self.runtime, self.context, "run-b"):
            second = await self.catalog.call_tool(ref, args, self.context, call_id="call-a")

        self.assertEqual(first, repeat)
        self.assertNotEqual(first, second)
        self.assertEqual(set(first), {"id"})
        (notice, context), (retried, _), (another_run, _) = self.sink.calls
        self.assertIs(context, self.context)
        self.assertEqual((notice.title, notice.body), ("Ready", "See the results.\nGrazie!"))
        self.assertRegex(notice.idempotency_key, r"^[0-9a-f]{64}$")
        self.assertEqual(notice.idempotency_key, retried.idempotency_key)
        self.assertNotEqual(notice.idempotency_key, another_run.idempotency_key)

    async def test_rejects_calls_without_exact_run_and_valid_text(self):
        ref = await self.tool_ref()
        valid = {"title": "Done", "body": "Finished"}
        with self.assertRaises(PermissionError):
            await self.catalog.call_tool(ref, valid, self.context)
        with execution_scope(self.runtime, replace(self.context, session_id="session-b"), "run-a"):
            with self.assertRaises(PermissionError):
                await self.catalog.call_tool(ref, valid, self.context)
        with execution_scope(self.runtime, self.context, "run-a"):
            for args in (
                {**valid, "recipient": "other-user"},
                {**valid, "idempotencyKey": "forged"},
                {**valid, "body": ""},
                {**valid, "body": "a" * 2001},
                {**valid, "body": "Embedded\x00NUL"},
                {**valid, "title": "a" * 121},
                {**valid, "title": "First\nSecond"},
            ):
                with self.subTest(args=args), self.assertRaises(ValueError):
                    await self.catalog.call_tool(ref, args, self.context)
        self.assertEqual(self.sink.calls, [])
