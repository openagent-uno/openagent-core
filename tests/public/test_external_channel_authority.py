"""External authors retain attribution without acquiring channel authority."""
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from openagent_core import (
    CapabilityCatalog, ExecutionContext, FunctionSource, IdempotencyConflict,
    PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings,
    ToolDefinition,
)
from openagent_core.contracts import AuthorizationDenied
from openagent_core.runtime import current_execution_context
from openagent_storage_sqlite import SqliteRuntimeStore


class ChannelGrantPolicy:
    """A host-owned grant binds authority, session, audience and revision."""

    def __init__(self, authority, audience):
        self.authority = authority
        self.audience = audience
        self.scope = "channel-grant:revision-1"
        self.seen = []

    async def authorize(self, context, action, resource, *, audience=()):
        self.seen.append((context, action, resource, audience))
        return (
            context.authority == self.authority
            and context.session_id == "channel-session"
            and context.audience == audience == self.audience
            and context.scopes == (self.scope,)
            and resource.tenant_id == self.authority.tenant_id
            and (resource.kind != "session" or resource.resource_id == context.session_id)
        )


class ToolExecutor:
    def __init__(self):
        self.contexts = []

    async def execute(self, request, context, runtime):
        self.contexts.append((context, current_execution_context()))
        tool, = await runtime.capabilities.discover(context)
        return await runtime.capabilities.call_tool(
            tool.tool_ref, {"input": request.input}, context, call_id="channel-call",
        )


class ExternalChannelAuthority(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = TemporaryDirectory()
        root = Path(self.tmp.name)
        # External identity namespace, but the host tenant remains the boundary.
        self.sender = PrincipalRef("external-provider", "tenant-a", "sender-a")
        self.authority = PrincipalRef("host-a", "tenant-a", "channel-grant-a", "service")
        self.audience = (PrincipalRef("host-a", "tenant-a", "channel-a", "channel"),)
        self.context = ExecutionContext(
            self.sender, self.sender, self.authority, "channel-session", "agent-a",
            self.audience, scopes=("channel-grant:revision-1",), ingress_id="delivery-a",
        )
        self.policy = ChannelGrantPolicy(self.authority, self.audience)
        self.executor = ToolExecutor()
        self.tool_contexts = []

        async def read(arguments, context):
            self.tool_contexts.append((context, current_execution_context()))
            return {"value": arguments["input"]}

        source = FunctionSource((ToolDefinition("read", "Read", {}),), {"read": read})
        catalog = CapabilityCatalog(self.policy)
        catalog.register("data", source, source, target_label="Shared data")
        self.store = SqliteRuntimeStore(root / "state.sqlite3")
        self.runtime = Runtime(
            RuntimeSettings("agent-a", root),
            RuntimeServices(self.store, self.executor, self.policy, catalog),
        )
        self.request = RunRequest("channel-run", "channel-session", "delivery-a", "hello")
        await self.runtime.start()

    async def asyncTearDown(self):
        await self.runtime.close()
        self.tmp.cleanup()

    async def complete(self):
        await self.runtime.submit(self.request, self.context)
        record = await self.runtime.wait(self.request.run_id, self.context)
        self.assertEqual(record.status, "success")
        self.assertEqual(record.output, {"value": "hello"})

    async def test_external_attribution_survives_admission_execution_tool_and_replay(self):
        await self.complete()
        self.assertNotEqual(self.sender, self.authority)
        self.assertEqual(self.executor.contexts, [(self.context, self.context)])
        self.assertEqual(self.tool_contexts, [(self.context, self.context)])
        accepted = await self.runtime.accepted_request(self.request.run_id, self.context)
        self.assertEqual(accepted.author, self.sender)
        self.assertEqual(accepted.initiator, self.sender)
        self.assertEqual(accepted.authority, self.authority)
        self.assertEqual(accepted.audience, self.audience)
        author, = self.store.connection.execute(
            "SELECT author_principal_id FROM session_messages WHERE role='user'",
        ).fetchone()
        self.assertEqual(author, self.sender.key)
        events = await self.runtime.events(self.request.run_id, 0, self.context)
        self.assertTrue(any(event.kind == "tool.completed" for event in events))
        actions = {action for _, action, _, _ in self.policy.seen}
        self.assertTrue({"run.submit", "run.execute", "tool.call", "tool.publish",
                         "run.publish", "run.replay"}.issubset(actions))
        for context, _, _, audience in self.policy.seen:
            self.assertEqual(context, self.context)
            self.assertEqual(audience, self.audience)

    async def test_wrong_audience_or_sender_authority_cannot_admit_or_replay(self):
        wrong_audience = (PrincipalRef("host-a", "tenant-a", "channel-b", "channel"),)
        for context in (
            replace(self.context, audience=wrong_audience),
            replace(self.context, authority=self.sender),
        ):
            with self.assertRaises(AuthorizationDenied):
                await self.runtime.submit(self.request, context)
        self.assertIsNone(await self.store.get(self.request.run_id))
        self.assertEqual(self.executor.contexts, [])
        self.assertEqual(self.tool_contexts, [])
        await self.complete()
        with self.assertRaises(AuthorizationDenied):
            await self.runtime.events(
                self.request.run_id, 0, replace(self.context, audience=wrong_audience),
            )

    async def test_changed_host_grant_rechecks_replay_without_rebinding_admission(self):
        await self.complete()
        self.policy.scope = "channel-grant:revision-2"
        with self.assertRaises(AuthorizationDenied):
            await self.runtime.events(self.request.run_id, 0, self.context)
        current = replace(self.context, scopes=(self.policy.scope,))
        with self.assertRaises(IdempotencyConflict):
            await self.runtime.submit(self.request, current)
        accepted = await self.runtime.accepted_request(self.request.run_id, current)
        self.assertEqual(accepted.scopes, self.context.scopes)
        self.assertEqual(accepted.author, self.sender)
        self.assertEqual(accepted.authority, self.authority)
        self.assertEqual(len(self.tool_contexts), 1)


if __name__ == "__main__":
    unittest.main()
