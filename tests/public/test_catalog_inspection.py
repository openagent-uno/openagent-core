from dataclasses import asdict
import unittest
from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import PrincipalRef, CapabilityLease
from openagent_core.administration import ManagementContext


class Authorizer:
    def __init__(self): self.revoked = False
    async def authorize(self, context, action, resource, *, audience=()):
        return action == "catalog.inspect" and not self.revoked and not audience


class Source:
    def __init__(self, action=None): self.action = action; self.inspections = 0
    async def inspect(self, context):
        self.inspections += 1
        if self.action: self.action()
        return (ToolDefinition("read", "Read schema", {"type":"object"}),)
    async def discover(self, context): raise AssertionError("Inventory cannot create an execution context")
    async def call_tool(self, name, arguments, context): raise AssertionError("Inventory cannot execute tools")


class CatalogInspectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.policy = Authorizer()
        self.catalog = CapabilityCatalog(self.policy)
        self.context = ManagementContext(PrincipalRef("external", "tenant", "alice", "user"), "agent")

    async def test_no_executable_refs_device_leases_or_implicit_source_fallback(self):
        source = Source()
        temporary = Source()
        self.catalog.register("durable", source, source, target_label="Sandbox")
        self.catalog.register("device", temporary, temporary, target_label="Device", lease=CapabilityLease("device", "instance", "generation"))
        self.catalog.register("no-inspector", object(), object(), target_label="Other")
        rows = await self.catalog.inspect(self.context, authorizer=self.policy)
        self.assertEqual(["durable"], [row.source_id for row in rows])
        self.assertNotIn("tool_ref", asdict(rows[0]))
        self.assertEqual(0, temporary.inspections)
        with self.assertRaises(CapabilityUnavailable):
            await self.catalog.call_tool("durable/read", {}, self.context)

    async def test_current_authority_and_registration_checked_after_source_io(self):
        source = Source(lambda: setattr(self.policy, "revoked", True))
        self.catalog.register("durable", source, source, target_label="Sandbox")
        self.assertEqual((), await self.catalog.inspect(self.context, authorizer=self.policy))
        self.assertEqual(1, source.inspections)
        self.assertEqual((), await self.catalog.inspect(self.context, authorizer=self.policy))
        self.assertEqual(1, source.inspections)
        self.policy.revoked = False
        source.action = lambda: self.catalog.revoke("durable")
        self.assertEqual((), await self.catalog.inspect(self.context, authorizer=self.policy))

    async def test_host_can_check_current_trusted_source_without_granting_access(self):
        source = Source()
        self.catalog.register("native-module", source, source, target_label="Module")
        self.assertTrue(self.catalog.has_source("native-module", self.context))
        self.assertFalse(self.catalog.has_source("unregistered", self.context))
        self.catalog.revoke("native-module")
        self.assertFalse(self.catalog.has_source("native-module", self.context))
