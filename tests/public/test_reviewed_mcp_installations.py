from dataclasses import replace
from pathlib import Path
import unittest

from openagent_core import CapabilityCatalog, ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.mcp.reviewed_installations import ReviewedMCP, ReviewedMCPInstallationCapability
from openagent_core.runtime import execution_scope


class ReviewedMCPTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        owner = PrincipalRef("host-a", "tenant-a", "owner")
        self.context = ExecutionContext(owner, owner, owner, "session-a", "agent-a", (owner,))
        self.offer = ReviewedMCP("status", "a" * 64, "Service status", "Read approved service health.")
        self.installed = set()
        self.calls = []
        self.deny = set()
        self.catalog = CapabilityCatalog(self)
        source = ReviewedMCPInstallationCapability(self)
        self.catalog.register("reviewed-mcps", source, source, target_label="Approved integrations")
        self.runtime = Runtime(RuntimeSettings("agent-a", Path.cwd()), RuntimeServices(self, None, self, self.catalog))

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.deny and context == self.context and audience == self.context.audience

    async def begin_tool(self, run_id, call_id, binding, arguments):
        pass

    async def finish_tool(self, run_id, call_id, *, result=None, error=None):
        pass

    async def offers(self, context):
        return (self.offer,)

    async def install(self, context, offer):
        self.assertIs(offer, self.offer)
        self.calls.append(offer)
        self.installed.add((context.tenant_id, context.agent_id, offer.id, offer.revision))

    async def test_scoped_offer_is_discovered_and_retry_has_one_installation(self):
        self.assertEqual((), await self.catalog.discover(self.context))
        with execution_scope(self.runtime, self.context, "run-a"):
            refs = {tool.name: tool.tool_ref for tool in await self.catalog.discover(self.context)}
            result = await self.catalog.call_tool(refs["list_reviewed_mcps"], {}, self.context)
            self.assertEqual({"id", "revision", "title", "description"}, result["offers"][0].keys())
            selection = {"id": self.offer.id, "revision": self.offer.revision}
            for _ in range(2):
                self.assertEqual({"installed": True, **selection}, await self.catalog.call_tool(
                    refs["install_reviewed_mcp"], selection, self.context))
            self.assertEqual(1, len(self.installed))

    async def test_rejects_unreviewed_input_stale_offers_and_revoked_authority(self):
        with execution_scope(self.runtime, self.context, "run-a"):
            ref = next(tool.tool_ref for tool in await self.catalog.discover(self.context)
                       if tool.name == "install_reviewed_mcp")
            selection = {"id": self.offer.id, "revision": self.offer.revision}
            with self.assertRaises(ValueError):
                await self.catalog.call_tool(ref, {**selection, "url": "https://example.invalid"}, self.context)
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(ref, {**selection, "id": "unreviewed"}, self.context)
            self.offer = replace(self.offer, revision="b" * 64)
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(ref, selection, self.context)
            self.deny.add("catalog.install")
            with self.assertRaises(PermissionError):
                await self.catalog.call_tool(ref, {**selection, "revision": self.offer.revision}, self.context)
            self.deny.add("catalog.list")
            self.assertEqual((), await self.catalog.discover(self.context))
        self.assertEqual([], self.calls)
