"""A device its owner lent to their own automations (DelegatedDeviceSource).

Deferred work never carries temporary leases, so an automation never reaches a
device through the interactive path. The host can register a device for one
owner's deferred, authorized runs after that owner's explicit consent; only
such runs see and call it, and revocation ends it.
"""
import unittest
from dataclasses import replace

from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.core.execution_origin import TurnExecutionOrigin
from openagent_core.mcp.catalog import (
    register_delegated_device_capabilities,
    register_interactive_capabilities,
    revoke_delegated_device_capabilities,
)
from test_mcp_catalog import Policy, Registry


class DelegatedDeviceSources(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.catalog = CapabilityCatalog(Policy())
        self.owner = PrincipalRef('test', 'tenant', 'alice')
        agent = PrincipalRef('test', 'tenant', 'agent', 'agent')
        self.registry = Registry()
        self.origin = TurnExecutionOrigin('device-0', 'client', 1, 'Studio Mac', self.registry)
        self.source_ids = register_delegated_device_capabilities(
            self.catalog, self.origin, source_namespace='automation/task/loop-1/device-0',
            owner=self.owner, agent_id='agent')
        self.automation = ExecutionContext(agent, self.owner, self.owner, 'loop-session', 'agent',
                                           (self.owner,), delegation_id='grant-1', deferred=True)

    async def test_the_owner_s_authorized_automation_sees_and_calls_the_device(self):
        tools = await self.catalog.discover(self.automation)
        self.assertEqual({'Studio Mac'}, {tool.target_label for tool in tools})
        result = await self.catalog.call_tool(tools[0].tool_ref, {}, self.automation)
        self.assertEqual('device-0', result['structuredContent']['device'])
        self.assertEqual(1, len(self.registry.calls))

    async def test_no_other_run_can_borrow_the_lent_device(self):
        tools = await self.catalog.discover(self.automation)
        other = PrincipalRef('test', 'tenant', 'bob')
        interactive = ExecutionContext(self.owner, self.owner, self.owner, 'chat', 'agent',
                                       (self.owner,), ingress_id='verified-interaction')
        for context in (
            interactive,
            replace(self.automation, initiator=other, authority=other, audience=(other,)),
            replace(self.automation, authority=other),
            replace(self.automation, agent_id='another-agent'),
        ):
            self.assertFalse(await self.catalog.discover(context))
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools[0].tool_ref, {}, context)
        self.assertEqual(0, len(self.registry.calls))

    async def test_revocation_and_disconnect_end_the_loan(self):
        tools = await self.catalog.discover(self.automation)
        self.registry.revoked = True
        with self.assertRaises(CapabilityUnavailable):
            await self.catalog.call_tool(tools[0].tool_ref, {}, self.automation)
        self.registry.revoked = False
        revoke_delegated_device_capabilities(self.catalog, self.source_ids)
        self.assertFalse(await self.catalog.discover(self.automation))

    async def test_only_a_user_can_lend_a_device_to_one_agent(self):
        agent = PrincipalRef('test', 'tenant', 'agent', 'agent')
        for owner, agent_id, namespace in ((agent, 'agent', 'automation/x'),
                                           (self.owner, '', 'automation/y'),
                                           (self.owner, 'agent', 'client:z')):
            with self.assertRaises(ValueError):
                register_delegated_device_capabilities(self.catalog, self.origin, source_namespace=namespace,
                                                       owner=owner, agent_id=agent_id)

    async def test_the_interactive_path_still_refuses_deferred_work(self):
        with self.assertRaises(ValueError):
            register_interactive_capabilities(self.catalog, self.origin, source_namespace='interactive',
                                              interaction_context=self.automation)


if __name__ == '__main__':
    unittest.main()
