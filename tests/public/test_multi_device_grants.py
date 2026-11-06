"""Host-authorized multiple devices retain exact routing and ingress isolation."""
import unittest
from dataclasses import replace
from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.core.execution_origin import TurnExecutionOrigin, execution_origin_scope
from openagent_core.mcp.catalog import register_interactive_capabilities, revoke_interactive_capabilities
from test_mcp_catalog import Policy, Registry


class MultiDeviceGrants(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.catalog = CapabilityCatalog(Policy())
        p = PrincipalRef('test', 'tenant', 'alice')
        self.context = ExecutionContext(p, p, p, 'session', 'agent', (p,), ingress_id='verified-interaction')
        self.registries = [Registry(), Registry()]
        self.origins = [TurnExecutionOrigin(f'device-{i}', 'client', 1, f'Computer {i}', registry) for i, registry in enumerate(self.registries)]
        self.leases = tuple(lease for i, origin in enumerate(self.origins) for lease in register_interactive_capabilities(
            self.catalog, origin, source_namespace=f'interaction/target-{i}', interaction_context=self.context))
        self.context = replace(self.context, capabilities=self.leases)

    async def test_multiple_exact_destinations_without_an_originating_tool_host(self):
        tools = await self.catalog.discover(self.context)
        self.assertEqual({'Computer 0', 'Computer 1'}, {tool.target_label for tool in tools})
        for tool in tools:
            result = await self.catalog.call_tool(tool.tool_ref, {}, self.context)
            self.assertEqual('device-' + tool.source_id.split('/')[1].removeprefix('target-'), result['structuredContent']['device'])
        self.assertEqual([1, 1], [len(r.calls) for r in self.registries])

    async def test_context_cannot_be_borrowed_by_another_ingress_or_authority(self):
        tools = await self.catalog.discover(self.context)
        other = PrincipalRef('test', 'tenant', 'bob')
        for context in (replace(self.context, ingress_id='other'), replace(self.context, ingress_id=None),
                        replace(self.context, initiator=other), replace(self.context, authority=other),
                        replace(self.context, audience=(self.context.initiator, other)), replace(self.context, agent_id='another-agent')):
            self.assertFalse(await self.catalog.discover(context))
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools[0].tool_ref, {}, context)
        self.assertEqual([0, 0], [len(r.calls) for r in self.registries])

    async def test_children_inherit_only_inline_and_revocation_never_falls_back(self):
        tools = await self.catalog.discover(self.context)
        agent = PrincipalRef('test', 'tenant', 'worker', 'agent')
        inline = self.context.child(session_id='child', run_id='parent', agent=agent)
        self.assertEqual(2, len(await self.catalog.discover(inline)))
        deferred = replace(self.context, session_id='deferred', capabilities=(), ingress_id=None, deferred=True, delegation_id='verified-task')
        self.assertFalse(await self.catalog.discover(deferred))
        self.registries[0].revoked = True
        with self.assertRaises(CapabilityUnavailable):
            await self.catalog.call_tool(tools[0].tool_ref, {}, self.context)
        self.assertEqual(0, len(self.registries[1].calls))
        revoke_interactive_capabilities(self.catalog, self.leases[:1])
        self.assertEqual(1, len(await self.catalog.discover(self.context)))

    async def test_explicit_binding_is_required_and_legacy_single_origin_stays_scoped(self):
        for context in (replace(self.context, ingress_id=None), replace(self.context, session_id='deferred', capabilities=(), ingress_id=None, deferred=True, delegation_id='verified-task')):
            with self.assertRaises(ValueError):
                register_interactive_capabilities(self.catalog, self.origins[0], source_namespace='bad', interaction_context=context)
        catalog = CapabilityCatalog(Policy())
        leases = register_interactive_capabilities(catalog, self.origins[0], source_namespace='legacy')
        context = replace(self.context, capabilities=leases)
        self.assertFalse(await catalog.discover(context))
        with execution_origin_scope(self.origins[0]):
            self.assertEqual(1, len(await catalog.discover(context)))

    async def test_host_can_explicitly_authorize_a_shared_multi_user_audience(self):
        other = PrincipalRef('test', 'tenant', 'bob')
        context = replace(self.context, audience=(self.context.initiator, other), capabilities=())
        catalog = CapabilityCatalog(Policy())
        leases = register_interactive_capabilities(catalog, self.origins[0],
            source_namespace='shared-explicit-grant', interaction_context=context)
        context = replace(context, capabilities=leases)
        tools = await catalog.discover(context)
        self.assertEqual(1, len(tools))
        await catalog.call_tool(tools[0].tool_ref, {}, context)
        self.assertEqual(1, len(self.registries[0].calls))
