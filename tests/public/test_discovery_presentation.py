from __future__ import annotations

import unittest
from types import SimpleNamespace

from openagent_core import (
    CapabilityCatalog, CapabilityUnavailable, DiscoveryPresentation,
    DISCOVERY_PRESENTATION_API_VERSION, ExecutionContext, FunctionSource,
    PrincipalRef, ToolDefinition,
)
from openagent_core.engine import MCPPool
from openagent_core.contracts import require_authorized
from openagent_core.core.tool_scope import reset_tool_allowlist, set_tool_allowlist
from openagent_core.runtime import execution_scope


class Policy:
    def __init__(self):
        self.denied = {"hidden-source"}

    async def authorize(self, context, action, resource, *, audience=()):
        return resource.resource_id not in self.denied


class RecordingRuntime(SimpleNamespace):
    def __init__(self, catalog):
        super().__init__(capabilities=catalog, services=SimpleNamespace(store=self, executor=None),
                         settings=SimpleNamespace(environment=()))
        self.events = []

    async def authorize(self, context, action, resource, *, audience=()):
        await require_authorized(self.capabilities.authorizer, context, action, resource, audience=audience)

    async def begin_tool(self, *args):
        self.events.append("begin")

    async def finish_tool(self, *args, **kwargs):
        self.events.append("finish")


class DiscoveryPresentationContract(unittest.IsolatedAsyncioTestCase):
    async def make_pool(self, presentation=None):
        pool = MCPPool.from_config([{"builtin": "tool-search"}], include_defaults=False)
        if presentation is not None:
            pool.configure_discovery(presentation)
        await pool.connect_all()
        self.addAsyncCleanup(pool.close_all)
        pool.bind_capability_catalog(self.catalog)
        toolkits = pool.runtime_toolkits_tool_search_only()
        self.assertEqual(len(toolkits), 1)
        toolkit = toolkits[0]
        return pool, {**toolkit.functions, **toolkit.async_functions}

    async def asyncSetUp(self):
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        principal = PrincipalRef("host-a", "tenant-a", "person-a")
        self.context = ExecutionContext(principal, principal, principal,
            "session-a", "agent-a", (principal,))
        self.runtime = RecordingRuntime(self.catalog)
        self.calls = []

        def implementation(name):
            async def execute(arguments, context):
                self.calls.append((name, arguments))
                return {"selected": name}
            return execute

        definitions = tuple(ToolDefinition(f"operation_{i:02d}",
            "Inspect records and panels. " * 20,
            {"type": "object", "properties": {"value": {"type": "string"}}})
            for i in range(54))
        source = FunctionSource(definitions, {d.name: implementation(d.name) for d in definitions})
        self.catalog.register("records", source, source, target_label="service-a")
        hidden = FunctionSource((ToolDefinition("private_operation", "Not visible", {}),),
            {"private_operation": implementation("private_operation")})
        self.catalog.register("hidden-source", hidden, hidden, target_label="service-hidden")

    async def test_source_query_finds_later_devices_without_disclosing_hidden_sources(self):
        for i in range(25):
            source = FunctionSource((ToolDefinition("capture", "Capture screen", {}),),
                {"capture": lambda args, context: None})
            self.catalog.register(f"desktop-{i}", source, source,
                target_label=f"User laptop {i:02d}")
        _pool, tools = await self.make_pool()
        list_sources = tools["tool_search_list_servers"].entrypoint
        with execution_scope(self.runtime, self.context, "source-query"):
            first = await list_sources(limit=20)
            self.assertIsNotNone(first["next_offset"])
            last = await list_sources(query="LAPTOP 24")
            self.assertEqual(["desktop-24"], [row["source_ref"] for row in last])
            self.assertEqual([], await list_sources(query="hidden"))
            self.policy.denied.add("desktop-24")
            self.assertEqual([], await list_sources(query="laptop 24"))
            with self.assertRaises(ValueError):
                await list_sources(query="x" * 513)
        self.assertEqual([], self.calls)

    async def test_public_pool_hints_preserve_authority_references_and_paging(self):
        default_pool, default = await self.make_pool()
        configured_pool, configured = await self.make_pool(DiscoveryPresentation(
            capability_sources={"records", "hidden-source", "missing-source"},
            capability_limit=2,
            source_summaries={"records": "Records and panels", "hidden-source": "Private hint"},
            tool_descriptions={"tool_search_list_servers": "List authorized sources and their hints."},
        ))
        self.assertEqual(DISCOVERY_PRESENTATION_API_VERSION, 1)
        self.assertEqual(configured["tool_search_list_servers"].description,
            "List authorized sources and their hints.")
        for name in default:
            self.assertEqual(default[name].parameters, configured[name].parameters)
        with execution_scope(self.runtime, self.context, "run-a"):
            original = await default["tool_search_list_servers"].entrypoint()
            hints = await configured["tool_search_list_servers"].entrypoint()
            self.assertEqual(original, [{"name": "records", "source_ref": "records",
                "target_label": "service-a", "tool_count": 54}])
            self.assertEqual(len(hints), 1)
            row = hints[0]
            self.assertEqual(row["description"], "Records and panels")
            self.assertEqual([x["name"] for x in row["capabilities"]], ["operation_00", "operation_01"])
            self.assertTrue(all(len(x["description"]) <= 160 for x in row["capabilities"]))
            self.assertEqual(row["capabilities_total"], 54)
            self.assertTrue(row["capabilities_has_more"])
            first = await configured["tool_search_list_tools"].entrypoint(source_ref="records", limit=50)
            rest = await configured["tool_search_list_tools"].entrypoint(source_ref="records", limit=50, offset=50)
            self.assertEqual((first["total"], len(first["tools"]), len(rest["tools"])), (54, 50, 4))
            self.assertTrue(first["has_more"])
            self.assertFalse(rest["has_more"])
            old_first = await default["tool_search_list_tools"].entrypoint(source_ref="records", limit=50)
            self.assertEqual(first, old_first)
            ref = first["tools"][0]["tool_ref"]
            described = await configured["tool_search_describe_tool"].entrypoint(tool_ref=ref)
            self.assertEqual(described["name"], "operation_00")
            self.assertIn("value", described["input_schema"]["properties"])
            result = await configured["tool_search_call_tool"].entrypoint(tool_ref=ref, args={"value": "exact"})
            self.assertEqual(result, {"selected": "operation_00"})
            self.assertEqual(self.runtime.events, ["begin", "finish"])
            # Hints do not grant a registration, and revocation removes hints and refs.
            self.catalog.revoke("records")
            self.assertEqual(await configured["tool_search_list_servers"].entrypoint(), [])
            with self.assertRaises((CapabilityUnavailable, LookupError)):
                await configured["tool_search_describe_tool"].entrypoint(tool_ref=ref)
            with self.assertRaises(CapabilityUnavailable):
                await configured["tool_search_call_tool"].entrypoint(tool_ref=ref, args={})
        self.assertEqual(self.calls, [("operation_00", {"value": "exact"})])
        with self.assertRaises(PermissionError):
            await configured["tool_search_list_servers"].entrypoint()
        other = SimpleNamespace(capabilities=CapabilityCatalog(self.policy))
        with execution_scope(other, self.context, "run-other"):
            with self.assertRaises(PermissionError):
                await configured["tool_search_list_servers"].entrypoint()
        with self.assertRaises(RuntimeError):
            configured_pool.configure_discovery(DiscoveryPresentation())

    async def test_named_calls_require_an_exact_grant_and_current_catalog_authority(self):
        _, default = await self.make_pool()
        self.assertNotIn("tool_search_call_named_tool", default)
        _, configured = await self.make_pool(DiscoveryPresentation(named_tools={
            ("records", "operation_00"), ("hidden-source", "private_operation"),
        }))
        call = configured["tool_search_call_named_tool"].entrypoint
        with self.assertRaises(PermissionError):
            await call(source_ref="records", tool_name="operation_00")
        with execution_scope(self.runtime, self.context, "run-a"):
            result = await call(source_ref="records", tool_name="operation_00", args='{"value":"exact"}')
            self.assertEqual(result, {"selected": "operation_00"})
            for source, name in (("records", "operation_01"), ("records", "operation-00"),
                                 ("other-records", "operation_00")):
                with self.assertRaises(PermissionError):
                    await call(source_ref=source, tool_name=name)
            with self.assertRaises(LookupError):
                await call(source_ref="hidden-source", tool_name="private_operation")
            token = set_tool_allowlist({"other-records"})
            try:
                with self.assertRaises(PermissionError):
                    await call(source_ref="records", tool_name="operation_00")
            finally:
                reset_tool_allowlist(token)
            self.policy.denied.add("records/operation_00")
            with self.assertRaises(PermissionError):
                await call(source_ref="records", tool_name="operation_00")
            self.catalog.revoke("records")
            with self.assertRaises(LookupError):
                await call(source_ref="records", tool_name="operation_00")
        self.assertEqual(self.calls, [("operation_00", {"value": "exact"})])
        self.assertEqual(self.runtime.events, ["begin", "finish"])
        other = SimpleNamespace(capabilities=CapabilityCatalog(self.policy))
        with execution_scope(other, self.context, "run-other"):
            with self.assertRaises(PermissionError):
                await call(source_ref="records", tool_name="operation_00")

    async def test_authorized_named_calls_reach_any_discoverable_tool_and_nothing_more(self):
        _, tools = await self.make_pool(DiscoveryPresentation(named_calls="authorized"))
        call = tools["tool_search_call_named_tool"].entrypoint
        with self.assertRaises(PermissionError):
            await call(source_ref="records", tool_name="operation_07")  # no execution
        with execution_scope(self.runtime, self.context, "run-a"):
            self.assertEqual({"selected": "operation_07"},
                             await call(source_ref="records", tool_name="operation_07", args={"value": "x"}))
            for source, name in (("records", "operation-07"), ("records", "missing"),
                                 ("hidden-source", "private_operation")):
                with self.assertRaises(LookupError):
                    await call(source_ref=source, tool_name=name)
            token = set_tool_allowlist({"other-records"})
            try:
                with self.assertRaises(PermissionError):
                    await call(source_ref="records", tool_name="operation_07")
            finally:
                reset_tool_allowlist(token)
            self.policy.denied.add("records/operation_07")
            with self.assertRaises(PermissionError):
                await call(source_ref="records", tool_name="operation_07")
            self.catalog.revoke("records")
            with self.assertRaises(LookupError):
                await call(source_ref="records", tool_name="operation_08")
        self.assertEqual(self.calls, [("operation_07", {"value": "x"})])
        with self.assertRaises(ValueError):
            DiscoveryPresentation(named_calls="anything")

    async def test_a_named_call_on_a_source_with_several_targets_names_the_target(self):
        def capture(label):
            async def execute(arguments, context):
                self.calls.append((label, arguments))
                return {"on": label}
            return execute
        from dataclasses import replace

        class OneSourceOnTwoTargets(CapabilityCatalog):
            # A host may present one logical source on several devices.
            async def discover(self, context):
                return tuple(replace(tool, source_id="screens") if tool.source_id.startswith("screens-")
                             else tool for tool in await super().discover(context))

        self.catalog = OneSourceOnTwoTargets(self.policy)
        self.runtime = RecordingRuntime(self.catalog)
        for label in ("laptop", "desktop"):
            source = FunctionSource((ToolDefinition("capture", "Capture screen", {}),),
                                    {"capture": capture(label)})
            self.catalog.register(f"screens-{label}", source, source, target_label=label)
        _, tools = await self.make_pool(DiscoveryPresentation(named_calls="authorized"))
        call = tools["tool_search_call_named_tool"].entrypoint
        with execution_scope(self.runtime, self.context, "run-a"):
            with self.assertRaises(LookupError) as ambiguous:
                await call(source_ref="screens", tool_name="capture")
            self.assertIn("desktop, laptop", str(ambiguous.exception))
            self.assertEqual({"on": "desktop"},
                             await call(source_ref="screens", tool_name="capture", target_label="desktop"))
            with self.assertRaises(LookupError):
                await call(source_ref="screens", tool_name="capture", target_label="phone")

    async def test_a_short_tool_page_carries_schemas_and_a_long_one_stays_compact(self):
        _, tools = await self.make_pool()
        list_tools = tools["tool_search_list_tools"].entrypoint
        with execution_scope(self.runtime, self.context, "run-a"):
            short = await list_tools(source_ref="records", limit=2)
            long = await list_tools(source_ref="records", limit=10)
        self.assertEqual([{"type": "object", "properties": {"value": {"type": "string"}}}] * 2,
                         [tool["input_schema"] for tool in short["tools"]])
        self.assertTrue(all("input_schema" not in tool for tool in long["tools"]))

    def test_presentation_is_copied_bounded_and_rejects_unknown_tool_overrides(self):
        summaries = {"records": "Original"}
        named_tools = {("records", "operation_00")}
        presentation = DiscoveryPresentation(source_summaries=summaries, named_tools=named_tools)
        summaries["records"] = "Changed"
        named_tools.clear()
        self.assertEqual(presentation.source_summaries["records"], "Original")
        self.assertEqual(presentation.named_tools, frozenset({("records", "operation_00")}))
        with self.assertRaises(TypeError):
            presentation.source_summaries["records"] = "Changed"
        for limit in (0, 51, True):
            with self.assertRaises(ValueError):
                DiscoveryPresentation(capability_limit=limit)
        with self.assertRaises(ValueError):
            DiscoveryPresentation(tool_descriptions={"operation_00": "Cannot replace a leaf"})
        with self.assertRaises(TypeError):
            DiscoveryPresentation(capability_sources="records")
        for invalid in ({("records", "")}, {("records",)},
                        {("records", "x" * 257)}, {("records", f"op_{i}") for i in range(51)}):
            with self.assertRaises(ValueError):
                DiscoveryPresentation(named_tools=invalid)
