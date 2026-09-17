from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
import sqlite3
import json

from openagent_core import (
    CapabilityCatalog,
    CapabilityUnavailable,
    ExecutionContext,
    FunctionSource,
    ModuleCatalog,
    ModuleConfig,
    ModuleReferenceConflict,
    PrincipalRef,
    RunRequest,
    Runtime,
    RuntimeProfile,
    RuntimeServices,
    RuntimeSettings,
    SessionRef,
    ToolDefinition,
)
from openagent_core.runtime import execution_scope
from openagent_module_search import descriptor as search_descriptor
from openagent_module_sessions import descriptor as sessions_descriptor
from openagent_module_events import descriptor as events_descriptor
from openagent_module_mcp import descriptor as mcp_descriptor
from openagent_module_scheduler import descriptor as scheduler_descriptor
from openagent_module_vault import descriptor as vault_descriptor
from openagent_module_workflows import descriptor as workflows_descriptor
from openagent_module_attachments import descriptor as attachments_descriptor
from openagent_module_budget import descriptor as budget_descriptor
from openagent_module_delegation import descriptor as delegation_descriptor
from openagent_module_logs import descriptor as logs_descriptor
from openagent_module_models import descriptor as models_descriptor
from openagent_module_ptc import descriptor as ptc_descriptor
from openagent_module_skills import descriptor as skills_descriptor
from openagent_module_tool_discovery import descriptor as tool_discovery_descriptor
from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.modules import ServiceRegistry
from openagent_core.contracts import DelegationService, ModelCatalog, SessionStore
from openagent_core.code_execution import CodeExecutor
from openagent_core.mcp.pool import MCPPool
from openagent_core.automation_runtime import AutomationRuntime
from openagent_core.automation import AutomationRepository, durable_module_references
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    def __init__(self) -> None:
        self.denied: set[str] = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied and resource.tenant_id == context.tenant_id


class ProfileExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Queue()
        self.holds: dict[str, asyncio.Event] = {}
        self.observed: dict[str, frozenset[str]] = {}

    async def execute(self, request, context, runtime):
        self.observed[request.run_id] = runtime.enabled_module_ids
        self.started.put_nowait(request.run_id)
        hold = self.holds.get(request.run_id)
        if hold is not None:
            await hold.wait()
        return request.input


class ModularRuntime(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.policy = Policy()
        self.executor = ProfileExecutor()
        self.store = SqliteRuntimeStore(self.directory / "state.sqlite3")
        principal = PrincipalRef("fixture", "tenant", "alice")
        self.context = ExecutionContext(
            principal, principal, principal, "session", "agent", (principal,)
        )
        self.catalog = ModuleCatalog((sessions_descriptor, search_descriptor))
        self.runtime = Runtime(
            RuntimeSettings("agent", self.directory),
            RuntimeServices(self.store, self.executor, self.policy),
            profile=RuntimeProfile(1, {
                "sessions": ModuleConfig(frozenset({"service", "agent_tools", "host_api"}))
            }),
            module_catalog=self.catalog,
        )

    async def asyncTearDown(self):
        await self.runtime.close()
        self.temporary.cleanup()

    async def test_sessions_are_native_and_vault_is_absent(self):
        await self.runtime.start()
        with execution_scope(
            SimpleNamespace(capabilities=object()),
            self.context,
            "fixture",
            module_generation=1,
            capability_revision=self.runtime.capabilities.snapshot_revision(),
        ):
            tools = await self.runtime.capabilities.discover(self.context)
            self.assertIn("sessions_create", {tool.name for tool in tools})
            self.assertIn("search_past_conversations", {tool.name for tool in tools})
            self.assertFalse(any(tool.source_id.startswith("vault") for tool in tools))
            create = next(tool for tool in tools if tool.name == "sessions_create")
            created = await self.runtime.capabilities.call_tool(
                create.tool_ref, {"title": "Fixture"}, self.context
            )
            reference = SessionRef.parse(created["session_ref"])
            self.assertEqual(reference.agent_id, "agent")
            renamed = next(tool for tool in tools if tool.name == "sessions_rename")
            await self.runtime.capabilities.call_tool(
                renamed.tool_ref,
                {"session_ref": created["session_ref"], "title": "Renamed"},
                self.context,
            )
            searched = next(tool for tool in tools if tool.name == "sessions_search")
            result = await self.runtime.capabilities.call_tool(
                searched.tool_ref, {"query": "Renamed"}, self.context
            )
            self.assertEqual(result["sessions"][0]["title"], "Renamed")
        self.assertEqual(
            [block.id for block in self.runtime.prompt_blocks],
            ["module.sessions.history"],
        )

    async def test_hot_reconfiguration_swaps_new_runs_and_drains_old_runs(self):
        await self.runtime.start()
        self.executor.holds["old"] = asyncio.Event()
        await self.runtime.submit(
            RunRequest("old", "session", "old", "old"), self.context
        )
        self.assertEqual(await self.executor.started.get(), "old")
        replacement = RuntimeProfile(2, {
            "sessions": ModuleConfig(frozenset({"service", "agent_tools", "host_api"})),
            "search": ModuleConfig(frozenset({"service", "agent_tools"})),
        })
        changing = asyncio.create_task(
            self.runtime.reconfigure(replacement, expected_generation=1, mode="drain")
        )
        for _ in range(100):
            if self.runtime.profile.generation == 2:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.runtime.profile.generation, 2)
        second_context = replace(self.context, session_id="second")
        await self.runtime.submit(
            RunRequest("new", "second", "new", "new"), second_context
        )
        await self.runtime.wait("new", second_context)
        self.assertEqual(self.executor.observed["old"], frozenset({"sessions"}))
        self.assertEqual(self.executor.observed["new"], frozenset({"sessions", "search"}))
        self.assertFalse(changing.done())
        self.executor.holds["old"].set()
        await self.runtime.wait("old", self.context)
        receipt = await changing
        self.assertEqual(receipt.generation, 2)
        self.assertEqual({status.id for status in receipt.activated}, {"sessions", "search"})


class CapabilitySnapshots(unittest.IsolatedAsyncioTestCase):
    async def test_added_source_waits_for_next_run_but_revoke_is_immediate(self):
        policy = Policy()
        catalog = CapabilityCatalog(policy, allow_dynamic=True)
        principal = PrincipalRef("fixture", "tenant", "alice")
        context = ExecutionContext(
            principal, principal, principal, "session", "agent", (principal,)
        )

        async def call(arguments, current_context):
            return {"ok": True}

        source = FunctionSource(
            (ToolDefinition("ping", "Ping", {"type": "object"}),),
            {"ping": call},
        )
        runtime = SimpleNamespace(capabilities=catalog)
        catalog.activate_generation(1)
        old_revision = catalog.snapshot_revision()
        catalog.register_user_source("late", source, source, target_label="Late")
        with execution_scope(
            runtime, context, "old", module_generation=1,
            capability_revision=old_revision,
        ):
            self.assertEqual(await catalog.discover(context), ())
        with execution_scope(
            runtime, context, "new", module_generation=1,
            capability_revision=catalog.snapshot_revision(),
        ):
            descriptor = (await catalog.discover(context))[0]
            catalog.revoke("late")
            with self.assertRaises(CapabilityUnavailable):
                await catalog.call_tool(descriptor.tool_ref, {}, context)


class ModuleCombinations(unittest.IsolatedAsyncioTestCase):
    def test_required_product_combinations_resolve_without_kernel_feature_logic(self):
        descriptors = (
            sessions_descriptor, search_descriptor, vault_descriptor, mcp_descriptor,
            workflows_descriptor, scheduler_descriptor, events_descriptor,
            attachments_descriptor, budget_descriptor, delegation_descriptor,
            logs_descriptor, models_descriptor, ptc_descriptor, skills_descriptor,
            tool_discovery_descriptor,
        )
        catalog = ModuleCatalog(descriptors)
        services = ServiceRegistry({
            SessionStore: object(), "automation_management": object(),
            "catalog_management": object(), "runtime.executor": object(),
            DelegationService: object(), ModelCatalog: object(), CodeExecutor: object(),
        })
        service = lambda **options: ModuleConfig(frozenset({"service"}), options)
        profiles = {
            "kernel-only": {},
            "sessions-only": {"sessions": service()},
            "sessions-vault": {
                "sessions": service(), "vault": service(vault_path="/tmp/vault"),
            },
            "scheduler-without-workflows": {"scheduler": service()},
            "workflows-without-scheduler": {"workflows": service()},
            "events-isolated": {"events": service()},
            "mcp-fixed": {"mcp": service(catalog_mode="fixed")},
            "mcp-managed": {"mcp": service(catalog_mode="managed")},
            "mcp-dynamic": {"mcp": service(catalog_mode="dynamic")},
            "native-without-mcp": {
                "sessions": service(), "skills": service(), "attachments": service(),
            },
        }
        all_except_vault = {descriptor.id: service() for descriptor in descriptors
                            if descriptor.id != "vault"}
        all_except_vault["mcp"] = service(catalog_mode="dynamic")
        all_except_vault["tool-discovery"] = ModuleConfig(
            frozenset({"service", "agent_tools"})
        )
        profiles["all-except-vault"] = all_except_vault
        for generation, (name, modules) in enumerate(profiles.items(), start=1):
            with self.subTest(profile=name):
                resolved = catalog.resolve(RuntimeProfile(generation, modules), services)
                self.assertEqual({item.id for item in resolved.descriptors}, set(modules))

        with self.assertRaisesRegex(Exception, "catalog_management"):
            catalog.resolve(
                RuntimeProfile(99, {"mcp": service(catalog_mode="dynamic")}),
                ServiceRegistry(),
            )

    async def _started(self, descriptors, modules, *, services=None):
        temporary = TemporaryDirectory()
        directory = Path(temporary.name)
        store = SqliteRuntimeStore(directory / "state.sqlite3")
        policy = Policy()
        executor = ProfileExecutor()
        runtime_services = RuntimeServices(
            store, executor, policy,
            registry=services or ServiceRegistry({"automation_management": object()}),
        )
        runtime = Runtime(
            RuntimeSettings("agent", directory), runtime_services,
            profile=RuntimeProfile(1, modules),
            module_catalog=ModuleCatalog(descriptors),
        )
        await runtime.start()
        principal = PrincipalRef("fixture", "tenant", "alice")
        context = ExecutionContext(principal, principal, principal, "session", "agent", (principal,))
        return temporary, runtime, context

    async def test_automation_domains_have_independent_tools_and_prompts(self):
        descriptors = (workflows_descriptor, scheduler_descriptor, events_descriptor)
        expectations = {
            "workflows": ("workflows", "module.workflows"),
            "scheduler": ("schedules", "module.scheduler"),
            "events": ("events", "module.events"),
        }
        for module_id, (source_id, prompt_id) in expectations.items():
            with self.subTest(module=module_id):
                temporary, runtime, context = await self._started(
                    descriptors,
                    {module_id: ModuleConfig(frozenset({"service", "agent_tools", "host_api"}),
                                             {"db_path": str(Path("/tmp") / f"{module_id}.sqlite")})},
                )
                try:
                    with execution_scope(runtime, context, "inspect", module_generation=1,
                                         capability_revision=runtime.capabilities.snapshot_revision()):
                        tools = await runtime.capabilities.discover(context)
                    self.assertEqual({tool.source_id for tool in tools}, {
                        source_id,
                        {"workflows":"workflow-manager", "scheduler":"scheduler", "events":"events-manager"}[module_id],
                    } if source_id != {"workflows":"workflow-manager", "scheduler":"scheduler", "events":"events-manager"}[module_id]
                        else {source_id})
                    self.assertEqual([block.id for block in runtime.prompt_blocks], [prompt_id])
                finally:
                    await runtime.close(); temporary.cleanup()

    async def test_vault_without_sessions_and_sessions_without_vault(self):
        temporary, runtime, context = await self._started(
            (vault_descriptor,),
            {"vault": ModuleConfig(frozenset({"service", "agent_tools", "host_api"}), {
                "db_path": str(Path("/tmp") / "vault-only.sqlite"),
                "vault_path": str(Path("/tmp") / "vault-only"),
            })},
        )
        try:
            self.assertEqual(runtime.enabled_module_ids, frozenset({"vault"}))
            self.assertNotIn("module.sessions.history", {block.id for block in runtime.prompt_blocks})
            self.assertTrue(runtime.services_for("search.providers"))
        finally:
            await runtime.close(); temporary.cleanup()

    async def test_mcp_manager_exists_only_in_dynamic_mode(self):
        class Management:
            user_sources = ()
            def bind_pool(self, pool): self.pool = pool
            def unbind_pool(self, pool): self.pool = None
        for mode, expected in (("fixed", False), ("managed", False), ("dynamic", True)):
            with self.subTest(mode=mode):
                registry = ServiceRegistry({"catalog_management": Management()})
                temporary, runtime, context = await self._started(
                    (mcp_descriptor,),
                    {"mcp": ModuleConfig(frozenset({"service", "agent_tools", "host_api"}), {
                        "catalog_mode": mode,
                        "pool_factory": lambda _context: MCPPool.from_config([], include_defaults=False),
                    })}, services=registry,
                )
                try:
                    with execution_scope(runtime, context, "inspect", module_generation=1,
                                         capability_revision=runtime.capabilities.snapshot_revision()):
                        sources = {tool.source_id for tool in await runtime.capabilities.discover(context)}
                    self.assertEqual("mcp" in sources, expected)
                    self.assertEqual("mcp-manager" in sources, expected)
                finally:
                    await runtime.close(); temporary.cleanup()

    async def test_worker_is_refcounted_across_shadow_swap_and_closed_on_removal(self):
        class Worker:
            starts = closes = drains = 0
            async def start(self): self.starts += 1
            async def drain(self): self.drains += 1
            async def close(self): self.closes += 1
        worker = Worker()
        descriptor = NativeCapabilityDescriptor("worker", "1.1.0b1", (), {})
        temporary, runtime, _context = await self._started(
            (descriptor,),
            {"worker": ModuleConfig(frozenset({"service", "workers"}), {"workers": (worker,)})},
        )
        try:
            self.assertEqual(worker.starts, 1)
            await runtime.reconfigure(RuntimeProfile(2, {
                "worker": ModuleConfig(frozenset({"service", "workers"}), {"workers": (worker,)})
            }), expected_generation=1)
            self.assertEqual((worker.starts, worker.drains, worker.closes), (1, 1, 0))
            await runtime.reconfigure(RuntimeProfile(3, {}), expected_generation=2)
            self.assertEqual(worker.closes, 1)
        finally:
            await runtime.close(); temporary.cleanup()

    async def test_durable_reference_preflight_blocks_module_removal(self):
        class Inspector:
            async def references_for_removed_modules(self, removed_modules, **_profiles):
                return {"workflows": ("event:event-1",)} if "workflows" in removed_modules else {}
        registry = ServiceRegistry({
            "automation_management": object(),
            "module.reference_inspector": Inspector(),
        })
        temporary, runtime, _context = await self._started(
            (workflows_descriptor,),
            {"workflows": ModuleConfig(frozenset({"service"}), {"db_path": "/tmp/preflight.sqlite"})},
            services=registry,
        )
        try:
            with self.assertRaises(ModuleReferenceConflict) as caught:
                await runtime.reconfigure(RuntimeProfile(2, {}), expected_generation=1)
            self.assertEqual(caught.exception.references["workflows"], ("event:event-1",))
            self.assertEqual(runtime.profile.generation, 1)
        finally:
            await runtime.close(); temporary.cleanup()

    async def test_automation_domain_workers_activate_and_stop_independently(self):
        class Scheduler:
            def __init__(self):
                self.domains = set(); self.starts = self.stops = 0
                self._workflow_tasks = set()
            async def set_enabled_domains(self, domains): self.domains = set(domains)
            async def start(self): self.starts += 1
            async def stop(self): self.stops += 1
            async def _check_and_run(self): pass
            async def _drain_task_run_requests(self): pass
            async def _drain_event_deliveries(self): pass
        automation = AutomationRuntime(
            SimpleNamespace(db_path="/tmp/domains.sqlite"),
            SimpleNamespace(_mcp=None), object(),
        )
        automation.scheduler = Scheduler()
        workflows = automation.worker("workflows")
        scheduler = automation.worker("scheduler")
        events = automation.worker("events")
        host = SimpleNamespace()
        await workflows.start(host)
        self.assertEqual(automation.scheduler.domains, {"workflows"})
        await scheduler.start(host); await events.start(host)
        self.assertEqual(automation.scheduler.domains, {"workflows", "scheduler", "events"})
        self.assertEqual(automation.scheduler.starts, 1)
        await scheduler.close()
        self.assertEqual(automation.scheduler.domains, {"workflows", "events"})
        self.assertEqual(automation.scheduler.stops, 0)
        await workflows.close(); await events.close()
        self.assertEqual(automation.scheduler.domains, set())
        self.assertEqual(automation.scheduler.stops, 1)

    async def test_durable_reference_inspector_names_dependent_resources(self):
        temporary = TemporaryDirectory()
        path = Path(temporary.name) / "automation.sqlite"
        connection = sqlite3.connect(path)
        connection.executescript("""
            CREATE TABLE events(id TEXT,enabled INTEGER,action_kind TEXT,action_ref TEXT);
            CREATE TABLE workflow_schedules(id TEXT,workflow_id TEXT,node_id TEXT,enabled INTEGER);
            CREATE TABLE mcps(name TEXT,enabled INTEGER);
            CREATE TABLE workflow_tasks(id TEXT,enabled INTEGER,graph_json TEXT);
            INSERT INTO events VALUES('event-workflow',1,'workflow','wf-1');
            INSERT INTO events VALUES('event-task',1,'scheduled_task','task-1');
            INSERT INTO workflow_schedules VALUES('schedule-1','wf-1','node-schedule',1);
            INSERT INTO mcps VALUES('external-crm',1);
        """)
        graph = {"nodes": [{"id": "node-tool", "type": "mcp-tool",
                             "config": {"mcp_name": "external-crm"}}]}
        connection.execute("INSERT INTO workflow_tasks VALUES(?,?,?)",
                           ("wf-1", 1, json.dumps(graph)))
        connection.commit(); connection.close()
        try:
            references = await durable_module_references(
                AutomationRepository(path),
                frozenset({"workflows", "scheduler", "mcp"}),
            )
            self.assertTrue(any("event-workflow" in value for value in references["workflows"]))
            self.assertTrue(any("event-task" in value for value in references["scheduler"]))
            self.assertTrue(any("external-crm" in value for value in references["mcp"]))
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
