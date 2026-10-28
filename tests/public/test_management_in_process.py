"""Synthetic SQLite / public Runtime qualification; no provider or subprocess."""
from __future__ import annotations

import ast
import asyncio
from dataclasses import replace
import inspect
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from typing import get_origin, get_type_hints
from unittest.mock import patch, AsyncMock

from mcp.server.fastmcp import FastMCP

from openagent_core import (ExecutionContext, ModuleCatalog, ModuleConfig, PrincipalRef, RunRequest,
    Runtime, RuntimeProfile, RuntimeServices, RuntimeSettings)
from openagent_core.memory.db import MemoryDB
from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.mcp.builtins import resolve_builtin_entry
from openagent_core.mcp.management_adapters import (ManagementToolkit, _TOOLS,
    build_model_toolkit, build_budget_toolkit, build_skill_data_toolkit)
from openagent_core.mcp.management_context import _binding, scoped_database
from openagent_core.mcp.pool import MCPPool
from openagent_core.mcp.servers.model_manager import server as models
from openagent_core.mcp.servers.budget_manager import server as budgets
from openagent_core.mcp.servers.skill_data import server as skills
from openagent_core.runtime import execution_scope, runtime_scope
from openagent_storage_sqlite import SqliteRuntimeStore


FACTORIES = {"model-manager": build_model_toolkit, "budget-manager": build_budget_toolkit,
             "skill-data": build_skill_data_toolkit}
MODULES = {"model-manager": models, "budget-manager": budgets, "skill-data": skills}


async def call(toolkit, tool_name, **arguments):
    """Read the documented structured output for domain/lifecycle assertions.

    Complete wire comparisons live in test_management_wire_envelopes and use
    the unmodified entrypoint/broker result, never this payload accessor.
    """
    public_name = toolkit.name.replace("-", "_") + "_" + tool_name
    result = await toolkit.async_functions[public_name].entrypoint(**arguments)
    wire = result.mcp_result
    assert isinstance(wire, dict) and isinstance(wire.get('content'), list)
    if wire['isError']:
        return result
    annotation = get_type_hints(getattr(toolkit._module, tool_name)).get('return')
    structured = wire['structuredContent']
    return structured['result'] if get_origin(annotation) is list else structured


class ManagementAdapters(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name) / "agent.sqlite3"
        self.db = MemoryDB(str(self.path))
        await self.db.connect()
        self.toolkits = []
        self.legacy = (models._db, budgets._db)

    async def asyncTearDown(self):
        for toolkit in reversed(self.toolkits):
            await toolkit.aclose()
        models._db, budgets._db = self.legacy
        await self.db.close()
        self.temp.cleanup()

    async def toolkit(self, role, path=None):
        toolkit = await FACTORIES[role](env={"OPENAGENT_DB_PATH": str(path or self.path)})
        self.toolkits.append(toolkit)
        return toolkit

    async def test_default_transport_unchanged(self):
        for name in FACTORIES:
            entry = resolve_builtin_entry(name)
            self.assertFalse(entry.get("in_process", False))
            self.assertIn("command", entry)
        self.assertFalse(NativeCapabilityDescriptor("models", "1", ("model-manager",), {}).in_process_management)

    async def test_unbound_stdio_handlers_keep_their_own_legacy_database(self):
        with patch.object(models, "_db", None), patch.object(budgets, "_db", None), \
             patch.dict(os.environ, {"OPENAGENT_DB_PATH": str(self.path)}):
            model_db, budget_db = await models._get_db(), await budgets._get_db()
            try:
                self.assertIs(model_db, await models._get_db())
                self.assertIs(budget_db, await budgets._get_db())
                self.assertIsNot(model_db, budget_db)
                self.assertEqual(skills._db_path(), str(self.path))
                self.assertIsNone(await scoped_database("model-manager"))
            finally:
                await model_db.close()
                await budget_db.close()

    async def test_only_exact_boolean_and_three_sources_are_selectable(self):
        for flag in (1, "true", None):
            with self.assertRaises(TypeError):
                resolve_builtin_entry("model-manager", in_process=flag)
            with self.assertRaises(TypeError):
                NativeCapabilityDescriptor("models", "1", ("model-manager",), {}, in_process_management=flag)
        with self.assertRaises(ValueError):
            resolve_builtin_entry("vault", in_process=True)
        with self.assertRaises(ValueError):
            NativeCapabilityDescriptor("vault", "1", ("vault",), {}, in_process_management=True)

    async def test_no_ambient_or_relative_database_fallback(self):
        for factory in FACTORIES.values():
            for env in ({}, {"OPENAGENT_DB_PATH": "relative.db"}, {"OPENAGENT_DB_PATH": ""}):
                with self.assertRaises(ValueError):
                    await factory(env=env)

    async def test_exact_exported_tool_sets_and_signatures(self):
        for role, module in MODULES.items():
            tree = ast.parse(Path(module.__file__).read_text())
            exported = {node.name for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                        and any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
                                and d.func.attr == "tool" for d in node.decorator_list)}
            toolkit = await self.toolkit(role)
            self.assertEqual({role.replace("-", "_") + "_" + name for name in exported}, set(toolkit.async_functions))
            self.assertEqual(exported, set(_TOOLS[role]))
            for name, fn in toolkit.async_functions.items():
                wrapped = fn.entrypoint
                original = getattr(module, name.removeprefix(role.replace("-", "_") + "_"))
                self.assertEqual(inspect.signature(wrapped), inspect.signature(original, eval_str=True))
                self.assertEqual(inspect.getdoc(wrapped), inspect.getdoc(original))

    async def test_input_schemas_match_public_mcp_sdk(self):
        for role, module in MODULES.items():
            toolkit = await self.toolkit(role)
            sdk = {role.replace("-", "_") + "_" + tool.name: tool for tool in await module.mcp.list_tools()}
            for name, fn in toolkit.async_functions.items():
                fn.process_entrypoint()
                self.assertEqual(sdk[name].inputSchema, fn.parameters, name)
                self.assertEqual(sdk[name].description, fn.description, name)

    async def test_model_read_results_match_original_handlers_and_mask_credentials(self):
        toolkit = await self.toolkit("model-manager")
        await call(toolkit, "add_provider", name="fixture", api_key="synthetic-not-a-secret")
        with patch.object(models, "_db", self.db):
            for name in ("list_providers", "list_models", "list_supported_frameworks"):
                self.assertEqual(await call(toolkit, name), await getattr(models, name)())
        providers = await call(toolkit, "list_providers")
        self.assertTrue(providers[0]["has_api_key"])
        self.assertNotIn("synthetic-not-a-secret", json.dumps(providers))

    async def test_model_crud_pins_and_last_model_guard_use_original_state(self):
        toolkit = await self.toolkit("model-manager")
        provider = await call(toolkit, "add_provider", name="fixture", api_key="synthetic")
        await call(toolkit, "update_provider", provider_id=provider["id"], base_url="https://invalid.example")
        model = await call(toolkit, "add_model", provider_id=provider["id"], model="synthetic-one")
        model_id = model["id"]
        with patch.object(models, "_db", self.db):
            self.assertEqual(await call(toolkit, "get_model", model_id=model_id), await models.get_model(model_id))
            self.assertEqual(await call(toolkit, "list_supported_providers"), await models.list_supported_providers())
            with self.assertRaisesRegex(ValueError, "last enabled model"):
                await models.remove_model(model_id=model_id)
            rejected = await toolkit.async_functions["model_manager_remove_model"].entrypoint(model_id=model_id)
            self.assertTrue(rejected.mcp_result['isError'])
            self.assertIn('last enabled model', rejected.mcp_result['content'][0]['text'])
        updated = await call(toolkit, "update_model", model_id=model_id, display_name="Synthetic updated")
        self.assertEqual(updated["display_name"], "Synthetic updated")
        classified = await call(toolkit, "set_classifier_model", model_id=model_id)
        self.assertTrue(classified["is_classifier"])
        self.assertFalse((await call(toolkit, "disable_model", model_id=model_id))["enabled"])
        self.assertTrue((await call(toolkit, "enable_model", model_id=model_id))["enabled"])
        runtime_id = model["runtime_id"]
        self.assertTrue((await call(toolkit, "pin_session", session_id="synthetic-session", runtime_id=runtime_id))["pinned"])
        self.assertFalse((await call(toolkit, "unpin_session", session_id="synthetic-session"))["pinned"])
        await call(toolkit, "disable_model", model_id=model_id)
        self.assertTrue((await call(toolkit, "remove_model", model_id=model_id))["removed"])
        self.assertIsNone(await self.db.get_model(model_id))
        self.assertTrue((await call(toolkit, "remove_provider", provider_id=provider["id"]))["removed"])

    async def test_provider_operations_use_only_the_bound_database_credentials(self):
        toolkit = await self.toolkit("model-manager")
        provider = await call(toolkit, "add_provider", name="fixture", api_key="synthetic-bound")
        model = await call(toolkit, "add_model", provider_id=provider["id"], model="synthetic")
        discovery = AsyncMock(return_value=[{"id": "fixture-discovery"}])
        smoke = AsyncMock(return_value=(None, SimpleNamespace(content="fixture-probe")))
        with patch("openagent_core.models.discovery.list_provider_models", discovery), \
             patch("openagent_core.models.runtime.run_provider_smoke_test", smoke):
            self.assertEqual(await call(toolkit, "list_available_models", provider_id=provider["id"]),
                             [{"id": "fixture-discovery"}])
            self.assertEqual(discovery.await_args.kwargs["api_key"], "synthetic-bound")
            result = await call(toolkit, "test_model", runtime_id=model["runtime_id"])
            self.assertEqual(result["response"], "fixture-probe")
            self.assertIn("synthetic-bound", json.dumps(smoke.await_args.args[1]))

    async def test_budget_writes_usage_upsert_and_errors_match_original(self):
        toolkit = await self.toolkit("budget-manager")
        row = await call(toolkit, "set_budget", scope="global", amount=2.0)
        again = await call(toolkit, "set_budget", scope="global", amount=3.0)
        self.assertEqual(row["id"], again["id"])
        self.assertEqual(again["amount"], 3.0)
        with patch.object(budgets, "_db", self.db):
            for name in ("list_budgets", "get_budget_usage"):
                self.assertEqual(await call(toolkit, name), await getattr(budgets, name)())
            with self.assertRaisesRegex(ValueError, "amount"):
                await budgets.set_budget(scope="global", amount=-1.0)
            rejected = await toolkit.async_functions['budget_manager_set_budget'].entrypoint(scope='global', amount=-1.0)
            self.assertTrue(rejected.mcp_result['isError'])
            self.assertIn('amount', rejected.mcp_result['content'][0]['text'])
        self.assertEqual(await call(toolkit, "remove_budget", id=again["id"]), {"ok": True, "id": again["id"]})
        self.assertEqual(await self.db.list_budgets(), [])

    async def test_skill_read_results_and_error_envelopes_match_original(self):
        toolkit = await self.toolkit("skill-data")
        conn = await self.db._ensure_connected()
        await conn.execute("INSERT INTO skill_events(id,skill_id,source,content,timestamp,received_at) VALUES (?,?,?,?,?,?)",
                           ("event", "fixture", "synthetic", "synthetic event", "2026-10-28T10:00:00Z", 1.0))
        await conn.commit()
        with patch.object(skills, "_db_path", return_value=str(self.path)):
            # The exact original handlers produce both success and error envelopes.
            for name, kwargs in (("list_skills", {}), ("search_events", {"query": "synthetic"}),
                                 ("recent_events", {}), ("list_entities", {}),
                                 ("list_events_by_entity", {"entity_id": "missing"}),
                                 ("search_events", {"query": " "})):
                self.assertEqual(await call(toolkit, name, **kwargs), await getattr(skills, name)(**kwargs))

    async def test_skill_data_database_failures_keep_original_error_envelope(self):
        missing = Path(self.temp.name) / "not-created-parent" / "missing.db"
        toolkit = await self.toolkit("skill-data", missing)
        with patch.object(skills, "_db_path", return_value=str(missing)):
            result = await call(toolkit, "list_skills")
            self.assertEqual(result, await skills.list_skills())
        self.assertFalse(result["ok"])
        self.assertFalse(missing.exists())

    async def test_parallel_instances_have_distinct_connections_and_rows(self):
        other_path = Path(self.temp.name) / "other.sqlite3"
        one, two = await self.toolkit("model-manager"), await self.toolkit("model-manager", other_path)
        await asyncio.gather(call(one, "add_provider", name="one", api_key="fixture-one"),
                             call(two, "add_provider", name="two", api_key="fixture-two"))
        a, b = await asyncio.gather(call(one, "list_providers"), call(two, "list_providers"))
        self.assertEqual([row["name"] for row in a], ["one"])
        self.assertEqual([row["name"] for row in b], ["two"])
        self.assertEqual(a[0]["id"], b[0]["id"])  # Colliding app IDs do not merge DBs.
        self.assertIsNot(one._db, two._db)
        self.assertIs(models._db, self.legacy[0])
        await one.aclose()
        self.assertEqual([row["name"] for row in await call(two, "list_providers")], ["two"])

    async def test_closed_instance_fails_before_database_access(self):
        toolkit = await self.toolkit("budget-manager")
        await call(toolkit, "list_budgets")
        db = toolkit._db
        await toolkit.aclose()
        self.assertIsNone(db._conn)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            await call(toolkit, "list_budgets")
        await toolkit.aclose()

    async def test_connection_failure_closes_partial_owned_connection(self):
        toolkit = await self.toolkit("model-manager")
        original = MemoryDB.connect
        observed = []
        async def failing(db):
            await original(db)
            observed.append(db)
            raise ValueError("synthetic connect failure")
        with patch.object(MemoryDB, "connect", failing):
            rejected = await toolkit.async_functions['model_manager_list_providers'].entrypoint()
            self.assertTrue(rejected.mcp_result['isError'])
            self.assertIn('synthetic connect failure', rejected.mcp_result['content'][0]['text'])
        self.assertIsNone(observed[0]._conn)
        self.assertIsNone(toolkit._db)
        self.assertEqual(await call(toolkit, "list_providers"), [])

    async def test_cancelled_connect_closes_connection_and_can_retry(self):
        toolkit = await self.toolkit("model-manager")
        original = MemoryDB.connect
        ready = asyncio.Event()
        observed = []
        async def delayed(db):
            await original(db)
            observed.append(db)
            ready.set()
            await asyncio.Event().wait()
        with patch.object(MemoryDB, "connect", delayed):
            task = asyncio.create_task(call(toolkit, "list_providers"))
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertIsNone(observed[0]._conn)
        self.assertEqual(await call(toolkit, "list_providers"), [])

    async def test_close_cancels_only_own_pending_calls(self):
        ready = asyncio.Event()
        cleaned = asyncio.Event()
        async def wait():
            ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        sdk = FastMCP('synthetic-management-close')
        for name in _TOOLS['budget-manager']:
            sdk.add_tool(wait, name=name)
        module = SimpleNamespace(mcp=sdk, **{name: wait for name in _TOOLS["budget-manager"]})
        toolkit = ManagementToolkit("budget-manager", module, {"OPENAGENT_DB_PATH": str(self.path)})
        await toolkit.prepare(module)
        self.toolkits.append(toolkit)
        pending = asyncio.create_task(toolkit.tools[0]())
        unrelated = asyncio.create_task(asyncio.sleep(0.03, result="unrelated"))
        await ready.wait()
        await toolkit.aclose()
        self.assertTrue(pending.cancelled())
        self.assertTrue(cleaned.is_set())
        self.assertEqual(await unrelated, "unrelated")

    async def test_wrong_role_and_inherited_background_binding_fail_closed(self):
        toolkit = await self.toolkit("model-manager")
        token = _binding.set((toolkit, asyncio.current_task()))
        try:
            with self.assertRaisesRegex(RuntimeError, "binding"):
                await scoped_database("budget-manager")
            with self.assertRaisesRegex(RuntimeError, "binding"):
                await asyncio.create_task(scoped_database("model-manager"))
        finally:
            _binding.reset(token)
        self.assertIsNone(toolkit._db)

    async def test_pool_close_owns_adapter_lifecycle(self):
        pool = MCPPool.from_config([{"builtin": "budget-manager", "in_process": True,
            "env": {"OPENAGENT_DB_PATH": str(self.path)}}], include_defaults=False)
        await pool.connect_all()
        toolkit = pool.toolkit_by_name("budget-manager")
        await call(toolkit, "list_budgets")
        await pool.close_all()
        self.assertTrue(toolkit._closed)
        self.assertIsNone(toolkit._db)

    async def test_pool_reload_replaces_and_closes_only_the_old_instance(self):
        pool = MCPPool.from_config([{"builtin": "budget-manager", "in_process": True,
            "env": {"OPENAGENT_DB_PATH": str(self.path)}}], include_defaults=False)
        await pool.connect_all()
        old = pool.toolkit_by_name("budget-manager")
        await call(old, "list_budgets")
        pool._db = object()
        with patch.object(pool, "rebuild_specs", AsyncMock(return_value=list(pool.specs))):
            await pool.reload()
        replacement = pool.toolkit_by_name("budget-manager")
        try:
            self.assertIsNot(old, replacement)
            self.assertTrue(old._closed)
            self.assertEqual(await call(replacement, "list_budgets"), [])
        finally:
            await pool.close_all()

    async def test_failed_pool_replacement_also_closes_unreachable_old_instance(self):
        pool = MCPPool.from_config([{"builtin": "model-manager", "in_process": True,
            "env": {"OPENAGENT_DB_PATH": str(self.path)}}], include_defaults=False)
        await pool.connect_all()
        old = pool.toolkit_by_name("model-manager")
        await call(old, "list_providers")
        pool._db = object()
        with patch.object(pool, "rebuild_specs", AsyncMock(return_value=[])), \
             patch.object(pool, "connect_all", AsyncMock(side_effect=RuntimeError("synthetic replacement failure"))):
            with self.assertRaisesRegex(RuntimeError, "synthetic replacement failure"):
                await pool.reload()
        self.assertTrue(old._closed)
        self.assertIsNone(old._db)

    async def test_descriptor_rejects_mismatched_or_relative_db_before_pool_creation(self):
        descriptor = NativeCapabilityDescriptor("model-manager", "1", ("model-manager",), {},
                                                in_process_management=True)
        for options in ({"db_path": "relative.db"}, {"db_path": str(self.path),
                        "environment": {"OPENAGENT_DB_PATH": str(self.path) + ".other"}}):
            context = SimpleNamespace(config=ModuleConfig(options=options),
                                      profile=RuntimeProfile(1, {}))
            instance = await descriptor.prepare(context)
            with patch.object(MCPPool, "from_config") as factory:
                with self.assertRaises(ValueError):
                    await instance.start()
            factory.assert_not_called()

    async def test_skills_descriptor_only_opts_the_management_source_in(self):
        descriptor = NativeCapabilityDescriptor("skills", "1", ("skills", "skill-data"), {},
                                                in_process_management=True)
        context = SimpleNamespace(config=ModuleConfig(options={"db_path": str(self.path)}),
                                  profile=RuntimeProfile(1, {}))
        instance = await descriptor.prepare(context)
        with patch.object(MCPPool, "from_config", side_effect=ValueError("captured configuration")) as factory:
            with self.assertRaisesRegex(ValueError, "captured configuration"):
                await instance.start()
        entries = factory.call_args.args[0]
        self.assertNotIn("in_process", entries[0])
        self.assertIs(entries[1]["in_process"], True)


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied and resource.tenant_id == context.tenant_id


class HoldingExecutor:
    def __init__(self):
        self.started = asyncio.Event()

    async def execute(self, request, context, runtime):
        self.started.set()
        await asyncio.Event().wait()


class RuntimeIsolation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.runtimes = []
        self.policies = []
        self.contexts = []
        for name in ("first", "second"):
            path = Path(self.temp.name) / name
            path.mkdir()
            policy = Policy()
            principal = PrincipalRef("fixture", name, name)
            context = ExecutionContext(principal, principal, principal, "session", name, (principal,))
            descriptors = tuple(NativeCapabilityDescriptor(
                role, "1", (role,), {role: role}, in_process_management=True) for role in FACTORIES)
            profile = RuntimeProfile(1, {role: ModuleConfig(frozenset({"service", "agent_tools"}),
                {"db_path": str(path / "agent.sqlite3")}) for role in FACTORIES})
            runtime = Runtime(RuntimeSettings(name, path),
                RuntimeServices(SqliteRuntimeStore(path / "canonical.sqlite3"), HoldingExecutor(), policy),
                profile=profile, module_catalog=ModuleCatalog(descriptors))
            self.runtimes.append(runtime)
            self.policies.append(policy)
            self.contexts.append(context)
        await asyncio.gather(*(runtime.start() for runtime in self.runtimes))
        for runtime, context in zip(self.runtimes, self.contexts):
            await runtime.submit(RunRequest("fixture", "session", "fixture", "synthetic"), context)
            await runtime.services.executor.started.wait()

    async def asyncTearDown(self):
        await asyncio.gather(*(runtime.close() for runtime in self.runtimes))
        self.temp.cleanup()

    async def invoke(self, index, name, arguments):
        runtime, context = self.runtimes[index], self.contexts[index]
        with execution_scope(runtime, context, "fixture", module_generation=1,
                             capability_revision=runtime.capabilities.snapshot_revision()):
            tools = await runtime.capabilities.discover(context)
            tool = next(tool for tool in tools if tool.name == name)
            return await runtime.capabilities.call_tool(tool.tool_ref, arguments, context)

    async def test_two_real_runtimes_concurrent_tools_preserve_database_and_policy(self):
        await asyncio.gather(self.invoke(0, "model_manager_add_provider", {"name": "first", "api_key": "synthetic-first"}),
                             self.invoke(1, "model_manager_add_provider", {"name": "second", "api_key": "synthetic-second"}))
        a, b = await asyncio.gather(self.invoke(0, "model_manager_list_providers", {}),
                                   self.invoke(1, "model_manager_list_providers", {}))
        self.assertFalse(a['isError'])
        self.assertFalse(b['isError'])
        self.assertEqual([row["name"] for row in a['structuredContent']['result']], ["first"])
        self.assertEqual([row["name"] for row in b['structuredContent']['result']], ["second"])
        self.policies[0].denied.add("tool.call")
        with self.assertRaises(PermissionError):
            await self.invoke(0, "model_manager_list_providers", {})
        self.assertEqual([row['name'] for row in (await self.invoke(1, 'model_manager_list_providers', {}))['structuredContent']['result']], ['second'])
        await self.runtimes[0].close()
        self.assertEqual([row['name'] for row in (await self.invoke(1, 'model_manager_list_providers', {}))['structuredContent']['result']], ['second'])

    async def test_foreign_runtime_cannot_use_retained_toolkit(self):
        runtime = self.runtimes[0]
        with runtime_scope(runtime):
            toolkit = await build_model_toolkit(env={"OPENAGENT_DB_PATH": str(Path(self.temp.name) / "forbidden.db")})
        try:
            with runtime_scope(self.runtimes[1]):
                with self.assertRaises(PermissionError):
                    await call(toolkit, "list_providers")
            self.assertIsNone(toolkit._db)
        finally:
            await toolkit.aclose()

    async def test_reference_from_other_runtime_is_not_portable(self):
        runtime, context = self.runtimes[0], self.contexts[0]
        with execution_scope(runtime, context, "fixture", module_generation=1,
                             capability_revision=runtime.capabilities.snapshot_revision()):
            tool = next(tool for tool in await runtime.capabilities.discover(context) if tool.name == "model_manager_list_providers")
        other, foreign = self.runtimes[1], self.contexts[1]
        from openagent_core import CapabilityUnavailable
        with execution_scope(other, foreign, "fixture", module_generation=1,
                             capability_revision=other.capabilities.snapshot_revision()):
            with self.assertRaises(CapabilityUnavailable):
                await other.capabilities.call_tool(tool.tool_ref, {}, foreign)

    async def test_current_policy_revocation_rechecks_an_already_discovered_reference(self):
        runtime, context = self.runtimes[0], self.contexts[0]
        with execution_scope(runtime, context, "fixture", module_generation=1,
                             capability_revision=runtime.capabilities.snapshot_revision()):
            tool = next(tool for tool in await runtime.capabilities.discover(context) if tool.name == "model_manager_list_providers")
            self.policies[0].denied.add("tool.call")
            with self.assertRaises(PermissionError):
                await runtime.capabilities.call_tool(tool.tool_ref, {}, context)
            self.policies[0].denied.clear()
            value = await runtime.capabilities.call_tool(tool.tool_ref, {}, context)
            self.assertFalse(value['isError'])
            self.assertEqual(value['structuredContent']['result'], [])

    async def test_tool_family_scope_and_denylist_remain_enforced(self):
        from openagent_core.core.tool_scope import set_tool_allowlist, reset_tool_allowlist
        token = set_tool_allowlist(("budget-manager",))
        try:
            runtime, context = self.runtimes[0], self.contexts[0]
            with execution_scope(runtime, context, "fixture", module_generation=1,
                                 capability_revision=runtime.capabilities.snapshot_revision()):
                self.assertFalse(any(tool.name == "model_manager_list_providers"
                    for tool in await runtime.capabilities.discover(context)))
        finally:
            reset_tool_allowlist(token)
        runtime, context = self.runtimes[0], self.contexts[0]
        with execution_scope(runtime, context, "fixture", module_generation=1,
                             capability_revision=runtime.capabilities.snapshot_revision()):
            tool = next(tool for tool in await runtime.capabilities.discover(context) if tool.name == "model_manager_list_providers")
            object.__setattr__(runtime, "settings", replace(runtime.settings,
                environment=(("OPENAGENT_MCP_TOOL_DENYLIST", "model-manager:model_manager_list_providers"),)))
            self.assertFalse(any(item.name == "model_manager_list_providers"
                for item in await runtime.capabilities.discover(context)))
            from openagent_core import CapabilityUnavailable
            with self.assertRaises(CapabilityUnavailable):
                await runtime.capabilities.call_tool(tool.tool_ref, {}, context)


if __name__ == "__main__":
    unittest.main()
