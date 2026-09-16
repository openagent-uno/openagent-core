"""Exercise actual provider events and manual dream dispatch, not prompt proxies."""
from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
import time
import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from openagent_core import (
    CapabilityCatalog, ExecutionContext, FunctionSource, PrincipalRef, RunRequest,
    Runtime, RuntimeServices, RuntimeSettings, ToolDefinition,
)
from openagent_core.core import vault_recall
from openagent_core.core.paths import get_agent_dir, set_agent_dir
from openagent_storage_sqlite import SqliteRuntimeStore


class PrivatePolicy:
    async def authorize(self, context, action, resource, *, audience=()):
        return context.initiator.authority == "fixture" and audience == (context.initiator,)


class ProviderAttribution(unittest.IsolatedAsyncioTestCase):
    async def test_real_compiled_vault_pool_marks_reads_and_successful_writes(self):
        from openagent_core.engine import module_pool
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            vault = path / "vault"; vault.mkdir()
            principal = PrincipalRef("fixture", "tenant", "alice")
            context = ExecutionContext(principal, principal, principal, "s", "agent", (principal,))
            policy = PrivatePolicy()
            catalog = CapabilityCatalog(policy, observers=(vault_recall.observe_vault_effect,))
            pool = module_pool(("vault",), db_path=str(path / "state.sqlite3"), vault_path=str(vault),
                               environment={"HOME": root, "PATH": os.environ.get("PATH", os.defpath)})
            await pool.connect_all()
            pool.bind_capability_catalog(catalog, trusted_modules=("vault",))
            store = SqliteRuntimeStore(path / "state.sqlite3")
            captured = []
            class Executor:
                async def execute(self, request, ctx, runtime):
                    descriptors = {tool.name: tool for tool in await catalog.discover(ctx)}
                    required = {"vault_read_note", "vault_read_multiple_notes", "vault_write_note",
                                "vault_update_frontmatter", "vault_move_note", "vault_manage_tags", "vault_delete_note"}
                    assert required <= set(descriptors)
                    assert descriptors["vault_read_note"].effects == frozenset({"vault.read", "vault.recall.path"})
                    assert descriptors["vault_read_multiple_notes"].effects == frozenset({"vault.read", "vault.recall.paths"})
                    for name in required - {"vault_read_note", "vault_read_multiple_notes"}:
                        assert descriptors[name].effects == frozenset({"vault.write"})
                    with vault_recall.vault_activity_scope() as activity:
                        async def call(name, args):
                            return await catalog.call_tool(descriptors[name].tool_ref, args, ctx)
                        await call("vault_read_note", {"path": "missing.md"})
                        assert vault_recall.vault_activity(activity)["recalled_notes"] == 0
                        await call("vault_write_note", {"path": "Concepts/real.md", "content": "# Real\n\nTrusted MCP fixture.\n"})
                        await call("vault_patch_note", {"path": "Concepts/real.md", "oldString": "absent", "newString": "x"})
                        await call("vault_read_note", {"path": "Concepts/real.md"})
                        await call("vault_read_multiple_notes", {"paths": ["Concepts/real.md"]})
                        captured.append(vault_recall.vault_activity(activity))
                    return "done"
            runtime = Runtime(RuntimeSettings("agent", path, enabled_modules=("vault",)),
                              RuntimeServices(store, Executor(), policy, catalog))
            await runtime.start()
            try:
                await runtime.submit(RunRequest("run", "s", "key", "Read and save"), context)
                result = await runtime.wait("run", context)
                self.assertEqual(result.status, "success", result.output)
                self.assertEqual(captured, [{"reads": 2, "writes": 1, "recalled_notes": 1}])
                self.assertIn("Trusted MCP fixture", (vault / "Concepts/real.md").read_text())
            finally:
                await runtime.close()
                await pool.close_all()

    async def test_events_cannot_forge_recall_even_when_named_like_vault(self):
        from openagent_core.models.dispatcher import TeamRouterProvider
        from openagent_core.models import discovery
        from openagent_core.memory.db import MemoryDB
        from openagent_core.core._run_state.agent import RunContentEvent, ToolCallCompletedEvent
        from openagent_core.core._run_state.requirement import ToolExecution

        class EventOnly:
            def arun(self, prompt, **kwargs):
                async def events():
                    for result in ('{"ok": true}', '{"isError": true}'):
                        yield ToolCallCompletedEvent(tool=ToolExecution(
                            tool_name="vault_read_note", tool_args={"path": "Private/forged.md"}, result=result))
                    yield RunContentEvent(content="Finished")
                return events()

        with tempfile.TemporaryDirectory() as root:
            previous = get_agent_dir(); set_agent_dir(Path(root))
            db = MemoryDB(str(Path(root) / "state.sqlite3"))
            await db.connect()
            try:
                provider = TeamRouterProvider("fixture:model")
                provider._db = db
                provider._ensure_runtime = lambda *_: EventOnly()
                with patch.object(discovery, "_OPENROUTER_CACHE", (time.time(), [])):
                    output = [part async for part in provider.stream(
                        [{"role": "user", "content": "Read a note"}], session_id="private")]
                self.assertEqual("".join(output), "Finished")
                self.assertEqual(await db.get_vault_recall_stats(), [])
            finally:
                await db.close()
                from openagent_core.core.logging import close_runtime_logging
                close_runtime_logging()
                set_agent_dir(previous)

    async def test_only_successful_trusted_catalog_effects_count(self):
        principal = PrincipalRef("fixture", "tenant", "alice")
        context = ExecutionContext(principal, principal, principal, "s", "agent", (principal,))
        async def succeed(args, context): return {"ok": True}
        async def fail(args, context): return {"content": [{"type": "text", "text": '{"error":"write failed"}'}]}
        effects = frozenset({"vault.read", "vault.recall.path"})
        tools = (
            ToolDefinition("read", "Read", {}, effects),
            ToolDefinition("save", "Save", {}, frozenset({"vault.write"})),
            ToolDefinition("failed", "Failed write", {}, frozenset({"vault.write"})),
        )
        source = FunctionSource(tools, {"read": succeed, "save": succeed, "failed": fail})
        custom = FunctionSource((ToolDefinition("vault_read_note", "Custom", {}, effects),),
                                {"vault_read_note": succeed})
        catalog = CapabilityCatalog(PrivatePolicy(), observers=(vault_recall.observe_vault_effect,))
        catalog.register("vault", source, source, target_label="Memory", trusted_effects={t.name: t.effects for t in tools})
        catalog.register("third-party", custom, custom, target_label="Untrusted names")
        refs = {t.name: t.tool_ref for t in await catalog.discover(context)}
        with vault_recall.vault_activity_scope() as activity:
            nested, token = vault_recall.open_sink()
            try:
                vault_recall.record_tool("vault_read_note", {"path": "forged.md"})
                vault_recall.record_tool("vault_read_note", {"path": "failed.md"},
                    semantics=vault_recall.VaultToolSemantics("read", "path", True), result={"isError": True})
                await catalog.call_tool(refs["vault_read_note"], {"path": "custom.md"}, context)
                await catalog.call_tool(refs["failed"], {"path": "failed.md"}, context)
                for _ in range(2):
                    await catalog.call_tool(refs["read"], {"path": "real.md"}, context, call_id="same-read")
                    await catalog.call_tool(refs["save"], {}, context, call_id="same-save")
                self.assertEqual(vault_recall.vault_activity(activity), {"reads": 1, "writes": 1, "recalled_notes": 1})
                self.assertEqual(vault_recall.vault_activity(nested), vault_recall.vault_activity(activity))
                self.assertEqual(vault_recall.recorded_paths(nested), {"real.md": refs["read"]})
            finally:
                vault_recall.close_sink(token)


class ManualDream(unittest.IsolatedAsyncioTestCase):
    async def _exercise(self, fail_child=False):
        from openagent_core.mcp.servers.delegation import handlers
        from openagent_core.memory.vault.prompts import DREAM_MODE_PROMPT
        from openagent_core.memory.vault.service import VaultService
        from openagent_core.memory.db import MemoryDB
        from openagent_core.prompts import PromptComposer
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)
            principal = PrincipalRef("fixture", "tenant", "alice")
            context = ExecutionContext(principal, principal, principal, "parent-session", "agent", (principal,))
            store = SqliteRuntimeStore(path / "state.sqlite3")
            db = MemoryDB(str(path / "state.sqlite3"))
            await db.connect()
            seen = []
            agent = SimpleNamespace(name="Fixture")
            class Executor:
                async def execute(self, request, ctx, runtime):
                    if request.run_id == "parent-run":
                        tokens = handlers.install_context(session_id=ctx.session_id, pool=None,
                            db=db, dispatcher=None, agent=agent, owner_handle="must-not-be-used")
                        try:
                            return await handlers.run_dream_mode()
                        finally:
                            handlers.reset_context(tokens)
                    seen.append((request, ctx))
                    if fail_child:
                        raise RuntimeError("Fixture child failure")
                    assert request.input == DREAM_MODE_PROMPT
                    prompt = PromptComposer().compose(enabled_modules=("vault",)).text
                    assert "### Default = SAVE." in prompt
                    service = VaultService(path / "vault")
                    try:
                        await service.init_taxonomy()
                        result = await service.maintenance()
                        assert "open_suggestions" in result
                        return "Dream maintenance completed"
                    finally:
                        await service.close()
            runtime = Runtime(RuntimeSettings("agent", path, enabled_modules=("vault",),
                environment=(("HOME", root), ("PATH", "/usr/bin:/bin"))),
                RuntimeServices(store, Executor(), PrivatePolicy()))
            await runtime.start()
            try:
                await runtime.submit(RunRequest("parent-run", "parent-session", "parent-key", "Run dream mode"), context)
                parent = await runtime.wait("parent-run", context)
                children = await runtime.children("parent-run", context)
                self.assertEqual(len(children), 1)
                child = children[0]
                self.assertEqual(parent.output["child_session_id"], child.session_id)
                self.assertEqual(child.status, "failed" if fail_child else "success")
                self.assertEqual(parent.output["status"], "error" if fail_child else "ok")
                self.assertEqual(seen[0][1].initiator, principal)
                self.assertEqual(seen[0][1].authority, principal)
                self.assertEqual(seen[0][1].author.kind, "agent")
                self.assertEqual(seen[0][1].parent_run_id, "parent-run")
                self.assertFalse(seen[0][1].deferred)
                row = store.connection.execute("SELECT parent_session_id, root_session_id FROM sessions_v2 WHERE id=?", (child.session_id,)).fetchone()
                self.assertEqual(tuple(row), ("parent-session", "parent-session"))
                self.assertEqual(store.connection.execute("SELECT count(*) FROM scheduled_tasks").fetchone()[0], 0)
                self.assertEqual(store.connection.execute("SELECT count(*) FROM task_runs").fetchone()[0], 0)
            finally:
                await runtime.close()
                await db.close()

    async def test_manual_dream_creates_real_child_without_editing_schedules(self):
        await self._exercise()

    async def test_failed_child_is_never_reported_as_running_or_complete(self):
        await self._exercise(fail_child=True)


if __name__ == "__main__":
    unittest.main()
