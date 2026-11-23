"""Exercise the document vault through real runtime turns and service calls."""
import asyncio
import os
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import (
    ExecutionContext, ModuleCatalog, ModuleConfig, PrincipalRef, RunRequest,
    Runtime, RuntimeProfile, RuntimeServices, RuntimeSettings,
)
from openagent_core.core.agent import _with_recall
from openagent_core.memory.vault.documents import RevisionConflict, SqlDocumentVault
from openagent_core.modules import ServiceRegistry
from openagent_module_vault import descriptor
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    denied = False

    async def authorize(self, context, action, resource, *, audience=()):
        if resource.kind == "vault-space":
            return not self.denied and all(p == context.initiator for p in audience)
        return True


class VaultTurn:
    async def execute(self, request, context, runtime):
        tools = {tool.name: tool.tool_ref for tool in await runtime.capabilities.discover(context)}
        if request.input == "remember":
            await runtime.capabilities.call_tool(tools["vault_write_note"], {
                "path": "topics/color.md", "content": "# Cobalt\n[[topics/Palette#top]]",
                "expected_revision": 0,
            }, context)
            return "saved"
        if request.input == "recall":
            with patch("openagent_core.core.agent._recall_enabled", return_value=True):
                return await _with_recall(None, context.session_id, "cobalt", "recall")
        page = await runtime.capabilities.call_tool(tools["vault_search"], {"query": "cobalt"}, context)
        return ",".join(item["path"] for item in page["hits"])


class DocumentVault(unittest.IsolatedAsyncioTestCase):
    database_url = None

    async def asyncSetUp(self):
        self.directory = Path(self.enterContext(TemporaryDirectory()))
        self.engine = create_async_engine(self.database_url or "sqlite+aiosqlite:///" + str(self.directory / "vault.db"))
        self.addAsyncCleanup(self.engine.dispose)
        self.tenant = uuid.uuid4().hex
        self.policy = Policy()

        async def resolve(context):
            return context.initiator.subject_id

        self.store = SqlDocumentVault(self.engine, self.policy, resolve)
        await self.store.initialize()
        self.a = self.context("a")
        self.b = self.context("b")
        self.runtime = Runtime(
            RuntimeSettings("agent"),
            RuntimeServices(SqliteRuntimeStore(self.directory / "runs.db"), VaultTurn(), self.policy,
                            registry=ServiceRegistry({SqlDocumentVault: self.store})),
            profile=RuntimeProfile(1, {"vault": ModuleConfig(
                frozenset({"service", "host_api", "agent_tools"}), {"backend": "documents"}
            )}),
            module_catalog=ModuleCatalog((descriptor,)),
        )
        self.addAsyncCleanup(self.runtime.close)
        await self.runtime.start()

    def context(self, who, recipients=None):
        p = PrincipalRef("test", self.tenant, who)
        return ExecutionContext(p, p, p, who + "-session", "agent", recipients or (p,))

    async def turn(self, context, text):
        run = RunRequest(uuid.uuid4().hex, context.session_id, uuid.uuid4().hex, text)
        await self.runtime.submit(run, context)
        result = await self.runtime.wait(run.run_id, context)
        self.assertEqual(result.status, "success", result)
        return result.output

    async def test_session_memory_recall_and_isolation(self):
        self.assertEqual(await self.turn(self.a, "remember"), "saved")
        self.assertEqual(await self.turn(self.b, "search"), "")
        self.assertEqual(await self.turn(self.a, "search"), "topics/color.md")
        self.assertIn("topics/color.md", await self.turn(self.a, "recall"))
        self.assertEqual(await self.turn(self.b, "recall"), "recall")
        self.assertEqual((await self.store.backlinks(self.a, "topics/palette.md"))["paths"], ["topics/color.md"])
        self.assertEqual((await self.store.backlinks(self.b, "topics/palette.md"))["paths"], [])
        with self.assertRaises(PermissionError):
            await self.store.read(self.context("a", (self.a.initiator, self.b.initiator)), "topics/color.md")

    async def test_revisions_conflicts_paging_and_forget(self):
        async def write(text, expected):
            return await self.store.write(self.a, "a.md", text, expected_revision=expected)
        results = await asyncio.gather(write("first", 0), write("another", 0), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, RevisionConflict) for r in results), 1)
        original = await self.store.read(self.a, "a.md")
        await write("unique blue", 1)
        self.assertEqual((await self.store.read(self.a, "a.md", revision=1))["content"], original["content"])
        await self.store.write(self.a, "b.md", "other", expected_revision=0)
        page = await self.store.list_notes(self.a, limit=1)
        self.assertEqual(page["next_cursor"], "a.md")
        self.assertEqual((await self.store.list_notes(self.a, after=page["next_cursor"]))["notes"][0]["path"], "b.md")
        self.assertEqual((await self.store.history(self.a, "a.md", limit=1))["next_cursor"], 2)
        self.assertEqual((await self.store.history(self.a, "a.md", before=2))["revisions"], [1])
        self.assertEqual((await self.store.search(self.a, query="blue"))["hits"][0]["path"], "a.md")
        await self.store.forget(self.a, "a.md", expected_revision=2)
        self.assertFalse((await self.store.search(self.a, query="blue"))["hits"])
        self.assertFalse((await self.store.history(self.a, "a.md"))["revisions"])
        with self.assertRaises(LookupError):
            await self.store.read(self.a, "a.md", revision=1)
        with self.assertRaises(RevisionConflict):
            await write("late replay", 0)
        with self.assertRaises(LookupError):
            await self.store.read(self.a, "a.md")

    async def test_revocation_rolls_back_note_links_search_and_history(self):
        original = self.store.resolve_space
        calls = 0
        async def revoke(context):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.policy.denied = True
            return await original(context)
        self.store.resolve_space = revoke
        with self.assertRaises(PermissionError):
            await self.store.write(self.a, "a.md", "secret [[x]]", expected_revision=0)
        self.policy.denied = False
        self.assertEqual((await self.store.list_notes(self.a))["notes"], [])
        self.assertEqual((await self.store.backlinks(self.a, "x.md"))["paths"], [])
        self.assertEqual((await self.store.search(self.a, query="secret"))["hits"], [])
        self.assertEqual((await self.store.history(self.a, "a.md"))["revisions"], [])


@unittest.skipUnless(os.getenv("OPENAGENT_TEST_POSTGRES_URL"), "requires dedicated test PostgreSQL")
class PostgresDocumentVault(DocumentVault):
    database_url = os.getenv("OPENAGENT_TEST_POSTGRES_URL")
