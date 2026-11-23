"""Exercise the document vault through real runtime turns and service calls."""
import asyncio
import os
from contextlib import asynccontextmanager
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
from openagent_module_sessions import descriptor as sessions_descriptor
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
            ), "sessions": ModuleConfig()}),
            module_catalog=ModuleCatalog((descriptor, sessions_descriptor)),
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

    async def test_forget_removes_transitive_copies_and_restores_independent_revision(self):
        store, actor = self.store, self.a
        await store.write(actor, "source.md", "private sample", expected_revision=0)
        await store.write(actor, "guide.md", "public instructions", expected_revision=0)
        async def copy(request, context, runtime):
            if request.input == "first":
                await store.read(context, "source.md")
                await store.write(context, "guide.md", "private sample plus instructions", expected_revision=1)
            else:
                await store.read(context, "guide.md")
                await store.write(context, "copy.md", "private sample", expected_revision=0)
            return "copied"
        with patch.object(self.runtime.services.executor, "execute", copy):
            await self.turn(actor, "first")
            await self.turn(actor, "second")
        result = await store.forget(actor, "source.md", expected_revision=1)
        self.assertTrue(result["forgotten"])
        self.assertEqual((await store.read(actor, "guide.md"))["content"], "public instructions")
        for path, rev in (("source.md",1), ("guide.md",2), ("copy.md",1)):
            with self.assertRaises(LookupError):
                await store.read(actor,path,revision=rev)
        self.assertEqual((await store.search(actor, query="private"))["hits"], [])


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

    async def test_admin_and_agent_share_one_vault_with_reversible_delete(self):
        from openagent_core.administration import ManagementContext
        from openagent_core.memory.vault.document_admin import DocumentVaultAdministration
        admin = DocumentVaultAdministration(self.store, self.policy, self.a)
        context = ManagementContext(self.a.authority, self.a.agent_id)
        await self.turn(self.a, "remember")
        result = await admin.execute(context, "read", path="topics/color.md")
        self.assertIn("Cobalt", result["content"])
        self.assertEqual(result["links"], ["topics/Palette#top"])
        await admin.execute(context, "write", path="topics/palette.md", body={"content": "# Palette"})
        graph = await admin.execute(context, "graph")
        self.assertEqual(graph["edges"], [{"source": "topics/color.md", "target": "topics/palette.md"}])
        self.assertIn("violations", await admin.execute(context, "gate"))
        await admin.execute(context, "delete", path="topics/color.md")
        self.assertFalse((await self.store.search(self.a, query="Cobalt"))["hits"])
        self.assertEqual((await self.store.read(self.a, "topics/color.md", revision=1))["content"], result["content"])
        await self.store.forget(self.a, "topics/color.md", expected_revision=2)
        with self.assertRaises(LookupError):
            await self.store.read(self.a, "topics/color.md", revision=1)

    async def test_personal_and_shared_spaces_with_the_same_note_path_are_distinct(self):
        from dataclasses import replace
        from openagent_core.memory.personal_catalog import PersonalMemoryCatalog, PersonalMemorySpace
        from openagent_gateway import create_app
        from openagent_sdk import PersonalClient, RunHTTPError
        from aiohttp.test_utils import TestServer

        grants = {"a": {"a", "shared"}, "b": {"b", "shared"}}
        async def resolve(context):
            return context.scopes[0] if context.scopes else context.initiator.subject_id
        self.store.resolve_space = resolve
        async def select(context, ident):
            if ident not in grants[context.initiator.subject_id]:
                raise PermissionError("Space unavailable")
            return PersonalMemorySpace(ident, "shared" if ident == "shared" else "personal"), replace(context, scopes=(ident,))
        async def spaces(context, *, after, limit):
            return [key for key in sorted(grants[context.initiator.subject_id]) if after is None or key > after][:limit]

        catalog = PersonalMemoryCatalog(spaces, select)
        class Host:
            def service(_, name):
                return catalog if name is PersonalMemoryCatalog else self.runtime.service(name)
        @asynccontextmanager
        async def identity(request, resource):
            yield self.context(request.headers["Authorization"])
        path = "manual.md"
        await self.store.write(self.a, path, "my private note", expected_revision=0)
        await self.store.write(replace(self.a, scopes=("shared",)), path, "household note", expected_revision=0)
        await self.store.write(self.b, path, "another private note", expected_revision=0)
        server = TestServer(create_app(Host(), None, resolve_personal_context=identity, manage_lifecycle=False))
        await server.start_server()
        self.addAsyncCleanup(server.close)
        client = PersonalClient(str(server.make_url("")).rstrip("/"), lambda: {"Authorization": "a"})
        first = await client.memories(query="note", limit=1)
        second = await client.memories(query="note", limit=1, cursor=first["next_cursor"])
        self.assertEqual([(r["space"],r["visibility"]) for p in (first,second) for r in p["hits"]],
                         [("a","personal"), ("shared","shared")])
        self.assertEqual((await client.memory(path,space="shared"))["content"], "household note")
        with self.assertRaises(RunHTTPError):
            await client.memory(path, space="b")
        grants["a"].remove("shared")
        self.assertFalse((await client.memories(cursor=first["next_cursor"]))["notes"])
        with self.assertRaises(RunHTTPError):
            await client.memory(path, space="shared")

    async def test_authenticated_catalog_without_an_open_chat(self):
        from aiohttp import web
        from openagent_gateway import create_app
        from openagent_sdk import PersonalClient, RunHTTPError

        @asynccontextmanager
        async def personal(request, resource):
            token = request.headers.get("Authorization")
            if token not in {"Bearer a", "Bearer b"}:
                raise PermissionError("Unknown credential")
            yield self.context(token[-1])

        await self.turn(self.a, "remember")
        await self.turn(self.b, "search")
        await self.store.write(self.a, "extra.md", "Cobalt", expected_revision=0)
        app = create_app(self.runtime, None, resolve_personal_context=personal, manage_lifecycle=False)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = "http://127.0.0.1:" + str(runner.addresses[0][1])
        a = PersonalClient(url, lambda: {"Authorization": "Bearer a"})
        b = PersonalClient(url, lambda: {"Authorization": "Bearer b"})
        page = await a.sessions()
        self.assertEqual([s["session_id"] for s in page["sessions"]], [self.a.session_id])
        ref = page["sessions"][0]["session_ref"]
        self.assertTrue((await a.session(ref))["messages"])
        with self.assertRaises(RunHTTPError) as denial:
            await b.session(ref)
        self.assertEqual(denial.exception.status, 403)
        self.assertEqual((await b.memories())["notes"], [])
        self.assertEqual((await b.memories(query="cobalt"))["hits"], [])
        page = await a.memories(query="cobalt", limit=1)
        self.assertEqual(page["hits"][0]["path"], "extra.md")
        second = await a.memories(query="cobalt", cursor=page["next_cursor"], limit=1)
        self.assertIsNone(second["next_cursor"])
        self.assertIn("Cobalt", (await a.memory(second["hits"][0]["path"]))["content"])
        with self.assertRaises(RunHTTPError) as missing:
            await b.memory("extra.md")
        self.assertEqual(missing.exception.status, 404)

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
