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
        if request.input == "invalid-path-then-retry":
            for path in ("/topics/Test.MD", "a/../Test.MD", "z" * 701 + ".md"):
                try:
                    await runtime.capabilities.call_tool(tools["vault_write_note"], {
                        "path": path, "content": "private", "expected_revision": 0,
                    }, context)
                except ValueError:
                    continue
                raise AssertionError("Invalid path was accepted")
            await runtime.capabilities.call_tool(tools["vault_write_note"], {
                "path": "Tópics/File With Spaces.MD", "content": "example", "expected_revision": 0,
            }, context)
            return "saved"
        if request.input == "reconcile":
            write = tools["vault_write_note"]
            args = {"path": "Case.MD", "content": "unchanged", "expected_revision": 1}
            conflict = await runtime.capabilities.call_tool(write, args, context)
            assert conflict["isError"] and conflict["structuredContent"]["effects"] == "none"
            assert "read" in conflict["structuredContent"]["recovery"].lower()
            saved = await runtime.capabilities.call_tool(write, {**args, "expected_revision": 0}, context)
            assert saved["revision"] == 1
            duplicate = await runtime.capabilities.call_tool(write, {**args, "content": "raced",
                "expected_revision": 0}, context)
            assert duplicate["isError"] and duplicate["structuredContent"]["error_type"] == "revision_conflict"
            return "reconciled"
        if request.input.startswith("recall-envelope"):
            from openagent_core.runtime import check_current_publication
            with patch("openagent_core.core.agent._recall_enabled", return_value=True):
                text = await _with_recall(None, context.session_id, request.input, "original user input")
            if getattr(self, "on_recall", None):
                self.on_recall(text)
            # The provider transport calls this exact guard before it sends.
            await check_current_publication()
            return text
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


    async def test_invalid_syntax_never_creates_uncertain_effect_or_changes_the_requested_path(self):
        import re
        [write] = [t for t in await self.runtime.capabilities.discover(self.a) if t.name == "vault_write_note"]
        schema = write.input_schema["properties"]["path"]
        self.assertFalse(re.search(schema["pattern"], "/filename.md"))
        self.assertTrue(re.search(schema["pattern"], "Tópics/File With Spaces.MD"))
        req = RunRequest(uuid.uuid4().hex, self.a.session_id, uuid.uuid4().hex, "invalid-path-then-retry")
        await self.runtime.submit(req, self.a)
        self.assertEqual((await self.runtime.wait(req.run_id, self.a)).status, "success")
        kinds = [e.kind for e in await self.runtime.events(req.run_id, 0, self.a)]
        self.assertEqual(kinds.count("tool.invoking"), 1)
        self.assertEqual(kinds.count("tool.completed"), 1)
        saved = await self.store.read(self.a, "Tópics/File With Spaces.MD")
        self.assertEqual(saved["content"], "example")

    async def test_conditional_write_conflict_reports_no_effect_and_keeps_the_winning_revision(self):
        req = RunRequest(uuid.uuid4().hex, self.a.session_id, uuid.uuid4().hex, "reconcile")
        await self.runtime.submit(req, self.a)
        self.assertEqual((await self.runtime.wait(req.run_id, self.a)).status, "success")
        receipts = [e.payload for e in await self.runtime.events(req.run_id, 0, self.a) if e.kind == "tool.completed"]
        self.assertEqual(len(receipts), 3)
        for receipt in (receipts[0], receipts[2]):
            self.assertEqual(receipt["error"]["effects"], "none")
            self.assertEqual(receipt["error"]["error_type"], "revision_conflict")
        saved = await self.store.read(self.a, "Case.MD")
        self.assertEqual((saved["content"], saved["revision"]), ("unchanged", 1))

    async def test_bounded_question_recall_precedes_provider_and_rechecks_its_note(self):
        from dataclasses import replace
        from openagent_core.memory_access import DocumentRecallQuery
        await self.store.write(self.a, "heating.md", "Furnace manual calibration instructions.", expected_revision=0)
        await self.store.write(self.a, "other.md", "Manual for a sewing machine.", expected_revision=0)
        foreign = PrincipalRef("test", "separate-" + self.tenant, self.a.authority.subject_id)
        other = replace(self.a, initiator=foreign, authority=foreign, author=foreign, audience=(foreign,))
        await self.store.write(other, "secret.md", "Furnace calibration manual private.", expected_revision=0)
        self.assertTrue(await self.store.contains(self.a, "heating.md"))
        self.assertFalse(await self.store.contains(other, "heating.md"))
        question = "Where can I find how to do furnace calibration? This is a differently worded question."
        raw = "recall-envelope " + "unrelated noise " * 350 + question
        self.assertEqual((await self.store.search(self.a, query=question))["hits"], [])
        self.assertEqual((await self.store.recall(self.a, query=raw, limit=1))[0]["path"], "heating.md")
        class Selector:
            async def select_query(self, context, raw_input):
                assert context == self_context and raw_input == raw
                return question
        self_context = self.a
        self.runtime.services.registry.bind(DocumentRecallQuery, Selector())
        text = await self.turn(self.a, raw)
        self.assertIn("heating.md", text)
        self.assertNotIn("secret.md", text)
        self.assertNotIn(question, text)
        self.assertTrue(text.endswith("original user input"))
        policy = self.policy
        allowed = policy.authorize
        denied = False
        async def note_policy(context, action, resource, *, audience=()):
            if denied and resource.kind == "vault-note":
                return False
            return await allowed(context, action, resource, audience=audience)
        policy.authorize = note_policy
        def revoke_after_preparation(prepared):
            nonlocal denied
            self.assertIn("heating.md", prepared)
            denied = True
        self.runtime.services.executor.on_recall = revoke_after_preparation
        request = RunRequest(uuid.uuid4().hex, self.a.session_id, uuid.uuid4().hex, raw)
        await self.runtime.submit(request, self.a)
        record = await self.runtime.wait(request.run_id, self.a)
        self.assertNotEqual(record.status, "success")
        await self.store.delete(self.a, "heating.md", expected_revision=1)
        self.assertFalse(await self.store.contains(self.a, "heating.md"))

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

    async def test_editor_filename_search_and_metadata_survive_an_older_index(self):
        import sqlalchemy as sa
        from openagent_core.administration import ManagementContext
        from openagent_core.memory.vault.document_admin import DocumentVaultAdministration
        from openagent_core.memory.vault.documents import notes, properties

        admin = DocumentVaultAdministration(self.store, self.policy, self.a)
        context = ManagementContext(self.a.authority, self.a.agent_id)
        text = "---\ntags: [green]\ntype: reference\n---\nCaffè, bodyonly."
        path = "references/Needle%_File.md"
        await self.store.write(self.a, path, text, expected_revision=0)
        await self.store.write(self.a, "references/needleXYfile.md", "plain", expected_revision=0)
        await self.store.write(self.b, "other/Needle%_File.md", text, expected_revision=0)
        for old in ("current", "stale", "missing"):
            if old == "stale":
                async with self.store.engine.begin() as con:
                    await con.execute(properties.update().where(properties.c.note_id.in_(
                        sa.select(notes.c.id).where(notes.c.path == path))).values(revision=0,tags=["stale"],size_bytes=0))
            if old == "missing":
                async with self.store.engine.begin() as con:
                    await con.execute(properties.delete().where(properties.c.note_id.in_(
                        sa.select(notes.c.id).where(notes.c.path == path))))
            result = await admin.execute(context, "search/files", query={"q": "NEEDLE%_"})
            self.assertEqual(result["results"], [{"path": path, "title": "Needle%_File", "tags": ["green"]}])
            self.assertFalse((await admin.execute(context, "search/files", query={"q":"bodyonly"}))["results"])
            self.assertEqual((await admin.execute(context, "search", query={"q":"bodyonly"}))["results"][0]["path"], path)
            page = await admin.execute(context, "notes", query={"limit": 1})
            entry = page["notes"][0]
            self.assertEqual((entry["path"], entry["size"], entry["type"], entry["tags"]), (path,len(text.encode()),"reference",["green"]))
            self.assertIsInstance(entry["modified"], float)
            self.assertNotIn("content", entry)
            self.assertEqual((await admin.execute(context,"read",path=path))["modified"],entry["modified"])
            next_page = await admin.execute(context,"notes",query={"limit":1,"after":page["next_cursor"]})
            self.assertEqual([n["path"] for n in next_page["notes"]], ["references/needleXYfile.md"])

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

    def administration(self, actor, **kwargs):
        from openagent_core.administration import ManagementContext
        from openagent_core.memory.vault.document_admin import DocumentVaultAdministration
        service = DocumentVaultAdministration(self.store, self.policy, actor, **kwargs)
        context = ManagementContext(actor.authority, actor.agent_id)
        async def call(operation, **fields):
            return await service.execute(context, operation, **fields)
        return call

    async def test_folder_move_backlinks_history_forget_and_independent_path_reuse(self):
        admin = self.administration(self.a)
        async def write(path, text, revision=0):
            return await self.store.write(self.a, path, text, expected_revision=revision)
        await write("old/a.md", "privateword")
        await write("old/b.md", "[[old/a#top|one]]")
        await write("ref.md", "[[old/a#heading|two]] [[old/b^here]]")
        await self.store.write(self.b, "old/a.md", "another space", expected_revision=0)
        token = await self.store.note_receipts(await self.store._scope(self.a, "memory.read"), [("old/a.md",1)])
        result = await admin("move", body={"from": "old", "to": "new"})
        self.assertEqual((result["notes_moved"], result["notes_updated"], result["links_rewritten"]), (2,2,3))
        self.assertEqual((await self.store.read(self.a, "ref.md"))["content"], "[[new/a#heading|two]] [[new/b^here]]")
        detail = await admin("commit", query={"hash":result["commit"]})
        self.assertEqual(detail["author"], self.a.author.key)
        self.assertEqual((await self.store.read(self.b, "old/a.md"))["content"], "another space")
        await write("old/a.md", "different lifetime", 2)
        await admin("move", body={"from":"new/a.md","to":"end.md"})
        await self.store.forget(self.a, "end.md", expected_revision=1)
        self.assertEqual((await self.store.read(self.a, "old/a.md"))["content"], "different lifetime")
        self.assertEqual(await self.store.resolve_note_receipts(await self.store._scope(self.a, "memory.read"),token), [])
        for path in ("old/a.md", "new/a.md", "end.md"):
            with self.assertRaises(LookupError):
                await self.store.read(self.a, path, revision=1)
        self.assertEqual((await self.store.search(self.a, query="privateword"))["hits"], [])

    async def test_retained_projection_keeps_protected_bytes_out_of_host_mirrors(self):
        await self.store.write(self.a, "policy.md","private cobalt",expected_revision=0)
        async def read(request,context,runtime):
            return (await self.store.read(context,"policy.md"))["content"]
        ident=uuid.uuid4().hex
        with patch.object(self.runtime.services.executor,"execute",read):
            await self.runtime.submit(RunRequest(ident,self.a.session_id,ident,"read"),self.a)
            self.assertEqual((await self.runtime.wait(ident,self.a)).output,"private cobalt")
        for expired in (False,True):
            if expired:
                await self.store.forget(self.a,"policy.md",expected_revision=1)
            record,page=await self.runtime.retained_run_projection(ident,self.a,after=0,limit=2)
            self.assertEqual(record.status,"success")
            self.assertIsNone(record.output)
            self.assertTrue(page.events)
            self.assertTrue(all(e.kind=="run.protected" and not e.payload for e in page.events))
            with self.assertRaises(PermissionError):
                await self.runtime.retained_run_projection(ident,self.b)
        independent=self.context("other")
        ident=uuid.uuid4().hex
        async def ordinary(request,context,runtime): return "safe reply"
        with patch.object(self.runtime.services.executor,"execute",ordinary):
            await self.runtime.submit(RunRequest(ident,independent.session_id,ident,"hello"),independent)
            await self.runtime.wait(ident,independent)
        record,page=await self.runtime.retained_run_projection(ident,independent,after=0)
        self.assertEqual(record.output,"safe reply")
        self.assertNotIn("run.protected",{e.kind for e in page.events})

    async def test_editor_validation_and_raw_material_scope(self):
        admin = self.administration(self.a, validate_on_write=True)
        long_note = "line\n" * 500
        refusal = await admin("write", path="concepts/alpha.md", body={"content": long_note})
        self.assertTrue(refusal["blocked"])
        self.assertEqual((await self.store.list_notes(self.a))["notes"], [])
        await admin("write", path="sources/raw.md", body={"content": long_note})
        self.assertEqual((await self.store.read(self.a,"sources/raw.md"))["content"], long_note)
        repaired = await admin("write", path="concepts/alpha.md", body={"content": "[[ sources/raw ]]"})
        self.assertTrue(repaired["applied"])
        self.assertIn("[[sources/raw]]", (await self.store.read(self.a,"concepts/alpha.md"))["content"])

    async def test_competing_moves_serialize_and_preserve_ownership(self):
        admin = self.administration(self.a)
        await self.store.write(self.a, "source.md", "sample", expected_revision=0)
        responses = await asyncio.gather(*[admin("move", body={"from": "source.md", "to": target})
                                           for target in ("first.md", "second.md")])
        self.assertEqual(sum(response.get("notes_moved",0) for response in responses), 1)
        self.assertEqual(sum("error" in response for response in responses), 1)
        self.assertEqual(len((await self.store.list_notes(self.a))["notes"]), 1)
        other = self.context("different")
        self.assertEqual((await self.store.list_notes(other))["notes"], [])

    async def test_maintenance_scaffold_repair_generated_outputs_and_forget(self):
        admin = self.administration(self.a)
        created = await admin("init")
        self.assertIn("concepts/", created["created"])
        self.assertIn("workspace/_templates/note.md", created["created"])
        self.assertEqual((await admin("init"))["created"], [])
        content = "---\ntitle: A\nsummary: secretword\n---\n[[ b ]]"
        await self.store.write(self.a, "a.md", content, expected_revision=0)
        dry = await admin("doctor")
        self.assertIn("a.md", {r["path"] for r in dry["fix"]["fixed"]})
        self.assertEqual((await self.store.read(self.a,"a.md"))["revision"],1)
        repaired = await admin("doctor",query={"apply":True})
        self.assertIn("a.md", {r["path"] for r in repaired["fix"]["fixed"]})
        self.assertIsNotNone(repaired["commit"])
        generated = await admin("derived")
        self.assertIsNotNone(generated["commit"])
        self.assertIn("secretword", (await admin("read", path="llms.txt"))["content"])
        self.assertIsNone((await admin("derived"))["commit"])
        await self.store.forget(self.a, "a.md", expected_revision=2)
        for path in ("llms.txt", "_showcase/showcase.md"):
            with self.assertRaises(LookupError):
                await admin("read", path=path)
        self.assertIsNotNone((await admin("derived"))["commit"])
        self.assertNotIn("secretword", (await admin("read", path="llms.txt"))["content"])
        self.assertEqual((await self.store.search(self.a, query="secretword"))["hits"], [])

    async def test_sync_heals_index_and_rejected_move_rolls_back_every_note(self):
        from openagent_core.memory.vault.documents import links
        admin = self.administration(self.a)
        await self.store.write(self.a, "a.md", "old", expected_revision=0)
        await self.store.write(self.a, "ref.md", "[[a]]", expected_revision=0)
        scope = await self.store._scope(self.a, "memory.read")
        async with self.engine.begin() as con:
            await con.execute(links.delete().filter_by(**scope))
        self.assertEqual((await self.store.backlinks(self.a, "a.md"))["paths"], [])
        res = await admin("index/sync", query={"force":True})
        self.assertEqual((res["updated"],res["links"],res["total"]), (2,1,2))
        self.assertEqual((await self.store.backlinks(self.a, "a.md"))["paths"], ["ref.md"])
        original = self.policy.authorize
        calls = 0
        async def changing(ctx, action, resource, *, audience=()):
            nonlocal calls
            if action == "vault.write":
                calls += 1
                return calls == 1
            return await original(ctx,action,resource,audience=audience)
        with patch.object(self.policy, "authorize", changing), self.assertRaises(PermissionError):
            await admin("move", body={"from":"a.md","to":"b.md"})
        self.assertEqual((await self.store.read(self.a,"ref.md"))["content"], "[[a]]")
        with self.assertRaises(LookupError):
            await self.store.read(self.a, "b.md")
        with self.assertRaisesRegex(ValueError,"budget"):
            await self.administration(self.a,max_maintenance_bytes=1)("derived")
        for value in ("../bad", "/bad", "a//b"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                await admin("move",body={"from":"a.md","to":value})

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
