"""Return an admitted user's immutable input while document publication is guarded."""
import asyncio
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings, ServiceRegistry
from openagent_core.artifacts import ArtifactRepository
from openagent_core.engine import AgentExecutor, MemoryDB
from openagent_core.mcp.servers.attachments.adapters import build_runtime_toolkit
from openagent_core.mcp.servers.tool_search.adapters import _invoke_registered_tool
from openagent_core.memory.artifacts import link_attachments_to_latest_message, public_attachment_ref
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.runtime import execution_scope
from openagent_storage_sqlite import SqliteRuntimeStore


class DocumentSessionAttachments(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = MemoryDB(str(self.root / "runs.sqlite"))
        await self.db.connect()
        self.addAsyncCleanup(self.db.close)
        self.store = SqliteRuntimeStore(self.db.db_path)
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        engine = create_async_engine("sqlite+aiosqlite:///" + str(self.root / "notes.sqlite"))
        self.addAsyncCleanup(engine.dispose)
        self.actor = PrincipalRef("host", "tenant", "author")
        self.context = ExecutionContext(self.actor, self.actor, self.actor, "thread", "agent", (self.actor,))
        self.denied = set()

        async def authorize(context, action, resource, *, audience=()):
            return (context.tenant_id == resource.tenant_id and (action, resource.resource_id) not in self.denied
                    and (action != "run.publish" or audience == context.audience))

        async def space(ctx):
            return ctx.initiator.subject_id

        policy = SimpleNamespace(authorize=authorize)
        self.vault = SqlDocumentVault(engine, policy, space)
        await self.vault.initialize()
        await self.vault.write(self.context, "note.md", "fixture note", expected_revision=0)
        self.agent = SimpleNamespace(memory_db=self.db)
        self.executor = AgentExecutor(self.agent)
        self.runtime = Runtime(RuntimeSettings("agent", self.root), RuntimeServices(self.store, self.executor,
            policy, registry=ServiceRegistry({SqlDocumentVault: self.vault})))
        self.repository = ArtifactRepository(self.db, policy, runtime=self.runtime)
        self.toolkit = build_runtime_toolkit()
        self.pool = SimpleNamespace(toolkit_by_name=lambda name: self.toolkit if name == "attachments" else None)
        await self.store.accept(RunRequest("seed", "thread", "seed", "Open conversation"), self.context)
        self.content = b"original fixture attachment"
        self.original = await self.repository.upload(self.context, self.content, filename="original.txt", session_id="thread")

    async def call(self, name, **arguments):
        return await _invoke_registered_tool(self.pool, "attachments", name, arguments)

    async def admit(self, identifier, context=None, attachments=()):
        context = context or self.context
        request = RunRequest(identifier, context.session_id, identifier, "Return the original",
                             attachments=tuple(public_attachment_ref(item) for item in attachments))
        await self.store.accept(request, context)
        await self.store.transition(identifier, "running")
        return request

    async def page_access(self):
        from importlib.resources import files
        from openagent_core.memory.legacy_document_history import SessionProjectionGuard
        from openagent_core.memory.operational.access import AccessContext
        from openagent_core.workspace_administration import SessionInspection

        conn = await self.db._ensure_connected()
        await conn.executescript(files("openagent_core.memory.operational.sql").joinpath("custom_views_v1.sql").read_text())
        inspector = SessionInspection(self.db, self.runtime.services.authorizer, runtime=self.runtime)
        guard = SessionProjectionGuard(lambda sid: inspector.authorize(self.context, sid, connection=conn))
        key = self.actor.key
        return conn, AccessContext("tenant", key, "user", "author", "", frozenset({key}), frozenset(), guard)

    async def test_paginated_native_attachments_select_only_the_delivered_files(self):
        from openagent_core.memory.message_parts import canonical_parts_for_messages_on_connection

        conn, access = await self.page_access()
        other = replace(self.context, session_id="elsewhere")
        await self.admit("other", other)
        unrelated = await self.repository.upload(other, b"elsewhere", filename="other.txt", session_id=other.session_id)
        preview = await self.repository.upload(self.context, b"preview", filename="preview.txt", session_id="thread")
        original = dict(self.original, filename="untrusted.txt", path="/untrusted", url="untrusted")
        await self.admit("selected", attachments=(original, unrelated))
        parts = [{"kind": "text", "text": "Before ", "private": "untrusted"},
                 *[{"kind": "attachment", "attachment": item} for item in (preview, unrelated, original, original)],
                 {"kind": "text", "text": "after."}]
        answer = "Before after."
        await self.store.append_event("selected", "run.stream", {"kind": "done", "text": answer, "parts": parts,
            "final_answer": {"text": answer, "attachments": [original, unrelated]}})
        await self.store.transition("selected", "success", output=answer)
        user, assistant = [row[0] for row in await (await conn.execute(
            "SELECT id FROM session_messages WHERE run_id='selected' ORDER BY ordinal")).fetchall()]

        async def read(viewer=access):
            return await canonical_parts_for_messages_on_connection(conn, [user, assistant], access=viewer)

        page = await read()
        attachment, = [p for p in page[user] if p["kind"] == "attachment"]
        self.assertEqual(attachment["attachment"]["filename"], "original.txt")
        self.assertEqual(attachment["attachment"]["artifact_id"], original["artifact_id"])
        self.assertEqual(page[assistant], [{"kind": "text", "text": "Before "}, attachment,
                                          {"kind": "text", "text": "after."}])
        self.assertNotIn("untrusted", json.dumps(page))
        self.assertEqual(await read(replace(access, principal_id="peer", principal_ids=frozenset({"peer"}))), {})
        self.assertEqual(await read(replace(access, tenant_id="another-tenant")), {})

        # The same content later acquires a protected source. Its independent
        # authorized link cannot publish the name when that source is revoked.
        source = await self.admit("source", other)
        with execution_scope(self.runtime, other, source.run_id):
            await self.vault.read(other, "note.md")
            shared = await self.repository.upload(other, self.content, filename="shared.txt", session_id=other.session_id)
        self.assertEqual(shared["artifact_id"], original["artifact_id"])
        self.assertIn(attachment, (await read())[assistant])
        self.denied.add(("session.read", other.session_id))
        self.assertNotIn(original["artifact_id"], json.dumps(await read()))
        with self.assertRaises(PermissionError):
            await access.projection.recheck()
        self.denied.clear()
        await conn.execute("UPDATE session_messages SET source_version=source_version+1 WHERE id=?", (assistant,))
        self.assertNotIn(assistant, await read())
        await conn.execute("UPDATE session_messages SET author_principal_id='other' WHERE id=?", (user,))
        self.assertNotIn(user, await read())

    async def test_paged_legacy_terminal_frames_batch_files_without_changing_history(self):
        from openagent_core.memory.message_parts import canonical_parts_for_messages_on_connection

        conn, access = await self.page_access()
        messages, expected = [], []
        for i in range(6):
            rid = f"old-{i}"
            original = await self.repository.upload(self.context, bytes([i]), filename=f"{i}.txt", session_id="thread")
            if i == 5:
                await self.repository.upload(self.context, bytes([i]), filename="another-name.txt", session_id="thread")
            await self.admit(rid)
            id_only = {"artifact_id": original["artifact_id"]}
            await self.store.append_event(rid, "run.stream", {"kind": "done", "text": "Delivered",
                                          "attachments": [id_only, id_only]})
            await self.store.transition(rid, "success", output="Delivered")
            row = await (await conn.execute("SELECT id FROM session_messages WHERE run_id=? AND role='assistant'", (rid,))).fetchone()
            messages.append(row[0])
            expected.append(original["artifact_id"])

        async def trace(ids):
            statements = []
            await conn.set_trace_callback(statements.append)
            try:
                hydrated = await canonical_parts_for_messages_on_connection(conn, ids, access=access)
            finally:
                await conn.set_trace_callback(None)
            return hydrated, len(statements)

        unchanged = conn.total_changes
        one, single_count = await trace(messages[:1])
        whole, page_count = await trace(messages)
        self.assertGreater(single_count, 0)
        self.assertEqual(page_count, single_count)
        self.assertEqual(one[messages[0]], whole[messages[0]])
        self.assertEqual([[p["attachment"]["artifact_id"] for p in whole[mid] if p["kind"] == "attachment"]
                          for mid in messages], [[aid] for aid in expected])
        self.assertEqual(whole[messages[0]][1]["attachment"]["filename"], "0.txt")
        self.assertEqual(whole[messages[-1]][1]["attachment"]["filename"], expected[-1])
        self.assertEqual(conn.total_changes, unchanged)

        await conn.execute("DELETE FROM artifact_links WHERE resource_type='session' AND artifact_id=?", (expected[0],))
        await conn.execute("UPDATE artifacts SET deleted_at_ms=created_at_ms WHERE id=?", (expected[1],))
        await conn.execute(
            "INSERT INTO domain_events(event_id,tenant_id,resource_type,resource_id,session_id,run_id,"
            "event_type,occurred_at_ms,schema_version,metadata_json,sensitivity) "
            "VALUES('withheld','tenant','run','old-2','thread','old-2','run.stream',1,1,?,'secret')",
            (json.dumps({"kind": "done", "text": "Delivered", "attachments": [self.original]}),),
        )
        remaining, _ = await trace(messages)
        for aid in expected[:3]:
            self.assertNotIn(aid, json.dumps(remaining))

    async def test_bad_or_oversized_historical_source_does_not_hide_its_neighbor(self):
        from openagent_core.memory.message_parts import MAX_TEXT_PART_BYTES, canonical_parts_for_messages_on_connection

        conn, access = await self.page_access()
        ref = {"artifact_id": self.original["artifact_id"]}
        large = "x" * (MAX_TEXT_PART_BYTES + 1)
        for rid in ("healthy", "string", "large-input", "bad-text", "large-frame"):
            await self.admit(rid, attachments=(ref,))
            frame = {"kind": "done", "text": "Delivered", "attachments": [ref]}
            if rid == "bad-text":
                frame["parts"] = [{"kind": "text"}, {"kind": "attachment", "attachment": ref}]
            if rid == "large-frame":
                frame["unused"] = large
            await self.store.append_event(rid, "run.stream", frame)
            await self.store.transition(rid, "success", output="Delivered")
        await conn.execute("UPDATE session_runs SET metadata_json=json_set(metadata_json,'$.attachments',?) WHERE id='string'",
                           ("malformed historical list",))
        await conn.execute("UPDATE session_runs SET metadata_json=json_set(metadata_json,'$.attachments[0].unused',?) "
                           "WHERE id='large-input'", (large,))
        rows = await (await conn.execute(
            "SELECT id,run_id,role FROM session_messages WHERE run_id!='seed' ORDER BY sequence")).fetchall()
        page = await canonical_parts_for_messages_on_connection(conn, [r["id"] for r in rows], access=access)
        denied = {("string", "user"), ("large-input", "user"), ("bad-text", "assistant"), ("large-frame", "assistant")}
        for row in rows:
            with self.subTest(run=row["run_id"], role=row["role"]):
                files = [part["attachment"]["artifact_id"] for part in page.get(row["id"], []) if part["kind"] == "attachment"]
                self.assertEqual(files, [] if (row["run_id"], row["role"]) in denied else [ref["artifact_id"]])

    async def test_first_turn_returns_original_bytes_and_historical_turn_rediscovers_them(self):
        ctx = self.context
        original = self.original
        request = await self.admit("first", attachments=(original,))
        conn = await self.db._ensure_connected()
        self.assertEqual((await (await conn.execute(
            "SELECT count(*) FROM artifact_links WHERE resource_type='message'")).fetchone())[0], 0)
        # A host publisher can create a session input link with exactly the
        # same schema, even though this second object was never a human input.
        generated = await self.repository.upload(ctx, b"generated content", filename="generated.txt", session_id="thread")

        async def stream(*_args, **_kwargs):
            await self.vault.read(ctx, "note.md")
            files = await self.call("find_session_attachments")
            self.assertEqual([row["filename"] for row in files["attachments"]], ["original.txt"])
            self.assertFalse((await self.call("resend_session_attachment", artifact_id=generated["artifact_id"]))["ok"])
            with self.assertRaisesRegex(RuntimeError, "cannot retain document-derived content"):
                await self.call("send_file_to_user", path="/fixture/generated.txt")
            reply = await self.call("resend_session_attachment", artifact_id=original["artifact_id"])
            self.assertTrue(reply["ok"])
            text = "The original. " + reply["marker"]
            yield {"kind": "done", "text": text, "final_answer": {"text": text}}

        self.agent.run_stream = stream
        with execution_scope(self.runtime, ctx, request.run_id):
            await self.executor.execute(request, ctx, self.runtime)
        events = await self.store.events(request.run_id, 0)
        done = next(event.payload for event in events if event.kind == "run.stream" and event.payload.get("kind") == "done")
        ref, = done["final_answer"]["attachments"]
        self.assertEqual(ref["artifact_id"], original["artifact_id"])
        self.assertEqual(ref["artifact_link_id"], original["artifact_link_id"])
        self.assertEqual(ref["filename"], "original.txt")
        self.assertEqual(ref["sha256"], hashlib.sha256(self.content).hexdigest())
        self.assertEqual((await self.repository.read(ctx, artifact_id=ref["artifact_id"])).content, self.content)
        self.assertNotIn("path", ref)
        self.assertNotIn("fixture note", json.dumps(done))
        # HTTP admissions persist the user message and accepted references
        # together, without necessarily creating a per-message artifact link.
        self.assertEqual((await (await conn.execute(
            "SELECT count(*) FROM artifact_links WHERE resource_type='message' AND artifact_id=?",
            (original["artifact_id"],))).fetchone())[0], 0)
        await self.store.transition(request.run_id, "success", output="Delivered")

        # Previously materialized message inputs survive a historical turn
        # whose accepted request did not record the attachment array.
        old = await self.admit("older")
        historical = await self.repository.upload(ctx, b"other original", filename="archive.txt", session_id="thread")
        await link_attachments_to_latest_message(self.db, "thread", [historical], role="user",
                                                 admitted_run=(ctx.tenant_id, old.run_id))
        following = await self.admit("following")
        with execution_scope(self.runtime, ctx, following.run_id):
            await self.vault.read(ctx, "note.md")
            previous = await self.call("resend_session_attachment", artifact_id=original["artifact_id"])
            self.assertTrue(previous["ok"])
            self.assertEqual(previous["attachment"]["artifact_link_id"], original["artifact_link_id"])
            files = await self.call("find_session_attachments", query="ARCHIVE")
            self.assertEqual([row["artifact_id"] for row in files["attachments"]], [historical["artifact_id"]])
        await self.vault.forget(ctx, "note.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await self.runtime.events_page(request.run_id, 0, ctx)

    async def test_deduplicated_input_keeps_other_protected_source_grant(self):
        ctx = self.context
        other = replace(ctx, session_id="independent")
        prior = await self.admit("source", other)
        shared = b"same bytes from two uploads"
        with execution_scope(self.runtime, other, prior.run_id):
            await self.vault.read(other, "note.md")
            source = await self.repository.upload(other, shared, filename="first.txt", session_id=other.session_id)
        original = await self.repository.upload(ctx, shared, filename="local.txt", session_id=ctx.session_id)
        self.assertEqual(source["artifact_id"], original["artifact_id"])
        present = await self.admit("present", attachments=(original,))
        with execution_scope(self.runtime, ctx, present.run_id):
            await self.vault.read(ctx, "note.md")
            self.assertTrue((await self.call("resend_session_attachment", artifact_id=original["artifact_id"]))["ok"])
            self.denied.add(("session.read", other.session_id))
            self.assertEqual((await self.call("find_session_attachments"))["attachments"], [])
            self.assertFalse((await self.call("resend_session_attachment", artifact_id=original["artifact_id"]))["ok"])

    async def test_historical_input_requires_its_author_message_and_live_delegation(self):
        from openagent_core.contracts import DelegationService

        ctx = self.context
        delegated = replace(ctx, delegation_id="original-grant")
        grants = SimpleNamespace(validate=AsyncMock(return_value=True))
        self.runtime.services.registry.bind(DelegationService, grants)
        await self.admit("source", delegated, (self.original,))
        current = await self.admit("current")
        future = await self.repository.upload(ctx, b"future", filename="future.txt", session_id=ctx.session_id)
        await self.admit("concurrent-newer", attachments=(future,))
        with execution_scope(self.runtime, ctx, current.run_id):
            await self.vault.read(ctx, "note.md")
            files = await self.call("find_session_attachments")
            self.assertEqual([item["artifact_id"] for item in files["attachments"]], [self.original["artifact_id"]])
            grants.validate.return_value = False
            self.assertFalse((await self.call("resend_session_attachment", artifact_id=self.original["artifact_id"]))["ok"])
            grants.validate.return_value = True
            # A previously accepted upload needs the matching visible human message.
            conn = await self.db._ensure_connected()
            await conn.execute("UPDATE session_messages SET author_principal_id=? WHERE run_id='source' AND role='user'",
                               (replace(self.actor, subject_id="other").key,))
            await conn.commit()
            self.assertFalse((await self.call("resend_session_attachment", artifact_id=self.original["artifact_id"]))["ok"])

    async def test_id_only_admission_uses_the_existing_session_input_name(self):
        ctx = self.context
        ident = self.original["artifact_id"]
        minimal = {"artifact_id": ident}
        current = await self.admit("id-only", attachments=(minimal,))
        # A later upload of identical bytes cannot name the earlier input.
        duplicate = await self.repository.upload(ctx, self.content, filename="later.txt", session_id=ctx.session_id)
        # Exercise this ordering even on a filesystem with coarse timestamps.
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE artifact_links SET created_at_ms=(SELECT created_at_ms+1 FROM session_runs WHERE id=?) WHERE id=?",
                           (current.run_id, duplicate["artifact_link_id"]))
        await conn.commit()
        with execution_scope(self.runtime, ctx, current.run_id):
            await self.vault.read(ctx, "note.md")
            listing = await self.call("find_session_attachments", query="original.txt")
            self.assertEqual([file["artifact_id"] for file in listing["attachments"]], [ident])
            resend = await self.call("resend_session_attachment", artifact_id=ident)
            self.assertEqual(resend["attachment"]["filename"], "original.txt")
            self.assertEqual(resend["attachment"]["artifact_link_id"], self.original["artifact_link_id"])
            self.assertEqual((await self.call("find_session_attachments", query="later.txt"))["attachments"], [])
        following = await self.admit("later")
        with execution_scope(self.runtime, ctx, following.run_id):
            await self.vault.read(ctx, "note.md")
            previous = await self.call("find_session_attachments", query="original.txt")
            self.assertEqual([file["artifact_id"] for file in previous["attachments"]], [ident])
        # Two earlier spellings for a new id-only input are ambiguous. The
        # accepted identifier can still be delivered without guessing a name.
        ambiguous = await self.repository.upload(ctx, b"duplicate", filename="left.txt", session_id=ctx.session_id)
        await self.repository.upload(ctx, b"duplicate", filename="right.txt", session_id=ctx.session_id)
        duplicate_id = ambiguous["artifact_id"]
        later = await self.admit("ambiguous", attachments=({"artifact_id": duplicate_id},))
        with execution_scope(self.runtime, ctx, later.run_id):
            await self.vault.read(ctx, "note.md")
            returned = await self.call("resend_session_attachment", artifact_id=duplicate_id)
            self.assertEqual(returned["attachment"]["filename"], duplicate_id)
            for name in ("left.txt", "right.txt"):
                self.assertEqual((await self.call("find_session_attachments", query=name))["attachments"], [])

    async def test_changed_audience_revoked_note_and_other_session(self):
        ctx = self.context
        original = self.original
        first = await self.admit("first", attachments=(original,))
        other = replace(ctx, session_id="other")
        second = await self.admit("concurrent", other)
        marker_ready, release = asyncio.Event(), asyncio.Event()

        async def stream(*_args, **_kwargs):
            await self.vault.read(ctx, "note.md")
            reply = await self.call("resend_session_attachment", artifact_id=original["artifact_id"])
            self.assertTrue(reply["ok"])
            marker_ready.set()
            await release.wait()
            yield {"kind": "done", "text": reply["marker"], "final_answer": {"text": reply["marker"]}}

        self.agent.run_stream = stream

        async def execute():
            with execution_scope(self.runtime, ctx, first.run_id):
                return await self.executor.execute(first, ctx, self.runtime)

        async with asyncio.timeout(5):
            task = asyncio.create_task(execute())
            try:
                await marker_ready.wait()
                with execution_scope(self.runtime, other, second.run_id):
                    await self.vault.read(other, "note.md")
                    self.assertFalse((await self.call("resend_session_attachment", artifact_id=original["artifact_id"]))["ok"])
                    self.assertEqual((await self.call("find_session_attachments"))["attachments"], [])
                self.denied.add(("run.publish", ctx.session_id))
                release.set()
                with self.assertRaises(PermissionError):
                    await task
            finally:
                release.set()
                await asyncio.gather(task, return_exceptions=True)
        self.assertFalse(any(event.kind == "run.stream" and event.payload.get("kind") == "done"
                             for event in await self.store.events(first.run_id, 0)))
        self.denied.clear()
        recipient = PrincipalRef("host", ctx.tenant_id, "new-recipient")
        widened = replace(ctx, audience=(ctx.initiator, recipient))
        run = await self.admit("widened", widened)
        with execution_scope(self.runtime, widened, run.run_id):
            # Reading this session's protected history itself rejects the new audience.
            with self.assertRaisesRegex(RuntimeError, "audience"):
                await self.call("find_session_attachments")
        await self.vault.forget(ctx, "note.md", expected_revision=1)
        cold = await self.admit("after-forget")
        with execution_scope(self.runtime, ctx, cold.run_id):
            with self.assertRaises(RuntimeError):
                await self.call("resend_session_attachment", artifact_id=original["artifact_id"])

    async def test_lookup_limits_exact_resolution_and_direct_path_fences(self):
        from openagent_core.content_privacy import ToolResultPrivacyPublisher
        from openagent_core.memory.artifact_document_history import visible

        ctx = self.context
        files = [await self.repository.upload(ctx, bytes([i]), filename=f"input-{i}.txt", session_id=ctx.session_id)
                 for i in range(24)]
        request = await self.admit("list", attachments=tuple(files))
        with execution_scope(self.runtime, ctx, request.run_id):
            await self.vault.read(ctx, "note.md")
            with patch("openagent_core.memory.artifact_document_history.visible", wraps=visible) as check:
                result = await self.call("find_session_attachments", query="input-")
                self.assertTrue(result["truncated"])
                self.assertEqual(len(result["attachments"]), 20)
                self.assertEqual(check.await_count, 21)
                check.reset_mock()
                original = await self.call("resend_session_attachment", artifact_id=files[0]["artifact_id"])
                self.assertTrue(original["ok"])
                self.assertEqual(check.await_count, 1)
            empty_host = SimpleNamespace(memory_db=None)
            with patch.object(self.executor, "agent", empty_host):
                self.assertFalse((await self.call("resend_session_attachment", artifact_id=files[0]["artifact_id"]))["ok"])
            # A directly written final marker cannot turn a new sandbox path
            # into an input. Neither a host read nor a new artifact is needed.
            publisher = SimpleNamespace(publish=AsyncMock(side_effect=AssertionError("unexpected host read")))
            self.runtime.services.registry.bind("attachments.publisher", publisher)
            async def stream(*_args, **_kwargs):
                text = "Existing and new " + original["marker"] + " [FILE:/fixture/new.txt]"
                yield {"kind": "done", "text": text, "final_answer": {"text": text}}
            self.agent.run_stream = stream
            await self.executor.execute(request, ctx, self.runtime)
            publisher.publish.assert_not_awaited()
            done = next(event.payload for event in await self.store.events(request.run_id, 0)
                        if event.kind == "run.stream" and event.payload.get("kind") == "done")
            self.assertEqual([part["artifact_id"] for part in done["attachments"]], [files[0]["artifact_id"]])
            # Independent connector privacy still fences both metadata and delivery.
            self.runtime.services.registry.bind(ToolResultPrivacyPublisher,
                SimpleNamespace(origins_for_execution=AsyncMock(return_value=("protected origin",))))
            with self.assertRaisesRegex(RuntimeError, "attested writer"):
                await self.call("find_session_attachments")
            with self.assertRaisesRegex(RuntimeError, "attested writer"):
                await self.call("resend_session_attachment", artifact_id=files[0]["artifact_id"])
