"""Canonical UI and model outputs inherit the lifetime of every note used by a turn."""

import asyncio
import os
import unittest
from dataclasses import replace
from unittest.mock import patch
from uuid import uuid4

import test_document_dream_evidence as fixtures
from openagent_core.contracts import RunRequest, canonical_json
from openagent_core.provider_history import previous_model_messages
from openagent_core.runtime import (
    current_execution_context,
    current_run_id,
    current_runtime,
)


class Reader:
    def __init__(self):
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.histories = []

    async def run_stream(self, prompt, **kwargs):
        ctx, runtime = current_execution_context(), current_runtime()
        if prompt in ("read", "read and wait", "delegate"):
            tools = {
                v.name: v.tool_ref for v in await runtime.capabilities.discover(ctx)
            }
            answer = (
                await runtime.capabilities.call_tool(
                    tools["vault_read_note"], {"path": "source.md"}, ctx
                )
            )["content"]
            self.started.set()
            if prompt == "read and wait":
                await self.finish.wait()
            elif prompt == "delegate":
                self.child_id = uuid4().hex
                child = RunRequest(self.child_id, uuid4().hex, self.child_id, answer)
                await runtime.spawn(child, ctx)
        elif prompt == "history":
            messages = await previous_model_messages(ctx.session_id, current_run_id())
            self.histories.append([m.content for m in messages])
            answer = " ".join(self.histories[-1])
        else:
            answer = prompt
        yield {"kind": "done", "text": answer, "final_answer": {"text": answer}}


class DocumentRunLineageTests(unittest.IsolatedAsyncioTestCase):
    backend = "sqlite"
    new_runtime = fixtures.DreamEvidenceTests.new_runtime
    turn = fixtures.DreamEvidenceTests.turn

    async def asyncSetUp(self):
        await fixtures.DreamEvidenceTests.asyncSetUp(self)
        self.reader = Reader()
        self.agent.run_stream = self.reader.run_stream
        self.secret = "synthetic-private-phrase"
        await self.vault.write(self.user, "source.md", self.secret, expected_revision=0)

    async def test_unlinked_sqlite_output_revalidates_its_original_revision(self):
        if self.backend != "sqlite":
            self.skipTest("SQLite filesystem attachment repository")
        from types import SimpleNamespace
        from openagent_core.artifacts import ArtifactRepository

        db = SimpleNamespace(db_path=str(self.rt.services.store.path))
        repository = ArtifactRepository(db, self.policy, runtime=self.rt)
        files = []

        async def stream(prompt, **kwargs):
            ctx = current_execution_context()
            text = (await self.vault.read(ctx, "source.md"))["content"]
            files.append(await repository.upload(ctx, text.encode(), filename="derived.txt"))
            yield {"kind": "done", "text": "Ready"}

        self.agent.run_stream = stream
        self.assertEqual((await self.turn(self.user, "publish")).status, "success")
        artifact = files[0]["artifact_id"]
        independent = await repository.upload(self.user, b"other", filename="independent.txt")
        self.assertEqual((await repository.read(self.user, artifact_id=artifact)).content, self.secret.encode())
        with self.assertRaises(LookupError):
            await ArtifactRepository(db, self.policy).read(self.user, artifact_id=artifact)
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        with self.assertRaises(LookupError):
            await repository.read(self.user, artifact_id=artifact)
        self.assertEqual((await repository.read(self.user, artifact_id=independent["artifact_id"])).content, b"other")

    async def cold_runtime(self):
        if self.backend == "sqlite":
            await self.rt.close()
        self.rt = await self.new_runtime()

    async def test_native_wire_uses_independent_history_after_forget(self):
        import json

        from aiohttp import web
        from aiohttp.test_utils import TestServer
        from openagent_core import (
            ModuleCatalog,
            ModuleConfig,
            Runtime,
            RuntimeProfile,
            RuntimeServices,
            RuntimeSettings,
        )
        from openagent_core.engine import Agent, AgentExecutor, NativeProvider
        from openagent_core.memory.vault.documents import SqlDocumentVault
        from openagent_core.modules import ServiceRegistry
        from openagent_module_vault import descriptor

        await self.turn(self.user, "independent-wire-input")
        await self.turn(self.user, "read")
        await self.rt.close()
        seen = []

        async def provider(request):
            body = await request.json()
            seen.append(body["messages"])
            answer = (
                self.secret
                if self.secret in canonical_json(body["messages"])
                else "independent-answer"
            )
            frames = [
                {
                    "id": "local",
                    "object": "chat.completion.chunk",
                    "created": 1,
                    "model": "fixture",
                    "choices": [{"index": 0, "delta": val, "finish_reason": reason}],
                }
                for val, reason in (
                    ({"role": "assistant", "content": answer}, None),
                    ({}, "stop"),
                )
            ]
            return web.Response(
                text="".join("data: " + json.dumps(f) + "\n\n" for f in frames)
                + "data: [DONE]\n\n",
                content_type="text/event-stream",
            )

        app = web.Application()
        app.router.add_post("/v1/chat/completions", provider)
        async with TestServer(app) as endpoint:
            model = NativeProvider(
                "openai:fixture", api_key="test", base_url=str(endpoint.make_url("/v1"))
            )
            agent = Agent(
                name=self.agent_id, model=model, config={"skills": {"enabled": False}}
            )
            executor = AgentExecutor(agent)
            if self.backend == "sqlite":
                from openagent_storage_sqlite import SqliteRuntimeStore

                store = SqliteRuntimeStore(self.root / "runtime.db")
            else:
                from openagent_storage_postgres import PostgresRuntimeStore

                store = PostgresRuntimeStore(
                    os.environ["OPENAGENT_TEST_POSTGRES_DSN"],
                    agent_id=self.agent_id,
                    schema="test_" + self.agent_id,
                )
            runtime = Runtime(
                RuntimeSettings(self.agent_id, workspace=self.root),
                RuntimeServices(
                    store,
                    executor,
                    self.policy,
                    registry=ServiceRegistry({SqlDocumentVault: self.vault}),
                ),
                profile=RuntimeProfile(
                    1,
                    {
                        "vault": ModuleConfig(
                            frozenset({"service"}), {"backend": "documents"}
                        )
                    },
                ),
                module_catalog=ModuleCatalog((descriptor,)),
                modules=(executor,),
            )
            await runtime.start()
            self.addAsyncCleanup(runtime.close)
            self.rt = runtime
            with patch.object(
                model,
                "_ensured_runtime_db_path",
                side_effect=AssertionError("unexpected local model DB"),
            ):
                before = await self.turn(self.user, "native reader")
                self.assertEqual(before.status, "success")
                self.assertIn(self.secret, canonical_json(seen[-1]))
                await self.vault.forget(self.user, "source.md", expected_revision=1)
                after = await self.turn(self.user, "native reader again")
                self.assertEqual(after.status, "success")
                self.assertEqual(len(seen), 2)
                self.assertNotIn(self.secret, canonical_json(seen[-1]))
                self.assertIn("independent-wire-input", canonical_json(seen[-1]))
                with self.assertRaises(PermissionError):
                    await runtime.final_answer(before.run_id, self.user)

    async def test_forget_during_work_fences_future_model_output(self):
        ident = uuid4().hex
        await self.rt.submit(
            RunRequest(ident, self.user.session_id, ident, "read and wait"), self.user
        )
        await asyncio.wait_for(self.reader.started.wait(), 4)
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        self.reader.finish.set()
        await asyncio.wait_for(self.rt._tasks[ident], 4)
        self.assertEqual((await self.rt.services.store.get(ident)).status, "failed")
        events = await self.rt.services.store.events(ident, 0)
        self.assertFalse(
            any(
                e.kind == "run.stream" and e.payload.get("kind") == "done"
                for e in events
            )
        )
        with self.assertRaises(PermissionError):
            await self.rt.final_answer(ident, self.user)

    async def test_history_continues_after_forget_and_cannot_republish_an_old_copy(
        self,
    ):
        await self.turn(self.user, "independent")
        first = await self.turn(self.user, "read")
        self.assertEqual(first.output, self.secret)
        copy = await self.turn(self.user, "history")
        self.assertIn(self.secret, self.reader.histories[-1])
        self.assertEqual(copy.status, "success")
        self.assertTrue(
            await self.rt.services.store.document_sources(self.user, run_id=copy.run_id)
        )
        await self.cold_runtime()
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        for record in (first, copy):
            for read in (
                self.rt.get_run,
                self.rt.final_answer,
                self.rt.accepted_request,
            ):
                with self.assertRaises(PermissionError):
                    await read(record.run_id, self.user)
            with self.assertRaises(PermissionError):
                await self.rt.events_page(record.run_id, 0, self.user)
        again = await self.turn(self.user, "history")
        self.assertEqual(again.status, "success")
        self.assertIn("independent", self.reader.histories[-1])
        self.assertNotIn(self.secret, canonical_json(self.reader.histories[-1]))
        self.assertEqual(
            (
                await self.rt.service("sessions.service").search(
                    self.user, query=self.secret
                )
            )["sessions"],
            [],
        )

    async def test_failed_forget_rolls_back_and_revoked_scope_or_foreign_user_cannot_replay(
        self,
    ):
        run = await self.turn(self.user, "read")

        async def fault(*args):
            raise RuntimeError("deliberate transaction rollback")

        with patch(
            "openagent_core.memory.vault.document_lineage.forget_derivations", fault
        ), self.assertRaises(RuntimeError):
            await self.vault.forget(self.user, "source.md", expected_revision=1)
        await self.cold_runtime()
        self.assertEqual(
            (await self.rt.final_answer(run.run_id, self.user)).final_answer["text"],
            self.secret,
        )
        foreign = replace(
            self.user,
            authority=self.other,
            initiator=self.other,
            author=self.other,
            audience=(self.other,),
        )
        # Even an overbroad host space mapping must respect the admitted run audience.
        with patch.object(self.vault, "resolve_space", return_value="owner"):
            with self.assertRaises(PermissionError):
                await self.rt.get_run(run.run_id, foreign)
            with self.assertRaises(PermissionError):
                await self.rt.service("sessions.service").detail(
                    foreign, replace(self.room, session_id=self.user.session_id).token
                )
        self.policy.blocked_source = "owner"
        with self.assertRaises(PermissionError):
            await self.rt.final_answer(run.run_id, self.user)

    async def test_dream_report_inherits_the_reviewed_ordinary_run(self):
        await self.turn(replace(self.user, session_id=self.room.session_id), "read")
        self.agent.run_stream = fixtures.Agent.run_stream.__get__(self.agent)
        await self.vault.write(
            self.user, "guide.md", "Synthetic guide.", expected_revision=0
        )
        await self.queue.request_now(self.vault, self.user, "manual")
        run = await self.worker.run_due_once(self.rt)
        await asyncio.wait_for(self.agent.reviewed.wait(), 8)
        self.agent.finish.set()
        self.assertEqual(
            (await self.rt.wait(run.run_id, self.delegated)).status, "success"
        )
        self.assertIn(self.secret, canonical_json(self.agent.sample))
        await self.cold_runtime()
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await self.rt.final_answer(run.run_id, self.user)

    async def test_child_inherits_source_before_its_mission_is_stored(self):
        parent = await self.turn(self.user, "delegate")
        self.assertEqual(parent.status, "success")
        self.assertTrue(
            await self.rt.services.store.document_sources(
                self.user, run_id=self.reader.child_id
            )
        )
        await self.vault.forget(self.user, "source.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await self.rt.accepted_request(self.reader.child_id, self.user)


@unittest.skipUnless(
    os.environ.get("OPENAGENT_TEST_POSTGRES_DSN"), "requires PostgreSQL"
)
class PostgresDocumentRunLineageTests(DocumentRunLineageTests):
    backend = "postgres"
