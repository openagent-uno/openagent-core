"""Two-mission maintenance with genuine canonical runs, replay and per-user grants."""

import asyncio
import os
import tempfile
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from aiohttp.test_utils import TestClient, TestServer
from openagent_core import (
    ExecutionContext,
    ModuleCatalog,
    ModuleConfig,
    PrincipalRef,
    Runtime,
    RuntimeProfile,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_core.capabilities import (
    CapabilityUnavailable,
    FunctionSource,
    ToolDefinition,
)
from openagent_core.contracts import RunRequest, SessionRef, canonical_json
from openagent_core.dream_evidence import REVIEW_TOOL, DocumentDreamEvidence
from openagent_core.engine import AgentExecutor
from openagent_core.memory.vault.document_dream import (
    DOCUMENT_DREAM_REVIEW_PROMPT,
    DocumentDreamQueue,
    DocumentDreamWorker,
)
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.modules import ServiceRegistry
from openagent_core.runtime import current_execution_context, current_runtime
from openagent_gateway import create_app
from openagent_module_sessions import descriptor as sessions
from openagent_module_vault import descriptor as vault_module
from openagent_storage_sqlite import SqliteRuntimeStore
from sqlalchemy.ext.asyncio import create_async_engine


class Policy:
    grant = True
    blocked_source = None

    async def authorize(self, context, action, resource, *, audience=()):
        return resource.tenant_id == context.tenant_id and resource.resource_id != self.blocked_source

    async def validate(self, context):
        return self.grant and context.delegation_id == "review-grant"


class Agent:
    def __init__(self):
        self.reviewed = asyncio.Event()
        self.finish = asyncio.Event()

    async def run_stream(self, prompt, **kwargs):
        if prompt == "simulate failed job":
            raise ValueError("private provider diagnostic")
        answer = prompt
        if prompt == DOCUMENT_DREAM_REVIEW_PROMPT:
            ctx, rt = current_execution_context(), current_runtime()
            tools = {t.name: t.tool_ref for t in await rt.capabilities.discover(ctx)}
            assert tools and all(name.startswith("vault_") for name in tools)

            async def call(name, **args):
                return await rt.capabilities.call_tool(tools[name], args, ctx)

            note = await call("vault_read_note", path="guide.md")
            await call(
                "vault_write_note",
                path="guide.md",
                expected_revision=note["revision"],
                content=note["content"] + "\n[[related]]",
            )
            self.review_ref = tools[REVIEW_TOOL]
            # Both model calls reach the live catalog concurrently. The SQL
            # receipt allows only one of them to spend the pass's read budget.
            outcomes = await asyncio.gather(call(REVIEW_TOOL), call(REVIEW_TOOL), return_exceptions=True)
            assert sum(isinstance(res, PermissionError) for res in outcomes) == 1, outcomes
            self.sample = next(res for res in outcomes if isinstance(res, dict))
            with self.case.assertRaises(PermissionError):
                await call("vault_write_note", path="copied.md", expected_revision=0, content="copy")
            with self.case.assertRaises(CapabilityUnavailable):
                await rt.capabilities.call_tool(self.operation, {}, ctx)
            self.reviewed.set()
            await self.finish.wait()
            answer = "Proposal: review repeated export jobs on Mondays. Awaiting user approval."
        yield {"kind": "done", "text": answer, "final_answer": {"text": answer}}


class DreamEvidenceTests(unittest.IsolatedAsyncioTestCase):
    backend = "sqlite"

    async def asyncSetUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.agent_id = uuid4().hex
        self.owner = PrincipalRef("local", "tenant-" + self.agent_id, "owner")
        self.other = replace(self.owner, subject_id="participant")
        self.user = ExecutionContext(
            self.owner, self.owner, self.owner, "personal", self.agent_id, (self.owner,)
        )
        self.room = SessionRef("local", self.user.tenant_id, self.agent_id, "report-room")
        self.mapping = (self.room,)
        self.policy = Policy()
        self.agent = Agent()
        self.agent.case = self
        self.engine = create_async_engine(
            os.environ["OPENAGENT_TEST_POSTGRES_URL"]
            if self.backend == "postgres"
            else "sqlite+aiosqlite:///" + str(self.root / "vault.db")
        )
        self.addAsyncCleanup(self.engine.dispose)
        self.queue = DocumentDreamQueue(self.engine, delay_seconds=3600)

        async def space(ctx):
            return ctx.initiator.subject_id

        self.vault = SqlDocumentVault(self.engine, self.policy, space, dream_queue=self.queue)
        await self.vault.initialize()

        async def delegate(claim):
            self.delegated = replace(
                self.user,
                deferred=True,
                delegation_id="review-grant",
                scopes=(claim.context_scope,),
                session_id=claim.session_id,
            )
            return self.delegated

        async def sources(ctx, scope):
            self.assertEqual(dict(scope), {"tenant": ctx.tenant_id, "space": "owner", "agent": ctx.agent_id})
            return self.mapping

        self.worker = DocumentDreamWorker(self.queue, delegate, space, tick_seconds=3600)
        self.evidence = DocumentDreamEvidence(sources)
        self.rt = await self.new_runtime()

    async def new_runtime(self, *, enable=True):
        if self.backend == "postgres":
            from openagent_storage_postgres import PostgresRuntimeStore

            store = PostgresRuntimeStore(
                os.environ["OPENAGENT_TEST_POSTGRES_DSN"],
                agent_id=self.agent_id,
                schema="test_" + self.agent_id,
            )
        else:
            store = SqliteRuntimeStore(self.root / "runtime.db")
        services = {SqlDocumentVault: self.vault, DocumentDreamWorker: self.worker}
        if enable:
            services[DocumentDreamEvidence] = self.evidence
        runtime = Runtime(
            RuntimeSettings(self.agent_id),
            RuntimeServices(
                store,
                AgentExecutor(self.agent),
                self.policy,
                delegations=self.policy,
                registry=ServiceRegistry(services),
            ),
            profile=RuntimeProfile(
                1,
                {
                    "sessions": ModuleConfig(frozenset({"service", "agent_tools"})),
                    "vault": ModuleConfig(
                        frozenset({"service", "agent_tools", "workers"}), {"backend": "documents"}
                    ),
                },
            ),
            module_catalog=ModuleCatalog((vault_module, sessions)),
        )
        await runtime.start()
        self.addAsyncCleanup(runtime.close)

        async def operational_call(args, ctx):
            self.fail("Dream reached an operational tool")

        name = "create_workflow"
        operational = FunctionSource(
            (ToolDefinition(name, "Create a workflow", {"type": "object"}),), {name: operational_call}
        )
        runtime.capabilities.register("operations", operational, operational, target_label="operations")
        self.agent.operation = (await runtime.capabilities.discover(self.user, source_id="operations"))[
            0
        ].tool_ref
        return runtime

    async def turn(self, ctx, prompt):
        run_id = uuid4().hex
        await self.rt.submit(RunRequest(run_id, ctx.session_id, run_id, prompt), ctx)
        return await self.rt.wait(run_id, ctx)

    async def launch(self):
        await self.vault.write(self.user, "guide.md", "Reference.", expected_revision=0)
        await self.queue.request_now(self.vault, self.user, "manual")
        record = await self.worker.run_due_once(self.rt)
        self.assertIsNotNone(record)
        return record.run_id

    async def test_both_missions_budget_and_historical_access_from_new_runtime(self):
        shared = replace(self.user, session_id=self.room.session_id, audience=(self.owner, self.other))
        shared_record = await self.turn(shared, "Weekly export " + "界" * 1200)
        private = ExecutionContext(
            self.other, self.other, self.other, shared.session_id, self.agent_id, (self.other,)
        )
        await self.turn(private, "PRIVATE FROM OTHER TURN")
        await self.turn(replace(shared, audience=(self.owner,)), "simulate failed job")
        run_id = await self.launch()
        await asyncio.wait_for(self.agent.reviewed.wait(), 10)
        self.agent.finish.set()
        self.assertEqual((await self.rt.wait(run_id, self.delegated)).status, "success")
        self.assertLessEqual(len(canonical_json(self.agent.sample).encode()), self.evidence.max_bytes)
        view = canonical_json(self.agent.sample)
        for value in ("Weekly export", "run.failed", "run.success"):
            self.assertIn(value, view)
        for value in ("界" * 257, "PRIVATE FROM OTHER TURN", "private provider diagnostic", "raw_envelope"):
            self.assertNotIn(value, view)
        self.assertEqual((await self.vault.backlinks(self.user, "related.md"))["paths"], ["guide.md"])
        self.assertEqual((await self.vault.list_notes(self.user))["notes"][0]["path"], "guide.md")
        with self.assertRaises(CapabilityUnavailable):
            await self.rt.capabilities.call_tool(self.agent.review_ref, {}, self.user)
        if self.backend == "sqlite":
            await self.rt.close()
        observer = await self.new_runtime()
        viewer = self.user

        @asynccontextmanager
        async def resolve(request, *args):
            yield viewer

        app = create_app(observer, resolve, resolve_personal_context=resolve, manage_lifecycle=False)
        ref = replace(self.room, session_id=self.delegated.session_id)
        async with TestClient(TestServer(app)) as api:

            async def check(status):
                for path in (
                    f"/api/v1/runs/{run_id}",
                    f"/api/v1/runs/{run_id}/events",
                    f"/api/v1/runs/{run_id}/final-answer",
                ):
                    response = await api.get(path)
                    self.assertEqual(response.status, status, path)
                response = await api.get("/api/v1/me/sessions", params={"ref": ref.token})
                self.assertEqual(response.status, status)

            await check(200)
            viewer = replace(private, session_id="personal")
            await check(403)
            viewer = self.user
            self.mapping = ()
            await check(403)
            filtered = await observer.service("sessions.service").search(
                viewer, query="Awaiting user approval"
            )
            self.assertFalse(filtered["sessions"])
            self.mapping = (self.room,)
            await check(200)
            self.policy.grant = False
            await check(403)
            self.policy.grant = True
            self.policy.blocked_source = self.room.session_id
            await check(403)
            self.policy.blocked_source = None
            # Additional unrelated turns keep the original sample valid.
            if self.backend == "postgres":
                await self.turn(shared, "Additional work")
            await check(200)
            # Simulate a source redaction inside this disposable store. Replay
            # must refuse the old completed proposal and historical tool result.
            store = observer.services.store
            if self.backend == "postgres":
                async with store._db() as db:
                    await db.execute(
                        "UPDATE messages SET body=to_jsonb('redacted'::text) "
                        "WHERE agent=%s AND run_id=%s AND role='user'",
                        (self.agent_id, shared_record.run_id),
                    )
            else:
                store.connection.execute(
                    "UPDATE session_messages SET text='redacted' WHERE run_id=? AND role='user'",
                    (shared_record.run_id,),
                )
            await check(403)

    async def test_byte_limit_omits_over_budget_run(self):
        await self.rt.close()
        self.evidence = replace(self.evidence, max_bytes=256)
        self.rt = await self.new_runtime()
        await self.turn(replace(self.user, session_id=self.room.session_id), "漢" * 1000)
        run_id = await self.launch()
        await asyncio.wait_for(self.agent.reviewed.wait(), 10)
        self.agent.finish.set()
        self.assertEqual((await self.rt.wait(run_id, self.delegated)).status, "success")
        self.assertLessEqual(len(canonical_json(self.agent.sample).encode()), 256)
        self.assertTrue(self.agent.sample["truncated"])
        self.assertEqual(self.agent.sample["sessions"][0]["runs"], [])

    async def test_unmapped_scope_uses_only_first_mission_and_private_tool_is_hidden(self):
        from openagent_core.memory.vault.document_dream import DOCUMENT_DREAM_PROMPT

        self.mapping = None
        self.assertNotIn(REVIEW_TOOL, {t.name for t in await self.rt.capabilities.discover(self.user)})
        run_id = await self.launch()
        record = await self.rt.wait(run_id, self.delegated)
        self.assertEqual(record.status, "success")
        admitted = await self.rt.accepted_request(run_id, self.user)
        self.assertEqual(admitted.request.input, DOCUMENT_DREAM_PROMPT)
        self.assertNotIn(REVIEW_TOOL, {t.name for t in await self.rt.capabilities.discover(self.delegated)})
        self.assertIsNone(await self.rt.services.store.dream_evidence_receipt(run_id))

    async def test_revoke_while_reading_no_exposure_or_result(self):
        await self.turn(replace(self.user, session_id=self.room.session_id), "private source material")
        store = self.rt.services.store
        read = store.dream_session_activity

        async def revoke(*args, **kwargs):
            result = await read(*args, **kwargs)
            self.mapping = ()
            return result

        store.dream_session_activity = revoke
        run_id = await self.launch()
        async with asyncio.timeout(10):
            while not (await store.get(run_id)).terminal:
                await asyncio.sleep(0.02)
        self.assertEqual((await store.get(run_id)).status, "failed")
        details = str(await store.events(run_id))
        self.assertNotIn("private source material", details)
        self.assertNotIn("Awaiting user approval", details)

    async def test_revoked_after_review_prevents_final_model_output(self):
        await self.turn(replace(self.user, session_id=self.room.session_id), "authorized first")
        run_id = await self.launch()
        await asyncio.wait_for(self.agent.reviewed.wait(), 10)
        self.policy.grant = False
        self.agent.finish.set()
        store = self.rt.services.store
        async with asyncio.timeout(10):
            while not (await store.get(run_id)).terminal:
                await asyncio.sleep(0.02)
        self.assertEqual((await store.get(run_id)).status, "failed")
        self.assertNotIn("Awaiting user approval", str(await store.events(run_id)))

    async def test_foreign_ref_and_excess_mapping_denied_before_agent(self):
        await self.vault.write(self.user, "guide.md", "Reference.", expected_revision=0)
        for idx, selection in enumerate(
            (
                (replace(self.room, tenant_id="foreign"),),
                (replace(self.room, authority="foreign"),),
                (replace(self.room, agent_id="foreign"),),
                (self.room,) * 9,
            )
        ):
            self.mapping = selection
            await self.queue.request_now(self.vault, self.user, "request-" + str(idx))
            self.assertIsNone(await self.worker.run_due_once(self.rt))
        self.assertFalse(self.agent.reviewed.is_set())


@unittest.skipUnless(
    os.environ.get("OPENAGENT_TEST_POSTGRES_URL") and os.environ.get("OPENAGENT_TEST_POSTGRES_DSN"),
    "No isolated PostgreSQL runtime and vault configured",
)
class PostgresDreamEvidenceTests(DreamEvidenceTests):
    backend = "postgres"

    async def test_native_provider_uses_catalog_and_both_missions(self):
        import ast
        import json

        from aiohttp import web
        from openagent_core.engine import Agent as NativeAgent
        from openagent_core.engine import NativeProvider
        from openagent_core.mcp.pool import MCPPool
        from openagent_storage_postgres import PostgresRuntimeStore

        await self.turn(replace(self.user, session_id=self.room.session_id), "Sample monthly inventory")
        await self.vault.write(self.user, "guide.md", "Inventory.", expected_revision=0)
        seen, refs = [], {}

        def objects(value):
            if isinstance(value, str):
                if value.startswith(("{", "[")):
                    try:
                        yield from objects(json.loads(value))
                    except ValueError:
                        # The native SDK may present dictionary-valued tool
                        # results using Python's literal representation.
                        try:
                            yield from objects(ast.literal_eval(value))
                        except (ValueError, SyntaxError):
                            pass
            elif isinstance(value, dict):
                yield value
                for item in value.values():
                    yield from objects(item)
            elif isinstance(value, list):
                for item in value:
                    yield from objects(item)

        errors = []

        async def provider(request):
            payload = await request.json()
            messages = payload["messages"]
            last = messages[-1]["content"]
            seen.append(last)
            step = len(seen)
            try:
                for item in objects(last):
                    if "tool_ref" in item and "name" in item:
                        refs[item["name"]] = item["tool_ref"]
                if step == 1:
                    self.assertIn("vault_review_activity", str(last))
                    name, args = "tool_search_list_tools", {"source_ref": "vault"}
                elif step == 2:
                    self.assertTrue(all(n.startswith("vault_") for n in refs))
                    name, args = (
                        "tool_search_call_tool",
                        {"tool_ref": refs["vault_read_note"], "args": {"path": "guide.md"}},
                    )
                elif step == 3:
                    self.assertIn("Inventory.", str(last))
                    name, args = (
                        "tool_search_call_tool",
                        {
                            "tool_ref": refs["vault_write_note"],
                            "args": {
                                "path": "guide.md",
                                "expected_revision": 1,
                                "content": "Inventory. See [[checks]].",
                            },
                        },
                    )
                elif step == 4:
                    self.assertIn("guide.md", str(last))
                    name, args = "tool_search_call_tool", {"tool_ref": refs[REVIEW_TOOL], "args": {}}
                else:
                    self.assertEqual(step, 5)
                    self.assertIn("Sample monthly inventory", str(last))
                    name = None
            except (AssertionError, KeyError, TypeError) as exc:
                errors.append(f"Provider step {step} failed: {exc}")
                name = None
            delta = {"role": "assistant"}
            if name:
                delta["tool_calls"] = [
                    {
                        "index": 0,
                        "id": "call-" + str(step),
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(args)},
                    }
                ]
            else:
                delta["content"] = "Propose a monthly inventory workflow for approval. [FILE:/synthetic-report]"
                delta["audio"] = {"id": "test-voice", "data": "YWFh", "transcript": "", "expires_at": 9999999999}

            def frame(data, finish):
                return {
                    "id": "chat-" + str(step),
                    "model": "local-fixture",
                    "created": 1,
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": data, "finish_reason": finish}],
                }

            frames = (frame(delta, None), frame({}, "tool_calls" if name else "stop"))
            return web.Response(
                text="".join("data: " + json.dumps(f) + "\n\n" for f in frames) + "data: [DONE]\n\n",
                content_type="text/event-stream",
            )

        app = web.Application()
        app.router.add_post("/v1/chat/completions", provider)
        async with TestServer(app) as endpoint:
            model = NativeProvider(
                "openai:local-fixture", api_key="test", base_url=str(endpoint.make_url("/v1"))
            )
            agent = NativeAgent(
                name=self.agent_id,
                model=model,
                config={"skills": {"enabled": False}},
                mcp_pool=MCPPool.from_config([{"builtin": "tool-search"}], include_defaults=False),
            )
            executor = AgentExecutor(agent)
            native = Runtime(
                RuntimeSettings(self.agent_id),
                RuntimeServices(
                    PostgresRuntimeStore(
                        os.environ["OPENAGENT_TEST_POSTGRES_DSN"],
                        agent_id=self.agent_id,
                        schema="test_" + self.agent_id,
                    ),
                    executor,
                    self.policy,
                    delegations=self.policy,
                    registry=ServiceRegistry(
                        {
                            SqlDocumentVault: self.vault,
                            DocumentDreamEvidence: self.evidence,
                            DocumentDreamWorker: self.worker,
                        }
                    ),
                ),
                profile=RuntimeProfile(
                    1,
                    {
                        "vault": ModuleConfig(
                            frozenset({"service", "agent_tools", "workers"}), {"backend": "documents"}
                        )
                    },
                ),
                module_catalog=ModuleCatalog((vault_module,)),
                modules=(executor,),
            )
            published = []
            class Publisher:
                async def publish_content(self, context, content, **options):
                    published.append(content)
                    raise AssertionError("Maintenance attempted native media publication")
            native.services.registry.bind("attachments.publisher", Publisher())
            await native.start()
            try:
                await self.queue.request_now(self.vault, self.user, "wire")
                run = await self.worker.run_due_once(native)
                done = await asyncio.wait_for(native.wait(run.run_id, self.delegated), 15)
                self.assertFalse(errors, errors)
                self.assertEqual(done.status, "success", done)
                self.assertEqual(len(seen), 5, str(seen)[-800:])
                self.assertEqual((await self.vault.backlinks(self.user, "checks.md"))["paths"], ["guide.md"])
                final = await native.final_answer(run.run_id, self.user)
                self.assertIn("inventory workflow", str(final.final_answer))
                self.assertNotIn("synthetic-report", str(final.final_answer))
                self.assertFalse(final.final_answer["attachments"])
                self.assertFalse(published)
            finally:
                await native.close()
