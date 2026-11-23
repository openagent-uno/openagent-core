"""A real engine turn and catalog operating on an explicitly delegated SQL vault."""

import asyncio
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

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
from openagent_core.engine import AgentExecutor
from openagent_core.memory.vault.document_dream import (
    DOCUMENT_DREAM_PROMPT,
    DocumentDreamQueue,
    DocumentDreamWorker,
    spaces,
)
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.modules import ServiceRegistry
from openagent_core.runtime import current_execution_context, current_runtime
from openagent_module_vault import descriptor
from openagent_storage_sqlite import SqliteRuntimeStore


class GrantPolicy:
    enabled = True

    async def authorize(self, context, action, resource, *, audience=()):
        return (
            self.enabled
            and resource.tenant_id == context.tenant_id
            and all(p == context.initiator for p in audience)
        )

    async def validate(self, context):
        return self.enabled and context.delegation_id == "vault-grant"


class MaintenanceAgent:
    """A deterministic streaming agent at the model boundary."""

    def __init__(self):
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.turns = 0

    async def run_stream(self, prompt, **kwargs):
        assert prompt == DOCUMENT_DREAM_PROMPT
        self.turns += 1
        runtime, context = current_runtime(), current_execution_context()
        tools = {t.name: t.tool_ref for t in await runtime.capabilities.discover(context)}

        async def call(name, **args):
            return await runtime.capabilities.call_tool(tools[name], args, context)

        source = await call("vault_read_note", path="manual.md")
        self.entered.set()
        await self.release.wait()
        await call(
            "vault_write_note",
            path="manual.md",
            expected_revision=source["revision"],
            content=source["content"] + "\n[[device]]",
        )
        yield {
            "kind": "done",
            "text": "Linked manual to device.",
            "final_answer": {"text": "Linked manual to device."},
        }


class DocumentDream(unittest.IsolatedAsyncioTestCase):
    url = None

    async def asyncSetUp(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.engine = create_async_engine(self.url or "sqlite+aiosqlite:///" + str(root / "documents.db"))
        self.addAsyncCleanup(self.engine.dispose)
        self.now = 10.0
        self.agent_id = uuid.uuid4().hex
        self.p = PrincipalRef("authority", "tenant-" + self.agent_id, "person")
        self.user = ExecutionContext(self.p, self.p, self.p, "interactive", self.agent_id, (self.p,))
        self.policy, self.agent = GrantPolicy(), MaintenanceAgent()
        self.queue = DocumentDreamQueue(
            self.engine, delay_seconds=3600, lease_seconds=10, clock=lambda: self.now
        )

        async def space(context):
            return context.initiator.subject_id

        self.vault = SqlDocumentVault(self.engine, self.policy, space, dream_queue=self.queue)
        await self.vault.initialize()
        self.last = None

        async def resolve(claim):
            self.last = replace(
                self.user,
                session_id=claim.session_id,
                scopes=(claim.context_scope,),
                deferred=True,
                delegation_id="vault-grant",
            )
            return self.last

        self.worker = DocumentDreamWorker(self.queue, resolve, space, tick_seconds=3600)
        services = ServiceRegistry({SqlDocumentVault: self.vault, DocumentDreamWorker: self.worker})
        self.runtime = Runtime(
            RuntimeSettings(self.agent_id),
            RuntimeServices(
                SqliteRuntimeStore(root / "runs.db"),
                AgentExecutor(self.agent),
                self.policy,
                delegations=self.policy,
                registry=services,
            ),
            profile=RuntimeProfile(
                1,
                {
                    "vault": ModuleConfig(
                        frozenset({"service", "workers", "agent_tools"}), {"backend": "documents"}
                    )
                },
            ),
            module_catalog=ModuleCatalog((descriptor,)),
        )
        await self.runtime.start()
        self.addAsyncCleanup(self.runtime.close)

    async def test_an_admitted_scheduler_occurrence_uses_one_vault_only_run(self):
        from openagent_core.contracts import RunRequest
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        self.agent.release.set()
        authority = replace(self.user, deferred=True, delegation_id="vault-grant")
        vault, worker = self.vault, self.worker
        class Scheduled:
            async def execute(_, request, context, runtime):
                first = await worker.run_occurrence(runtime, vault, context, "schedule-click")
                retry = await worker.run_occurrence(runtime, vault, context, "schedule-click")
                assert first.run_id == retry.run_id
                return retry.status
        await self.runtime.execute_operation(
            RunRequest("schedule", authority.session_id,"schedule","maintenance"),authority,Scheduled())
        outcome = await asyncio.wait_for(self.runtime.wait("schedule",authority), 8)
        self.assertEqual((outcome.status,outcome.output), ("success","success"))
        self.assertEqual(self.agent.turns, 1)

    async def test_enrollment_does_not_queue_other_spaces_or_replay_withdrawn_requests(self):
        enabled = False

        async def enrolled(con, scope):
            self.assertTrue(con.in_transaction())
            self.assertEqual(scope["space"], self.p.subject_id)
            return enabled

        self.queue.enrolled = enrolled
        await self.vault.write(self.user, "private.md", "Private.", expected_revision=0)
        with self.assertRaises(PermissionError):
            await self.queue.request_now(self.vault, self.user, "same")
        self.now += 4000
        self.assertIsNone(await self.worker.run_due_once(self.runtime))
        enabled = True
        await self.queue.request_now(self.vault, self.user, "same")
        enabled = False
        with self.assertRaises(PermissionError):
            await self.queue.request_now(self.vault, self.user, "same")

    async def test_manual_receipt_and_scheduled_completion(self):
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        self.assertIsNone(await self.worker.run_due_once(self.runtime))
        key = await self.queue.request_now(self.vault, self.user, "click")
        self.assertEqual(key, await self.queue.request_now(self.vault, self.user, "click"))
        record = await self.worker.run_due_once(self.runtime)
        await asyncio.wait_for(self.agent.entered.wait(), 5)
        await self.queue.request_now(self.vault, self.user, "join-active")
        self.now += 11
        self.assertEqual((await self.worker.run_due_once(self.runtime)).run_id, record.run_id)
        self.agent.release.set()
        result = await self.runtime.wait(record.run_id, self.last)
        self.assertEqual(result.status, "success")
        self.assertEqual((await self.vault.backlinks(self.user, "device.md"))["paths"], ["manual.md"])
        self.now += 11
        self.assertEqual((await self.worker.run_due_once(self.runtime)).status, "success")
        self.now += 4000
        self.assertIsNone(await self.worker.run_due_once(self.runtime))
        self.assertEqual(self.agent.turns, 1)

    async def test_foreground_change_invalidates_derived_write(self):
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        await self.vault.write(self.user, "device.md", "source", expected_revision=0)
        self.now += 4000
        record = await self.worker.run_due_once(self.runtime)
        await asyncio.wait_for(self.agent.entered.wait(), 5)
        await self.vault.forget(self.user, "device.md", expected_revision=1)
        self.agent.release.set()
        self.assertEqual((await self.runtime.wait(record.run_id, self.last)).status, "failed")
        self.assertEqual((await self.vault.read(self.user, "manual.md"))["revision"], 1)
        self.assertEqual((await self.vault.backlinks(self.user, "device.md"))["paths"], [])

    async def test_claim_fence_and_authorization(self):
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        self.now += 4000
        claims = await asyncio.gather(self.queue.claim(self.agent_id), self.queue.claim(self.agent_id))
        owners = [claim for claim in claims if claim]
        self.assertEqual(len(owners), 1)
        self.now += 11
        second = await self.queue.claim(self.agent_id)
        self.assertEqual(second.run_id, owners[0].run_id)
        self.assertFalse(await self.queue.finish(owners[0], "success"))
        self.policy.enabled = False
        self.now += 11
        self.assertIsNone(await self.worker.run_due_once(self.runtime))
        async with self.engine.connect() as con:
            state = (
                await con.execute(sa.select(spaces.c.status).filter_by(agent=self.agent_id))
            ).scalar_one()
        self.assertEqual(state, "denied")
        self.assertEqual(self.agent.turns, 0)

    async def test_bound_module_worker_runs_when_due_and_stops_on_close(self):
        await self.worker.close()
        self.worker.interval = 0.01
        await self.worker.start(self.runtime)
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        self.now += 4000
        await asyncio.wait_for(self.agent.entered.wait(), 5)
        self.agent.release.set()
        async with asyncio.timeout(5):
            while (await self.vault.read(self.user, "manual.md"))["revision"] == 1:
                await asyncio.sleep(0.01)
        await self.runtime.close()
        self.assertIsNone(self.worker.task)

    async def test_delegation_revoked_after_read_prevents_write(self):
        await self.vault.write(self.user, "manual.md", "manual", expected_revision=0)
        self.now += 4000
        record = await self.worker.run_due_once(self.runtime)
        await asyncio.wait_for(self.agent.entered.wait(), 5)

        # Keep the ordinary read/write policy unchanged. Only the unattended
        # delegation is revoked. The document write must still be rejected.
        async def revoked(context):
            return False

        self.policy.validate = revoked
        self.agent.release.set()
        async with asyncio.timeout(5):
            while not (await self.runtime.services.store.get(record.run_id)).terminal:
                await asyncio.sleep(0.01)
        self.assertEqual((await self.vault.read(self.user, "manual.md"))["revision"], 1)


@unittest.skipUnless(
    os.environ.get("OPENAGENT_TEST_POSTGRES_URL"), "No isolated PostgreSQL test database configured"
)
class PostgresDocumentDream(DocumentDream):
    url = os.environ.get("OPENAGENT_TEST_POSTGRES_URL")
