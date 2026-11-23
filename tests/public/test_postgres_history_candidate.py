"""A synthetic fenced store can be read through the native PostgreSQL runtime."""
from contextlib import asynccontextmanager
from dataclasses import replace
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest, SessionRef
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration


@unittest.skipUnless(os.getenv("OPENAGENT_TEST_POSTGRES_DSN"), "isolated PostgreSQL DSN required")
class RuntimeCandidate(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import psycopg
        from openagent_storage_sqlite import postgres_candidate, postgres_stage
        self.candidate = postgres_candidate
        self.stage = postgres_stage
        self.dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
        self.pg = self.enterContext(psycopg.connect(self.dsn, autocommit=True))
        self.stage_schema = "oa_stage_" + uuid4().hex
        self.schema = "oa_candidate_" + uuid4().hex
        for name in (self.stage_schema, self.schema):
            self.addCleanup(lambda name=name: self.pg.execute(psycopg.sql.SQL(
                "DROP SCHEMA IF EXISTS {} CASCADE").format(psycopg.sql.Identifier(name))))
        root = Path(self.enterContext(TemporaryDirectory()))
        self.source = root / "source"
        self.snapshot = root / "snapshot"
        self.clone = root / "clone"
        vault = self.source / "notes"
        vault.mkdir(parents=True)
        (vault / "note.md").write_text("synthetic memory")
        self.path = self.source / "state.sqlite"
        self.store = SqliteRuntimeStore(self.path)
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        person = PrincipalRef("host", "tenant", "owner")
        self.ctx = ExecutionContext(person, person, person, "thread", "agent", (person,), ("read",))
        self.ref = SessionRef("host", "tenant", "agent", "thread")
        await self.store.create_session(self.ref, self.ctx, title="initial")
        self.request = RunRequest("run", "thread", "key", "question")
        await self.store.accept(self.request, self.ctx)
        await self.store.transition("run", "running")
        await self.store.transition("run", "success", output="answer")
        await self.store.rename_session(self.ref, self.ctx, title="renamed")
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='domain_events'")

    async def seal(self, *, history_only=False, tenants=None):
        @asynccontextmanager
        async def fence():
            yield "sealed"
        async def idle(_):
            pass
        await prepare_migration(self.source, self.snapshot, self.clone, databases=("state.sqlite",),
                                quiesce=fence, migrate=idle, absent_vault="notes" if history_only else None,
                                expected_tenants_by_database={"state.sqlite": tenants} if tenants else None)
        self.stage.transfer_history(self.snapshot, "state.sqlite", "notes", expected_tenant=None if tenants else "tenant",
                                    expected_tenants=tenants,
                                    dsn=self.dsn, schema=self.stage_schema, require_git_history=history_only,
                                    history_only=history_only)

    def build(self, verify=False, *, history_only=False, tenants=None):
        return self.candidate.build_runtime_candidate(self.snapshot, "state.sqlite", "notes", expected_tenant=None if tenants else "tenant",
                    expected_tenants=tenants,
                    agent_id="agent", dsn=self.dsn, stage_schema=self.stage_schema, candidate_schema=self.schema,
                    verify_only=verify, require_git_history=history_only, history_only=history_only)

    async def test_history_only_stage_and_native_candidate_retry_and_reader(self):
        from openagent_storage_postgres import PostgresRuntimeStore
        (self.source / "notes/note.md").unlink()
        (self.source / "notes").rmdir()
        await self.seal(history_only=True)
        arguments = dict(expected_tenant="tenant", dsn=self.dsn, schema=self.stage_schema, history_only=True)
        staged = self.stage.transfer_history(self.snapshot, "state.sqlite", "notes", **arguments)
        self.assertEqual(staged["vault_status"], "absent")
        self.assertEqual(staged, self.stage.transfer_history(self.snapshot, "state.sqlite", "notes",
                                                              verify_only=True, **arguments))
        report = self.build(history_only=True)
        self.assertEqual(report["vault_status"], "absent")
        self.assertEqual(report, self.build(history_only=True))
        self.assertEqual(report, self.build(True, history_only=True))
        with self.assertRaisesRegex(ValueError, "explicit selection"):
            self.build()
        pg = PostgresRuntimeStore(self.dsn, agent_id="agent", schema=self.schema)
        await pg.start()
        self.addAsyncCleanup(pg.close)
        self.assertEqual(await pg.read_session(self.ref, self.ctx, limit=20),
                         await self.store.read_session(self.ref, self.ctx, limit=20))
        for root in self.source, self.snapshot, self.clone:
            self.assertFalse((root / "notes").exists())

    async def test_staged_document_lifetimes_require_native_authorization_parity(self):
        db = self.store.connection
        db.execute("CREATE TABLE IF NOT EXISTS oa_document_run_sources(run_id,session_id,source_id,PRIMARY KEY(run_id,source_id))")
        db.execute("INSERT INTO oa_document_run_sources VALUES ('run','thread','token')")
        db.execute("CREATE TABLE oa_vault_read_receipts(path,revision,token)")
        db.execute("INSERT INTO oa_vault_read_receipts VALUES ('private.md',1,'token')")
        await self.seal()
        checked = self.stage.transfer_history(self.snapshot, "state.sqlite", "notes",
            expected_tenant="tenant", dsn=self.dsn, schema=self.stage_schema,
            verify_only=True, require_git_history=False)
        self.assertEqual(checked["verified_rows"]["oa_document_run_sources"], 1)
        self.assertEqual(checked["verified_rows"]["oa_vault_read_receipts"], 1)
        with self.assertRaisesRegex(self.stage.StageMismatch, "unresolved issues"):
            self.build()
        self.assertIsNone(self.pg.execute("SELECT to_regnamespace(%s)", (self.schema,)).fetchone()[0])

    async def test_public_reader_preserves_acls_ids_sequence_content_and_requests(self):
        from openagent_storage_postgres import PostgresRuntimeStore
        await self.seal()
        report = self.build()
        self.assertEqual(report["verified_rows"], dict(sessions=1, runs=1, messages=2, events=3, tool_calls=0, operations=0, handoffs=0))
        self.assertEqual(self.build(True), report)
        self.assertEqual(self.build(), report)
        pg = PostgresRuntimeStore(self.dsn, agent_id="agent", schema=self.schema)
        await pg.start()
        self.addAsyncCleanup(pg.close)
        with self.assertRaisesRegex(self.stage.StageMismatch, "offline"):
            self.build()
        self.assertEqual(await pg.read_session(self.ref,self.ctx,limit=20),
                         await self.store.read_session(self.ref,self.ctx,limit=20))
        first = await self.store.read_session(self.ref,self.ctx,limit=1)
        self.assertEqual((await pg.read_session(self.ref,self.ctx,limit=1,cursor=first["next_cursor"]))["messages"],
                         (await self.store.read_session(self.ref,self.ctx,limit=1,cursor=first["next_cursor"]))["messages"])
        self.assertEqual(await pg.get("run"), await self.store.get("run"))
        self.assertEqual(await pg.accepted_request("run"), await self.store.accepted_request("run"))
        self.assertEqual(await pg.events("run", 1), await self.store.events("run", 1))
        self.assertEqual((await pg.list_sessions(self.ctx,include_archived=False,limit=5))["sessions"],
                         (await self.store.list_sessions(self.ctx,include_archived=False,limit=5))["sessions"])
        actor = PrincipalRef("host","tenant","other")
        wrong_ctx = replace(self.ctx,author=actor,initiator=actor,authority=actor,audience=(actor,))
        with self.assertRaises(PermissionError):
            await pg.read_session(self.ref,wrong_ctx,limit=5)
        self.assertEqual((await pg.list_sessions(wrong_ctx,include_archived=False,limit=5))["sessions"], [])
        other = PrincipalRef("host","another-tenant","owner")
        with self.assertRaises(PermissionError):
            await pg.read_session(self.ref,replace(self.ctx,author=other,initiator=other,authority=other,
                                                 audience=(other,)),limit=5)
        record, accepted = await pg.accept(self.request,self.ctx)
        self.assertFalse(accepted)
        self.assertEqual(record.status,"success")
        await pg.accept(RunRequest("second","thread","second","later"),self.ctx)
        self.assertGreater((await pg.events("second"))[0].cursor,100)

    async def test_imported_tool_reopens_under_each_saved_identifier_without_reexecution(self):
        from openagent_storage_postgres import PostgresRuntimeStore
        await self.store.accept(RunRequest("tool-run", "thread", "second", "inspect"), self.ctx)
        await self.store.transition("tool-run", "running")
        binding = {"source_id": "host", "name": "inspect", "tool_ref": "inspect", "provider_call_id": "provider-call"}
        await self.store.begin_tool("tool-run", "call", binding, {"limit": 1})
        await self.store.finish_tool("tool-run", "call", result={"content": ["retained"]})
        await self.store.transition("tool-run", "success", output="inspected")
        original = await self.store.read_tool_invocation(self.ref, "call", self.ctx)
        await self.seal()
        self.assertEqual(self.build()["verified_rows"]["tool_calls"], 1)
        self.build(True)
        pg = PostgresRuntimeStore(self.dsn, agent_id="agent", schema=self.schema)
        await pg.start()
        self.addAsyncCleanup(pg.close)
        for selector in (original["id"], "call", "provider-call"):
            self.assertEqual(await pg.read_tool_invocation(self.ref, selector, self.ctx), original)
            self.assertEqual(await pg.read_tool_invocation_binding(self.ref, selector, self.ctx),
                             {"run_id": "tool-run", "binding": {"tool_ref": "inspect"}})
        self.assertEqual(await pg.read_session(self.ref, self.ctx, limit=20),
                         await self.store.read_session(self.ref, self.ctx, limit=20))
        self.pg.execute(f"UPDATE {self.schema}.tool_calls SET invocation_id=NULL")
        self.assertEqual(await pg.read_tool_invocation(self.ref, original["id"], self.ctx), original)
        await pg.accept(RunRequest("fresh", "thread", "fresh", "inspect"), self.ctx)
        await pg.transition("fresh", "running")
        await pg.begin_tool("fresh", "fresh-call", binding, {})
        running = await pg.read_tool_invocation(self.ref, "fresh-call", self.ctx)
        self.assertEqual(running["status"], "running")
        await pg.finish_tool("fresh", "fresh-call", result="complete")
        self.assertEqual((await pg.read_tool_invocation(self.ref, running["id"], self.ctx))["result"], "complete")
        await pg.transition("fresh", "success")
        actor = PrincipalRef("host", "tenant", "other")
        with self.assertRaises(PermissionError):
            await pg.read_tool_invocation(self.ref, "provider-call", replace(self.ctx, authority=actor))

    async def test_native_readers_share_storage_but_keep_two_tenant_catalogs_separate(self):
        from openagent_storage_postgres import PostgresRuntimeStore
        other = PrincipalRef("host", "another", "owner")
        ctx = ExecutionContext(other, other, other, "other-thread", "agent", (other,))
        ref = SessionRef("host", "another", "agent", "other-thread")
        await self.store.accept(RunRequest("other", ref.session_id, "key", "another tenant"), ctx)
        await self.store.transition("other", "success", output="private answer")
        tenants = ("tenant", "another")
        await self.seal(tenants=tenants)
        self.assertEqual(self.build(tenants=tenants)["verified_rows"]["sessions"], 2)
        self.build(True, tenants=tuple(reversed(tenants)))
        pg = PostgresRuntimeStore(self.dsn, agent_id="agent", schema=self.schema)
        await pg.start()
        self.addAsyncCleanup(pg.close)
        for person, session, other_ref in ((ctx, ref, self.ref), (self.ctx, self.ref, ref)):
            self.assertEqual(await pg.read_session(session, person, limit=10),
                             await self.store.read_session(session, person, limit=10))
            sessions = await pg.list_sessions(person, include_archived=False, limit=10)
            self.assertEqual([item["session_id"] for item in sessions["sessions"]], [session.session_id])
            with self.assertRaises(PermissionError):
                await pg.read_session(other_ref, person, limit=10)

    async def test_terminal_handoff_preserves_grant_binding_without_rescheduling_work(self):
        from openagent_core.durable_operations import OperationContinuation, OperationRunRequest
        from openagent_storage_postgres import PostgresRuntimeStore
        await self.store.accept(RunRequest("source", "thread", "source", "task"), self.ctx)
        await self.store.transition("source", "timed_out")
        ctx = replace(self.ctx, deferred=True, delegation_id="captured")
        desc = OperationContinuation("op", "inspect", 1, 2000000000, 10, 2,
                                     handoff_run_id="source", checkpoint_origin_run_id="source")
        for run, descriptor, status in (
                ("op", desc, "timed_out"), ("next", replace(desc, segment=1, previous_run_id="op"), "success")):
            await self.store.accept(OperationRunRequest(run, "thread", run, "task", continuation=descriptor), ctx)
            await self.store.transition(run, status)
        await self.store.stop_durable_operation("op", "success")
        await self.seal()
        self.assertEqual(self.build()["verified_rows"]["operations"], 1)
        self.assertEqual(self.build(True)["verified_rows"]["handoffs"], 1)
        pg = PostgresRuntimeStore(self.dsn, agent_id="agent", schema=self.schema)
        await pg.start()
        self.addAsyncCleanup(pg.close)
        self.assertEqual((await pg.durable_operation("op"))["current_run_id"], "next")
        self.assertEqual(await pg.accepted_request("next"), await self.store.accepted_request("next"))
        self.assertFalse(await pg.ready_operations(2100000000))

    async def test_active_operation_cannot_start_a_new_worker_from_an_offline_receipt(self):
        from openagent_core.durable_operations import OperationContinuation, OperationRunRequest
        descriptor = OperationContinuation("op", "inspect", 1, 2000000000, 10, 2)
        await self.store.accept(OperationRunRequest("op", "thread", "op", "task", continuation=descriptor),
                                replace(self.ctx, deferred=True, delegation_id="captured"))
        await self.store.transition("op", "timed_out")
        await self.seal()
        with self.assertRaisesRegex(self.stage.StageMismatch, "active operation"):
            self.build()
        self.assertIsNone(self.pg.execute("SELECT to_regnamespace(%s)", (self.schema,)).fetchone()[0])

    async def test_rejects_modified_request_without_partial_candidate(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE session_runs SET input_json='\"another question\"'")
        await self.seal()
        with self.assertRaisesRegex(self.stage.StageMismatch,"digest"):
            self.build()
        self.assertIsNone(self.pg.execute("SELECT to_regnamespace(%s)",(self.schema,)).fetchone()[0])

    async def test_quarantine_staged_without_resurrecting_its_session(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE sessions_v2 SET visibility='quarantined',owner_principal_id=NULL")
        await self.seal()
        with self.assertRaisesRegex(self.stage.StageMismatch,"historical projection"):
            self.build()
        self.assertIsNone(self.pg.execute("SELECT to_regnamespace(%s)",(self.schema,)).fetchone()[0])

    async def test_verification_rejects_data_drift_without_overwrite(self):
        await self.seal()
        self.build()
        self.pg.execute(f"UPDATE {self.schema}.sessions SET title='unexpected'")
        for verify in (True,False):
            with self.assertRaisesRegex(self.stage.StageMismatch,"differs"):
                self.build(verify)
        self.assertEqual(self.pg.execute(f"SELECT title FROM {self.schema}.sessions").fetchone(),("unexpected",))
