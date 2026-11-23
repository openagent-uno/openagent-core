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

    async def seal(self):
        @asynccontextmanager
        async def fence():
            yield "sealed"
        async def idle(_):
            pass
        await prepare_migration(self.source, self.snapshot, self.clone, databases=("state.sqlite",),
                                quiesce=fence, migrate=idle)
        self.stage.transfer_history(self.snapshot, "state.sqlite", "notes", expected_tenant="tenant",
                                    dsn=self.dsn, schema=self.stage_schema, require_git_history=False)

    def build(self, verify=False):
        return self.candidate.build_runtime_candidate(self.snapshot, "state.sqlite", "notes", expected_tenant="tenant",
                    agent_id="agent", dsn=self.dsn, stage_schema=self.stage_schema, candidate_schema=self.schema,
                    verify_only=verify, require_git_history=False)

    async def test_public_reader_preserves_acls_ids_sequence_content_and_requests(self):
        from openagent_storage_postgres import PostgresRuntimeStore
        await self.seal()
        report = self.build()
        self.assertEqual(report["verified_rows"], dict(sessions=1, runs=1, messages=2, events=3))
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
