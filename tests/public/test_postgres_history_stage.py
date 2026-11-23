"""Run against an isolated PostgreSQL server via OPENAGENT_TEST_POSTGRES_DSN."""
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration


@unittest.skipUnless(os.getenv("OPENAGENT_TEST_POSTGRES_DSN"), "isolated PostgreSQL DSN required")
class PostgresStage(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import psycopg
        from openagent_storage_sqlite import postgres_stage

        self.module = postgres_stage
        self.dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
        self.schema = "oa_stage_" + uuid4().hex
        self.destination = self.enterContext(psycopg.connect(self.dsn, autocommit=True))
        self.addCleanup(lambda: self.destination.execute(
            psycopg.sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(psycopg.sql.Identifier(self.schema))))
        root = Path(self.enterContext(TemporaryDirectory()))
        self.source = root / "source"
        self.snapshot = root / "snapshot"
        self.candidate = root / "candidate"
        vault = self.source / "memories"
        vault.mkdir(parents=True)
        (vault / "private.md").write_text("private fixture")
        self.database = self.source / "state.sqlite"
        store = SqliteRuntimeStore(self.database)
        await store.start()
        self.addAsyncCleanup(store.close)
        actor = PrincipalRef("authority", "tenant", "owner")
        ctx = ExecutionContext(actor, actor, actor, "session", "agent", (actor,))
        await store.accept(RunRequest("run", "session", "key", "question"), ctx)
        await store.transition("run", "running")
        await store.transition("run", "success", output="answer")
        data = b"private" * 180000
        digest = hashlib.sha256(data).hexdigest()
        self.cas_path = self.source / "artifacts" / "sha256" / digest[:2] / digest
        self.cas_path.parent.mkdir(parents=True)
        self.cas_path.write_bytes(data)
        with sqlite3.connect(self.database) as db:
            db.execute("CREATE TABLE sessions(session_id TEXT PRIMARY KEY, opaque BLOB)")
            db.execute("INSERT INTO sessions VALUES('session',?)", (bytes(range(255)),))
            db.execute("""INSERT INTO artifacts(id,tenant_id,owner_principal_id,visibility,direction,kind,mime,
                storage_key,sha256,size_bytes,storage_state,retention_class,created_at_ms,updated_at_ms)
                VALUES('artifact','tenant',?,'private','output','file','application/octet-stream',?,?,?,
                       'available','normal',0,0)""", (actor.key, f"sha256/{digest[:2]}/{digest}", digest,len(data)))
            db.execute("""INSERT INTO artifact_links(id,tenant_id,resource_type,resource_id,artifact_id,relation,created_at_ms)
                          VALUES('link','tenant','session','session','artifact','attachment',0)""")
            db.execute("""UPDATE sessions_v2 SET visibility='quarantined',owner_principal_id=NULL""")

    async def seal(self):
        @asynccontextmanager
        async def fence():
            yield "host-fenced"
        async def idle(_):
            return None
        await prepare_migration(self.source,self.snapshot,self.candidate,databases=("state.sqlite",),quiesce=fence,migrate=idle)

    def transfer(self, verify=False):
        return self.module.transfer_history(self.snapshot, "state.sqlite", "memories",
                expected_tenant="tenant", dsn=self.dsn, schema=self.schema, verify_only=verify, require_git_history=False)

    async def test_resumes_without_overwrite_and_rechecks_all_source_bytes(self):
        await self.seal()
        transfer = self.module._transfer_collection
        def interrupt(pg, name, source, verify):
            if name == "table/sessions_v2":
                raise RuntimeError("synthetic interruption")
            return transfer(pg, name, source, verify)
        with patch.object(self.module, "_transfer_collection", interrupt), self.assertRaises(RuntimeError):
            self.transfer()
        report = self.transfer()
        self.assertFalse(report["runtime_activated"])
        self.assertEqual(report["verified_rows"]["sessions"], 1)
        self.assertEqual(report["verified_rows"]["session_messages"], 2)
        self.assertEqual(report["verified_rows"]["artifact_links"], 1)
        self.assertEqual(report["verified_artifact_blobs"], 1)
        self.assertEqual(self.transfer(verify=True), report)
        raw, = self.destination.execute(f"""SELECT payload FROM {self.schema}.parts
                            WHERE collection='table/sessions_v2' AND part=1""").fetchone()
        self.assertIn("quarantined", json.loads(raw))
        self.destination.execute(f"""UPDATE {self.schema}.parts SET payload='tampered'
                            WHERE collection LIKE 'cas/%' AND part=1""")
        with self.assertRaisesRegex(self.module.StageMismatch, "payload differs"):
            self.transfer(True)
        with self.assertRaisesRegex(self.module.StageMismatch, "payload differs"):
            self.transfer(False)

    async def test_available_missing_cas_refuses_parity(self):
        self.cas_path.unlink()
        await self.seal()
        with self.assertRaisesRegex(self.module.StageMismatch, "artifact bytes missing"):
            self.transfer()

    async def test_public_grants_and_foreign_source_fail_closed(self):
        await self.seal()
        self.transfer()
        self.destination.execute(f"GRANT USAGE ON SCHEMA {self.schema} TO PUBLIC")
        with self.assertRaisesRegex(self.module.StageMismatch, "privileges"):
            self.transfer(True)
        self.destination.execute(f"REVOKE USAGE ON SCHEMA {self.schema} FROM PUBLIC")
        self.destination.execute(f"UPDATE {self.schema}.identity SET receipt='another-source'")
        with self.assertRaisesRegex(self.module.StageMismatch, "another snapshot"):
            self.transfer(False)
