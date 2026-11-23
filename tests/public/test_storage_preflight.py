"""The migration inventory reads only a host-fenced, verified synthetic snapshot."""
import contextlib
import io
import json
from pathlib import Path
import sqlite3
import subprocess
from tempfile import TemporaryDirectory
import unittest

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration, verify_snapshot
from openagent_storage_sqlite.preflight import inspect_migration_source, main


class MigrationInventory(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        root = self.enterContext(TemporaryDirectory())
        self.root = Path(root)
        self.source = self.root / "source"
        self.vault = self.source / "vault"
        self.vault.mkdir(parents=True)
        self.database = self.source / "state.sqlite"
        self.tenant = "tenant-should-stay-private"
        self.store = SqliteRuntimeStore(self.database)
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        person = PrincipalRef("host", self.tenant, "owner-should-stay-private")
        context = ExecutionContext(person, person, person, "session-secret", "agent", (person,))
        await self.store.accept(RunRequest("run-secret", "session-secret", "key", "input-secret"), context)
        self.backup = self.root / "snapshot"

    def git(self, *args):
        subprocess.run(["git", "-C", str(self.vault), "-c", "user.name=Fixture",
                        "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgSign=false",
                        "-c", "core.hooksPath=/dev/null", *args], capture_output=True, check=True)

    async def snapshot(self):
        @contextlib.asynccontextmanager
        async def fence():
            yield "fixture-fence"
        async def idle(_):
            return None
        await prepare_migration(self.source, self.backup, self.root / "candidate",
                                databases=("state.sqlite",), quiesce=fence, migrate=idle)

    def inspect(self, **kw):
        return inspect_migration_source(self.backup, "state.sqlite", "vault",
                                        expected_tenant=self.tenant, **kw)

    async def test_wal_history_and_projection_gap_are_counted_without_disclosure(self):
        with sqlite3.connect(self.database) as db:
            db.execute("CREATE TABLE sessions(session_id TEXT PRIMARY KEY)")
            db.executemany("INSERT INTO sessions VALUES(?)", [("session-secret",), ("legacy-only-secret",)])
        self.git("init", "--quiet")
        path = self.vault / "note-name-secret.md"
        path.write_text("body-secret", encoding="utf8")
        self.git("add", ".")
        self.git("commit", "-qm", "message-secret")
        path.rename(self.vault / "renamed-secret.md")
        self.git("add", "-A")
        self.git("commit", "-qm", "rename-secret")
        self.assertTrue(Path(str(self.database) + "-wal").stat().st_size > 0)
        await self.snapshot()
        before = verify_snapshot(self.backup)
        report = self.inspect()
        self.assertEqual(report["counts"]["sessions_v2"], 1)
        self.assertEqual(report["counts"]["session_runs"], 1)
        self.assertEqual(report["counts"]["session_messages"], 1)
        self.assertEqual(report["legacy"]["missing_live_projections"], 1)
        self.assertEqual(report["legacy"]["legacy_sessions"], 2)
        self.assertEqual(report["vault"]["markdown_files"], 1)
        self.assertEqual(report["vault"]["git_commits"], 2)
        self.assertEqual(report["issues"], ["unfinished_runtime_runs", "legacy_projection_incomplete"])
        self.assertNotIn("secret", json.dumps(report))
        self.assertNotIn(self.tenant, json.dumps(report))
        self.assertEqual(verify_snapshot(self.backup), before)
        with self.assertRaises(PermissionError):
            inspect_migration_source(self.backup, "state.sqlite", "vault", expected_tenant="foreign")

    async def test_document_run_sources_require_an_equivalent_destination(self):
        db = self.store.connection
        db.execute("CREATE TABLE IF NOT EXISTS oa_document_run_sources(run_id,session_id,source_id,PRIMARY KEY(run_id,source_id))")
        db.execute("INSERT INTO oa_document_run_sources VALUES ('run-secret','session-secret','opaque-source')")
        self.git("init", "-b", "main")
        (self.vault / "readme.md").write_text("fixture")
        self.git("add", ".")
        self.git("commit", "-m", "fixture")
        await self.snapshot()
        evidence = self.inspect()
        self.assertEqual(evidence["counts"]["oa_document_run_sources"], 1)
        self.assertIn("runtime_document_lineage_projection_required", evidence["issues"])
        self.assertNotIn("opaque-source", str(evidence))

    async def test_unversioned_vault_and_checked_input(self):
        (self.vault / "name-secret.md").write_text("body-secret")
        await self.store.transition("run-secret", "running")
        await self.store.transition("run-secret", "success", output="output-secret")
        await self.snapshot()
        with self.assertRaises(ValueError):
            self.inspect()
        report = self.inspect(require_git_history=False)
        self.assertEqual(report["vault"]["markdown_files"], 1)
        self.assertIsNone(report["vault"]["git_commits"])
        self.assertIsNone(report["legacy"])
        self.assertFalse(report["issues"])
        (self.backup / "vault" / "name-secret.md").write_text("tampered")
        console = io.StringIO()
        with contextlib.redirect_stdout(console):
            status = main([str(self.backup), "--database", "state.sqlite", "--vault", "vault",
                           "--expected-tenant", self.tenant])
        self.assertEqual(status, 2)
        self.assertEqual(json.loads(console.getvalue()), {"error": "snapshot_preflight_rejected"})

    async def test_rejects_external_git_objects(self):
        self.git("init", "--quiet")
        (self.vault / ".git/objects/info/alternates").write_text("/elsewhere")
        await self.snapshot()
        with self.assertRaisesRegex(ValueError, "self-contained"):
            self.inspect()
