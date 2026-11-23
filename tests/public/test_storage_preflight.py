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
from openagent_storage_sqlite.migration import (MANIFEST, prepare_migration, restore_to_new_directory,
                                                verify_migration_restore, verify_snapshot)
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

    async def snapshot(self, *, absent_vault=None, expected_tenants_by_database=None):
        @contextlib.asynccontextmanager
        async def fence():
            yield "fixture-fence"
        async def idle(_):
            return None
        return await prepare_migration(self.source, self.backup, self.root / "candidate",
                                databases=("state.sqlite",), quiesce=fence, migrate=idle, absent_vault=absent_vault,
                                expected_tenants_by_database=expected_tenants_by_database)

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

    async def test_absent_vault_requires_a_source_receipt_and_restores_all_bytes(self):
        self.vault.rmdir()
        receipt = await self.snapshot(absent_vault="vault")
        digest = receipt["manifest_sha256"]
        sealed = verify_snapshot(self.backup)
        self.assertEqual(sealed["format"], 2)
        self.assertEqual(sealed["vault"], {"path": "vault", "status": "absent"})
        self.assertEqual(self.inspect(history_only=True)["vault"]["status"], "absent")
        with self.assertRaisesRegex(ValueError, "explicit selection"):
            self.inspect(require_git_history=False)
        with self.assertRaisesRegex(ValueError, "exact fenced"):
            inspect_migration_source(self.backup, "state.sqlite", "other", expected_tenant=self.tenant,
                                     history_only=True)
        target = self.root / "rollback"
        with self.assertRaisesRegex(ValueError, "host receipt"):
            restore_to_new_directory(self.backup, target, expected_manifest_sha256="0"*64)
        self.assertFalse(target.exists())
        restore_to_new_directory(self.backup, target, expected_manifest_sha256=digest)
        verify = lambda: verify_migration_restore(self.backup, target, expected_manifest_sha256=digest)
        self.assertEqual(verify(), verify())
        with self.assertRaises(FileExistsError):
            restore_to_new_directory(self.backup, target, expected_manifest_sha256=digest)
        self.assertTrue(all(not (root / "vault").exists() for root in
                            (self.source, self.backup, self.root / "candidate", target)))
        (target / "vault").mkdir()
        with self.assertRaisesRegex(ValueError, "absent vault"):
            verify()
        (target / "vault").rmdir()
        (target / "state.sqlite").write_bytes(b"changed rollback")
        with self.assertRaisesRegex(ValueError, "Restored files differ"):
            verify()
        (self.backup / "vault").mkdir()
        with self.assertRaisesRegex(ValueError, "absent vault"):
            self.inspect(history_only=True)
        (self.backup / "vault").rmdir()
        # Changing even the declared absent path invalidates the independently held receipt.
        sealed["vault"]["path"] = "other"
        (self.backup / MANIFEST).write_text(json.dumps(sealed))
        with self.assertRaisesRegex(ValueError, "host receipt"):
            restore_to_new_directory(self.backup, self.root / "other", expected_manifest_sha256=digest)

    async def test_multi_tenant_requires_the_exact_sealed_set_for_the_database(self):
        self.vault.rmdir()
        second = PrincipalRef("host", "second-tenant", "second-owner")
        ctx = ExecutionContext(second, second, second, "second-session", "agent", (second,))
        await self.store.accept(RunRequest("second-run", "second-session", "second-key", "other"), ctx)
        tenants = (second.tenant_id, self.tenant)
        await self.snapshot(absent_vault="vault", expected_tenants_by_database={"state.sqlite": tenants})
        def inspect(**terms):
            return inspect_migration_source(self.backup, "state.sqlite", "vault", history_only=True, **terms)
        self.assertEqual(inspect(expected_tenants=tenants), inspect(expected_tenants=tuple(reversed(tenants))))
        self.assertEqual(inspect(expected_tenants=tenants)["counts"]["sessions_v2"], 2)
        for arguments in ({"expected_tenant": self.tenant},
                          {"expected_tenants": ("foreign", self.tenant)},
                          {"expected_tenants": (*tenants, "extra")}):
            with self.assertRaises(PermissionError):
                inspect(**arguments)
        for arguments in ({"expected_tenants": (self.tenant, self.tenant)},
                          {"expected_tenants": ("", self.tenant)},
                          {"expected_tenants": tenants, "expected_tenant": self.tenant}):
            with self.assertRaises(ValueError):
                inspect(**arguments)
        self.assertNotIn(second.tenant_id, json.dumps(inspect(expected_tenants=tenants)))
        with contextlib.redirect_stdout(io.StringIO()) as console:
            status = main([str(self.backup), "--database", "state.sqlite", "--vault", "vault", "--history-only",
                           "--expected-tenant", self.tenant, "--expected-tenant", second.tenant_id])
        self.assertEqual(status, 1)  # both fixture runs are still open
        self.assertEqual(json.loads(console.getvalue())["counts"]["sessions_v2"], 2)

    async def test_fence_rejects_an_inventory_that_adds_or_omits_an_owner(self):
        self.vault.rmdir()
        with self.assertRaisesRegex(PermissionError, "exact host"):
            await self.snapshot(absent_vault="vault",
                                expected_tenants_by_database={"state.sqlite": [self.tenant, "extra"]})
        self.assertFalse((self.backup / MANIFEST).exists())

    async def test_history_only_refuses_even_an_empty_directory_at_the_fenced_source(self):
        with self.assertRaisesRegex(ValueError, "absent vault"):
            await self.snapshot(absent_vault="vault")
        self.assertFalse(self.backup.exists())
        self.vault.rmdir()
        self.vault.symlink_to("deliberately-missing-fixture")
        with self.assertRaisesRegex(ValueError, "absent vault"):
            await self.snapshot(absent_vault="vault/nested")
        self.assertFalse(self.backup.exists())
        self.vault.unlink()
        with self.assertRaisesRegex(ValueError, "relative"):
            await self.snapshot(absent_vault="../other")
        await self.snapshot()
        with self.assertRaisesRegex(ValueError, "exact fenced"):
            self.inspect(history_only=True)

    async def test_history_only_preserves_sql_document_migration_gate(self):
        self.vault.rmdir()
        with sqlite3.connect(self.database) as db:
            db.execute("CREATE TABLE oa_vault_notes(content TEXT)")
            db.execute("INSERT INTO oa_vault_notes VALUES('sensitive')")
        await self.snapshot(absent_vault="vault")
        inventory = self.inspect(history_only=True)
        self.assertIn("database_vault_requires_separate_migration", inventory["issues"])
        self.assertNotIn("sensitive", json.dumps(inventory))

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
