from contextlib import asynccontextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration
from openagent_storage_sqlite import vault_import
from openagent_storage_sqlite.vault_git_source import VaultSource, VaultImportMismatch


@asynccontextmanager
async def fence():
    yield "synthetic host fence"


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return (
            all(p == context.authority for p in audience)
            and resource.resource_id == context.authority.subject_id
        )


class DocumentImport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.source = self.root / "state"
        self.sealed = self.root / "sealed"
        self.gitdir = self.source / "notes"
        self.gitdir.mkdir(parents=True)
        self.person = PrincipalRef("host", "t-" + uuid4().hex, "a")
        self.ctx = ExecutionContext(
            self.person, self.person, self.person, "session", "agent", (self.person,)
        )
        self.runtime = SqliteRuntimeStore(self.source / "state.db")
        await self.runtime.start()
        await self.runtime.accept(RunRequest("run", "session", "k", "test"), self.ctx)
        await self.runtime.transition("run", "running")
        await self.runtime.transition("run", "success")
        await self.runtime.close()
        self.git("init", "-b", "main")
        (self.gitdir / "old.md").write_bytes(b"# Intro\r\n\r\n[[topic]]\r\n")
        (self.gitdir / "secret.md").write_text("hidden corpus")
        self.git("add", ".")
        self.git(
            "commit", "-m", "start", "-m", "Origin: chat\nSession: original-session"
        )
        self.first = self.git("rev-parse", "HEAD").strip()
        self.git("branch", "side")
        self.git("mv", "old.md", "active.md")
        self.git("commit", "-m", "rename")
        self.git("switch", "side")
        (self.gitdir / "old.md").write_text("# Side branch")
        self.git("commit", "-am", "side")
        self.git("switch", "main")
        (self.gitdir / "active.md").write_bytes(b"# Working draft\r\n\r\n[[topic]]\r\n")
        (self.gitdir / "new.md").write_text("# Untracked")

        async def noop(_):
            pass

        await prepare_migration(
            self.source,
            self.sealed,
            self.root / "unused",
            databases=("state.db",),
            quiesce=fence,
            migrate=noop,
        )
        self.src = VaultSource(self.sealed, "notes", ["old.md", "active.md", "new.md"])
        self.engine = create_async_engine(
            os.getenv("OPENAGENT_TEST_VAULT_DATABASE_URL")
            or ("sqlite+aiosqlite:///" + str(self.root / "target.db"))
        )
        self.addAsyncCleanup(self.engine.dispose)

        async def resolve(ctx):
            return ctx.authority.subject_id

        self.vault = SqlDocumentVault(self.engine, Policy(), resolve)
        await self.vault.initialize()

    def git(self, *args, repo=None):
        return subprocess.run(
            [
                "git",
                "-C",
                str(repo or self.gitdir),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgSign=false",
                "-c",
                "core.hooksPath=/dev/null",
                *args,
            ],
            capture_output=True,
            check=True,
        ).stdout.decode()

    async def run_import(self):
        return await vault_import.import_vault(
            self.vault, self.ctx, self.src, "state.db"
        )

    async def test_import_readback_acl_and_restore_with_new_revisions(self):
        report = await self.run_import()
        self.assertEqual(report, await self.run_import())
        self.assertEqual(report["selected_paths"], 3)
        docs = await self.vault.list_notes(self.ctx)
        self.assertEqual([r["path"] for r in docs["notes"]], ["active.md", "new.md"])
        first = await self.vault.revision_origin(self.ctx, "old.md", revision=1)
        self.assertEqual(first["commit"], self.first)
        self.assertIn(["Origin", "chat"], first["origin"])
        self.assertIn("Fixture", first["author"])
        origin = await self.vault.revision_origin(self.ctx, "active.md", revision=1)
        self.assertEqual(origin["peer"], "old.md")
        self.assertEqual(
            (await self.vault.read(self.ctx, "old.md", revision=1))["content"],
            "# Intro\r\n\r\n[[topic]]\r\n",
        )
        with self.assertRaises(LookupError):
            await self.vault.read(self.ctx, "old.md")
        path = await self.vault.read(self.ctx, "active.md")
        self.assertEqual(path["content"], "# Working draft\r\n\r\n[[topic]]\r\n")
        self.assertEqual(
            (await self.vault.backlinks(self.ctx, "topic.md"))["paths"], ["active.md"]
        )
        self.assertEqual(
            (await self.vault.search(self.ctx, query="Working"))["hits"][0]["path"],
            "active.md",
        )
        other = PrincipalRef("host", self.person.tenant_id, "b")
        ctx = replace(
            self.ctx, authority=other, initiator=other, author=other, audience=(other,)
        )
        self.assertIsNone(await self.vault.revision_origin(ctx, "old.md", revision=1))
        self.assertEqual((await self.vault.list_notes(ctx))["notes"], [])
        self.assertEqual(
            (await self.vault.search(self.ctx, query="hidden"))["hits"], []
        )
        for text in ("# Latest 1", "# Latest 2"):
            path = await self.vault.write(
                self.ctx, "active.md", text, expected_revision=path["revision"]
            )
        await self.vault.write(self.ctx, "after.md", "after", expected_revision=0)
        restored = self.root / "restored"
        receipt = await vault_import.restore_vault(
            self.vault,
            self.ctx,
            self.src,
            "state.db",
            restored,
            new_paths=["after.md"],
            quiesce=fence,
        )
        self.assertEqual(receipt["later_revisions"], 3)
        self.assertFalse((restored / "state" / "state.db").exists())
        repo = restored / "state" / "notes"
        self.assertEqual((repo / "active.md").read_text(), "# Latest 2")
        self.assertEqual((repo / "after.md").read_text(), "after")
        self.assertEqual((repo / "secret.md").read_text(), "hidden corpus")
        self.assertEqual(
            self.git("rev-list", "--all", repo=repo), self.git("rev-list", "--all")
        )
        entries = [
            json.loads(line)
            for line in (restored / "post-import-revisions.jsonl")
            .read_text()
            .splitlines()
        ]
        self.assertEqual(
            [e["content"] for e in entries], ["# Latest 1", "# Latest 2", "after"]
        )
        self.assertEqual(entries[0]["author"], self.person.key)
        self.assertEqual(
            (self.sealed / "notes" / "active.md").read_bytes(),
            b"# Working draft\r\n\r\n[[topic]]\r\n",
        )

    async def test_attested_working_tree_without_git_and_generated_rollback(self):
        from openagent_core.administration import ManagementContext
        from openagent_core.memory.vault.document_admin import DocumentVaultAdministration
        with self.assertRaises(VaultImportMismatch):
            VaultSource(self.sealed, "notes", ["active.md"], working_tree_only=True)
        corpus = self.source / "plain"
        corpus.mkdir()
        (corpus / "note.md").write_bytes(b"# Untracked\r\n")
        (corpus / "other.md").write_text("separate reader")
        (corpus / "llms.txt").write_text("old inventory")
        async def no_migration(_):
            pass
        sealed = self.root / "plainsealed"
        await prepare_migration(self.source, sealed, self.root / "plaincandidate",
                                databases=("state.db",), quiesce=fence, migrate=no_migration)
        with self.assertRaises(ValueError):
            VaultSource(sealed, "plain", ["note.md"])
        source = VaultSource(sealed, "plain", ["note.md","llms.txt"], working_tree_only=True)
        report = await vault_import.import_vault(self.vault, self.ctx, source, "state.db")
        self.assertEqual(report["selected_paths"], 2)
        row = await self.vault.revision_origin(self.ctx,"note.md",revision=1)
        self.assertEqual(row["source"], "working_tree")
        self.assertNotIn("author", row)
        with self.assertRaises(LookupError):
            await self.vault.read(self.ctx,"other.md")
        class AllowAdmin:
            async def authorize(self,*args,**kwargs): return True
        admin = DocumentVaultAdministration(self.vault, AllowAdmin(), self.ctx)
        management = ManagementContext(self.ctx.authority,self.ctx.agent_id)
        init = await admin.execute(management,"init")
        await admin.execute(management,"derived")
        new_paths = [p for p in init["created"] if not p.endswith("/")] + ["_showcase/showcase.md"]
        restored = self.root / "plainrestore"
        await vault_import.restore_vault(self.vault,self.ctx,source,"state.db",restored,
                                         new_paths=new_paths,quiesce=fence)
        tree = restored / "state" / "plain"
        self.assertTrue((tree / "concepts").is_dir())
        self.assertEqual((tree / "note.md").read_bytes(),b"# Untracked\r\n")
        self.assertNotIn("old inventory",(tree / "llms.txt").read_text())
        self.assertEqual((tree / "other.md").read_text(),"separate reader")

    async def test_resume_atomic_publication_and_refuse_forgotten_history_on_restore(
        self,
    ):
        real = vault_import._publish

        async def fail_after(con, vault, scope, src):
            await real(con, vault, scope, src)
            raise RuntimeError("interrupted")

        with (
            patch.object(vault_import, "_publish", fail_after),
            self.assertRaises(RuntimeError),
        ):
            await self.run_import()
        self.assertEqual((await self.vault.list_notes(self.ctx))["notes"], [])
        await self.run_import()
        shortened = VaultSource(self.sealed, "notes", ["new.md"])
        with self.assertRaises(VaultImportMismatch):
            await vault_import.import_vault(self.vault, self.ctx, shortened, "state.db")
        current = await self.vault.read(self.ctx, "active.md")
        await self.vault.forget(
            self.ctx, "active.md", expected_revision=current["revision"]
        )
        self.assertIsNone(
            await self.vault.revision_origin(self.ctx, "active.md", revision=1)
        )
        with self.assertRaisesRegex(VaultImportMismatch, "erasure"):
            await vault_import.restore_vault(
                self.vault,
                self.ctx,
                self.src,
                "state.db",
                self.root / "new",
                quiesce=fence,
            )
        self.assertFalse((self.root / "new").exists())

    async def test_other_tenant_refused_before_space_receipt(self):
        p = PrincipalRef("host", "other-tenant", "a")
        ctx = replace(self.ctx, authority=p, initiator=p, author=p, audience=(p,))
        with self.assertRaises(PermissionError):
            await vault_import.import_vault(self.vault, ctx, self.src, "state.db")

    async def test_detects_changed_staging_before_publication(self):
        import sqlalchemy as sa

        original = vault_import._stage

        async def change(con, scope, chunk):
            await original(con, scope, chunk)
            await con.execute(
                sa.update(vault_import.inbox)
                .filter_by(**scope, path="new.md")
                .values(content="changed")
            )

        with (
            patch.object(vault_import, "_stage", change),
            self.assertRaisesRegex(VaultImportMismatch, "unexpected"),
        ):
            await self.run_import()
        self.assertEqual((await self.vault.list_notes(self.ctx))["notes"], [])

    async def test_can_forget_history_only_renamed_note(self):
        await self.run_import()
        rev = 1
        while await self.vault.revision_origin(self.ctx, "old.md", revision=rev + 1):
            rev += 1
        await self.vault.forget(self.ctx, "old.md", expected_revision=rev)
        self.assertIsNone(
            await self.vault.revision_origin(self.ctx, "old.md", revision=1)
        )
        with self.assertRaises(LookupError):
            await self.vault.read(self.ctx, "old.md", revision=1)

    async def test_desktop_history_restores_exact_branch_and_current_reset_requires_consent(
        self,
    ):
        from openagent_core.administration import ManagementContext
        from openagent_core.memory.vault.document_admin import (
            DocumentVaultAdministration,
        )

        await self.run_import()
        admin = DocumentVaultAdministration(self.vault, self.vault.authorizer, self.ctx)
        management = ManagementContext(self.person, "agent")

        # Product management policy is independent of the memory-space grant.
        class Granted:
            async def authorize(self, *args, **kwargs):
                return True

        admin.authorizer = Granted()

        async def op(name, **kwargs):
            return await admin.execute(management, name, **kwargs)

        initial = await op("history")
        self.assertEqual(initial["commits"][0]["subject"], "Captured working tree")
        old = (await op("history", query={"path": "old.md"}))["commits"]
        self.assertTrue(any(c["hash"] == self.first for c in old))
        detail = await op("commit", query={"hash": self.first})
        self.assertEqual(detail["files"], [{"path": "old.md", "status": "A"}])
        self.assertNotIn("secret.md", str(detail))
        self.assertIn("Intro", detail["diff"])
        side = self.git("rev-parse", "side").strip()
        await op("restore", body={"hash": side})
        self.assertEqual(
            (await self.vault.read(self.ctx, "old.md"))["content"], "# Side branch"
        )
        for path in ("active.md", "new.md"):
            with self.assertRaises(LookupError):
                await self.vault.read(self.ctx, path)
        saved = (await op("history"))["commits"][0]["hash"]
        await op("write", path="old.md", body={"content": "# newer"})
        await op("write", path="old.md", body={"content": "# Side branch"})
        with self.assertRaises(ValueError):
            await op("reset", body={"hash": saved})
        before = (await op("history"))["commits"]
        result = await op("reset", body={"hash": saved, "confirm": True})
        self.assertEqual(result["deleted"], 2)
        self.assertEqual(
            (await self.vault.read(self.ctx, "old.md"))["content"], "# Side branch"
        )
        with self.assertRaises(LookupError):
            await op("commit", query={"hash": before[0]["hash"]})
        self.assertIn(
            "error", await op("reset", body={"hash": self.first, "confirm": True})
        )
        await op("restore", body={"hash": initial["commits"][0]["hash"]})
        self.assertIn(
            "Working draft", (await self.vault.read(self.ctx, "active.md"))["content"]
        )

    async def test_normal_deletion_is_honored_by_private_rollback(self):
        await self.run_import()
        head = await self.vault.read(self.ctx, "new.md")
        await self.vault.delete(self.ctx, "new.md", expected_revision=head["revision"])
        destination = self.root / "native-deletion"
        receipt = await vault_import.restore_vault(
            self.vault, self.ctx, self.src, "state.db", destination, quiesce=fence
        )
        self.assertEqual(receipt["later_revisions"], 1)
        self.assertFalse((destination / "state/notes/new.md").exists())
        self.assertTrue((self.sealed / "notes/new.md").is_file())
        row = json.loads((destination / "post-import-revisions.jsonl").read_text())
        self.assertIsNone(row["content"])
        self.assertEqual(row["author"], self.person.key)

    async def test_activation_requires_exact_scope_and_verified_digest(self):
        report = await self.run_import()
        proof = dict(
            tenant=self.person.tenant_id,
            agent="agent",
            space="a",
            source_manifest_sha256=report["source_manifest_sha256"],
            corpus_sha256=report["corpus_sha256"],
        )
        await vault_import.require_import_receipt(self.engine, **proof)
        for k, v in (("tenant", "other"), ("space", "b"), ("corpus_sha256", "0" * 64)):
            with self.assertRaises(VaultImportMismatch):
                await vault_import.require_import_receipt(
                    self.engine, **{**proof, k: v}
                )

    async def test_legacy_rollback_requires_continuous_document_source_authorization(
        self,
    ):
        import sqlite3
        from contextlib import closing

        with closing(sqlite3.connect(":memory:")) as con:
            vault_import.guard_legacy_session_reader(con)
            con.execute(
                "CREATE TABLE oa_document_run_sources(run_id, session_id, source_id)"
            )
            vault_import.guard_legacy_session_reader(con)
            # A retained receipt may subsequently be forgotten or lose its audience.
            # Legacy readers interpret neither these bindings nor their token lifetimes.
            con.execute("CREATE TABLE oa_vault_read_receipts(token)")
            con.execute("INSERT INTO oa_vault_read_receipts VALUES ('opaque-revision')")
            con.execute(
                "INSERT INTO oa_document_run_sources VALUES ('r', 's', 'opaque-revision')"
            )
            for retained in (True, False):
                if not retained:
                    con.execute("DELETE FROM oa_vault_read_receipts")
                with self.assertRaises(vault_import.LegacyReaderPrivacyConflict):
                    vault_import.guard_legacy_session_reader(con)

    async def test_standalone_rollback_keeps_new_session_and_note_together(self):
        from openagent_core.contracts import SessionRef
        from openagent_storage_sqlite.migration import restore_to_new_directory

        candidate = self.root / "standalone"
        restore_to_new_directory(self.sealed, candidate)
        path = candidate / "state.db"
        engine = create_async_engine("sqlite+aiosqlite:///" + str(path))
        self.addAsyncCleanup(engine.dispose)
        vault = SqlDocumentVault(
            engine, self.vault.authorizer, self.vault.resolve_space
        )
        await vault.initialize()
        await vault_import.import_vault(vault, self.ctx, self.src, "state.db")
        await vault.write(self.ctx, "later.md", "durable", expected_revision=0)
        current = SqliteRuntimeStore(path)
        await current.start()
        request = RunRequest("later-run", "later-session", "later-key", "question")
        ctx = replace(self.ctx, session_id="later-session")
        await current.accept(request, ctx)
        await current.transition(request.run_id, "running")
        await current.transition(request.run_id, "success", output="answer")
        await current.close()
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from openagent_core.administration import ManagementContext
        from openagent_core.artifacts import ArtifactRepository

        policy = SimpleNamespace(authorize=AsyncMock(return_value=True))
        artifacts = ArtifactRepository(SimpleNamespace(db_path=str(path)), policy)
        authenticated = ManagementContext(self.person, "agent")
        uploaded = await artifacts.upload(
            authenticated,
            b"later attachment",
            filename="later.txt",
            session_id="later-session",
        )
        target = self.root / "return-to-files"
        report = await vault_import.restore_standalone_memory_and_sessions(
            vault,
            ctx,
            self.src,
            "state.db",
            target,
            new_paths=["later.md"],
            quiesce=fence,
        )
        self.assertEqual(report["session_counts"]["sessions_v2"], 2)
        self.assertEqual((target / "state/notes/later.md").read_bytes(), b"durable")
        self.assertEqual(report["verified_artifact_blobs"], 1)
        restored_artifacts = ArtifactRepository(
            SimpleNamespace(db_path=str(target / "state/state.db")), policy
        )
        result = await restored_artifacts.read(
            authenticated, artifact_id=uploaded["artifact_id"]
        )
        self.assertEqual(result.content, b"later attachment")
        recovered = SqliteRuntimeStore(target / "state/state.db")
        await recovered.start()
        self.addAsyncCleanup(recovered.close)
        self.assertEqual((await recovered.get(request.run_id)).output, "answer")
        info = await recovered.read_session(
            SessionRef("host", self.person.tenant_id, "agent", "later-session"),
            ctx,
            limit=30,
        )
        self.assertEqual([m["text"] for m in info["messages"]], ["question", "answer"])
