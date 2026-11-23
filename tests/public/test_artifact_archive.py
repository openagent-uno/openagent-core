"""A real S3-compatible endpoint and PostgreSQL qualify sealed input attachment portability."""

from contextlib import asynccontextmanager, closing
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from aiohttp import ClientSession, web
from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import (
    ExecutionContext,
    PrincipalRef,
    RunRequest,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_core.contracts import SessionRef
from openagent_core.modules import ServiceRegistry
from openagent_core.object_artifacts import S3Chunks, SqlObjectArtifacts
from openagent_core.archived_artifacts import SqlArtifactArchive
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_gateway import create_app
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration


@unittest.skipUnless(
    all(
        os.getenv(k)
        for k in (
            "OPENAGENT_TEST_POSTGRES_DSN",
            "OPENAGENT_TEST_POSTGRES_URL",
            "OPENAGENT_TEST_S3_ENDPOINT",
            "OPENAGENT_TEST_S3_KEY",
            "OPENAGENT_TEST_S3_SECRET",
        )
    ),
    "isolated PostgreSQL and S3 required",
)
class ArtifactArchive(unittest.IsolatedAsyncioTestCase):
    async def test_original_ids_links_unlinked_owner_and_forget_on_gateway(self):
        from aiobotocore.session import get_session
        import psycopg
        from psycopg import sql
        from openagent_storage_postgres import PostgresRuntimeStore
        from openagent_storage_sqlite.artifact_archive import (
            import_artifact_archive,
            verify_artifact_candidate,
        )
        from openagent_storage_sqlite.postgres_stage import StageMismatch

        root = Path(self.enterContext(TemporaryDirectory()))
        source, snapshot = root / "source", root / "snapshot"
        source.mkdir()
        dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
        engine = create_async_engine(os.environ["OPENAGENT_TEST_POSTGRES_URL"])
        self.addAsyncCleanup(engine.dispose)
        pg = self.enterContext(psycopg.connect(dsn, autocommit=True))
        archive_schema, ledger_schema = (
            "oa_archive_" + uuid4().hex,
            "oa_runtime_" + uuid4().hex,
        )
        stage_schema = "oa_stage_" + uuid4().hex
        candidate_schema = "oa_candidate_" + uuid4().hex
        for schema in (archive_schema, ledger_schema, stage_schema, candidate_schema):
            self.addCleanup(
                lambda name=schema: pg.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(name)
                    )
                )
            )
        client = await self.enterAsyncContext(
            get_session().create_client(
                "s3",
                endpoint_url=os.environ["OPENAGENT_TEST_S3_ENDPOINT"],
                region_name="us-east-1",
                aws_access_key_id=os.environ["OPENAGENT_TEST_S3_KEY"],
                aws_secret_access_key=os.environ["OPENAGENT_TEST_S3_SECRET"],
            )
        )
        bucket = "oa-" + uuid4().hex
        await client.create_bucket(Bucket=bucket)

        async def release_own_bucket():
            listing = await client.list_objects_v2(Bucket=bucket)
            for o in listing.get("Contents", ()):
                await client.delete_object(Bucket=bucket, Key=o["Key"])
            await client.delete_bucket(Bucket=bucket)

        self.addAsyncCleanup(release_own_bucket)
        chunks = S3Chunks(client, bucket)
        store = SqliteRuntimeStore(source / "state.sqlite")
        await store.start()
        self.addAsyncCleanup(store.close)
        ledger = PostgresRuntimeStore(dsn, agent_id="agent", schema=ledger_schema)
        await ledger.start()
        self.addAsyncCleanup(ledger.close)
        owner = PrincipalRef("host", uuid4().hex, "owner")

        def context(session, actor=owner):
            return ExecutionContext(actor, actor, actor, session, "agent", (actor,))

        library = context("library")
        for target in (store, ledger):
            await target.create_session(
                SessionRef("host", owner.tenant_id, "agent", "library"),
                library,
                title="library",
            )

        class Policy:
            async def authorize(self, ctx, action, resource, *, audience=()):
                return ctx.tenant_id == resource.tenant_id

        policy = Policy()

        async def resolve_space(ctx):
            return ctx.initiator.key

        vault = SqlDocumentVault(engine, policy, resolve_space)
        await vault.initialize()
        registry = ServiceRegistry({SqlDocumentVault: vault})
        runtime = Runtime(
            RuntimeSettings("agent"),
            RuntimeServices(ledger, None, policy, registry=registry),
        )
        archive = SqlArtifactArchive(
            engine, chunks, schema=archive_schema, runtime=lambda: runtime
        )
        publisher = SqlObjectArtifacts(engine, chunks, policy, archive=archive)
        await publisher.initialize()

        originals, identifiers, saved_runs = [], [], []
        sqlite = store.connection
        for i in range(16):
            data = bytes([i]) * 19
            digest = hashlib.sha256(data).hexdigest()
            key, ident = f"sha256/{digest[:2]}/{digest}", "art_" + uuid4().hex
            kind = "voice" if i < 14 else "video"
            mime = "audio/mpeg" if i < 14 else "video/mp4"
            cas = source / "artifacts" / key
            cas.parent.mkdir(parents=True, exist_ok=True)
            cas.write_bytes(data)
            filename = str(i) + ".bin"
            sqlite.execute(
                """INSERT INTO artifacts(id,tenant_id,owner_principal_id,visibility,direction,kind,mime,
                original_filename,storage_key,sha256,size_bytes,storage_state,retention_class,created_at_ms,updated_at_ms)
                VALUES(?,?,?,'private','input',?,?,?,?,?,?,'available','normal',0,0)""",
                (
                    ident,
                    owner.tenant_id,
                    owner.key,
                    kind,
                    mime,
                    filename,
                    key,
                    digest,
                    len(data),
                ),
            )
            if i < 14:
                sqlite.execute(
                    """INSERT INTO artifact_links(id,tenant_id,resource_type,resource_id,artifact_id,relation,
                   ordinal,display_name,created_at_ms) VALUES(?,?,'session','library',?,'input_attachment',?,?,0)""",
                    ("alink_" + str(i), owner.tenant_id, ident, i, filename),
                )
            if i >= 12:
                ctx = context("thread-" + str(i))
                ref = dict(
                    artifact_id=ident,
                    filename=filename,
                    kind=kind,
                    type=kind,
                    sha256=digest,
                    size_bytes=len(data),
                    mime_type=mime,
                    url=f"/api/artifacts/{ident}/content",
                    path="/original/artifacts/" + key,
                )
                req = RunRequest(
                    "run-" + str(i),
                    ctx.session_id,
                    "key-" + str(i),
                    "inspect",
                    attachments=(ref,),
                )
                await store.accept(req, ctx)
                await store.transition(req.run_id, "running")
                await store.transition(req.run_id, "success", output="received")
                await ledger.accept(req, ctx)
                await ledger.transition(req.run_id, "running")
                saved_runs.append(req.run_id)
            originals.append(data)
            identifiers.append(ident)

        @asynccontextmanager
        async def fence():
            yield "offline-original"

        async def idle(_):
            pass

        await prepare_migration(
            source,
            snapshot,
            root / "clone",
            databases=("state.sqlite",),
            quiesce=fence,
            migrate=idle,
            absent_vault="memories",
        )
        args = dict(
            expected_tenant=owner.tenant_id,
            agent_id="agent",
            legacy_cas_root="/original/artifacts",
            dsn=dsn,
            schema=archive_schema,
            blobs=chunks,
            history_only=True,
            chunk_size=7,
        )
        report = await import_artifact_archive(
            snapshot, "state.sqlite", "memories", **args
        )
        self.assertEqual(
            report["verified"],
            dict(artifacts=16, links=14, request_references=4, bytes=16 * 19),
        )
        self.assertEqual(
            await import_artifact_archive(snapshot, "state.sqlite", "memories", **args),
            report,
        )
        self.assertEqual(
            await import_artifact_archive(
                snapshot, "state.sqlite", "memories", verify_only=True, **args
            ),
            report,
        )
        with closing(sqlite3.connect(snapshot / "state.sqlite")) as db:
            db.row_factory = sqlite3.Row
            verify = dict(
                schema=archive_schema,
                manifest_sha256=report["manifest_sha256"],
                database_sha256=hashlib.sha256(
                    (snapshot / "state.sqlite").read_bytes()
                ).hexdigest(),
                agent_id="agent",
                tenants=owner.tenant_id,
            )
            self.assertEqual(
                verify_artifact_candidate(pg, db, **verify), frozenset(saved_runs)
            )
            with self.assertRaises(StageMismatch):
                verify_artifact_candidate(pg, db, **{**verify, "agent_id": "another"})
            with pg.transaction(force_rollback=True):
                pg.execute(
                    sql.SQL(
                        "UPDATE {}.archive_objects SET descriptor=jsonb_set(descriptor,"
                        "ARRAY['record','original_filename'],'\"substituted\"'::jsonb) WHERE id=%s"
                    ).format(sql.Identifier(archive_schema)),
                    (identifiers[0],),
                )
                with self.assertRaisesRegex(StageMismatch, "metadata differs"):
                    verify_artifact_candidate(pg, db, **verify)

        from openagent_storage_sqlite.postgres_candidate import build_runtime_candidate
        from openagent_storage_sqlite.postgres_stage import transfer_history
        selection = dict(expected_tenant=owner.tenant_id, dsn=dsn, history_only=True)
        source_args = (snapshot, "state.sqlite", "memories")
        transfer_history(*source_args, schema=stage_schema, **selection)
        candidate_args = dict(agent_id="agent", stage_schema=stage_schema, candidate_schema=candidate_schema, **selection)
        with self.assertRaises(StageMismatch):
            build_runtime_candidate(*source_args, **candidate_args)
        candidate_args["artifact_archive_schema"] = archive_schema
        native = build_runtime_candidate(*source_args, **candidate_args)
        self.assertEqual(native["verified_rows"]["runs"], len(saved_runs))
        self.assertEqual(native, build_runtime_candidate(*source_args, verify_only=True, **candidate_args))
        active = PostgresRuntimeStore(dsn, agent_id="agent", schema=candidate_schema)
        await active.start()
        self.addAsyncCleanup(active.close)
        for ident in saved_runs:
            self.assertEqual(await active.accepted_request(ident), await store.accepted_request(ident))

        # Exercise ordinary authenticated download for linked and owner-only source attachments.
        class Host:
            def service(self, key):
                return publisher if key == "attachments.publisher" else None

        bob = replace(owner, subject_id="bob")
        identities = {
            "owner": library,
            "bob": context("library", bob),
            "foreign": context("library", replace(owner, tenant_id="foreign")),
        }

        @asynccontextmanager
        async def resolve(request, ident):
            ctx = identities.get(request.headers.get("Authorization"))
            if ctx is None:
                raise PermissionError("Unknown reader")
            yield ctx

        app = create_app(
            Host(), None, manage_lifecycle=False, resolve_artifact_context=resolve
        )
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        http = await self.enterAsyncContext(ClientSession())

        async def get(i, reader):
            async with http.get(
                url + f"/api/artifacts/{identifiers[i]}/content",
                headers={"Authorization": reader},
            ) as response:
                return response.status, await response.read()

        for i in range(16):
            self.assertEqual(await get(i, "owner"), (200, originals[i]))
            ref = await publisher.resolve(library, identifiers[i])
            self.assertEqual(
                ref.get("artifact_link_id"), "alink_" + str(i) if i < 14 else None
            )
            self.assertNotIn("path", ref)
        for reader in ("foreign", "unknown", "bob"):
            self.assertIn((await get(15, reader))[0], (403, 404))
        protected = context("thread-15")
        await vault.write(protected, "secret.md", "body", expected_revision=0)
        tokens = await vault.note_receipts(
            {"tenant": owner.tenant_id, "agent": "agent", "space": owner.key},
            {("secret.md", 1)},
        )
        await ledger.bind_document_sources(protected, "run-15", tokens)
        self.assertEqual(await get(15, "owner"), (200, originals[15]))
        stream = publisher.read_stream(library, identifiers[15])
        self.assertEqual(await anext(stream), originals[15][:7])
        await vault.forget(protected, "secret.md", expected_revision=1)
        with self.assertRaises(PermissionError):
            await anext(stream)
        self.assertIn((await get(15, "owner"))[0], (403, 404))
        self.assertEqual(await get(14, "owner"), (200, originals[14]))
        with self.assertRaises(LookupError):
            await publisher.resolve_session_output(
                context("thread-14"), "run-14", identifiers[14]
            )
        digest = hashlib.sha256(originals[0][:7]).hexdigest()
        key = chunks.prefix + SqlObjectArtifacts._key(library, digest)
        await client.put_object(Bucket=bucket, Key=key, Body=b"changed")
        with self.assertRaisesRegex((IOError, StageMismatch), "disagree|integrity"):
            await import_artifact_archive(
                snapshot, "state.sqlite", "memories", verify_only=True, **args
            )
        with self.assertRaisesRegex(IOError, "integrity"):
            await anext(publisher.read_stream(library, identifiers[0]))
