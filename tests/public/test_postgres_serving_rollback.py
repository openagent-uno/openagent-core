"""Two independent HTTP deployments switch on the same live ledger and object store."""

import asyncio
import json
import os
import unittest
from contextlib import asynccontextmanager
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from aiohttp import ClientSession, web
from openagent_core import (
    CapabilityCatalog,
    ExecutionContext,
    FunctionSource,
    PrincipalRef,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
    ToolDefinition,
)
from openagent_core.contracts import RunRequest, ServingUnavailable, canonical_json
from openagent_core.durable_operations import DurableOperationRunner, OperationGrant
from openagent_core.engine import AgentExecutor
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.modules import ServiceRegistry
from openagent_core.object_artifacts import S3Chunks, SqlObjectArtifacts
from openagent_core.operations import (
    CompleteOperation,
    OperationInvocation,
    StagedOperation,
    ToolStep,
)
from openagent_gateway import create_app
from sqlalchemy.ext.asyncio import create_async_engine


def digest(value):
    return sha256(canonical_json(value).encode()).hexdigest()


@unittest.skipUnless(
    all(
        os.getenv("OPENAGENT_TEST_" + k)
        for k in ("POSTGRES_DSN", "POSTGRES_URL", "S3_ENDPOINT", "S3_KEY", "S3_SECRET")
    ),
    "disposable PostgreSQL and S3 required",
)
class CloudRollback(unittest.IsolatedAsyncioTestCase):
    async def test_guarded_switch_retains_two_tenants_receipts_revocation_and_future_writes(self):
        import psycopg
        from aiobotocore.session import get_session
        from openagent_storage_postgres import PostgresRuntimeStore
        from openagent_storage_postgres.serving import (
            CloudServingTarget,
            ContractEvidence,
            PostgresServingGeneration,
            switch_serving_generation,
        )
        from psycopg import sql

        dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
        schema, agent_id = "oa_route_" + uuid4().hex, "a-" + uuid4().hex
        pg = self.enterContext(psycopg.connect(dsn, autocommit=True))
        pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.addCleanup(
            lambda: pg.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        )
        pg.execute(sql.SQL("SET search_path TO {}, pg_catalog").format(sql.Identifier(schema)))
        pg.execute("CREATE TABLE host_grants (actor text PRIMARY KEY, artifacts boolean NOT NULL)")
        pg.execute("INSERT INTO host_grants VALUES('a',true),('b',true)")
        http = await self.enterAsyncContext(ClientSession())
        bucket = "oa-" + uuid4().hex
        counts, artifacts = [], {}
        sessions = {"a": "one", "b": "two"}

        class HostPolicy:
            async def authorize(self, ctx, action, resource, *, audience=()):
                if ctx.tenant_id != resource.tenant_id:
                    return False
                return (
                    action != "artifact.read"
                    or pg.execute(
                        "SELECT artifacts FROM host_grants WHERE actor=%s",
                        (ctx.initiator.subject_id,),
                    ).fetchone()[0]
                )

        class Delegations:
            async def capture(self, request, ctx, **binding):
                return OperationGrant("grant-" + request.run_id, binding["expires_at"])

            async def validate(self, ctx):
                return ctx.deferred and bool(ctx.delegation_id)

        def ctx(actor, session=None):
            p = PrincipalRef("test-authority", actor, actor)
            return ExecutionContext(p, p, p, session or sessions[actor], agent_id, (p,))

        async def read(url, path, actor):
            async with http.get(url + path, headers={"Authorization": actor}) as response:
                return response.status, await response.read()

        async def post(url, actor, ident, session=None):
            body = {
                "run_id": ident,
                "session_id": session or sessions[actor],
                "input": "produce",
                "idempotency_key": ident,
            }
            async with http.post(
                url + "/api/v1/runs", json=body, headers={"Authorization": actor}
            ) as response:
                return response.status, await response.json()

        async def localhost(app):
            runner = web.AppRunner(app)
            await runner.setup()
            self.addAsyncCleanup(runner.cleanup)
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            return f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"

        async def make(build, generation):
            engine = create_async_engine(os.environ["OPENAGENT_TEST_POSTGRES_URL"])
            self.addAsyncCleanup(engine.dispose)
            client = await self.enterAsyncContext(
                get_session().create_client(
                    "s3",
                    endpoint_url=os.environ["OPENAGENT_TEST_S3_ENDPOINT"],
                    region_name="us-east-1",
                    aws_access_key_id=os.environ["OPENAGENT_TEST_S3_KEY"],
                    aws_secret_access_key=os.environ["OPENAGENT_TEST_S3_SECRET"],
                )
            )
            if generation == 1:
                await client.create_bucket(Bucket=bucket)

                async def cleanup_own_bucket():
                    listing = await client.list_objects_v2(Bucket=bucket)
                    for item in listing.get("Contents", ()):
                        await client.delete_object(Bucket=bucket, Key=item["Key"])
                    await client.delete_bucket(Bucket=bucket)

                self.addAsyncCleanup(cleanup_own_bucket)
            store = PostgresRuntimeStore(dsn, agent_id=agent_id, schema=schema)
            policy = HostPolicy()
            repo = SqlObjectArtifacts(engine, S3Chunks(client, bucket), policy)
            await repo.initialize()

            async def space(context):
                return context.initiator.key

            vault = SqlDocumentVault(engine, policy, space)
            await vault.initialize()
            grants = Delegations()
            catalog = CapabilityCatalog(policy)

            async def effect(arguments, context):
                counts.append(arguments["key"])
                data = f"receipt for {context.tenant_id}".encode()
                item = await repo.publish_content(
                    context, data, filename="result.txt", mime_type="text/plain"
                )
                artifacts[arguments["key"]] = item["artifact_id"]
                return {"artifact": item["artifact_id"]}

            tools = FunctionSource((ToolDefinition("write", "Publish", {}),), {"write": effect})
            catalog.register("records", tools, tools, target_label="Records")

            def step(state):
                if "produce" not in state.results:
                    return ToolStep(
                        "produce",
                        "records",
                        "write",
                        {"key": state.origin_run_id},
                        idempotent=False,
                    )
                return CompleteOperation("receipt committed")

            definitions = (StagedOperation("write-result", 1, step),)

            def select(request, context):
                return OperationInvocation("write-result", inputs={})

            inert = SimpleNamespace(
                config={}, capability_pool=None, initialize=AsyncMock(), shutdown=AsyncMock()
            )
            executor = AgentExecutor(inert, operations=definitions, resolve_operation=select)
            durable = DurableOperationRunner(
                operations=definitions, resolve_operation=select, delegation=grants
            )
            reg = ServiceRegistry({"attachments.publisher": repo, SqlDocumentVault: vault})
            runtime = Runtime(
                RuntimeSettings(agent_id),
                RuntimeServices(store, executor, policy, catalog, registry=reg, delegations=grants),
                modules=(executor,),
            )
            await runtime.start()
            self.addAsyncCleanup(runtime.close)
            # The prospective build has its background scheduler stopped. start()
            # binds reads; close() drains the observer while preserving replay.
            await durable.start(runtime)
            self.addAsyncCleanup(durable.close)
            await durable.close()
            gate = PostgresServingGeneration(
                store, deployment=build, generation=generation, binding_sha256=digest("bindings")
            )
            await gate.open()
            self.addAsyncCleanup(gate.close)
            if generation == 1:
                await gate.install()

            @asynccontextmanager
            async def identity(request, action, run_id, session_id):
                actor = request.headers.get("Authorization")
                if actor not in sessions:
                    raise PermissionError()
                yield ctx(actor, session_id)

            @asynccontextmanager
            async def identity_artifact(request, ident):
                actor = request.headers.get("Authorization")
                if actor not in sessions:
                    raise PermissionError()
                yield ctx(actor)

            def app(**extra):
                return create_app(
                    runtime,
                    identity,
                    manage_lifecycle=False,
                    durable_operations=durable,
                    resolve_artifact_context=identity_artifact,
                    **extra,
                )

            public = await localhost(app(serving_generation=gate))
            private = app()

            @web.middleware
            async def read_only(req, handler):
                if req.method != "GET":
                    raise web.HTTPForbidden()
                return await handler(req)

            private.middlewares.insert(0, read_only)
            internal = await localhost(private)

            async def probe():
                allowed, denied, effects = [], [], []
                for actor, ident in (("a", "run-a"), ("b", "run-b")):
                    for suffix in ("", "/events", "/final-answer"):
                        status, body = await read(internal, "/api/v1/runs/" + ident + suffix, actor)
                        self.assertEqual(status, 200, body)
                        allowed.append(json.loads(body))
                    other = "b" if actor == "a" else "a"
                    status, _ = await read(internal, "/api/v1/runs/" + ident, other)
                    self.assertIn(status, (403, 404))
                    denied.append(status)
                    path = "/api/artifacts/" + artifacts[ident] + "/content"
                    status, cas = await read(internal, path, actor)
                    if actor == "b":
                        self.assertEqual(status, 403)
                        denied.append(status)
                    else:
                        self.assertEqual((status, cas), (200, b"receipt for a"))
                        allowed.append(sha256(cas).hexdigest())
                    async with store._db() as db:
                        row = await (
                            await db.execute(
                                "SELECT intent,result,error,finished FROM tool_calls WHERE agent=%s AND run_id=%s",
                                (agent_id, ident),
                            )
                        ).fetchone()
                    self.assertTrue(row["finished"])
                    effects.append((row, (await store.durable_operation(ident))["state"]))
                    if actor == "a":
                        allowed.append((await vault.read(ctx(actor), "personal.md"))["content"])
                    else:
                        with self.assertRaises(LookupError):
                            await vault.read(ctx(actor), "personal.md", revision=1)
                        denied.append("forgotten")
                return ContractEvidence(digest(allowed), digest(denied), digest(effects))

            return CloudServingTarget(gate, repo, vault, probe), public, durable

        @asynccontextmanager
        async def fence(source, target):
            # Both observers have been drained. Pin the remaining host policy
            # database for all authenticated observations and the route commit.
            async with await psycopg.AsyncConnection.connect(dsn) as lock_db:
                await lock_db.execute(
                    sql.SQL("LOCK TABLE {}.host_grants IN SHARE MODE NOWAIT").format(
                        sql.Identifier(schema)
                    )
                )
                yield digest([schema, source.generation, target.generation, "host-writers-drained"])

        async def switch(a, b, *, change_id):
            return await switch_serving_generation(a, b, change_id=change_id, writer_fence=fence)

        current, blue, operations = await make("current-image", 1)
        standby, green, old_ops = await make("previous-contract-on-cloud", 2)
        self.assertEqual((await post(green, "a", "wrong"))[0], 503)

        async def finish(runner, ident, actor):
            async with asyncio.timeout(10):
                while True:
                    r = await runner.get_run(ident, ctx(actor))
                    if r.terminal:
                        self.assertEqual((r.status, r.output), ("success", "receipt committed"))
                        await runner.tick()
                        return
                    await asyncio.sleep(0.025)

        for actor in ("a", "b"):
            ident = "run-" + actor
            status, body = await post(blue, actor, ident)
            self.assertEqual(status, 202, body)
            await finish(operations, ident, actor)
        self.assertEqual(counts, ["run-a", "run-b"])
        self.assertEqual(
            (await read(blue, "/api/artifacts/" + artifacts["run-b"] + "/content", "b"))[0], 200
        )
        pg.execute("UPDATE host_grants SET artifacts=false WHERE actor='b'")
        for actor in ("a", "b"):
            await current.documents.write(
                ctx(actor), "personal.md", "note-" + actor, expected_revision=0
            )
        await current.documents.forget(ctx("b"), "personal.md", expected_revision=1)
        change_id = uuid4()
        async with current.route.serving():
            # Concurrent readers share one lock connection and never consume
            # the operator slot, including during a slow authorized download.
            async def concurrent_reader():
                async with current.route.serving():
                    await asyncio.sleep(0.005)

            await asyncio.gather(*(concurrent_reader() for _ in range(40)))
            with self.assertRaisesRegex(ServingUnavailable, "Drain"):
                await switch(current, standby, change_id=change_id)
        await current.route.store.accept(RunRequest("pending", "one", "pending", "wait"), ctx("a"))
        with self.assertRaisesRegex(ServingUnavailable, "pending"):
            await switch(current, standby, change_id=change_id)
        await current.route.store.transition("pending", "cancelled")

        async def wrong_probe():
            return ContractEvidence(digest("wrong"), digest("wrong"), digest("wrong"))

        with self.assertRaisesRegex(ValueError, "disagrees"):
            await switch(current, replace(standby, probe=wrong_probe), change_id=change_id)
        wrong_prefix = SqlObjectArtifacts(
            standby.artifacts.engine,
            S3Chunks(standby.artifacts.blobs.client, bucket, prefix="wrong/"),
            standby.artifacts.authorizer,
        )
        with self.assertRaisesRegex(ValueError, "namespace"):
            await switch(current, replace(standby, artifacts=wrong_prefix), change_id=change_id)
        invisible = create_async_engine(
            os.environ["OPENAGENT_TEST_POSTGRES_URL"],
            connect_args={"options": "-c search_path=pg_catalog"},
        )
        self.addAsyncCleanup(invisible.dispose)
        wrong_index = SqlObjectArtifacts(
            invisible, standby.artifacts.blobs, standby.artifacts.authorizer
        )
        with self.assertRaisesRegex(ValueError, "relation"):
            await switch(current, replace(standby, artifacts=wrong_index), change_id=change_id)
        readonly = create_async_engine(
            os.environ["OPENAGENT_TEST_POSTGRES_URL"],
            connect_args={"options": "-c default_transaction_read_only=on"},
        )
        self.addAsyncCleanup(readonly.dispose)
        readonly_index = SqlObjectArtifacts(
            readonly, standby.artifacts.blobs, standby.artifacts.authorizer
        )
        with self.assertRaisesRegex(ValueError, "read-only"):
            await switch(current, replace(standby, artifacts=readonly_index), change_id=change_id)
        self.assertIsNone(await current.route.receipt(change_id))
        receipt = await switch(current, standby, change_id=change_id)
        self.assertEqual(receipt, await switch(current, standby, change_id=change_id))
        self.assertEqual(receipt, await standby.route.receipt(change_id))
        for witness in receipt["object_witnesses"]:
            self.assertEqual(
                sha256(
                    await standby.artifacts.blobs.get("serving-witness/" + witness, 32)
                ).hexdigest(),
                witness,
            )
        self.assertEqual((await post(blue, "a", "stale"))[0], 503)
        self.assertEqual((await post(green, "a", "run-a"))[0], 202)
        self.assertEqual(counts, ["run-a", "run-b"])
        self.assertEqual(
            (await read(green, "/api/artifacts/" + artifacts["run-b"] + "/content", "b"))[0], 403
        )
        self.assertEqual(
            (await read(green, "/api/artifacts/" + artifacts["run-a"] + "/content", "b"))[0], 403
        )
        self.assertEqual((await post(green, "a", "after-rollback"))[0], 202)
        await finish(old_ops, "after-rollback", "a")
        self.assertEqual(
            (await read(green, "/api/artifacts/" + artifacts["after-rollback"] + "/content", "a")),
            (200, b"receipt for a"),
        )
        restored, old, restored_ops = await make("current-image", 3)
        self.assertEqual((await switch(standby, restored, change_id=uuid4()))["generation"], 3)
        with self.assertRaises(ServingUnavailable):
            await switch(current, standby, change_id=change_id)
        self.assertEqual((await read(green, "/api/v1/runs/after-rollback", "a"))[0], 503)
        self.assertEqual((await post(old, "a", "after-rollback"))[0], 202)
        await finish(restored_ops, "after-rollback", "a")
        self.assertEqual(counts, ["run-a", "run-b", "after-rollback"])
        self.assertEqual(
            (await read(old, "/api/artifacts/" + artifacts["run-b"] + "/content", "b"))[0], 403
        )


@unittest.skipUnless(
    os.getenv("OPENAGENT_TEST_POSTGRES_DSN") and os.getenv("OPENAGENT_TEST_POSTGRES_URL"),
    "disposable PostgreSQL required",
)
class DocumentOnlyRollback(unittest.IsolatedAsyncioTestCase):
    async def test_object_free_agent_checks_live_documents_and_never_needs_an_s3_client(self):
        import psycopg
        from openagent_storage_postgres import PostgresRuntimeStore
        from openagent_storage_postgres.serving import (
            CloudServingTarget,
            ContractEvidence,
            PostgresServingGeneration,
            switch_serving_generation,
        )
        from psycopg import sql

        schema = "oa_text_" + uuid4().hex
        agent = "text-" + uuid4().hex
        dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
        pg = self.enterContext(psycopg.connect(dsn, autocommit=True))
        pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.addCleanup(
            lambda: pg.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        )
        targets = []

        async def resolve(_):
            return "personal"

        policy = SimpleNamespace(authorize=AsyncMock(return_value=True))
        owner = PrincipalRef("private", "owner", "owner")
        ctx = ExecutionContext(owner, owner, owner, "thread", agent, (owner,))
        for i in (1, 2):
            store = PostgresRuntimeStore(dsn, agent_id=agent, schema=schema)
            await store.start()
            self.addAsyncCleanup(store.close)
            route = PostgresServingGeneration(
                store,
                deployment="build-" + str(i),
                generation=i,
                binding_sha256=digest("document-only"),
            )
            await route.open()
            self.addAsyncCleanup(route.close)
            if i == 1:
                await route.install()
                await store.accept(RunRequest("conversation", "thread", "message", "remember"), ctx)
                await store.transition("conversation", "success", output="complete")
            engine = create_async_engine(os.environ["OPENAGENT_TEST_POSTGRES_URL"])
            self.addAsyncCleanup(engine.dispose)
            vault = SqlDocumentVault(engine, policy, resolve)
            await vault.initialize()
            if i == 1:
                await vault.write(ctx, "recent.md", "post-cutover content", expected_revision=0)
                await vault.write(ctx, "forgotten.md", "private", expected_revision=0)
                await vault.forget(ctx, "forgotten.md", expected_revision=1)

            async def probe(v=vault, s=store):
                self.assertEqual(
                    (await v.read(ctx, "recent.md"))["content"], "post-cutover content"
                )
                with self.assertRaises(LookupError):
                    await v.read(ctx, "forgotten.md", revision=1)
                self.assertEqual((await s.get("conversation")).output, "complete")
                return ContractEvidence(
                    digest("post-cutover content"), digest("forgotten"), digest("complete")
                )

            targets.append(CloudServingTarget(route, None, vault, probe))
        a, b = targets

        @asynccontextmanager
        async def fence(start, end):
            yield digest([start.generation, end.generation])

        change = uuid4()
        with self.assertRaisesRegex(ValueError, "contract differs"):
            await switch_serving_generation(
                a, replace(b, artifacts=object()), change_id=change, writer_fence=fence
            )
        result = await switch_serving_generation(a, b, change_id=change, writer_fence=fence)
        self.assertEqual(result["object_witnesses"], [])
        self.assertEqual(await b.route.receipt(change), result)
        async with b.route.serving():
            self.assertEqual(
                (await b.documents.read(ctx, "recent.md"))["content"], "post-cutover content"
            )
