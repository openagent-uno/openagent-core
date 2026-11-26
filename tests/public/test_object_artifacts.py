import os
import base64
import uuid
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import create_async_engine
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.runtime import execution_scope
from openagent_core.object_artifacts import S3Chunks, SqlObjectArtifacts


class Chunks(dict):
    async def put(self, key, data):
        self[key] = data

    async def get(self, key, size):
        return self[key]


class ObjectArtifacts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.denied = set()
        self.directory = Path(self.enterContext(TemporaryDirectory()))
        self.engine = create_async_engine(os.getenv("OPENAGENT_TEST_POSTGRES_URL")
                                         or "sqlite+aiosqlite:///" + str(self.directory / "refs.sqlite"))
        self.addAsyncCleanup(self.engine.dispose)
        self.blobs = Chunks()
        self.store = SqlObjectArtifacts(self.engine, self.blobs, self, chunk_bytes=4, max_bytes=32)
        await self.store.initialize()
        a = PrincipalRef("host", uuid.uuid4().hex, "alice")
        self.ctx = ExecutionContext(a,a,a,"private","agent",(a,))

    async def authorize(self, context, action, resource, *, audience=()):
        return action not in self.denied and resource.tenant_id == context.tenant_id

    def service(self, key):
        return None

    async def test_retry_and_stream(self):
        with execution_scope(self, self.ctx, "producing-run"):
            async def publish():
                return await self.store.publish_content(self.ctx, b"0123456789", filename="fixture.bin",
                                                        mime_type="application/octet-stream")
            ref = await publish()
            self.assertNotIn("path", ref)
            self.assertEqual(await publish(), ref)
        ident = ref["artifact_id"]
        self.assertEqual(await self.store.resolve_session_output(self.ctx, "producing-run", ident), ref)
        with self.assertRaises(LookupError):
            await self.store.resolve_session_output(self.ctx, "another-run", ident)
        with self.assertRaises(LookupError):
            await self.store.resolve(replace(self.ctx, session_id="other"), ident)
        bob = replace(self.ctx.initiator, subject_id="bob")
        with self.assertRaises(LookupError):
            await self.store.resolve(replace(self.ctx, audience=(self.ctx.initiator, bob)), ident)
        self.assertEqual([chunk async for chunk in self.store.read_stream(self.ctx, ident)], [b"0123", b"4567", b"89"])
        chunks = self.store.read_stream(self.ctx, ident)
        self.assertEqual(await anext(chunks), b"0123")
        self.denied.add("artifact.publish")
        with self.assertRaises(PermissionError):
            await anext(chunks)

    async def test_tool_file_requires_matching_run_scoped_publisher_receipt(self):
        from openagent_core.artifacts import publish_tool_file_resources
        from openagent_core.contracts import require_authorized

        data = b"fixture bytes"
        blob = base64.b64encode(data).decode("ascii")
        envelope = {"content": [{"type": "resource",
            "resource": {"uri": "urn:fixture:download", "blob": blob},
            "_meta": {"openagent/file-delivery": {"filename": "fixture.dat"}}}]}

        async def authorize(context, action, resource, *, audience=()):
            await require_authorized(self, context, action, resource, audience=audience)

        runtime = SimpleNamespace(services=SimpleNamespace(authorizer=self, executor=None),
            authorize=authorize, service={"attachments.publisher": self.store}.get)
        run_id = "fixture-file-offer"
        with execution_scope(runtime, self.ctx, run_id):
            projected, (marker,) = await publish_tool_file_resources(envelope)
            self.assertIn(marker, projected["content"][0]["text"])
            self.assertNotIn(blob, str(projected))
            ident = marker.removeprefix("[FILE:artifact:").removesuffix("]")
            ref = await self.store.resolve_session_output(self.ctx, run_id, ident)
            self.assertEqual(ref["size_bytes"], len(data))
            self.assertEqual(b"".join([chunk async for chunk in self.store.read_stream(self.ctx, ident)]), data)

            async def resolve_wrong_size(context, owner_run, artifact_id):
                return {**await self.store.resolve_session_output(context, owner_run, artifact_id),
                        "size_bytes": len(data) + 1}

            unreliable = SimpleNamespace(publish_content=self.store.publish_content,
                resolve_session_output=resolve_wrong_size)
            runtime.service = {"attachments.publisher": unreliable}.get
            rejected, markers = await publish_tool_file_resources(envelope)
            self.assertEqual(markers, [])
            self.assertIn("could not be added", rejected["content"][0]["text"])
            self.assertNotIn(blob, str(rejected))

    async def test_private_ingress_upload_model_content_and_publication_recheck(self):
        from openagent_core.artifacts import ModelAttachmentReader
        from openagent_core.core.agent import _prepare_runtime_media
        from openagent_core.runtime import check_current_publication

        data = b"verified text"
        ref = await self.store.upload(self.ctx, data, filename="input.txt", mime_type="text/plain")
        self.assertNotIn("artifact_link_id", ref)
        self.assertEqual(await self.store.resolve(self.ctx, ref["artifact_id"]), ref)
        self.assertEqual(await self.store.upload(self.ctx,data,filename="input.txt",mime_type="text/plain"), ref)
        self.assertEqual((await self.store.read(self.ctx,artifact_id=ref["artifact_id"])).content,data)
        with self.assertRaises(LookupError):
            await self.store.resolve_session_output(self.ctx,"run",ref["artifact_id"])
        with self.assertRaises(PermissionError):
            await self.store.upload(self.ctx,data,filename="input.txt",session_id="unrelated")
        bob = replace(self.ctx.author,subject_id="bob")
        with self.assertRaises(LookupError):
            await self.store.read(replace(self.ctx,author=bob,initiator=bob,audience=(bob,)),artifact_id=ref["artifact_id"])
        publisher = self.store
        class Host:
            def service(self, key):
                return publisher if key is ModelAttachmentReader else None
        with execution_scope(Host(),self.ctx,"run"):
            with self.assertRaises(PermissionError):
                await self.store.model_content(self.ctx,{**ref,"path":"/never/open"})
            with self.assertRaises(ValueError):
                await self.store.model_content(self.ctx,{**ref,"sha256":"changed"})
            with self.assertRaises(ValueError):
                await self.store.model_content(self.ctx,ref,max_bytes=3)
            refs, (_, _, _, files) = await _prepare_runtime_media([ref])
            self.assertEqual(files[0].content,data)
            self.assertFalse(files[0].filepath)
            self.assertNotIn("path",refs[0])
            self.assertEqual(refs[0]["artifact_id"],ref["artifact_id"])
            output = await self.store.upload(self.ctx,b"audio",filename="speech.wav",mime_type="audio/wav",
                                             session_id=self.ctx.session_id)
            self.assertEqual(await self.store.resolve_session_output(self.ctx,"run",output["artifact_id"]),output)
            self.denied.add("artifact.read")
            with self.assertRaises(PermissionError):
                await check_current_publication()

    async def test_corrupt_chunk_and_oversized_stream(self):
        ref = await self.store.publish_content(self.ctx, b"abc", filename="x", mime_type="text/plain")
        self.blobs[next(iter(self.blobs))] = b"bad"
        with self.assertRaises(IOError):
            await anext(self.store.read_stream(self.ctx, ref["artifact_id"]))
        with self.assertRaises(ValueError):
            await self.store.publish_content(self.ctx, b"x"*33, filename="x", mime_type="text/plain")

    async def test_authentication_on_the_download_url(self):
        from contextlib import asynccontextmanager
        from aiohttp import ClientSession, web
        from openagent_gateway import create_app

        ref = await self.store.publish_content(self.ctx, b"gold", filename="a.bin", mime_type="application/octet-stream")
        @asynccontextmanager
        async def authorize(request, ident):
            if request.headers.get("Authorization") != "Bearer reader":
                raise PermissionError("Unknown identity")
            yield self.ctx
        class Host:
            def service(_, key):
                return self.store if key == "attachments.publisher" else None
        app = create_app(Host(), None, manage_lifecycle=False, resolve_artifact_context=authorize)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        url = f"http://127.0.0.1:{runner.addresses[0][1]}" + ref["url"]
        async with ClientSession() as client:
            async with client.get(url) as denied:
                self.assertEqual(denied.status, 403)
            async with client.get(url, headers={"Authorization": "Bearer reader"}) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(await response.read(), b"gold")
                self.assertIn("no-store", response.headers["Cache-Control"])

    @unittest.skipUnless(os.getenv("OPENAGENT_TEST_S3_ENDPOINT"), "dedicated private S3 test service required")
    async def test_s3_server(self):
        from aiobotocore.session import get_session
        client = await self.enterAsyncContext(get_session().create_client(
            "s3", endpoint_url=os.environ["OPENAGENT_TEST_S3_ENDPOINT"], region_name="us-east-1",
            aws_access_key_id=os.environ["OPENAGENT_TEST_S3_KEY"],
            aws_secret_access_key=os.environ["OPENAGENT_TEST_S3_SECRET"]))
        bucket = "oa-" + uuid.uuid4().hex
        await client.create_bucket(Bucket=bucket)
        self.store.blobs = S3Chunks(client, bucket)
        await self.test_retry_and_stream()
