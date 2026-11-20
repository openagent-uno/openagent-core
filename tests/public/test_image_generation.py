"""Image models migrate without touching existing ids and render bounded images."""
import base64
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from openagent_core.image_generation import (
    GeneratedImage, ImageGenerationError, generate_image_bytes, images_endpoint,
)
from openagent_core.memory.db import MemoryDB


class ImageCatalogTests(unittest.IsolatedAsyncioTestCase):
    async def test_image_model_is_separate_from_llm_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / "agent.sqlite")
            db = MemoryDB(path)
            await db.connect()
            provider = await db.upsert_provider(name="openai", framework="api-based", api_key="test")
            llm = await db.upsert_model(provider_id=provider, model="gpt-test")
            await db.close()
            # Simulate the beta schema still deployed in existing agents.
            with sqlite3.connect(path) as conn:
                conn.execute("DROP TABLE models")
                conn.execute("""CREATE TABLE models (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
                    model TEXT NOT NULL, display_name TEXT, tier_hint TEXT, description TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1, is_classifier INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    kind TEXT NOT NULL DEFAULT 'llm' CHECK (kind IN ('llm','tts','stt')),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    UNIQUE(provider_id,model))""")
                conn.execute("INSERT INTO models(id,provider_id,model,kind,created_at,updated_at) VALUES(?,?,?,?,1,1)",
                             (llm, provider, "gpt-test", "llm"))
            db = MemoryDB(path)
            await db.connect()
            image = await db.upsert_model(provider_id=provider, model="gpt-image-test", kind="image")
            self.assertEqual(llm, (await db.get_model(llm))["id"])
            self.assertEqual("image", (await db.get_model(image))["kind"])
            self.assertEqual(["gpt-test"], [row["model"] for row in await db.list_models(kind="llm")])
            await db.close()


class ImageTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_natural_image_tool_recovers_through_the_enabled_catalog(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import json, os, threading
        from openagent_core.mcp.servers.media_gen import server as media_server
        png = b'\x89PNG\r\n\x1a\nfixture-image'
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
                key = self.headers.get('Authorization'); seen.append((key, body['model']))
                limited = key == 'Bearer limited-fixture'
                result = ({'error': {'type': 'rate_limit_error', 'message': 'quota'}} if limited else
                          {'data': [{'b64_json': base64.b64encode(png).decode()}]})
                encoded = json.dumps(result).encode()
                self.send_response(429 if limited else 200); self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                path = str(Path(temporary) / 'agent.db'); db = MemoryDB(path); await db.connect()
                try:
                    for name, key in [('first', 'limited-fixture'), ('second', 'healthy-fixture')]:
                        pid = await db.upsert_provider(name=name, framework='api-based', api_key=key,
                            base_url=f'http://127.0.0.1:{server.server_port}/v1')
                        await db.upsert_model(provider_id=pid, model='image-fixture', kind='image')
                    target = Path(temporary) / 'result.png'
                    with patch.dict(os.environ, {'OPENAGENT_IMAGE_BASE_URL':'', 'OPENAI_API_KEY':''}), \
                         patch.object(media_server._conn, 'get', AsyncMock(return_value=db._conn)), \
                         patch('openagent_core.mcp.servers._common.db_path', return_value=path), \
                         patch.object(media_server, '_cache_path', return_value=target):
                        result = await media_server.generate_image('A blue circle')
                    self.assertTrue(result['ok'], result)
                    self.assertEqual(target.read_bytes(), png)
                    self.assertEqual(seen, [('Bearer limited-fixture','image-fixture'), ('Bearer healthy-fixture','image-fixture')])
                finally:
                    await db.close()
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    async def test_capable_chat_model_uses_provider_image_id(self):
        from openagent_core.mcp.servers.media_gen.server import _capability_backend
        from openagent_core.mcp.servers.media_gen import server as media_server

        with tempfile.TemporaryDirectory() as folder:
            db = MemoryDB(str(Path(folder) / "agent.sqlite"))
            await db.connect()
            provider = await db.upsert_provider(
                name="codex", framework="api-based", api_key="test",
                base_url="http://codex-proxy.test/v1",
            )
            await db.upsert_model(
                provider_id=provider, model="codex:gpt-5.6-sol:high",
                metadata={"capabilities": ["chat", "image_generation"],
                          "image_model_id": "gpt-5.6-sol:high"},
            )
            with patch.object(media_server._conn, "get", new=AsyncMock(return_value=db._conn)):
                backend = await _capability_backend()
                qualified = await _capability_backend("codex:codex:gpt-5.6-sol:high")
            self.assertEqual(
                ("http://codex-proxy.test/v1/images/generations", "test", "gpt-5.6-sol:high"),
                backend,
            )
            self.assertEqual(backend, qualified)
            await db.close()

    async def test_configured_image_returns_bytes_and_type(self):
        png = b"\x89PNG\r\n\x1a\n" + b"content"

        class Response:
            status_code = 200
            def json(self): return {"data": [{"b64_json": base64.b64encode(png).decode()}]}

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def post(self, url, headers, json):
                self.payload = json
                return Response()

        client = Client()
        with patch("openagent_core.image_generation.httpx.AsyncClient", return_value=client):
            image = await generate_image_bytes(
                endpoint=images_endpoint("", "openai"), api_key="test",
                model="gpt-image-test", prompt="A lighthouse",
            )
        self.assertEqual(png, image.content)
        self.assertEqual("image/png", image.mime_type)
        self.assertEqual("gpt-image-test", client.payload["model"])

    async def test_media_tool_returns_channel_delivery_marker(self):
        from openagent_core.mcp.servers.media_gen.server import generate_image

        png = b"\x89PNG\r\n\x1a\ncontent"
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "render.png"
            with patch("openagent_core.mcp.servers.media_gen.server._image_backend",
                       new=AsyncMock(return_value=(("https://example.test/v1/images/generations", "key", "image-test"), None))), \
                 patch("openagent_core.image_generation.generate_image_bytes",
                       new=AsyncMock(return_value=GeneratedImage(png, "image/png", "image-test", "1024x1024"))), \
                 patch("openagent_core.mcp.servers.media_gen.server._cache_path", return_value=target):
                result = await generate_image("A lighthouse")
            self.assertEqual(png, target.read_bytes())
            self.assertEqual(f"[IMAGE:{target}]", result["send_marker"])


class ImageRefusalTests(unittest.IsolatedAsyncioTestCase):
    """A refusal is reported with the provider's own cause, not just a status."""

    @staticmethod
    def _client(status, body):
        class Response:
            status_code = status
            def json(self):
                if isinstance(body, Exception):
                    raise body
                return body

        class Client:
            async def __aenter__(self): return self
            async def __aexit__(self, *_): pass
            async def post(self, url, headers, json): return Response()

        return patch("openagent_core.image_generation.httpx.AsyncClient", return_value=Client())

    async def _refusal(self, status, body):
        with self._client(status, body), self.assertRaises(ImageGenerationError) as caught:
            await generate_image_bytes(endpoint="https://example.test/v1/images/generations",
                                       api_key="key", model="image-test", prompt="A lighthouse")
        return caught.exception

    async def test_the_providers_message_and_error_type_are_kept(self):
        error = await self._refusal(429, {"error": {
            "message": "No account is available.\n  Last error: the session has ended, sign in again.",
            "type": "no_available_account", "code": 429}})
        self.assertEqual((429, "no_available_account"), (error.status, error.kind))
        self.assertEqual("the image provider answered HTTP 429 (no_available_account): No account is "
                         "available. Last error: the session has ended, sign in again.", str(error))

    async def test_other_error_shapes_and_unreadable_bodies_still_report_the_status(self):
        self.assertEqual("the image provider answered HTTP 400: Unsupported size",
                         str(await self._refusal(400, {"detail": "Unsupported size"})))
        self.assertEqual("the image provider answered HTTP 403 (forbidden): no",
                         str(await self._refusal(403, {"error": {"message": "no", "code": "forbidden"}})))
        self.assertEqual("the image provider answered HTTP 502",
                         str(await self._refusal(502, ValueError("not json"))))
        long = await self._refusal(500, {"error": "x" * 5000})
        self.assertLessEqual(len(str(long)), 650)

    async def test_the_tool_result_names_the_cause(self):
        from openagent_core.mcp.servers.media_gen.server import generate_image

        refused = ImageGenerationError(429, "The session has ended, sign in again.", "no_available_account")
        with patch("openagent_core.mcp.servers.media_gen.server._image_backend",
                   new=AsyncMock(return_value=(("https://example.test/v1/images/generations", "key", "image-test"), None))), \
             patch("openagent_core.image_generation.generate_image_bytes", new=AsyncMock(side_effect=refused)):
            result = await generate_image("A lighthouse")
        self.assertEqual({"ok": False, "status": 429, "error_type": "no_available_account",
                          "reason": "image generation failed: the image provider answered HTTP 429 "
                                    "(no_available_account): The session has ended, sign in again."}, result)


class AutomaticImageAcceptanceBoundary(unittest.IsolatedAsyncioTestCase):
    async def test_only_preacceptance_connection_failure_and_explicit_quota_can_retry(self):
        import httpx
        from openagent_core.image_generation import generate_catalog_image
        from openagent_core.models.providers.fallback import reset_breaker
        real_client=httpx.AsyncClient
        png=b'\x89PNG\r\n\x1a\nfixture-image'
        for failure in ('download','connection','server','policy','quota'):
            with self.subTest(failure=failure),tempfile.TemporaryDirectory() as temporary:
                reset_breaker();posts=[]
                db=MemoryDB(str(Path(temporary)/'state.sqlite'));await db.connect()
                try:
                    for name in ('first','second'):
                        pid=await db.upsert_provider(name=name,framework='api-based',api_key='fixture',base_url=f'https://{name}.example.test/v1')
                        await db.upsert_model(provider_id=pid,model='fixture-image',kind='image')
                    def handle(request):
                        if request.method=='GET':raise httpx.ConnectError('CDN unavailable',request=request)
                        posts.append(request.url.host)
                        if request.url.host=='first.example.test':
                            if failure=='connection':raise httpx.ConnectError('connection unavailable',request=request)
                            if failure=='download':return httpx.Response(200,json={'data':[{'url':'https://cdn.example.test/already-generated.png'}]})
                            status={'server':500,'policy':403,'quota':429}[failure]
                            return httpx.Response(status,json={'error':{'type':'content_policy' if failure=='policy' else 'fixture','message':'fixture'}})
                        return httpx.Response(200,json={'data':[{'b64_json':base64.b64encode(png).decode()}]})
                    def client(*args,**kwargs):return real_client(transport=httpx.MockTransport(handle))
                    with patch('openagent_core.image_generation.httpx.AsyncClient',side_effect=client):
                        if failure in ('connection','quota'):
                            image=await generate_catalog_image(db,prompt='A blue circle')
                            self.assertEqual(image.content,png)
                            self.assertEqual(posts,['first.example.test','second.example.test'])
                        else:
                            with self.assertRaises((httpx.ConnectError,ImageGenerationError)):
                                await generate_catalog_image(db,prompt='A blue circle')
                            self.assertEqual(posts,['first.example.test'])
                finally:await db.close();reset_breaker()
