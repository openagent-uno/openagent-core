"""Explicit OpenAI STT route: real HTTP, no vendor import or external API."""
import builtins
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from openagent_core import audio_worker


class OpenAITranscriptionRoute(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.status, self.body = 200, json.dumps({"text": " the current question "}).encode()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                owner.requests.append((self.path, self.headers.get("Authorization"), body))
                self.send_response(owner.status)
                self.send_header("Content-Length", str(len(owner.body)))
                self.send_header("Location", "/unexpected-redirect")
                self.end_headers()
                self.wfile.write(owner.body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "synthetic.wav"
        self.path.write_bytes(b"RIFF" + b"\0" * 40)
        self.row = {"provider_name": "openai", "model": "whisper-1", "api_key": "fixture-only",
                    "base_url": f"http://127.0.0.1:{self.server.server_port}/v1",
                    "metadata": {"language": "it", "prompt": "fixture vocabulary"}}

    async def asyncTearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.directory.cleanup()

    async def invoke(self, **kwargs):
        return await audio_worker.execute({"operation": "transcribe", "row": self.row,
                                           "path": str(self.path), **kwargs})

    async def test_explicit_route_does_not_import_vendor_aggregator_and_preserves_fields(self):
        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name == "litellm" or name.startswith("litellm.") or name.endswith("voice.voice"):
                raise AssertionError("The OpenAI route must not load the vendor aggregator")
            return original(name, *args, **kwargs)

        with patch("builtins.__import__", guarded):
            self.assertEqual({"text": "the current question"}, await self.invoke(language="en"))
        self.assertEqual(1, len(self.requests))
        path, auth, body = self.requests[0]
        self.assertEqual("/v1/audio/transcriptions", path)
        self.assertEqual("Bearer fixture-only", auth)
        for value in (b"whisper-1", b"synthetic.wav", b"fixture vocabulary", b'\r\nen\r\n'):
            self.assertIn(value, body)

    async def test_redirect_and_provider_errors_never_send_credentials_again(self):
        for status in (302, 401, 429, 500):
            with self.subTest(status=status):
                self.status = status
                before = len(self.requests)
                with self.assertRaises(RuntimeError):
                    await self.invoke()
                self.assertEqual(before + 1, len(self.requests))

    async def test_invalid_and_oversized_transcripts_fail_explicitly(self):
        for body in (b"not json", b"[]", b'{"text":42}', b"x" * ((256 << 10) + 1)):
            with self.subTest(size=len(body)):
                self.body = body
                with self.assertRaises((ValueError, json.JSONDecodeError)):
                    await self.invoke()

    async def test_empty_transcription_is_nullable_and_metadata_language_is_preserved(self):
        self.body = b'{"text":" "}'
        self.assertEqual({"text": None}, await self.invoke())
        self.assertIn(b"\r\nit\r\n", self.requests[0][2])

    async def test_input_bound_rejects_before_provider_io(self):
        self.path.write_bytes(b"")
        with self.assertRaises(ValueError):
            await self.invoke()
        self.assertEqual([], self.requests)
