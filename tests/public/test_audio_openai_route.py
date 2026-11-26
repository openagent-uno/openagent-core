"""Explicit OpenAI STT route: real HTTP, no vendor import or external API."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from openagent_core import audio_worker


GUARDED_TRANSCRIPTION = """
import asyncio, json, sys

sys.path.insert(0, sys.argv[1])
blocked = []

def is_vendor(name):
    return name == "litellm" or name.startswith("litellm.") or name.endswith("voice.voice")

class ImportGuard:
    def find_spec(self, fullname, path=None, target=None):
        if is_vendor(fullname):
            blocked.append(fullname)
            raise AssertionError(f"Transcription attempted to import {fullname}")

sys.meta_path.insert(0, ImportGuard())
from openagent_core import audio_worker

result = asyncio.run(audio_worker.execute(json.load(sys.stdin)))
assert not blocked and not any(map(is_vendor, sys.modules)), blocked
print(json.dumps(result))
"""


class OpenAITranscriptionRoute(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.status, self.body = 200, json.dumps({"text": " the current question "}).encode()
        self.encoding = None
        self.during_request = None
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                owner.requests.append((self.path, self.headers.get("Authorization"), body))
                if owner.during_request:
                    owner.during_request()
                self.send_response(owner.status)
                self.send_header("Content-Length", str(len(owner.body)))
                if owner.encoding:
                    self.send_header("Content-Encoding", owner.encoding)
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
        # This import runs in the fixture's HTTP thread while the worker is
        # transcribing. Its missing module must keep the runner's usual error.
        diagnostic_errors = []

        def concurrent_diagnostic():
            try:
                __import__("qualification_fixture.voice.voice")
            except Exception as error:
                diagnostic_errors.append(type(error))

        self.during_request = concurrent_diagnostic
        request = {"operation": "transcribe", "row": self.row, "path": str(self.path), "language": "en"}
        worker = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-I", "-c", GUARDED_TRANSCRIPTION, str(Path(audio_worker.__file__).resolve().parents[1])],
            input=json.dumps(request), capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(0, worker.returncode, worker.stderr)
        self.assertEqual({"text": "the current question"}, json.loads(worker.stdout))
        self.assertEqual([ModuleNotFoundError], diagnostic_errors)
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
        for data in (b"", b"x" * ((16 << 20) + 1)):
            self.path.write_bytes(data)
            with self.assertRaises(ValueError):
                await self.invoke()
        self.assertEqual([], self.requests)

    async def test_compressed_transcripts_are_rejected_before_decoding(self):
        self.encoding = "gzip"
        with self.assertRaises(ValueError):
            await self.invoke()
