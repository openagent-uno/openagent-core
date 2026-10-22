"""Normal installed VoiceService -> real isolated worker -> LiteLLM -> local HTTP.

Requires the public audio extra. No provider SDK patch, external service, model
inference, customer recording or paid API is involved.
"""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import importlib.metadata
import json
import tempfile
import threading
import unittest

from openagent_core.audio import VoiceService


class InstalledAudioWorkerHTTP(unittest.IsolatedAsyncioTestCase):
    async def test_normal_worker_returns_one_clean_italian_transcription(self):
        # Required dependency, not a skip: the release qualifier installs [audio].
        self.assertTrue(importlib.metadata.version('litellm'))
        requests=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_args):pass
            def do_POST(self):
                body=self.rfile.read(int(self.headers.get('Content-Length','0')))
                requests.append((self.path,self.headers.get('Authorization'),body))
                ok=(self.path=='/v1/audio/transcriptions' and
                    self.headers.get('Authorization')=='Bearer audio-route-fixture')
                data=json.dumps({'text':'Il quarzo sette è già pronto.'}).encode()
                self.send_response(200 if ok else 403)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(data)));self.end_headers()
                self.wfile.write(data)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        class DB:
            async def latest_audio_model(self,kind):
                return {'provider_name':'openai','model':'whisper-1',
                    'api_key':'audio-route-fixture',
                    'base_url':f'http://127.0.0.1:{server.server_port}/v1'}
        try:
            with tempfile.TemporaryDirectory() as directory:
                audio=Path(directory)/'synthetic.wav';audio.write_bytes(b'RIFF'+b'\x00'*40)
                text=await VoiceService(DB(),timeout_seconds=20).transcribe(audio,language='it')
                self.assertEqual('Il quarzo sette è già pronto.',text)
            self.assertEqual(1,len(requests))
            self.assertIn(b'name="language"',requests[0][2])
            self.assertIn(b'\r\nit\r\n',requests[0][2])
            self.assertIn(b'filename="synthetic.wav"',requests[0][2])
        finally:
            server.shutdown();server.server_close();thread.join(timeout=2)
