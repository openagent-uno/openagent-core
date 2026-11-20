"""Normal installed VoiceService -> real isolated worker -> local HTTP.

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
import subprocess
import sys

from openagent_core import __version__
from openagent_core.audio import VoiceService
from openagent_core.engine import MemoryDB


class InstalledAudioWorkerHTTP(unittest.IsolatedAsyncioTestCase):
    async def test_enabled_audio_catalog_recovers_from_quota_without_manual_model_selection(self):
        await self._catalog_recovery(429)

    async def test_enabled_audio_catalog_recovers_from_known_unavailable_model(self):
        await self._catalog_recovery(404)

    async def test_oversized_request_rejection_keeps_status_and_never_replays(self):
        await self._catalog_recovery(400)

    async def _catalog_recovery(self, failure_status):
        import asyncio
        requests = []
        wav = b'RIFF' + (36).to_bytes(4, 'little') + b'WAVEfmt ' + (16).to_bytes(4, 'little') + bytes.fromhex('01000100803e0000007d000002001000') + b'data' + bytes(4)
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                authorization = self.headers.get('Authorization')
                requests.append((self.path, authorization))
                limited = authorization == 'Bearer limited'
                transcription = self.path.endswith('/transcriptions')
                data = (json.dumps({'error':{'code':'model_not_found' if failure_status==404 else 'rate_limit_error','message':'fixture PRIVATE-DETAIL'}}).encode() if limited else
                        b'{"text":"recovered transcription"}' if transcription else wav)
                if limited and failure_status==400:data=b'x'*262145
                self.send_response(failure_status if limited else 200)
                self.send_header('Content-Type', 'application/json' if limited or transcription else 'audio/wav')
                self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                db = MemoryDB(str(Path(temporary) / 'agent.db')); await db.connect()
                try:
                    healthy = await db.upsert_provider(name='healthy-audio', framework='api-based',
                        api_key='healthy', base_url=f'http://127.0.0.1:{server.server_port}/v1', metadata={'vendor':'openai'})
                    limited = await db.upsert_provider(name='limited-audio', framework='api-based',
                        api_key='limited', base_url=f'http://127.0.0.1:{server.server_port}/v1', metadata={'vendor':'openai'})
                    for kind, model in [('stt','whisper-1'), ('tts','tts-1')]:
                        await db.upsert_model(provider_id=healthy, model=model, kind=kind, metadata={'response_format':'wav'})
                        await asyncio.sleep(.01)
                        await db.upsert_model(provider_id=limited, model=model, kind=kind, metadata={'response_format':'wav'})
                    source = Path(temporary) / 'synthetic.wav'; source.write_bytes(wav)
                    voice = VoiceService(db, timeout_seconds=20)
                    if failure_status==400:
                        from openagent_core.audio import AudioProviderError
                        with self.assertRaises(AudioProviderError) as raised:
                            await voice.transcribe(source)
                        self.assertEqual(raised.exception.status_code,400)
                        self.assertEqual(len(requests),1)
                        self.assertEqual(requests[0][1],'Bearer limited')
                        return
                    self.assertEqual(await voice.transcribe(source), 'recovered transcription')
                    result = await voice.synthesize('A short fixture sentence.')
                    self.assertEqual(result.data, wav)
                    for suffix in ('transcriptions', 'speech'):
                        self.assertEqual([auth for path, auth in requests if path.endswith('/'+suffix)],
                                         ['Bearer limited', 'Bearer healthy'])
                finally:
                    await db.close()
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    async def test_normal_worker_returns_one_clean_italian_transcription(self):
        # -I deliberately ignores source PYTHONPATH. Never qualify a current
        # parent against an older installed worker and call that a regression.
        installed=subprocess.check_output([sys.executable,'-I','-c',
            'import openagent_core; print(openagent_core.__version__)'],text=True).strip()
        self.assertEqual(installed,__version__,
            'The isolated audio worker must use the candidate Core wheel; run packaging/qualify.py')
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
