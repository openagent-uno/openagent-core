import asyncio
import base64
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.audio import VoiceService, SynthesizedAudio
from openagent_core.audio_stream import VoiceSTT, VoiceTTS


class AudioStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_local_fallback_and_actual_format(self):
        class DB:
            async def latest_audio_model(self, kind): return None
        class Fallback:
            async def transcribe(self, path, *, language=None):
                return Path(path).read_text() + ':' + language
            async def synthesize(self, text, *, language=None):
                return SynthesizedAudio(text.encode(), 'audio/wav')
        service = VoiceService(DB(), fallback=Fallback())
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'audio.wav'
            path.write_text('fixture')
            self.assertEqual(await VoiceSTT(service).transcribe_file(path, language='it'), 'fixture:it')
        async def text(): yield 'A complete utterance.'
        tts = VoiceTTS(service)
        self.assertEqual([part async for part in tts.synthesize_stream(text())], [b'A complete utterance.'])
        self.assertEqual(tts.audio_format, ('wav', 'audio/wav'))

    async def test_route_is_fixed_per_utterance_and_cancellation_propagates(self):
        class DB:
            calls = 0
            async def latest_audio_model(self, kind):
                self.calls += 1
                return dict(provider_name='openai', model_id=f'model-{self.calls}',
                    api_key='fixture-only', metadata_json='{}')
        db = DB()
        seen = []
        async def invoke(service, operation, row, **fields):
            seen.append(row['model_id'])
            return dict(audio=base64.b64encode(b'audio').decode(), media_type='audio/mpeg')
        async def text():
            yield 'The first sentence has enough words to synthesize. '
            yield 'The second sentence also has enough words to synthesize.'
        tts = VoiceTTS(VoiceService(db))
        with patch.object(VoiceService, 'invoke', invoke):
            self.assertTrue([part async for part in tts.synthesize_stream(text())])
            self.assertEqual(set(seen), {'model-1'})
            self.assertEqual(db.calls, 1)
            self.assertTrue([part async for part in tts.synthesize_stream(text())])
            self.assertIn('model-2', seen)
        async def cancelled(*args, **kwargs): raise asyncio.CancelledError()
        with patch.object(VoiceService, 'invoke', cancelled):
            with self.assertRaises(asyncio.CancelledError):
                [part async for part in tts.synthesize_stream(text())]
