"""A transcribed voice note remains an attachment without requiring audio LLM input."""

import asyncio
import unittest

from openagent_core.stream.events import TextFinal, now_ms
from openagent_core.stream.session import StreamSession


class _CaptureAgent:
    name = "capture"
    db = None

    def __init__(self):
        self.received = asyncio.Event()
        self.input = None

    async def run_stream(self, *, message, attachments=None, **_kwargs):
        self.input = (message, attachments)
        self.received.set()
        yield {"kind": "done", "text": "ok"}

    def last_response_meta(self, _session_id):
        return {"model": "capture"}


async def _no_provider(_db):
    return None


class TranscribedAudioInput(unittest.IsolatedAsyncioTestCase):
    async def _run_turn(self, source):
        agent = _CaptureAgent()
        session = StreamSession(agent, client_id="telegram", session_id="voice-test",
                                coalesce_window_ms=0)
        voice = {"type": "voice", "filename": "note.ogg", "mime_type": "audio/ogg",
                 "artifact_id": "original-audio"}
        image = {"type": "image", "filename": "frame.jpg", "mime_type": "image/jpeg",
                 "artifact_id": "image"}
        await session.start(stt_factory=_no_provider, tts_factory=_no_provider)
        try:
            await session.push_in(TextFinal(
                session_id="voice-test", seq=1, ts_ms=now_ms(),
                text="Please answer my spoken question", source=source,
                attachments=(voice, image),
            ))
            await asyncio.wait_for(agent.received.wait(), timeout=2)
            return agent.input, voice, image
        finally:
            await session.close()

    async def test_stt_sends_transcript_and_non_audio_media_to_model(self):
        model_input, _voice, image = await self._run_turn("stt")
        self.assertEqual("Please answer my spoken question", model_input[0])
        self.assertEqual([image], model_input[1])

    async def test_untranscribed_audio_still_uses_native_media_route(self):
        model_input, voice, image = await self._run_turn("user_typed")
        self.assertEqual([voice, image], model_input[1])
