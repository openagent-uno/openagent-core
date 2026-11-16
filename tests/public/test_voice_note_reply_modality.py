"""A voice note is input, not a request for a spoken reply.

A transcribed voice note (``source="stt"``) used to turn on speech for the
reply even when the session had speech off. A reply is spoken only when the
session itself has speech on (for example a live voice conversation).
"""
import asyncio
import unittest

from openagent_core.stream.events import TextFinal, now_ms
from openagent_core.stream.session import StreamSession


class _Agent:
    name = "capture"
    db = None

    def __init__(self):
        self.done = asyncio.Event()

    async def run_stream(self, *, message, attachments=None, **_kwargs):
        yield {"kind": "delta", "text": "Here is the answer."}
        self.done.set()
        yield {"kind": "done", "text": "Here is the answer."}

    def last_response_meta(self, _session_id):
        return {"model": "capture"}


class _RecordingTTS:
    audio_format = ("mp3", "audio/mpeg")
    voice_id = "fixture"

    def __init__(self):
        self.calls = []

    async def synthesize_stream(self, text, *args, **kwargs):
        self.calls.append(text)
        yield b"\x00"


async def _no_provider(_db):
    return None


class VoiceNoteReplyModality(unittest.IsolatedAsyncioTestCase):
    async def _turn(self, *, source, speak_enabled):
        agent, tts = _Agent(), _RecordingTTS()

        async def tts_factory(_db):
            return tts

        session = StreamSession(agent, client_id="client", session_id="voice-reply",
                                coalesce_window_ms=0, speak_enabled=speak_enabled)
        await session.start(stt_factory=_no_provider, tts_factory=tts_factory)
        try:
            await session.push_in(TextFinal(session_id="voice-reply", seq=1, ts_ms=now_ms(),
                                            text="What time is it?", source=source))
            await asyncio.wait_for(agent.done.wait(), timeout=2)
            await asyncio.sleep(0.2)
            return tts.calls
        finally:
            await session.close()

    async def test_a_voice_note_gets_a_text_reply_when_speech_is_off(self):
        self.assertEqual([], await self._turn(source="stt", speak_enabled=False))

    async def test_a_typed_message_stays_silent_when_speech_is_off(self):
        self.assertEqual([], await self._turn(source="user_typed", speak_enabled=False))

    async def test_a_session_with_speech_on_still_speaks(self):
        self.assertTrue(await self._turn(source="stt", speak_enabled=True))


if __name__ == "__main__":
    unittest.main()
