"""Reusable stream adapters for the explicitly composed public audio service."""
import asyncio
from contextlib import aclosing, suppress
from .audio import VoiceService
from .voice.stt_base import BaseSTT
from .voice.tts_base import BaseTTS, ElevenLabsWSTTS
from .voice.tts import resolve_tts_provider
from .voice.tts_chunker import SentenceChunker


class _ReplayText:
    """Retain only pre-audio text; a failed writer cannot cancel its source."""
    def __init__(self, source):
        self.source = source.__aiter__()
        self.buffer = []
        self.pending = None
        self.done = False
        self.committed = False
        self.characters = 0
        self.position = 0

    def commit(self):
        self.committed = True
        self.buffer = self.buffer[self.position:]
        self.position = 0

    async def chunks(self):
        self.position = 0
        while True:
            if self.position < len(self.buffer):
                if self.committed:
                    value = self.buffer.pop(0)
                else:
                    value = self.buffer[self.position]
                    self.position += 1
                yield value
                continue
            if self.done:
                return
            if self.pending is None:
                self.pending = asyncio.create_task(anext(self.source))
            try:
                value = await asyncio.shield(self.pending)
            except StopAsyncIteration:
                self.pending = None
                self.done = True
                return
            self.pending = None
            if not self.committed:
                self.characters += len(value)
                if self.characters > 131072:
                    raise ValueError('Speech input is too long before the first audio')
                self.buffer.append(value)
                self.position += 1
            yield value

    async def close(self):
        if self.pending is not None:
            self.pending.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self.pending
        close = getattr(self.source, 'aclose', None)
        if close is not None:
            await close()


class VoiceSTT(BaseSTT):
    def __init__(self, service): self.service = service

    async def transcribe_file(self, path, *, language=None):
        return await self.service.transcribe(path, language=language)


class VoiceTTS(BaseTTS):
    def __init__(self, service):
        self.service = service
        self._format = ('mp3', 'audio/mpeg')

    @property
    def audio_format(self): return self._format

    async def synthesize_full(self, text, *, language=None):
        result = await self.service.synthesize(text, language=language)
        if result is None: return None
        extension = {'audio/wav':'wav', 'audio/mpeg':'mp3', 'audio/ogg':'opus',
            'audio/flac':'flac', 'audio/pcm':'pcm', 'audio/aac':'aac'}.get(result.media_type, 'bin')
        self._format = (extension, result.media_type)
        return result.data

    async def synthesize_stream(self, text_chunks, *, language=None):
        # Fail over before the first audio only. Keep one voice/configuration
        # and encoding for the remainder, including later sentence requests.
        rows = await self.service.rows('tts')
        replay = _ReplayText(text_chunks)
        try:
            if rows:
                row = rows[0]
                class Selected:
                    async def latest_audio_model(self, kind): return row if kind == 'tts' else None
                config = await resolve_tts_provider(Selected())
                if (config is not None and config.vendor == 'elevenlabs' and config.stream_input
                        and row.get('base_url', '').rstrip('/') == 'https://api.elevenlabs.io/v1'):
                    adapter = ElevenLabsWSTTS(config)
                    emitted = False
                    try:
                        async with aclosing(adapter.synthesize_stream(replay.chunks(), language=language)) as stream:
                            async for chunk in stream:
                                if not chunk:
                                    continue
                                emitted = True
                                replay.commit()
                                self._format = adapter.audio_format
                                yield chunk
                        if emitted:
                            return
                    except Exception:
                        if emitted:
                            raise
                    # The refused token stream has not committed audio. The
                    # complete consumed prefix and any pending input survive.
                    rows = rows[1:]

            pinned = False
            local = False
            chunker = SentenceChunker()
            async def sentences():
                async for delta in replay.chunks():
                    for sentence in chunker.feed(delta or ''):
                        yield sentence
                tail = chunker.flush()
                if tail:
                    yield tail
            async with aclosing(sentences()) as chunks:
                async for sentence in chunks:
                    result = await self.service._synthesize_from_rows(
                        sentence, [] if local else rows, language=language,
                        allow_local=not pinned or local)
                    if result is None or not result.data:
                        continue
                    if not pinned:
                        pinned = True
                        local = result.model_ref == '_local'
                        rows = [row for row in rows if row.get('runtime_id') == result.model_ref]
                    replay.commit()
                    extension = {'audio/wav':'wav', 'audio/mpeg':'mp3', 'audio/ogg':'opus',
                        'audio/flac':'flac', 'audio/pcm':'pcm', 'audio/aac':'aac'}.get(result.media_type, 'bin')
                    self._format = (extension, result.media_type)
                    yield result.data
        finally:
            await replay.close()
