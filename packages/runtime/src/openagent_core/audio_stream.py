"""Reusable stream adapters for the explicitly composed public audio service."""
from .audio import VoiceService
from .voice.stt_base import BaseSTT
from .voice.tts_base import BaseTTS, ElevenLabsWSTTS
from .voice.tts import resolve_tts_provider


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
        # One utterance retains one configured route and encoding. The next
        # utterance resolves the current model configuration again.
        row = await self.service.row('tts')
        class Selected:
            async def latest_audio_model(self, kind): return row if kind == 'tts' else None
        selected = Selected()
        fixed = VoiceService(selected, fallback=self.service.fallback,
            worker_command=self.service.worker_command, timeout_seconds=self.service.timeout_seconds)
        config = await resolve_tts_provider(selected) if row is not None else None
        if config is not None and config.vendor == 'elevenlabs' and config.stream_input and row.get('base_url', '').rstrip('/') == 'https://api.elevenlabs.io/v1':
            # Preserve native token streaming with its explicit key and fixed
            # verified destination. Custom endpoints use the isolated worker.
            adapter = ElevenLabsWSTTS(config)
            self._format = adapter.audio_format
            async for chunk in adapter.synthesize_stream(text_chunks, language=language):
                yield chunk
            return
        adapter = VoiceTTS(fixed)
        async for chunk in BaseTTS.synthesize_stream(adapter, text_chunks, language=language):
            self._format = adapter.audio_format
            yield chunk
