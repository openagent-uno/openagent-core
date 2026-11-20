"""Utterance recovery retains consumed text and never replays audible output."""
import asyncio
import base64
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openagent_core.audio import VoiceService, AudioProviderError
from openagent_core.audio_stream import VoiceTTS


def row(name, vendor='openai', **metadata):
    return {'provider_name':vendor,'model':name,'runtime_id':vendor+':'+name,
            'api_key':'fixture','base_url':'https://api.elevenlabs.io/v1' if vendor=='elevenlabs' else 'https://fixture.test/v1',
            'metadata':metadata}


async def text():
    yield 'The first synthetic sentence. '
    yield 'The second synthetic sentence.'


@pytest.mark.asyncio
async def test_sentence_stream_recovers_once_then_pins_actual_voice_and_encoding():
    service=VoiceService(None)
    service.rows=AsyncMock(return_value=[row('limited'),row('healthy')])
    calls=[]
    async def invoke(operation,selected,**fields):
        calls.append((selected['model'],fields['text']))
        if selected['model']=='limited':raise AudioProviderError(429)
        return {'audio':base64.b64encode(fields['text'].encode()).decode(),'media_type':'audio/wav'}
    service.invoke=invoke
    adapter=VoiceTTS(service)
    output=[chunk async for chunk in adapter.synthesize_stream(text())]
    assert output==[b'The first synthetic sentence.',b'The second synthetic sentence.']
    assert [model for model,_ in calls]==['limited','healthy','healthy']
    assert adapter.audio_format==('wav','audio/wav')


@pytest.mark.asyncio
async def test_ws_failure_replays_consumed_text_and_preserves_pending_source(monkeypatch):
    from openagent_core import audio_stream
    waiting=asyncio.Event(); consumed=asyncio.Event()
    async def source():
        yield 'The first synthetic sentence. '
        consumed.set()
        await waiting.wait()
        yield 'The second synthetic sentence.'
    closed=[]
    class FailedWS:
        def __init__(self,config):pass
        async def synthesize_stream(self,chunks,**kwargs):
            iterator=chunks.__aiter__()
            assert 'first' in await anext(iterator)
            task=asyncio.create_task(anext(iterator))
            await consumed.wait()
            await asyncio.sleep(0)
            task.cancel()
            try:await task
            except asyncio.CancelledError:pass
            waiting.set()
            closed.append(True)
            raise RuntimeError('offline before audio')
            yield b''
    monkeypatch.setattr(audio_stream,'ElevenLabsWSTTS',FailedWS)
    service=VoiceService(None);service.rows=AsyncMock(return_value=[row('ws','elevenlabs',stream_input=True,voice_id='fixture'),row('rest')])
    seen=[]
    async def invoke(operation,selected,**fields):
        seen.append((selected['model'],fields['text']))
        return {'audio':base64.b64encode(fields['text'].encode()).decode(),'media_type':'audio/wav'}
    service.invoke=invoke
    result=await asyncio.wait_for(_collect(VoiceTTS(service),source()),2)
    assert result==[b'The first synthetic sentence.',b'The second synthetic sentence.']
    assert [model for model,_ in seen]==['rest','rest'] and closed==[True]


async def _collect(adapter,chunks):return [audio async for audio in adapter.synthesize_stream(chunks)]


@pytest.mark.asyncio
async def test_ws_failure_after_audible_output_never_replays_other_voice(monkeypatch):
    from openagent_core import audio_stream
    class PartialWS:
        audio_format=('mp3','audio/mpeg')
        def __init__(self,config):pass
        async def synthesize_stream(self,chunks,**kwargs):
            yield b'committed-audio'
            raise RuntimeError('connection lost after audio')
    monkeypatch.setattr(audio_stream,'ElevenLabsWSTTS',PartialWS)
    service=VoiceService(None);service.rows=AsyncMock(return_value=[row('ws','elevenlabs',stream_input=True,voice_id='fixture'),row('rest')])
    service.invoke=AsyncMock(side_effect=AssertionError('must not replay audible speech'))
    emitted=[]
    with pytest.raises(RuntimeError,match='after audio'):
        async for chunk in VoiceTTS(service).synthesize_stream(text()):emitted.append(chunk)
    assert emitted==[b'committed-audio']
    service.invoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_zero_audio_ws_completion_recovers_even_after_all_text_was_consumed(monkeypatch):
    from openagent_core import audio_stream
    class EmptyWS:
        def __init__(self,config):pass
        async def synthesize_stream(self,chunks,**kwargs):
            async for _ in chunks:pass
            if False:yield b''
    monkeypatch.setattr(audio_stream,'ElevenLabsWSTTS',EmptyWS)
    service=VoiceService(None);service.rows=AsyncMock(return_value=[row('ws','elevenlabs',stream_input=True,voice_id='fixture'),row('rest')])
    service.invoke=AsyncMock(return_value={'audio':base64.b64encode(b'fixture-audio').decode(),'media_type':'audio/wav'})
    assert await _collect(VoiceTTS(service),text())==[b'fixture-audio',b'fixture-audio']
    assert service.invoke.await_count==2
