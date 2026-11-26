"""The speech SDK can supply its byte iterator directly or asynchronously."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from openagent_core.voice import tts


def config(url):
    return tts.TTSConfig(vendor="openai", model_id="tts-1", voice_id="alloy", api_key="local-fixture", base_url=url)


@pytest.mark.asyncio
async def test_installed_sdk_streams_the_original_audio(monkeypatch):
    monkeypatch.setenv("LITELLM_LOCAL_MODEL_COST_MAP", "True")
    payload = b"first" * 4096 + b"last"

    async def speech(request):
        assert request.headers["Authorization"] == "Bearer local-fixture"
        assert (await request.json())["input"] == "fixture"
        return web.Response(body=payload, content_type="audio/mpeg")

    app = web.Application()
    app.router.add_post("/v1/audio/speech", speech)
    async with TestServer(app) as server:
        chunks = [chunk async for chunk in tts.synthesize_stream("fixture", config(str(server.make_url("/v1"))))]
    assert b"".join(chunks) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("await_iterator", [False, True])
@pytest.mark.parametrize("outcome", ["finish", "close", "cancel"])
async def test_byte_iterator_closes_once_and_never_repeats_a_chunk(monkeypatch, await_iterator, outcome):
    waiting = asyncio.Event()

    async def chunks():
        yield b"first"
        if outcome == "cancel":
            waiting.set()
            await asyncio.Event().wait()
        yield b"last"

    async def resolve():
        return chunks()

    def aiter_bytes(*, chunk_size):
        return resolve() if await_iterator else chunks()

    response = SimpleNamespace(aiter_bytes=aiter_bytes, aclose=AsyncMock())
    provider = SimpleNamespace(aspeech=AsyncMock(return_value=response))
    monkeypatch.setattr(tts, "_load_litellm", AsyncMock(return_value=provider))
    stream = tts.synthesize_stream("fixture", config(None))
    assert await anext(stream) == b"first"
    if outcome == "finish":
        assert [chunk async for chunk in stream] == [b"last"]
    elif outcome == "close":
        await stream.aclose()
    else:
        read = asyncio.create_task(anext(stream))
        await asyncio.wait_for(waiting.wait(), 2)
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await read
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    provider.aspeech.assert_awaited_once()
    response.aclose.assert_awaited_once()
