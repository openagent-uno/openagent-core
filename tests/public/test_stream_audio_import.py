"""Text streams and first-use cloud speech share one responsive runtime."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import threading


def test_text_stream_then_concurrent_cloud_speech(tmp_path):
    import openagent_core

    # Other tests may already have imported the vendor SDK in this interpreter.
    root = Path(openagent_core.__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, "-I", __file__, str(root), str(tmp_path)],
                            env={**os.environ, "LITELLM_LOCAL_MODEL_COST_MAP": "True"},
                            capture_output=True, text=True, timeout=35)
    assert result.returncode == 0, result.stderr


async def exercise(directory):
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    imports = []

    class ObserveImport:
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "litellm":
                imports.append(threading.get_ident())
            return None

    sys.meta_path.insert(0, ObserveImport())
    from openagent_core.stream.session import StreamSession
    from openagent_core.voice.stt_base import LiteLLMSTT
    from openagent_core.voice.tts import TTSConfig, synthesize_full

    session = StreamSession(object(), client_id="fixture", session_id="text", profile="batched", speak_enabled=False)
    await session.start()
    await session.close()
    assert not imports and "litellm" not in sys.modules, "Text-only stream loaded the cloud speech SDK"

    payload = b"fixture audio"
    path = directory / "sample.wav"
    path.write_bytes(payload)

    async def transcribe(request):
        assert request.headers["Authorization"] == "Bearer local-fixture"
        form = await request.post()
        assert form["file"].file.read() == payload and form["language"] == "en"
        return web.json_response({"text": "fixture transcription"})

    async def speak(request):
        assert request.headers["Authorization"] == "Bearer local-fixture"
        body = await request.json()
        assert body["input"] == "fixture speech" and body["voice"] == "alloy"
        return web.Response(body=payload, content_type="audio/mpeg")

    app = web.Application()
    app.router.add_post("/v1/audio/transcriptions", transcribe)
    app.router.add_post("/v1/audio/speech", speak)
    async with TestServer(app) as server:
        url = str(server.make_url("/v1"))
        stt = LiteLLMSTT({"provider_name": "openai", "model": "whisper-1", "api_key": "local-fixture", "base_url": url})
        tts = TTSConfig(vendor="openai", model_id="tts-1", voice_id="alloy", api_key="local-fixture", base_url=url)
        output = await asyncio.gather(stt.transcribe_file(str(path), language="en"), synthesize_full("fixture speech", tts))
        assert output == ["fixture transcription", payload], output
    assert len(imports) == 1 and imports[0] != threading.get_ident(), imports


if __name__ == "__main__":
    sys.path.insert(0, sys.argv[1])
    asyncio.run(exercise(Path(sys.argv[2])))
