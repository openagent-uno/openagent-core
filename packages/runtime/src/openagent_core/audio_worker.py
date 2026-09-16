"""Public isolated audio worker. Configuration arrives on stdin, never argv."""
from __future__ import annotations
import asyncio
import base64
import contextlib
import json
import os
import sys


async def execute(request):
    row=request["row"]
    if not row.get("api_key") or not row.get("base_url"):raise ValueError("Explicit audio route required")
    operation=request["operation"]
    if operation=="transcribe":
        from .voice.voice import _transcribe_via_litellm
        text=await _transcribe_via_litellm(request["path"],row,language=request.get("language"))
        return {"text":text}
    if operation=="synthesize":
        from .voice.tts import resolve_tts_provider,synthesize_full
        class ConfiguredRow:
            async def latest_audio_model(self,kind):return row
        config=await resolve_tts_provider(ConfiguredRow())
        if config is None:raise ValueError("Audio provider unavailable")
        data=await synthesize_full(request["text"],config,language=request.get("language"))
        if not data:return {"audio":None}
        if len(data)>16<<20:raise ValueError("Audio output too large")
        mime={"mp3":"audio/mpeg","wav":"audio/wav","flac":"audio/flac","opus":"audio/ogg","pcm":"audio/pcm","aac":"audio/aac"}.get(config.response_format,"application/octet-stream")
        return {"audio":base64.b64encode(data).decode(),"media_type":mime}
    raise ValueError("Unknown audio operation")


def main():
    # Vendor logs can include request configuration. The protocol's stdout is
    # solely a bounded result/error object; the parent also discards stderr.
    output=sys.stdout
    try:
        raw=sys.stdin.buffer.read(262145)
        if len(raw)>262144:raise ValueError("Request too large")
        with open(os.devnull,"w") as sink,contextlib.redirect_stdout(sink),contextlib.redirect_stderr(sink):
            result=asyncio.run(execute(json.loads(raw)))
    except BaseException:result={"error":"audio_provider_unavailable"}
    output.write(json.dumps(result));output.flush()


if __name__=="__main__":main()
