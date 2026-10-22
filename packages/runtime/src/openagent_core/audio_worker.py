"""Public isolated audio worker. Configuration arrives on stdin, never argv."""
from __future__ import annotations
import asyncio
import base64
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
    # This entrypoint runs only in the owned audio child. Keep its one protocol
    # pipe separate from fd1/fd2 for the entire process lifetime: vendor threads,
    # cached logging handlers and atexit hooks can outlive asyncio.run(). A
    # temporary Python redirect cannot isolate those writers. The private
    # duplicate is non-inheritable and closes before interpreter shutdown.
    protocol_fd=os.dup(1)
    os.set_inheritable(protocol_fd,False)
    with os.fdopen(protocol_fd,"w",encoding="utf-8") as output:
        sink=os.open(os.devnull,os.O_WRONLY)
        try:
            os.dup2(sink,1)
            os.dup2(sink,2)
        finally:os.close(sink)
        # Discard preexisting Python buffers only after their OS descriptors
        # point at the sink. Never restore the vendor stdout/stderr descriptors.
        for stream in (sys.stdout,sys.stderr):
            try:stream.flush()
            except (OSError,ValueError):pass
        try:
            raw=sys.stdin.buffer.read(262145)
            if len(raw)>262144:raise ValueError("Request too large")
            result=asyncio.run(execute(json.loads(raw)))
        except BaseException:result={"error":"audio_provider_unavailable"}
        output.write(json.dumps(result));output.flush()


if __name__=="__main__":main()
