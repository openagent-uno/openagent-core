"""Public isolated audio worker. Configuration arrives on stdin, never argv."""
from __future__ import annotations
import asyncio
import base64
import json
import os
import sys


def _safe_error_type(value):
    """Only protocol-owned availability codes can leave the worker."""
    aliases = {'model_not_found':'model_not_found', 'unsupported_model':'unsupported_model',
               'model_decommissioned':'unsupported_model', 'invalid_api_key':'invalid_api_key',
               'token_expired':'token_expired'}
    if isinstance(value, str):
        if value in aliases:
            return aliases[value]
        if len(value) <= 16384:
            try:value = json.loads(value)
            except ValueError:return ''
    if not isinstance(value, dict):
        return ''
    error = value.get('error')
    if isinstance(error, dict):
        value = error
    for key in ('code', 'type'):
        if isinstance(value.get(key), str) and value[key] in aliases:
            return aliases[value[key]]
    return ''


async def transcribe_openai(path, row, *, language=None):
    """Call the configured transcription endpoint without loading every SDK.

    The worker remains isolated. Credentials, destination and model come only
    from its explicit row; redirects and environment proxy discovery are off.
    """
    from pathlib import Path
    import httpx

    source = Path(path)
    with source.open("rb") as audio:
        content = audio.read((16 << 20) + 1)
    if not 0 < len(content) <= 16 << 20:
        raise ValueError("Audio input exceeds its bound")
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    fields = {"model": row["model"], "response_format": "json"}
    for key, value in (("language", language or metadata.get("language")),
                       ("prompt", metadata.get("prompt"))):
        if value is not None:
            fields[key] = value
    url = row["base_url"].rstrip("/") + "/audio/transcriptions"
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False,
                                timeout=httpx.Timeout(60, connect=5)) as client:
        async with client.stream("POST", url,
                    headers={"Authorization": "Bearer " + row["api_key"], "Accept-Encoding": "identity"},
                    data=fields, files={"file": (source.name, content, "application/octet-stream")}) as response:
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                if response.status_code != 200:
                    from .audio import AudioProviderError
                    raise AudioProviderError(response.status_code)
                raise ValueError("Unsupported transcript encoding")
            body = bytearray()
            try:
                async for chunk in response.aiter_raw(65536):
                    body.extend(chunk)
                    if len(body) > 256 << 10:
                        raise ValueError("Audio transcript exceeds its bound")
            except Exception:
                if response.status_code != 200:
                    # A malformed/oversized error body cannot turn a known
                    # request rejection into an unknown retryable failure.
                    from .audio import AudioProviderError
                    raise AudioProviderError(response.status_code) from None
                raise
            if response.status_code != 200:
                from .audio import AudioProviderError
                raise AudioProviderError(response.status_code, _safe_error_type(body.decode('utf-8', errors='replace')))
    result = json.loads(body)
    text = result.get("text") if isinstance(result, dict) else None
    if not isinstance(text, str):
        raise ValueError("Invalid audio transcript")
    return text.strip() or None


async def execute(request):
    row=request["row"]
    if not row.get("api_key") or not row.get("base_url"):raise ValueError("Explicit audio route required")
    operation=request["operation"]
    if operation=="transcribe":
        if str(row.get("provider_name") or "").strip().lower() == "openai":
            return {"text": await transcribe_openai(request["path"], row,
                                                   language=request.get("language"))}
        from .voice.voice import _transcribe_via_litellm
        text=await _transcribe_via_litellm(request["path"],row,language=request.get("language"),strict=True,max_retries=0)
        return {"text":text}
    if operation=="synthesize":
        from .voice.tts import resolve_tts_provider,synthesize_full
        class ConfiguredRow:
            async def latest_audio_model(self,kind):return row
        config=await resolve_tts_provider(ConfiguredRow())
        if config is None:raise ValueError("Audio provider unavailable")
        data=await synthesize_full(request["text"],config,language=request.get("language"),strict=True,max_retries=0)
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
        except BaseException as error:
            result={"error":"audio_provider_unavailable"}
            status=getattr(error,"status_code",None) or getattr(getattr(error,"response",None),"status_code",None)
            if isinstance(status,int):result["status"]=status
            for field in ('error_type', 'code', 'body', 'type'):
                kind = _safe_error_type(getattr(error, field, None))
                if kind:
                    result['error_type'] = kind
                    break
            if 'error_type' not in result:
                # Some SDK wrappers retain the provider's structured code
                # only on their already buffered HTTP response.
                try:
                    body = getattr(getattr(error, 'response', None), 'content', None)
                    if isinstance(body, bytes) and len(body) <= 16384:
                        kind = _safe_error_type(body.decode('utf-8', errors='replace'))
                        if kind:
                            result['error_type'] = kind
                except (ValueError, RuntimeError):
                    pass
        output.write(json.dumps(result));output.flush()


if __name__=="__main__":main()
