"""Explicit audio routing for hosts; no default users, sessions or model downloads."""
from __future__ import annotations
from dataclasses import dataclass
import asyncio
import base64
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class SynthesizedAudio:
    data: bytes
    media_type: str
    model_ref: str | None = None


class AudioFallback(Protocol):
    async def transcribe(self, path: Path, *, language: str | None = None) -> str | None: ...
    async def synthesize(self, text: str, *, language: str | None = None) -> SynthesizedAudio | None: ...


class AudioProviderError(RuntimeError):
    def __init__(self, status=None, error_type=""):
        super().__init__("Audio provider unavailable")
        self.status_code, self.error_type = status, error_type


def _recoverable_audio_error(error):
    if not isinstance(error, AudioProviderError):
        return True  # Older workers expose only a bounded availability failure.
    status = error.status_code
    return status is None or status in {401,408,429,500,502,503,504,529} or error.error_type in {
        "model_not_found", "unsupported_model", "invalid_api_key", "token_expired"}


class VoiceService:
    """Use the latest enabled audio model and explicit host fallback adapters.

    Local models are host-provisioned adapters, not an import/download side
    effect. Configured cloud calls always supply credentials and destination,
    so unrelated process environment cannot select another account or proxy.
    """
    def __init__(self, db, *, fallback: AudioFallback | None = None, worker_command: tuple[str, ...] | None = None, timeout_seconds: float = 120):
        self.db,self.fallback=db,fallback
        self.worker_command=worker_command or (sys.executable,"-I","-m","openagent_core.audio_worker")
        self.timeout_seconds=timeout_seconds

    @staticmethod
    def _configured_row(row):
        row=dict(row)
        vendor=str((row.get("provider_metadata") or {}).get("vendor") or row.get("provider_name") or "").lower().strip()
        row["provider_name"] = vendor
        # The default API destinations of these providers are part of their
        # adapter contract. Other adapters require an explicit configured URL.
        defaults={"openai":"https://api.openai.com/v1", "elevenlabs":"https://api.elevenlabs.io/v1", "groq":"https://api.groq.com/openai/v1"}
        row["base_url"]=row.get("base_url") or defaults.get(vendor)
        if not row.get("api_key") or not row.get("base_url"):
            raise ValueError("Audio provider requires explicit credentials and endpoint")
        return row

    async def rows(self, kind):
        if hasattr(self.db, "list_models_enriched"):
            from .models.catalog_routes import catalog_routes
            rows = await catalog_routes(self.db, kind)
        else:
            row = await self.db.latest_audio_model(kind)
            rows = [row] if row is not None else []
        result = []
        for row in rows:
            try:result.append(self._configured_row(row))
            except ValueError:continue
        return result

    async def row(self, kind):
        rows = await self.rows(kind)
        return rows[0] if rows else None

    async def invoke(self,operation,row,**fields):
        # LiteLLM has process-global SDK routing and environment fallbacks. Its
        # optional worker gets only this call's configuration over a private
        # pipe. A frozen host supplies its own worker entrypoint explicitly.
        payload=json.dumps({"operation":operation,"row":row,**fields}).encode()
        if len(payload)>262144:raise ValueError("Audio request is too large")
        with tempfile.TemporaryDirectory(prefix="openagent-audio-") as directory:
            environment={"PATH":os.defpath,"HOME":directory,"LANG":"C.UTF-8","PYTHONIOENCODING":"utf-8","LITELLM_LOCAL_MODEL_COST_MAP":"True"}
            process=await asyncio.create_subprocess_exec(*self.worker_command,stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL,cwd=directory,env=environment)
            async def exchange():
                process.stdin.write(payload);await process.stdin.drain();process.stdin.close()
                result=await process.stdout.read((32<<20)+1)
                if len(result)>32<<20:raise ValueError("Audio response is too large")
                await process.wait()
                if process.returncode:raise RuntimeError("Audio provider unavailable")
                body=json.loads(result)
                if body.get("error"):raise AudioProviderError(body.get("status"), body.get("error_type", ""))
                return body
            try:return await asyncio.wait_for(exchange(),self.timeout_seconds)
            finally:
                if process.returncode is None:
                    process.terminate()
                    try:await asyncio.wait_for(process.wait(),2)
                    except asyncio.TimeoutError:process.kill();await process.wait()

    async def transcribe(self,path: str | Path,*,language=None):
        path=Path(path).absolute()
        if not path.is_file():raise FileNotFoundError(path)
        last = None
        for row in await self.rows("stt"):
            try:result=await self.invoke("transcribe",row,path=str(path),language=language)
            except (RuntimeError,TimeoutError) as error:
                if not _recoverable_audio_error(error):raise
                last = error
                if row.get("runtime_id"):
                    from .models.catalog_routes import route_failed
                    route_failed(self.db,"stt",row)
                continue
            text=result.get("text")
            if isinstance(text,str) and text:
                if row.get("runtime_id"):
                    from .models.catalog_routes import route_succeeded
                    route_succeeded(self.db,"stt",row)
                return text
        if self.fallback is not None:return await self.fallback.transcribe(path,language=language)
        if last is not None:raise last
        return None

    async def synthesize(self,text: str,*,language=None):
        return await self._synthesize_from_rows(text, await self.rows("tts"), language=language)

    async def _synthesize_from_rows(self, text, rows, *, language=None, allow_local=True):
        if not isinstance(text,str) or not text.strip():raise ValueError("Text is required")
        if len(text)>131072:raise ValueError("Text is too long")
        last = None
        for row in rows:
            if row.get('runtime_id'):
                from .models.catalog_routes import authorized_model_use
                try:
                    await authorized_model_use(row['runtime_id'])
                except PermissionError:
                    continue
            try:result=await self.invoke("synthesize",row,text=text,language=language)
            except (RuntimeError,TimeoutError) as error:
                if not _recoverable_audio_error(error):raise
                last = error
                if row.get("runtime_id"):
                    from .models.catalog_routes import route_failed
                    route_failed(self.db,"tts",row)
                continue
            if result.get("audio"):
                if row.get("runtime_id"):
                    from .models.catalog_routes import route_succeeded
                    route_succeeded(self.db,"tts",row)
                return SynthesizedAudio(base64.b64decode(result["audio"],validate=True),result["media_type"], row.get("runtime_id"))
        if allow_local and self.fallback is not None:
            result = await self.fallback.synthesize(text,language=language)
            if result is not None:
                return SynthesizedAudio(result.data, result.media_type, "_local")
        if last is not None:raise last
        return None


def is_audio_file(filename: str | None,media_type: str | None = None):
    return bool((media_type and media_type.startswith("audio/")) or (filename and Path(filename).suffix.lower() in {".webm",".ogg",".mp3",".wav",".m4a",".opus",".flac",".aac"}))
