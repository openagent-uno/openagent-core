"""Opt-in speech synthesis for an authenticated agent tool call."""
from __future__ import annotations

import asyncio
import re
from typing import Any, Mapping

from .artifacts import ArtifactRepository
from .audio import VoiceService
from .capabilities import CapabilityUnavailable, ToolDefinition
from .contracts import AuthorizationDenied, ExecutionContext, ResourceRef, require_authorized
from .runtime import current_execution_context, current_run_id

_TEXT_LIMIT = 2000
_AUDIO_LIMIT = 8 * 1024 * 1024
_LANGUAGE = r"[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8}){0,3}"
_EXTENSIONS = {
    "audio/wav": "wav", "audio/mpeg": "mp3", "audio/flac": "flac",
    "audio/ogg": "ogg", "audio/aac": "aac", "audio/mp4": "m4a",
}
_SYNTHESIZE = ToolDefinition(
    "synthesize_speech",
    "Synthesize spoken text using the host-configured voice. Returns a private "
    "audio artifact for this session. Use for requested speech or narration.",
    {
        "type": "object",
        "properties": {
            "text": {"type": "string", "minLength": 1, "maxLength": _TEXT_LIMIT},
            "language": {"type": "string", "pattern": f"^{_LANGUAGE}$"},
        },
        "required": ["text"],
        "additionalProperties": False,
    },
    metadata={"ui": {"visibility": ["model"]}},
)


class SpeechSynthesisCapability:
    """A catalog source and executor using explicitly provisioned host services."""

    def __init__(self, voice: VoiceService, artifacts: ArtifactRepository) -> None:
        self._voice = voice
        self._artifacts = artifacts

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return (_SYNTHESIZE,)

    async def call_tool(
        self, name: str, arguments: Mapping[str, Any], context: ExecutionContext,
    ) -> dict[str, Any]:
        if name != _SYNTHESIZE.name:
            raise CapabilityUnavailable("Unknown speech synthesis tool")
        if not current_run_id() or current_execution_context() != context:
            raise AuthorizationDenied("Speech synthesis requires the current admitted run")
        if set(arguments) not in ({"text"}, {"text", "language"}):
            raise ValueError("Speech synthesis requires text and an optional language")
        supplied = arguments["text"]
        if not isinstance(supplied, str) or len(supplied) > _TEXT_LIMIT:
            raise ValueError(f"Speech text must contain 1–{_TEXT_LIMIT} characters")
        text = supplied.strip()
        if not text or "\x00" in text:
            raise ValueError("Speech text must be nonempty and contain no NUL characters")
        try:
            text.encode("utf-8")
        except UnicodeError:
            raise ValueError("Speech text must be valid UTF-8") from None
        language = arguments.get("language")
        if "language" in arguments and (
            not isinstance(language, str) or len(language) > 35 or not re.fullmatch(_LANGUAGE, language)
        ):
            raise ValueError("Invalid speech language tag")

        from .memory.vault.privacy import require_unprotected_execution
        await require_unprotected_execution("Speech synthesis")
        await self._artifacts.authorize(context, "artifact.write")
        await require_authorized(
            self._artifacts.authorizer, context, "artifact.write",
            ResourceRef("session", context.tenant_id, context.session_id), audience=context.audience,
        )
        try:
            async with asyncio.timeout(120):
                speech = await self._voice.synthesize(text, language=language)
        except Exception:
            raise CapabilityUnavailable("Speech synthesis is unavailable") from None
        if speech is None:
            raise CapabilityUnavailable("Speech synthesis returned no audio")
        if not isinstance(speech.data, bytes) or not 0 < len(speech.data) <= _AUDIO_LIMIT:
            raise ValueError("Speech audio is empty or exceeds the output limit")
        extension = _EXTENSIONS.get(speech.media_type) if isinstance(speech.media_type, str) else None
        if extension is None:
            raise ValueError("Speech synthesis returned an unsupported audio format")

        reference = await self._artifacts.upload(
            context, speech.data, filename=f"speech.{extension}", mime_type=speech.media_type,
            session_id=context.session_id,
        )
        from .memory.artifacts import public_attachment_ref
        artifact = public_attachment_ref(reference)
        # A host gateway chooses how to expose artifact downloads.
        artifact.pop("url", None)
        return {
            "content": [{"type": "text", "text": "Synthesized speech is ready as a private session artifact."}],
            "structuredContent": {"artifact": artifact},
            "isError": False,
        }
