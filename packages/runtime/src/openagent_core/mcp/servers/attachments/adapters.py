"""In-process tools for publishing a new file or resending a session attachment.

The agent includes a returned delivery marker in its terminal answer. The
engine strips the marker from visible text and publishes the authorized
attachment through the current chat transport.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


_IMAGE_EXTS = frozenset({
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".heic", ".tiff", ".svg",
})
_VIDEO_EXTS = frozenset({
    ".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v",
})
_VOICE_EXTS = frozenset({
    ".mp3", ".wav", ".m4a", ".ogg", ".oga", ".opus", ".flac", ".aac",
})


def _marker_kind(path: Path) -> str:
    """Pick IMAGE / VIDEO / VOICE / FILE from the file's extension.

    Image wins over video wins over voice on ties (``.webm`` is treated
    as video, not audio — matches how Telegram/Discord auto-preview it).
    """
    ext = path.suffix.lower()
    if ext in _IMAGE_EXTS:
        return "IMAGE"
    if ext in _VIDEO_EXTS:
        return "VIDEO"
    if ext in _VOICE_EXTS:
        return "VOICE"
    return "FILE"


def _send_file_impl(path: str) -> dict:
    """Validate the path and build the marker payload."""
    if not path or not isinstance(path, str):
        return {"ok": False, "error": "path required (absolute filesystem path)"}
    try:
        real = Path(path).expanduser().resolve(strict=True)
    except (FileNotFoundError, OSError):
        return {"ok": False, "error": f"path not found: {path}"}
    if not real.is_file():
        return {"ok": False, "error": f"path is not a regular file: {real}"}
    kind = _marker_kind(real)
    marker = f"[{kind}:{real}]"
    return {
        "ok": True,
        "marker": marker,
        "kind": kind.lower(),
        "path": str(real),
        "filename": real.name,
        "instruction": (
            f"Include {marker!r} VERBATIM in your reply to the user. "
            "The marker is stripped from the displayed text and the "
            "file is delivered as a proper chat attachment. Do NOT "
            "describe the path in prose instead — without the marker, "
            "nothing is attached."
        ),
    }


# ── Native runtime ──────────────────────────────────────────────────

def build_runtime_toolkit() -> Any:
    """Return a ``Toolkit`` for new files and existing session attachments.

    Follows the runtime Toolkit pattern: plain async callables with type
    hints + docstrings; the runtime introspects the signature to build
    the tool schema and the docstring becomes the description shown to
    the model.
    """
    from openagent_core.mcp._runtime import Toolkit

    async def send_file_to_user(path: str) -> dict:
        """Send a file from the agent's filesystem to the user as a chat attachment.

        Auto-detects whether to deliver the file as an image, video,
        voice note, or generic document from its extension. Images
        render inline in the desktop app; on Telegram/WhatsApp/Discord
        the platform's native media type is used.

        Returns a payload with a ``marker`` field (e.g.
        ``[FILE:/tmp/report.pdf]``) that you MUST include verbatim in
        your reply to the user — otherwise no attachment is delivered.
        Quoting the path in prose without the marker attaches nothing.

        Args:
            path: Absolute path to the file on the agent's filesystem.
        """
        from openagent_core.memory.vault.privacy import require_unprotected_execution
        await require_unprotected_execution("Attachment delivery")
        from openagent_core.runtime import current_runtime, current_execution_context
        runtime, context = current_runtime(), current_execution_context()
        publisher = runtime.service("attachments.publisher") if runtime is not None else None
        if publisher is not None:
            if context is None:
                raise PermissionError("Attachment delivery requires an authenticated execution")
            ref = await publisher.publish(context, path)
            from openagent_core.artifacts import session_output_reference
            db = getattr(getattr(runtime.services.executor, "agent", None), "memory_db", None)
            verified = await session_output_reference(db, tenant_id=context.tenant_id,
                session_id=context.session_id, value=ref) if db is not None else None
            if verified is None or not isinstance(ref.get("path"), str):
                raise ValueError("The host publisher did not return a session-linked artifact")
            marker = f"[FILE:{ref['path']}]"
            from openagent_core.core.output_delivery import record_delivery
            record_delivery(marker)
            return {"ok": True, "marker": marker, "attachment": verified,
                "instruction": "Include the marker verbatim in the final reply to deliver this attachment."}
        if runtime is not None:
            return {"ok": False, "error": "Attachment delivery capability is unavailable on this host."}
        result = _send_file_impl(path)
        if result.get("ok"):
            from openagent_core.core.output_delivery import record_delivery
            record_delivery(result["marker"])
        return result

    async def session_store():
        from openagent_core.contracts import ResourceRef
        from openagent_core.memory.vault.privacy import require_unprotected_execution
        from openagent_core.runtime import current_execution_context, current_runtime

        await require_unprotected_execution("Session attachments")
        runtime, context = current_runtime(), current_execution_context()
        if runtime is None or context is None:
            raise PermissionError("Session attachments require an authenticated execution")
        resource = ResourceRef("session", context.tenant_id, context.session_id)
        await runtime.authorize(context, "session.read", resource, audience=context.audience)
        db = getattr(getattr(runtime.services.executor, "agent", None), "memory_db", None)
        return runtime, context, resource, db

    async def find_session_attachments(query: str = "") -> dict:
        """Find original files already attached to this conversation, including earlier turns.

        Search by a distinctive part of the filename, or omit the query to
        see recent attachments. Use the returned artifact_id with
        resend_session_attachment to send an existing file in this chat.
        """
        from openagent_core.artifacts import find_session_attachments as list_attachments

        if len(query) > 200:
            raise ValueError("Search with up to 200 characters of the filename")
        runtime, context, resource, db = await session_store()
        if db is None:
            return {"ok": False, "error": "This host has no session attachment store."}
        result = await list_attachments(db, tenant_id=context.tenant_id,
            session_id=context.session_id, query=query.strip())
        await runtime.authorize(context, "session.read", resource, audience=context.audience)
        return {"ok": True, **result}

    async def resend_session_attachment(artifact_id: str) -> dict:
        """Send an original file already attached to this conversation, without re-uploading it.

        Pass the artifact_id from find_session_attachments. Include the
        returned marker verbatim in the final reply to deliver the file.
        """
        from openagent_core.artifacts import SESSION_ARTIFACT_PREFIX, session_output_reference
        from openagent_core.core.output_delivery import record_delivery

        runtime, context, resource, db = await session_store()
        await runtime.authorize(context, "run.publish", resource, audience=context.audience)
        ref = await session_output_reference(db, tenant_id=context.tenant_id,
            session_id=context.session_id, value={"artifact_id": artifact_id}) if db is not None else None
        await runtime.authorize(context, "session.read", resource, audience=context.audience)
        await runtime.authorize(context, "run.publish", resource, audience=context.audience)
        if ref is None:
            return {"ok": False, "error": "Attachment not available in this session."}
        marker = f"[FILE:{SESSION_ARTIFACT_PREFIX}{ref['artifact_id']}]"
        record_delivery(marker)
        return {"ok": True, "marker": marker, "attachment": ref,
            "instruction": "Include the marker verbatim in the final reply to deliver this attachment."}

    return Toolkit(
        name="attachments",
        tools=[send_file_to_user, find_session_attachments, resend_session_attachment],
    )
