"""Public host-authorized access to the engine's canonical attachment store.

CAS bytes and polymorphic links retain their existing schema. A filesystem
path is only a lookup hint: every read requires current authority to the owner
or an active linked session, and validates content integrity before delivery.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import stat
import tempfile
from typing import Any

from .contracts import ResourceRef, require_authorized

_DIGEST = re.compile(r"^[a-f0-9]{64}$")


@dataclass(frozen=True, slots=True)
class ArtifactContent:
    artifact_id: str
    filename: str
    mime_type: str
    sha256: str
    content: bytes


class ArtifactRepository:
    def __init__(self, db: Any, authorizer: Any, *, max_input_bytes=64 << 20, max_output_bytes=256 << 20):
        self.db, self.authorizer = db, authorizer
        self.max_input_bytes, self.max_output_bytes = max_input_bytes, max_output_bytes
        self._writes = asyncio.Lock()

    async def authorize(self, context, action):
        """Authorize a bounded read or upload before accepting its body."""
        if action not in {"artifact.read", "artifact.write"}:
            raise ValueError("Unknown artifact action")
        await self._authorize(context, action, "agent", context.agent_id)

    async def _authorize(self, context, action, kind, identifier):
        await require_authorized(self.authorizer, context, action,
            ResourceRef(kind, context.tenant_id, identifier), audience=getattr(context, "audience", ()))

    async def _allowed(self, context, action, kind, identifier):
        try:
            await self._authorize(context, action, kind, identifier)
            return True
        except PermissionError:
            return False

    async def _row(self, context, *, artifact_id=None, legacy_path=None):
        from .memory.artifacts import _connection, artifact_store_root
        if (artifact_id is None) == (legacy_path is None):
            raise ValueError("Exactly one artifact identifier or compatibility path is required")
        await self._authorize(context, "artifact.read", "agent", context.agent_id)
        value = artifact_id
        column = "id"
        if legacy_path is not None:
            root = artifact_store_root(self.db).absolute()
            path = Path(legacy_path).absolute()
            # Reject even in-root symlinks. No user-chosen path is ever opened.
            try:
                relative = path.relative_to(root)
            except ValueError:
                raise LookupError("Artifact not found") from None
            if len(relative.parts) != 3 or relative.parts[0] != "sha256":
                raise LookupError("Artifact not found")
            digest = relative.parts[2]
            if not _DIGEST.fullmatch(digest) or relative.parts[1] != digest[:2]:
                raise LookupError("Artifact not found")
            value, column = relative.as_posix(), "storage_key"
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ValueError("Invalid artifact reference")
        async with _connection(self.db) as conn:
            row = await (await conn.execute(
                f"SELECT * FROM artifacts WHERE {column}=? AND tenant_id=? "
                "AND deleted_at_ms IS NULL AND storage_state='available'",
                (value, context.tenant_id))).fetchone()
            if row is None:
                raise LookupError("Artifact not found")
            row = dict(row)
            links = await (await conn.execute(
                "SELECT l.display_name, s.id AS session_id FROM artifact_links l "
                "JOIN sessions_v2 s ON s.tenant_id=l.tenant_id AND s.deleted_at_ms IS NULL AND "
                "(l.resource_type='session' AND s.id=l.resource_id OR "
                "l.resource_type='message' AND s.id=(SELECT session_id FROM session_messages WHERE id=l.resource_id AND tenant_id=l.tenant_id) OR "
                "l.resource_type='tool_invocation' AND s.id=(SELECT session_id FROM tool_invocations WHERE id=l.resource_id AND tenant_id=l.tenant_id)) "
                "WHERE l.tenant_id=? AND l.artifact_id=? ORDER BY l.created_at_ms,l.id",
                (context.tenant_id, row["id"]))).fetchall()
        owner = row.get("owner_principal_id")
        if owner and await self._allowed(context, "artifact.read", "principal", str(owner)):
            return row, str(row.get("original_filename") or row["id"])
        for link in links:
            if await self._allowed(context, "artifact.read", "session", link["session_id"]):
                # Deduplicated bytes never reveal the original private filename.
                return row, str(link["display_name"] or row["id"])
        raise LookupError("Artifact not found")

    def _bytes(self, row):
        from .memory.artifacts import artifact_store_root, ArtifactIntegrityError
        digest, size = row["sha256"], row["size_bytes"]
        if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
            raise ArtifactIntegrityError("Invalid artifact digest")
        if size > self.max_output_bytes:
            raise ValueError("Artifact exceeds the configured output limit")
        root = artifact_store_root(self.db).absolute()
        expected = f"sha256/{digest[:2]}/{digest}"
        if row["storage_key"] != expected:
            raise ArtifactIntegrityError("Invalid content-addressed path")
        path = root / expected
        current = path
        while current != root.parent:
            if current.is_symlink():
                raise ArtifactIntegrityError("Artifact storage contains a symbolic link")
            current = current.parent
        import os
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != size:
                raise ArtifactIntegrityError("Artifact is not a bounded regular file")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                content = stream.read(self.max_output_bytes + 1)
        finally:
            os.close(descriptor)
        if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
            raise ArtifactIntegrityError("Artifact content does not match its digest")
        return content

    async def read(self, context, *, artifact_id=None, legacy_path=None):
        from .memory.artifacts import safe_attachment_filename
        row, filename = await self._row(context, artifact_id=artifact_id, legacy_path=legacy_path)
        content = await asyncio.to_thread(self._bytes, row)
        current, current_name = await self._row(context, artifact_id=row["id"])
        if current["sha256"] != row["sha256"] or current_name != filename:
            raise PermissionError("Artifact authorization changed during delivery")
        return ArtifactContent(row["id"], safe_attachment_filename(filename),
            str(row["mime"] or "application/octet-stream"), row["sha256"], content)

    async def upload(self, context, content: bytes, *, filename: str, mime_type="application/octet-stream", session_id=None):
        """Persist supplied bytes under the verified actor, never a client path."""
        from .memory.artifacts import (_connection, artifact_store_root, _copy_into_cas,
            _ensure_artifact_row, _ensure_link, _cas_path, _attachment_ref,
            safe_attachment_filename, attachment_kind)
        if not isinstance(content, bytes) or len(content) > self.max_input_bytes:
            raise ValueError("Upload exceeds the configured input limit")
        if not isinstance(mime_type, str) or len(mime_type) > 256 or any(ord(c) < 32 for c in mime_type):
            raise ValueError("Invalid content type")
        principal = getattr(context, "initiator", context.authority)
        await self._authorize(context, "artifact.write", "agent", context.agent_id)
        if session_id:
            await self._authorize(context, "artifact.write", "session", session_id)
        filename = safe_attachment_filename(filename)
        root = artifact_store_root(self.db)
        root.mkdir(parents=True, exist_ok=True)
        async with self._writes:
            with tempfile.TemporaryDirectory(prefix="upload-", dir=root) as temporary:
                source = Path(temporary) / "bytes"
                source.write_bytes(content)
                task = asyncio.create_task(asyncio.to_thread(_copy_into_cas, source, root / "sha256/00/pending", self.max_input_bytes))
                try:
                    digest, size = await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
            await self._authorize(context, "artifact.write", "agent", context.agent_id)
            if session_id:
                await self._authorize(context, "artifact.write", "session", session_id)
            async with _connection(self.db) as conn:
                await conn.execute("BEGIN IMMEDIATE")
                try:
                    row = await _ensure_artifact_row(conn, ownership={"tenant_id":context.tenant_id,
                        "owner_principal_id":principal.key,"owner_handle_snapshot":principal.subject_id},
                        sha256=digest, size_bytes=size, mime=mime_type, filename=filename,
                        direction="input", kind=attachment_kind(mime_type))
                    # An unlinked, deduplicated upload must not confer access to
                    # someone else's bytes. Linking is allowed only to a freshly
                    # authorized canonical session; otherwise keep owner-private.
                    if not session_id and row["owner_principal_id"] != principal.key:
                        raise PermissionError("Deduplicated uploads require an authorized session")
                    link = await _ensure_link(conn,artifact=row,session_id=session_id or "",relation="input_attachment",
                        display_name=filename,existing_link_id=None)
                    await conn.commit()
                except BaseException:
                    await conn.rollback()
                    raise
            return _attachment_ref(row,path=_cas_path(root,digest),filename=filename,
                kind=attachment_kind(mime_type),link_id=link)
