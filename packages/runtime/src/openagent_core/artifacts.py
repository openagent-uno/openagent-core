"""Public host-authorized access to the engine's canonical attachment store.

CAS bytes and polymorphic links retain their existing schema. A filesystem
path is only a lookup hint: every read requires current authority to the owner
or an active linked session, and validates content integrity before delivery.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Protocol

from .contracts import ResourceRef, require_authorized

_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_ARTIFACT_ID = re.compile(r"art_[a-f0-9]{32}")
SESSION_ARTIFACT_PREFIX = "artifact:"


@dataclass(frozen=True, slots=True)
class ArtifactContent:
    artifact_id: str
    filename: str
    mime_type: str
    sha256: str
    content: bytes


class InputArtifactRepository(Protocol):
    """Canonical host-bound upload, authorization and bounded download."""

    authorizer: Any

    async def authorize(self, context, action: str) -> None: ...
    async def upload(self, context, content: bytes, *, filename: str, mime_type: str,
                     session_id: str | None = None) -> dict[str, Any]: ...
    async def read(self, context, *, artifact_id: str) -> ArtifactContent: ...


class ModelAttachmentReader(Protocol):
    """Resolve admitted references to verified bytes without borrowing a worker path."""

    max_model_input_bytes: int

    async def model_content(self, context, attachment, *, max_bytes: int | None = None) -> ArtifactContent: ...


class ArtifactRepository:
    def __init__(self, db: Any, authorizer: Any, *, max_input_bytes=64 << 20, max_output_bytes=256 << 20, runtime=None):
        self.db, self.authorizer, self.runtime = db, authorizer, runtime
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
            from .memory.artifact_document_history import visible
            from .memory.legacy_document_history import SessionProjectionGuard
            from .runtime import current_runtime
            runtime = self.runtime or current_runtime()

            async def authorize_source(session_id):
                from .contracts import ExecutionContext
                from .workspace_administration import SessionInspection
                if (not isinstance(context, ExecutionContext) or runtime is None or
                        (self.runtime is None and runtime.services.authorizer is not self.authorizer)):
                    raise PermissionError("Document-dependent attachments require their runtime")
                await self._authorize(context, "artifact.read", "session", session_id)
                inspector = SessionInspection(self.db, runtime.services.authorizer, runtime=runtime)
                await inspector.authorize(context, session_id, connection=conn)

            if not await visible(conn, row, SessionProjectionGuard(authorize_source)):
                raise LookupError("Artifact not found")
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
                def stage_and_copy():
                    source.write_bytes(content)
                    return _copy_into_cas(source, root / "sha256/00/pending", self.max_input_bytes)
                task = asyncio.create_task(asyncio.to_thread(stage_and_copy))
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


class AttachmentPublisher(Protocol):
    """Explicit host-selected reader/publisher; no implicit filesystem access.

    Register under ``attachments.publisher`` in RuntimeServices. The host must
    authorize this exact context, read through its selected sandbox capability,
    and return an ArtifactRepository.upload reference linked to this session.
    """
    async def publish(self, context: Any, path: str) -> dict[str, Any]: ...


class ContentAttachmentPublisher(Protocol):
    """Optional admitted-byte publication on the same host-selected publisher.

    The host revalidates the current execution and recipient authority, then
    uploads bounded bytes into the current session. This does not authorize
    reading a path, another tool result, or a different device.
    """
    async def publish_content(self, context: Any, content: bytes, *, filename: str,
                              mime_type: str) -> dict[str, Any]: ...


class SessionOutputResolver(Protocol):
    async def resolve_session_output(
        self, context: Any, run_id: str, artifact_id: str
    ) -> dict[str, Any]:
        """Resolve an available authorized output relation in this exact run and session."""
        ...


class OpaqueOutputPublisher(SessionOutputResolver, Protocol):
    """Publish a host-authorized asset identified by a provider namespace and opaque ID."""
    reference_schemes: frozenset[str]

    async def publish_reference(self, context: Any, reference: str) -> dict[str, Any]: ...


async def publish_opaque_output(runtime, context, run_id, reference):
    from .memory.artifacts import public_attachment_ref
    from .memory.vault.privacy import require_unprotected_execution

    match = re.fullmatch(r"([a-z][a-z0-9-]{0,47}):([A-Za-z0-9][A-Za-z0-9._-]{0,159})", reference)
    if not match or match[1] in {"artifact", "data", "file", "http", "https"}:
        return None
    publisher = runtime.service(OpaqueOutputPublisher)
    if match[1] not in getattr(publisher, "reference_schemes", ()):
        return None
    resource = ResourceRef("session", context.tenant_id, context.session_id)
    await require_unprotected_execution("Host reference publication")
    await runtime.authorize(context, "artifact.write", resource, audience=context.audience)
    output = await publisher.publish_reference(context, reference)
    ident = output.get("artifact_id") if isinstance(output, dict) else None
    if not canonical_artifact_id(ident):
        return None
    await runtime.authorize(context, "artifact.read", resource, audience=context.audience)
    verified = await publisher.resolve_session_output(context, run_id, ident)
    await runtime.authorize(context, "artifact.read", resource, audience=context.audience)
    return public_attachment_ref(verified) if isinstance(verified, dict) and verified.get("artifact_id") == ident else None


# Inherited by local subtasks. The run ID keeps a child from contributing a
# marker to its parent unless that parent independently receives a validated ref.
_native_outputs = ContextVar("native_model_outputs", default=None)


@contextmanager
def native_output_scope(run_id):
    token = _native_outputs.set((run_id, set()))
    try:
        yield
    finally:
        _native_outputs.reset(token)


def remember_native_output(run_id, artifact_id):
    state = _native_outputs.get()
    if state is not None and state[0] == run_id:
        state[1].add(artifact_id)


def is_native_output(run_id, artifact_id):
    state = _native_outputs.get()
    return state is not None and state[0] == run_id and artifact_id in state[1]


async def tool_delivery_marker(content: bytes, mime: str, filename: str, *, verify_bytes: bool = False) -> str | None:
    from openagent_core.runtime import current_runtime, current_execution_context, current_run_id
    runtime, context = current_runtime(), current_execution_context()
    run_id = current_run_id()
    if runtime is None or context is None or not run_id:
        return None
    lookup = getattr(runtime, "service", None)
    publisher = lookup("attachments.publisher") if callable(lookup) else None
    publish = getattr(publisher, "publish_content", None)
    if not callable(publish):
        return None
    from openagent_core.memory.vault.privacy import require_unprotected_execution
    resource = ResourceRef("session", context.tenant_id, context.session_id)
    authorizer = runtime.services.authorizer
    verified = None
    try:
        await require_unprotected_execution("Tool result attachment delivery")
        await require_authorized(authorizer, context, "run.publish", resource, audience=context.audience)
        db = getattr(getattr(runtime.services.executor, "agent", None), "memory_db", None)
        resolve = getattr(publisher, "resolve_session_output", None)
        if db is not None or callable(resolve):
            ref = await publish(context, content, filename=filename, mime_type=mime)
            if db is not None:
                verified = await session_output_reference(db, tenant_id=context.tenant_id,
                    session_id=context.session_id, value=ref)
            elif isinstance(ref, dict) and canonical_artifact_id(ref.get("artifact_id")):
                await runtime.authorize(context, "artifact.read", resource, audience=context.audience)
                candidate = await resolve(context, run_id, ref["artifact_id"])
                await runtime.authorize(context, "artifact.read", resource, audience=context.audience)
                if isinstance(candidate, dict) and candidate.get("artifact_id") == ref["artifact_id"]:
                    verified = candidate
            if verify_bytes and verified is not None:
                digest = await asyncio.to_thread(lambda: hashlib.sha256(content).hexdigest())
                if verified.get("size_bytes") != len(content) or verified.get("sha256") != digest:
                    verified = None
    except Exception as exc:
        # Delivery is optional; keep the admitted preview if its publisher is
        # unavailable. Revocation still fails closed at the fresh check below.
        verified = None
        import logging
        logging.getLogger(__name__).warning("Tool attachment publication unavailable (%s)", type(exc).__name__)
    await require_authorized(authorizer, context, "run.publish", resource, audience=context.audience)
    if verified is None:
        return None
    return f"[FILE:{SESSION_ARTIFACT_PREFIX}{verified['artifact_id']}]"


def tool_file_delivery(block: Any) -> dict[str, Any] | None:
    if not isinstance(block, dict) or block.get("type") != "resource":
        return None
    metadata = block.get("_meta")
    delivery = metadata.get("openagent/file-delivery") if isinstance(metadata, dict) else None
    return delivery if isinstance(delivery, dict) else None


async def publish_tool_file_resources(envelope: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Replace explicitly offered binary resources with scoped delivery markers.

    The model receives metadata and a session artifact reference. Opaque file
    bytes never enter its text context or the result pagination cache.
    """
    import mimetypes
    from openagent_core.memory.artifacts import safe_attachment_filename

    result = []
    markers = []
    remaining = 32 * 1024 * 1024
    for block in envelope["content"]:
        delivery = tool_file_delivery(block)
        if delivery is None:
            result.append(block)
            continue
        resource = block.get("resource")
        filename = safe_attachment_filename(delivery.get("filename") or "download")
        marker = None
        blob = resource.get("blob") if isinstance(resource, dict) else None
        if isinstance(blob, str) and len(blob) <= 4 * ((remaining + 2) // 3) and envelope.get("isError") is not True:
            try:
                content = await asyncio.to_thread(base64.b64decode, blob, validate=True)
            except ValueError:
                pass
            else:
                if len(content) <= remaining:
                    remaining -= len(content)
                    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
                    marker = await tool_delivery_marker(content, mime, filename, verify_bytes=True)
        if marker is not None:
            markers.append(marker)
        notice = (f"File {filename} is available in this session. To send it to the user, include {marker}."
                  if marker else f"File {filename} could not be added to this session.")
        result.append({"type": "text", "text": notice})
    return {**envelope, "content": result}, markers


def canonical_artifact_id(value):
    return isinstance(value, str) and _ARTIFACT_ID.fullmatch(value) is not None


async def document_turn_originals(db, runtime, context, *, query="", artifact_id=None):
    """Resolve immutable user inputs with their original audience and live source grants."""
    from .memory.artifact_document_history import visible
    from .memory.artifacts import (_connection, _attachment_ref, attachment_kind,
                                  public_attachment_ref, safe_attachment_filename)
    from .memory.document_exposure import authorize_source_audience
    from .memory.legacy_document_history import SessionProjectionGuard
    from .memory.vault.privacy import require_unprotected_connector_execution
    from .runtime import current_run_id
    from .workspace_administration import SessionInspection

    store = runtime.services.store
    run_id = current_run_id()
    if (not run_id or (artifact_id is not None and not canonical_artifact_id(artifact_id))
            or db is None or not getattr(store, "path", None) or not getattr(db, "db_path", None)
            or Path(store.path).resolve() != Path(db.db_path).resolve()):
        return []
    session = ResourceRef("session", context.tenant_id, context.session_id)
    await runtime.authorize(context, "run.publish", session, audience=context.audience)

    inspector = SessionInspection(db, runtime.services.authorizer, runtime=runtime)
    authorized_sources = set()

    async def check_source_once(sid):
        if sid not in authorized_sources:
            await inspector.authorize(context, sid, connection=conn)
            authorized_sources.add(sid)

    # HTTP admission persists the user message and attachment references together.
    # Some transports also materialize individual message links after the turn.
    current_filter = "AND item.value ->> '$.artifact_id'=:aid" if artifact_id else ""
    history_filter = "AND sl.artifact_id=:aid" if artifact_id else ""
    sql = f"""
        WITH turn AS (
            SELECT ordinal, created_at_ms FROM session_runs
             WHERE id=:run AND tenant_id=:tenant AND session_id=:session AND agent_id=:agent
        ), inputs AS (
            SELECT r.id AS run_id, m.author_principal_id AS message_author, r.created_at_ms AS stamp,
                   item.value ->> '$.artifact_id' AS aid, item.value ->> '$.filename' AS filename,
                   item.value ->> '$.artifact_link_id' AS input_link
              FROM turn JOIN session_runs r ON r.session_id=:session AND r.ordinal<=turn.ordinal
              JOIN json_each(r.metadata_json, '$.attachments') item
              LEFT JOIN session_messages m ON m.run_id=r.id AND m.tenant_id=r.tenant_id
               AND m.session_id=r.session_id AND m.ordinal=0 AND m.role='user' AND m.author_kind='user'
               AND m.visibility='user_visible' AND m.status='complete'
             WHERE r.tenant_id=:tenant AND r.agent_id=:agent AND (r.id=:run OR m.author_principal_id IS NOT NULL)
               AND json_type(r.metadata_json, '$.attachments[0]')='object'
               AND item.type='object'
               AND r.metadata_json ->> '$.runtime_contract'=1
               AND r.metadata_json ->> '$.execution_context.author.kind'='user'
               {current_filter}
            UNION ALL
            SELECT CASE WHEN r.metadata_json ->> '$.runtime_contract'=1 THEN r.id END,
                   m.author_principal_id, m.created_at_ms, ml.artifact_id, ml.display_name, sl.id
              -- Start from this chat even when SQLite has no planner statistics.
              FROM artifact_links sl
              CROSS JOIN artifact_links ml INDEXED BY idx_artifact_links_artifact
                ON ml.tenant_id=sl.tenant_id AND ml.artifact_id=sl.artifact_id
               AND ml.resource_type='message' AND ml.relation='input_attachment'
              CROSS JOIN session_messages m ON m.id=ml.resource_id AND m.tenant_id=ml.tenant_id
              LEFT JOIN session_runs r ON r.id=m.run_id AND r.session_id=m.session_id
               AND r.tenant_id=m.tenant_id
              CROSS JOIN turn
             WHERE sl.tenant_id=:tenant AND sl.resource_type='session'
               AND sl.resource_id=:session AND sl.relation='input_attachment'
               AND m.session_id=:session AND m.role='user'
               AND (r.ordinal<=turn.ordinal OR (r.id IS NULL AND m.created_at_ms<=turn.created_at_ms))
               AND (r.agent_id=:agent OR r.metadata_json ->> '$.runtime_contract' IS NOT 1)
               AND m.author_kind='user' AND m.visibility='user_visible' AND m.status='complete'
               {history_filter}
        )
        SELECT a.*, i.run_id, i.message_author, l.id AS link_id,
               coalesce(i.filename, CASE WHEN NOT EXISTS (
                   SELECT 1 FROM artifact_links other
                    WHERE other.tenant_id=l.tenant_id AND other.artifact_id=l.artifact_id
                      AND other.resource_type='session' AND other.resource_id=l.resource_id
                      AND other.relation='input_attachment' AND other.created_at_ms<=i.stamp
                      AND other.display_name IS NOT l.display_name
               ) THEN l.display_name END, a.id) AS input_filename
          FROM inputs i JOIN artifacts a ON a.id=i.aid AND a.tenant_id=:tenant
          JOIN artifact_links l ON l.tenant_id=a.tenant_id AND l.artifact_id=a.id
           AND l.resource_type='session' AND l.resource_id=:session AND l.relation='input_attachment'
           AND (i.input_link IS NULL OR l.id=i.input_link)
           AND (i.filename IS NOT NULL OR l.created_at_ms<=i.stamp)
          JOIN sessions_v2 s ON s.tenant_id=l.tenant_id AND s.id=l.resource_id
         WHERE s.deleted_at_ms IS NULL AND a.deleted_at_ms IS NULL AND a.storage_state='available'
           AND instr(lower(input_filename), lower(:query))>0
         ORDER BY i.stamp DESC, a.id, l.id
    """
    guard = SessionProjectionGuard(check_source_once)
    refs = {}
    admitted = {}
    delegated = {}
    async with _connection(db) as conn:
        # A foreign or unavailable ID never needs a transcript lookup.
        if artifact_id is not None and not await (await conn.execute(
                """SELECT 1 FROM artifact_links l JOIN artifacts a ON a.id=l.artifact_id AND a.tenant_id=l.tenant_id
                    WHERE l.tenant_id=? AND l.artifact_id=? AND l.resource_type='session' AND l.resource_id=?
                      AND l.relation='input_attachment' AND a.deleted_at_ms IS NULL AND a.storage_state='available'
                    LIMIT 1""", (context.tenant_id, artifact_id, context.session_id))).fetchone():
            return []
        async with conn.execute(sql, dict(tenant=context.tenant_id, session=context.session_id,
                                          agent=context.agent_id, run=run_id, aid=artifact_id, query=query)) as rows:
            async for row in rows:
                ident, run = row["id"], row["run_id"]
                if ident in refs:
                    continue
                if run is None:
                    # Legacy transcripts lack a durable audience. Only an input
                    # authored by this exact private viewer can establish scope.
                    allowed = (
                        context.initiator.kind == "user" and context.authority == context.initiator
                        and context.audience == (context.initiator,) and row["message_author"] == context.initiator.key
                    )
                else:
                    if run not in admitted:
                        try:
                            accepted = await store.accepted_request(run)
                            await authorize_source_audience(runtime, accepted, context)
                            admitted[run] = (accepted.author.key if accepted.author.kind == "user"
                                             and accepted.request.session_id == context.session_id else None)
                            if admitted[run] and accepted.delegation_id:
                                delegated[run] = accepted
                        except (LookupError, PermissionError):
                            admitted[run] = None
                    allowed = admitted[run] and (row["message_author"] is None or row["message_author"] == admitted[run])
                if not allowed or not await visible(conn, row, guard):
                    continue
                filename = safe_attachment_filename(row["input_filename"])
                refs[ident] = public_attachment_ref(_attachment_ref(
                    row, path=Path(), filename=filename, kind=attachment_kind(row["mime"]), link_id=row["link_id"]))
                if len(refs) == 21 or artifact_id is not None:
                    break
        authorized_sources.clear()
        await guard.recheck()
        await check_source_once(context.session_id)
    for source in delegated.values():
        await authorize_source_audience(runtime, source, context)
    await require_unprotected_connector_execution("Session attachment delivery")
    await runtime.authorize(context, "run.publish", session, audience=context.audience)
    return list(refs.values())


async def find_session_attachments(db, *, tenant_id, session_id, query):
    """List available files linked to the authorized session, with no local paths."""
    from .memory.artifacts import _connection, safe_attachment_filename

    # A dedicated connection: the shared one may hold a snapshot taken before
    # an attachment this run just published (see session_output_reference).
    async with _connection(db) as conn:
        rows = await (await conn.execute(
            "WITH matches AS ("
            "SELECT a.id, a.mime, a.size_bytes, l.display_name, l.created_at_ms, "
            "ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY l.created_at_ms DESC, l.id DESC) AS rank "
            "FROM artifact_links l "
            "JOIN artifacts a ON a.tenant_id=l.tenant_id AND a.id=l.artifact_id "
            "JOIN sessions_v2 s ON s.tenant_id=l.tenant_id AND s.id=l.resource_id "
            "WHERE l.tenant_id=? AND l.resource_type='session' AND l.resource_id=? "
            "AND s.deleted_at_ms IS NULL AND a.deleted_at_ms IS NULL AND a.storage_state='available' "
            "AND instr(lower(coalesce(l.display_name, a.id)), lower(?)) > 0) "
            "SELECT * FROM matches WHERE rank=1 ORDER BY created_at_ms DESC, id DESC LIMIT 21",
            (tenant_id, session_id, query))).fetchall()
    return {
        "attachments": [
            {"artifact_id": row["id"], "filename": safe_attachment_filename(row["display_name"] or row["id"]),
             "mime_type": row["mime"] or "application/octet-stream", "size_bytes": row["size_bytes"]}
            for row in rows[:20]
        ],
        "truncated": len(rows) > 20,
    }


async def session_output_reference(db, *, tenant_id, session_id, value, link=False):
    """Resolve only already-published CAS bytes linked to the authorized session.

    The caller checks session publication/read authorization before and after
    use. This lookup never opens a model-supplied path or confers a new scope.
    """
    from .memory.artifacts import (artifact_store_root, _connection, _ensure_link,
        _attachment_ref, public_attachment_ref, safe_attachment_filename, attachment_kind)
    if not isinstance(value, dict):
        return None
    artifact_id = value.get("artifact_id")
    storage_key = None
    if not artifact_id:
        path = value.get("path")
        if not isinstance(path, str):
            return None
        try:
            relative = Path(path).absolute().relative_to(artifact_store_root(db).absolute())
        except ValueError:
            return None
        if len(relative.parts) != 3 or relative.parts[0] != "sha256":
            return None
        digest = relative.parts[2]
        if not _DIGEST.fullmatch(digest) or relative.parts[1] != digest[:2]:
            return None
        storage_key = relative.as_posix()
    elif not isinstance(artifact_id, str) or not _ARTIFACT_ID.fullmatch(artifact_id):
        return None
    # A dedicated connection, like the upload that wrote the link: during a run
    # the shared connection can sit inside an implicit transaction whose
    # snapshot predates that commit and would not see the artifact.
    async with _connection(db) as conn:
        row = await (await conn.execute(
            "SELECT a.*,l.display_name,l.id AS link_id FROM artifacts a "
            "JOIN artifact_links l ON l.tenant_id=a.tenant_id AND l.artifact_id=a.id "
            "JOIN sessions_v2 s ON s.id=l.resource_id AND s.tenant_id=l.tenant_id "
            "WHERE a.tenant_id=? AND l.resource_type='session' AND l.resource_id=? "
            "AND s.deleted_at_ms IS NULL AND a.deleted_at_ms IS NULL AND a.storage_state='available' "
            "AND a." + ("id" if artifact_id else "storage_key") + "=? ORDER BY CASE WHEN l.id=? THEN 0 ELSE 1 END,l.created_at_ms,l.id LIMIT 1",
            (tenant_id, session_id, artifact_id or storage_key, value.get("artifact_link_id", "")))).fetchone()
        if row is None:
            return None
        filename = safe_attachment_filename(row["display_name"] or row["id"])
        link_id = row["link_id"]
        if link:
            existing = await (await conn.execute(
                "SELECT id FROM artifact_links WHERE tenant_id=? AND artifact_id=? AND resource_type='session' "
                "AND resource_id=? AND relation='output_attachment' LIMIT 1",
                (tenant_id,row["id"],session_id))).fetchone()
            link_id = await _ensure_link(conn,artifact=row,session_id=session_id,relation="output_attachment",
                display_name=filename,existing_link_id=existing[0] if existing else None)
            await conn.commit()
        return public_attachment_ref(_attachment_ref(row,path=Path(),filename=filename,
            kind=attachment_kind(row["mime"]),link_id=link_id))
