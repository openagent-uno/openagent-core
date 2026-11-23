"""Scoped artifact references over a host-owned SQL index and private object store."""
from __future__ import annotations

import base64
import hashlib
import re
from collections.abc import AsyncIterable
from contextlib import nullcontext
from dataclasses import replace
from typing import Protocol

import sqlalchemy as sa

from .artifacts import ArtifactContent
from .contracts import ExecutionContext, ResourceRef, canonical_json, require_authorized
from .memory.artifacts import attachment_kind, safe_attachment_filename
from .memory.vault.privacy import require_unprotected_execution
from .runtime import current_execution_context, current_run_id

metadata = sa.MetaData()
objects = sa.Table(
    "oa_session_artifacts", metadata,
    sa.Column("artifact_id", sa.String(36), primary_key=True),
    sa.Column("tenant", sa.Text, nullable=False),
    sa.Column("agent", sa.Text, nullable=False),
    sa.Column("session_id", sa.Text, nullable=False),
    sa.Column("run_id", sa.Text),
    sa.Column("audience", sa.JSON, nullable=False),
    sa.Column("filename", sa.Text, nullable=False),
    sa.Column("mime_type", sa.Text, nullable=False),
    sa.Column("digest", sa.String(64), nullable=False),
    sa.Column("size", sa.BigInteger, nullable=False),
    sa.Column("chunks", sa.JSON, nullable=False),
)
sa.Index("oa_session_artifact_outputs", objects.c.tenant, objects.c.agent, objects.c.session_id, objects.c.run_id)


class ChunkStore(Protocol):
    async def put(self, key: str, data: bytes) -> None: ...
    async def get(self, key: str, size: int) -> bytes: ...


class S3Chunks:
    """Use an async S3 client supplied by the host. The bucket must be private."""
    def __init__(self, client, bucket, *, prefix="openagent/"):
        self.client, self.bucket, self.prefix = client, bucket, prefix

    async def put(self, key, data):
        await self.client.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=data,
                                     ContentMD5=base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode())

    async def put_immutable(self, key, data):
        """Create a private chunk or verify the exact pre-existing content."""
        from botocore.exceptions import ClientError
        try:
            await self.client.put_object(Bucket=self.bucket, Key=self.prefix + key, Body=data, IfNoneMatch="*",
                ContentMD5=base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode())
        except ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
        if await self.get(key, len(data)) != data:
            raise IOError("Existing archive chunk differs")

    async def get(self, key, size):
        result = await self.client.get_object(Bucket=self.bucket, Key=self.prefix + key)
        async with result["Body"] as stream:
            if result["ContentLength"] != size:
                raise IOError("Artifact chunk length changed")
            return await stream.read()


def _digest(value):
    return hashlib.sha256(value).hexdigest()


def artifact_chunk_key(tenant_id, agent_id, digest):
    namespace = _digest(canonical_json([tenant_id, agent_id]).encode())
    return namespace + "/sha256/" + digest


def _identity(row, *, owner_input=False):
    fields = [row[k] for k in ("tenant", "agent", "session_id", "run_id", "audience",
                                "filename", "mime_type", "digest")]
    if owner_input:
        fields.append("owner_ingress")
    return "art_" + _digest(canonical_json(fields).encode())[:32]


def _ingress_owner(row):
    if row["run_id"] is None and len(row["audience"]) == 1 and row["artifact_id"] == _identity(row, owner_input=True):
        return row["audience"][0]
    return None


def _public(row):
    ident = row["artifact_id"]
    kind = attachment_kind(row["mime_type"])
    ref = {"artifact_id": ident, "filename": row["filename"], "mime_type": row["mime_type"],
            "size_bytes": row["size"], "sha256": row["digest"], "kind": kind, "type": kind,
            "url": "/api/artifacts/" + ident + "/content"}
    if _ingress_owner(row) is None:
        ref["artifact_link_id"] = "lnk_" + ident[4:]
    return ref


class SqlObjectArtifacts:
    """The SQL row is the publication boundary. Chunks have no public URL."""
    def __init__(self, engine, blobs: ChunkStore, authorizer, *, chunk_bytes=4 << 20, max_bytes=256 << 20, archive=None, max_model_input_bytes=64 << 20):
        if not 1 <= chunk_bytes <= 16 << 20 or max_bytes < chunk_bytes or max_bytes > 4096 * chunk_bytes:
            raise ValueError("Invalid artifact byte limits")
        if engine.dialect.name not in {"postgresql", "sqlite"}:
            raise ValueError("Unsupported artifact index database")
        self.engine, self.blobs, self.authorizer = engine, blobs, authorizer
        self.chunk_bytes, self.max_bytes = chunk_bytes, max_bytes
        if type(max_model_input_bytes) is not int or max_model_input_bytes < 1:
            raise ValueError("Invalid model input budget")
        self.max_model_input_bytes = max_model_input_bytes
        self.archive = archive

    async def initialize(self):
        async with self.engine.begin() as con:
            await con.run_sync(metadata.create_all)

    async def _authorize(self, ctx, action):
        if not isinstance(ctx, ExecutionContext):
            raise TypeError("An authenticated execution context is required")
        target = ResourceRef("session", ctx.tenant_id, ctx.session_id)
        await require_authorized(self.authorizer, ctx, action, target, audience=ctx.audience)

    @staticmethod
    def _key(ctx, digest):
        return artifact_chunk_key(ctx.tenant_id, ctx.agent_id, digest)

    async def authorize(self, context, action):
        if action not in {"artifact.write", "artifact.read"}:
            raise ValueError("Unknown artifact action")
        await self._authorize(context, action)

    async def upload(self, context, content, *, filename, mime_type="application/octet-stream", session_id=None):
        if session_id is not None and session_id != context.session_id:
            raise PermissionError("Upload belongs to a different session")
        private = session_id is None
        if private and (current_execution_context() is not None or context.author != context.initiator
                or set(context.audience) != {context.initiator}):
            raise PermissionError("Unlinked input requires a resolved private ingress owner")
        async def stream():
            yield content
        return await self._publish_stream(context, stream(), filename=filename, mime_type=mime_type,
                                          private_input=private)

    async def publish_content(self, context, content, *, filename, mime_type):
        async def source():
            yield content
        return await self.publish_stream(context, source(), filename=filename, mime_type=mime_type)

    async def publish_stream(self, context, source: AsyncIterable[bytes], *, filename, mime_type):
        return await self._publish_stream(context, source, filename=filename, mime_type=mime_type, private_input=False)

    async def _publish_stream(self, context, source, *, filename, mime_type, private_input):
        execution = current_execution_context()
        if execution is not None and execution != context:
            raise PermissionError("Artifact publication must use the active context")
        await self._authorize(context, "artifact.write")
        await require_unprotected_execution("Object artifact publication")
        filename = safe_attachment_filename(filename)
        if not isinstance(mime_type, str) or not re.fullmatch(r"[A-Za-z0-9!#$&^_.+~-]{1,100}/[A-Za-z0-9!#$&^_.+~-]{1,100}", mime_type):
            raise ValueError("Invalid MIME type")
        parts, total, complete = [], 0, hashlib.sha256()
        chunk = bytearray()

        async def store_block(block):
            await self._authorize(context, "artifact.write")
            digest = _digest(block)
            await self.blobs.put(self._key(context, digest), block)
            parts.append([digest, len(block)])

        async for raw in source:
            if not isinstance(raw, (bytes, bytearray, memoryview)):
                raise TypeError("Artifact input must yield bytes")
            data = memoryview(raw).cast("B")
            total += len(data)
            if total > self.max_bytes:
                raise ValueError("Artifact exceeds configured limit")
            complete.update(data)
            while data:
                take = min(self.chunk_bytes - len(chunk), len(data))
                chunk.extend(data[:take])
                data = data[take:]
                if len(chunk) == self.chunk_bytes:
                    await store_block(bytes(chunk))
                    chunk.clear()
        if chunk:
            await store_block(bytes(chunk))
        digest = complete.hexdigest()
        run_id = current_run_id()
        audience = sorted({p.key for p in (*context.audience, context.initiator)})
        values = dict(tenant=context.tenant_id, agent=context.agent_id, session_id=context.session_id,
                      run_id=run_id, audience=audience, filename=filename, mime_type=mime_type,
                      digest=digest, size=total, chunks=parts)
        ident = values["artifact_id"] = _identity(values, owner_input=private_input)
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        async with self.engine.begin() as con:
            await self._authorize(context, "artifact.write")
            await self._authorize(context, "artifact.publish")
            await require_unprotected_execution("Object artifact publication")
            await con.execute(insert(objects).values(**values).on_conflict_do_nothing(index_elements=[objects.c.artifact_id]))
        await self._authorize(context, "artifact.publish")
        return _public(values)

    async def _row(self, ctx, ident):
        await self._authorize(ctx, "artifact.read")
        if not isinstance(ident, str) or len(ident) != 36 or not ident.startswith("art_"):
            raise LookupError("Artifact not found")
        stmt = sa.select(objects).filter_by(artifact_id=ident, tenant=ctx.tenant_id,
                                            agent=ctx.agent_id, session_id=ctx.session_id)
        async with self.engine.connect() as con:
            row = (await con.execute(stmt)).mappings().one_or_none()
        if row is None or not {p.key for p in (*ctx.audience, ctx.initiator)}.issubset(row["audience"]):
            raise LookupError("Artifact not found")
        owner = _ingress_owner(row)
        if owner is not None and {p.key for p in (*ctx.audience, ctx.author, ctx.initiator)} != {owner}:
            raise LookupError("Artifact not found")
        await self._authorize(ctx, "artifact.publish")
        return row

    async def resolve(self, context, artifact_id):
        if self.archive is not None:
            historical = await self.archive.resolve_if_present(context, artifact_id)
            if historical is not None:
                return historical
        return _public(await self._row(context, artifact_id))

    async def read(self, context, *, artifact_id):
        return await self._read_bounded(context, artifact_id, self.max_bytes)

    async def _read_bounded(self, context, artifact_id, limit):
        ref = await self.resolve(context, artifact_id)
        if ref["size_bytes"] > limit:
            raise ValueError("Artifact exceeds the admitted read budget")
        data = bytearray()
        async for chunk in self.read_stream(context, artifact_id):
            if len(data) + len(chunk) > limit:
                raise ValueError("Artifact exceeds the admitted read budget")
            data.extend(chunk)
        after = await self.resolve(context, artifact_id)
        if ref != after or len(data) != ref["size_bytes"] or _digest(data) != ref["sha256"]:
            raise IOError("Artifact changed during delivery")
        return ArtifactContent(artifact_id, ref["filename"], ref["mime_type"], ref["sha256"], bytes(data))

    async def model_content(self, context, attachment, *, max_bytes=None):
        from .runtime import guard_current_publication
        if current_execution_context() != context or not current_run_id():
            raise PermissionError("Model attachments require an admitted execution")
        if max_bytes is not None and (type(max_bytes) is not int or max_bytes < 0):
            raise ValueError("Invalid model attachment budget")
        attachment = dict(attachment)
        ident = attachment.get("artifact_id")
        run_id = current_run_id()
        has_path = "path" in attachment
        if has_path and self.archive is None:
            raise PermissionError("A historical path requires its verified original request")
        source = (self.archive.admitted_request(context, run_id, attachment, required=has_path)
                  if self.archive is not None else nullcontext(False))
        limit = self.max_model_input_bytes if max_bytes is None else min(max_bytes, self.max_model_input_bytes)
        async with source as historical:
            item = await self._read_bounded(context, ident, limit)
        facts = dict(mime_type=item.mime_type, sha256=item.sha256, size_bytes=len(item.content),
                     url=f"/api/artifacts/{ident}/content")
        if any(attachment[k] != v for k,v in facts.items() if k in attachment):
            raise ValueError("Admitted attachment metadata differs from storage")
        async def recheck():
            if historical:
                await self.archive.require_request(context, run_id, attachment)
            else:
                await self.resolve(context, ident)
        guard_current_publication(recheck)
        if historical and "filename" in attachment:
            return replace(item, filename=safe_attachment_filename(attachment["filename"]))
        return item

    async def resolve_session_output(self, context, run_id, artifact_id):
        row = await self._row(context, artifact_id)
        if not run_id or row["run_id"] != run_id:
            raise LookupError("The run did not produce this artifact")
        return _public(row)

    async def read_stream(self, context, artifact_id):
        if self.archive is not None:
            stream = await self.archive.archived_stream(context, artifact_id)
            if stream is not None:
                async for data in stream:
                    yield data
                return
        row = await self._row(context, artifact_id)
        complete = hashlib.sha256()
        for digest, size in row["chunks"]:
            await self._authorize(context, "artifact.read")
            data = await self.blobs.get(self._key(context, digest), size)
            if len(data) != size or _digest(data) != digest:
                raise IOError("Artifact integrity check failed")
            complete.update(data)
            await self._authorize(context, "artifact.read")
            await self._authorize(context, "artifact.publish")
            yield data
        if complete.hexdigest() != row["digest"]:
            raise IOError("Artifact digest mismatch")
