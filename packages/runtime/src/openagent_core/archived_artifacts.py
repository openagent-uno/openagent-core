"""Read an immutable historical artifact index through the admitted runtime."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import re

import sqlalchemy as sa

from .contracts import ResourceRef, canonical_json
from .memory.artifacts import safe_attachment_filename
from .memory.document_exposure import authorize_document_history
from .object_artifacts import SqlObjectArtifacts


def archive_schema_name(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"oa_archive_[a-z0-9_]{1,40}", value
    ):
        raise ValueError("A dedicated artifact archive schema is required")
    return value


class SqlArtifactArchive:
    """The runtime supplier must return the current agent bound to the imported ledger."""

    def __init__(self, engine, blobs, *, schema, runtime):
        if engine.dialect.name != "postgresql" or not callable(runtime):
            raise ValueError("An archive requires PostgreSQL and the active runtime")
        self.engine, self.blobs = engine, blobs
        self.schema, self.runtime = archive_schema_name(schema), runtime

    async def _find(self, context, ident):
        if not isinstance(ident, str) or not re.fullmatch(r"art_[0-9a-f]{32}", ident):
            return None
        stmt = sa.text(
            f'SELECT descriptor, chunks FROM "{self.schema}".archive_objects '
            "WHERE tenant=:tenant AND agent=:agent AND id=:ident"
        )
        async with self.engine.connect() as con:
            row = (
                await con.execute(
                    stmt,
                    dict(tenant=context.tenant_id, agent=context.agent_id, ident=ident),
                )
            ).first()
        return None if row is None else (row[0], row[1])

    async def _access(self, ctx, item):
        runtime = self.runtime()
        session = ResourceRef("session", ctx.tenant_id, ctx.session_id)
        await runtime.authorize(ctx, "artifact.read", session, audience=ctx.audience)
        readers = {p.key for p in (*ctx.audience, ctx.initiator, ctx.author)}
        owner = item["record"]["owner_principal_id"]
        # Private deduplicated inputs can have several independent source sessions.
        # An owner or another link may not launder a withdrawn source.
        for origin in item["origins"]:
            source = replace(ctx, session_id=origin["session_id"])
            await runtime.authorize(
                source,
                "session.read",
                ResourceRef("session", ctx.tenant_id, source.session_id),
                audience=ctx.audience,
            )
            check = getattr(runtime.services.store, "require_artifact_origin", None)
            if not callable(check) or not callable(
                getattr(runtime.services.store, "document_sources", None)
            ):
                raise PermissionError(
                    "Archived origin has no current authority or document lineage"
                )
            await check(source, source.session_id, origin["run_id"])
            if origin["run_id"] is not None:
                await authorize_document_history(
                    runtime, source, run_id=origin["run_id"]
                )
        match = next(
            (
                link
                for link in item["links"]
                if link["resource_type"] == "session"
                and link["resource_id"] == ctx.session_id
            ),
            None,
        )
        eligible = any(
            origin["session_id"] == ctx.session_id
            and readers.issubset(origin["audience"])
            for origin in item["origins"]
        )
        if readers == {owner}:
            await runtime.authorize(
                ctx,
                "artifact.read",
                ResourceRef("principal", ctx.tenant_id, owner),
                audience=ctx.audience,
            )
            filename = item["record"]["original_filename"]
        elif match is not None and eligible:
            filename = match["display_name"]
        else:
            raise LookupError("Artifact not found")
        await runtime.authorize(ctx, "artifact.publish", session, audience=ctx.audience)
        row = item["record"]
        ref = dict(
            artifact_id=row["id"],
            filename=safe_attachment_filename(filename or row["id"]),
            mime_type=row["mime"],
            size_bytes=row["size_bytes"],
            sha256=row["sha256"],
            kind=row["kind"],
            type=row["kind"],
            url=f"/api/artifacts/{row['id']}/content",
        )
        if match is not None:
            ref["artifact_link_id"] = match["id"]
        return ref

    async def resolve_if_present(self, ctx, ident):
        found = await self._find(ctx, ident)
        return None if found is None else await self._access(ctx, found[0])

    async def require_request(self, ctx, run_id, reference):
        found = await self._find(ctx, reference.get("artifact_id"))
        if found is not None:
            digest = hashlib.sha256(canonical_json(reference).encode()).hexdigest()
            if any(origin["run_id"] == run_id and origin["session_id"] == ctx.session_id
                   and origin.get("request_sha256") == digest for origin in found[0]["origins"]):
                await self._access(ctx, found[0])
                return
        raise PermissionError("The admitted attachment differs from its sealed request")

    async def archived_stream(self, ctx, ident):
        found = await self._find(ctx, ident)
        if found is None:
            return None
        item, chunks = found
        await self._access(ctx, item)

        async def content():
            complete = hashlib.sha256()
            for digest, size in chunks:
                await self._access(ctx, item)
                data = await self.blobs.get(SqlObjectArtifacts._key(ctx, digest), size)
                if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                    raise IOError("Archived artifact integrity check failed")
                complete.update(data)
                await self._access(ctx, item)
                yield data
            if complete.hexdigest() != item["record"]["sha256"]:
                raise IOError("Archived artifact digest differs")

        return content()
