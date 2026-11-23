"""Seal original attachment identities, private links and verified object bytes for a cloud candidate."""

from __future__ import annotations

from contextlib import closing
import hashlib
from itertools import groupby
import json
from pathlib import Path, PurePosixPath
import re
import sqlite3

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from openagent_core.archived_artifacts import archive_schema_name
from openagent_core.object_artifacts import artifact_chunk_key
from openagent_core.contracts import PrincipalRef, canonical_json
from openagent_core.memory.artifacts import safe_attachment_filename
from .migration import MANIFEST, _digest, _relative, _tenant_selection, verify_snapshot
from .postgres_stage import StageMismatch
from .preflight import _inspect_verified_source


def _hash(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _principal(raw, tenant):
    principal = PrincipalRef(**json.loads(raw))
    if principal.key != raw or principal.tenant_id != tenant:
        raise StageMismatch("Noncanonical historical artifact principal")
    return raw


def _tenants(value):
    return [value] if isinstance(value, str) else list(value)


def _requests(db, allowed, agent):
    # SQLite sorts the immutable source on disk if necessary. Keep one artifact's
    # links and origins in memory, irrespective of the number of host sessions.
    for (
        r
    ) in db.execute("""SELECT r.id,r.session_id,r.tenant_id,r.agent_id,r.metadata_json,
                a.key AS position,a.type AS attachment_type,a.value AS attachment
            FROM session_runs r JOIN json_each(r.metadata_json,'$.attachments') a
            ORDER BY r.tenant_id,json_extract(a.value,'$.artifact_id'),r.id,a.key"""):
        if r["attachment_type"] != "object" or type(r["position"]) is not int:
            raise StageMismatch(
                "Attachment request requires canonical ordered object references"
            )
        ref = json.loads(r["attachment"])
        if not isinstance(ref.get("artifact_id"), str):
            raise StageMismatch("Attachment request requires an explicit canonical ID")
        meta = json.loads(r["metadata_json"])
        ctx = meta.get("execution_context", {})
        if (
            r["tenant_id"] not in allowed
            or r["agent_id"] != agent
            or meta.get("runtime_contract") != 1
            or ctx.get("session_id") != r["session_id"]
            or ctx.get("agent_id") != agent
        ):
            raise StageMismatch("Attachment request lacks its original admission")
        audience = sorted(
            {_principal(canonical_json(p), r["tenant_id"]) for p in ctx["audience"]}
        )
        creator = _principal(canonical_json(ctx["author"]), r["tenant_id"])
        yield (
            (r["tenant_id"], ref["artifact_id"]),
            (r["id"], r["session_id"], r["position"], creator, audience, ref),
        )


def _origin_session(db, tenant, agent, session):
    row = db.execute(
        "SELECT owner_principal_id,visibility,deleted_at_ms FROM sessions_v2 "
        "WHERE id=? AND tenant_id=? AND agent_id=?",
        (session, tenant, agent),
    ).fetchone()
    if not row or row["visibility"] != "private" or row["deleted_at_ms"] is not None:
        raise StageMismatch("Attachment requires an available private source session")
    return _principal(row["owner_principal_id"], tenant)


def _records(db, agent, allowed, legacy_root):
    groups = groupby(_requests(db, allowed, agent), key=lambda row: row[0])
    pending = next(groups, None)
    for row in db.execute("SELECT * FROM artifacts ORDER BY tenant_id,id"):
        record = dict(row)
        tenant, ident, digest = row["tenant_id"], row["id"], row["sha256"]
        if (
            tenant not in allowed
            or not re.fullmatch(r"art_[a-f0-9]{32}", ident)
            or row["visibility"] != "private"
            or row["direction"] != "input"
            or row["storage_state"] != "available"
            or row["deleted_at_ms"] is not None
            or not re.fullmatch(r"[a-f0-9]{64}", digest)
            or row["storage_key"] != f"sha256/{digest[:2]}/{digest}"
            or not row["mime"]
            or not row["kind"]
        ):
            raise StageMismatch(
                "Artifact requires an explicit availability, scope or direction mapping"
            )
        owner = _principal(row["owner_principal_id"], tenant)
        links = [
            dict(link)
            for link in db.execute(
                "SELECT * FROM artifact_links WHERE tenant_id=? AND artifact_id=? ORDER BY id",
                (tenant, ident),
            )
        ]
        if row["ref_count"] != len(links):
            raise StageMismatch("Artifact link cardinality differs")
        origins = []
        for link in links:
            if (
                link["resource_type"] != "session"
                or link["relation"] != "input_attachment"
            ):
                raise StageMismatch(
                    "Attachment requires an explicit historical link mapping"
                )
            session = link["resource_id"]
            link_owner = _origin_session(db, tenant, agent, session)
            origins.append(dict(session_id=session, run_id=None, audience=[link_owner]))
        if pending and pending[0] < (tenant, ident):
            raise StageMismatch("An admitted attachment has no archived object")
        has_requests = pending is not None and pending[0] == (tenant, ident)
        for _, (run, session, index, creator, audience, ref) in (
            pending[1] if has_requests else ()
        ):
            allowed_fields = {
                "artifact_id",
                "artifact_link_id",
                "kind",
                "type",
                "filename",
                "mime_type",
                "path",
                "sha256",
                "size_bytes",
                "url",
            }
            session_owner = _origin_session(db, tenant, agent, session)
            linked = [link for link in links if link["resource_id"] == session]
            if (
                creator != session_owner
                or not set(audience).issubset({session_owner})
                or (creator != owner and not linked)
                or set(ref) - allowed_fields
                or ref.get("artifact_link_id") is not None
                and ref["artifact_link_id"] not in {link["id"] for link in linked}
            ):
                raise StageMismatch(
                    "Attachment audience, link or request metadata differs"
                )
            exact = dict(
                artifact_id=ident,
                sha256=digest,
                size_bytes=row["size_bytes"],
                mime_type=row["mime"],
                url=f"/api/artifacts/{ident}/content",
                kind=row["kind"],
                type=row["kind"],
            )
            if (
                any(ref.get(k) != v for k, v in exact.items() if k in ref)
                or "path" in ref
                and ref["path"] != str(legacy_root / row["storage_key"])
                or "filename" in ref
                and ref["filename"] != safe_attachment_filename(ref["filename"])
            ):
                raise StageMismatch(
                    "Attachment reference disagrees with its canonical object"
                )
            origins.append(
                dict(
                    session_id=session,
                    run_id=run,
                    index=index,
                    audience=audience,
                    request_sha256=_hash(ref),
                )
            )
        if has_requests:
            pending = next(groups, None)
        yield dict(record=record, links=links, origins=origins)
    if pending:
        raise StageMismatch("An admitted attachment has no archived object")


def _table(schema, name):
    return sql.Identifier(archive_schema_name(schema), name)


def _private(pg, schema):
    owner = pg.execute(
        """SELECT n.nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user),
              EXISTS(SELECT 1 FROM aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
                     WHERE a.grantee<>n.nspowner)
            FROM pg_namespace n WHERE n.nspname=%s""",
        (schema,),
    ).fetchone()
    if owner != (True, False):
        raise StageMismatch(
            "Archive requires a private schema owned by the migration role"
        )
    if pg.execute(
        """SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
            LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
            WHERE n.nspname=%s AND c.relkind='r' AND (a.grantee<>c.relowner OR c.relowner<>n.nspowner)
            LIMIT 1""",
        (schema,),
    ).fetchone():
        raise StageMismatch("Archive has unexpected privileges")


def _header(pg, schema):
    return pg.execute(
        sql.SQL("SELECT binding FROM {} WHERE singleton").format(
            _table(schema, "archive_receipt")
        )
    ).fetchone()


def _base(root):
    path = PurePosixPath(root)
    if (
        not path.is_absolute()
        or path == PurePosixPath("/")
        or ".." in path.parts
        or str(path) != root
    ):
        raise ValueError(
            "The original absolute CAS directory must be attested by the host"
        )
    return path


def verify_artifact_candidate(
    pg, db, *, schema, manifest_sha256, database_sha256, agent_id, tenants
):
    """Read the sealed index and match every source descriptor, link and admitted reference."""
    _private(pg, archive_schema_name(schema))
    header = _header(pg, schema)
    allowed = sorted(_tenants(tenants))
    if (
        not header
        or header[0]["manifest"] != manifest_sha256
        or header[0]["database"] != database_sha256
        or header[0]["agent"] != agent_id
        or header[0]["tenants"] != allowed
    ):
        raise StageMismatch("Artifact archive belongs to a different source or agent")
    header = header[0]
    q = sql.SQL(
        "SELECT descriptor,chunks FROM {} WHERE tenant=%s AND agent=%s AND id=%s"
    ).format(_table(schema, "archive_objects"))
    fingerprint = hashlib.sha256()
    runs = set()
    count = 0
    for item in _records(db, agent_id, allowed, _base(header["legacy_cas_root"])):
        row = item["record"]
        saved = pg.execute(q, (row["tenant_id"], agent_id, row["id"])).fetchone()
        if not saved or saved[0] != item:
            raise StageMismatch(
                "Archived attachment metadata differs from the sealed source"
            )
        chunks = saved[1]
        if (
            any(
                not isinstance(part, list)
                or len(part) != 2
                or not re.fullmatch(r"[0-9a-f]{64}", part[0])
                or type(part[1]) is not int
                or not 0 < part[1] <= header["chunk_size"]
                for part in chunks
            )
            or sum(part[1] for part in chunks) != row["size_bytes"]
        ):
            raise StageMismatch("Archived attachment byte index differs")
        fingerprint.update(bytes.fromhex(_hash([item, chunks])))
        runs.update(
            origin["run_id"]
            for origin in item["origins"]
            if origin["run_id"] is not None
        )
        count += 1
    if (
        pg.execute(
            sql.SQL("SELECT count(*) FROM {}").format(_table(schema, "archive_objects"))
        ).fetchone()[0]
        != count
        or fingerprint.hexdigest() != header["index_sha256"]
    ):
        raise StageMismatch("Artifact archive is incomplete or has changed")
    return frozenset(runs)


async def import_artifact_archive(
    snapshot,
    database,
    vault,
    *,
    expected_tenant=None,
    expected_tenants=None,
    agent_id,
    legacy_cas_root,
    dsn,
    schema,
    blobs,
    chunk_size=4 << 20,
    history_only=False,
    require_git_history=True,
    verify_only=False,
):
    """Prepare a private PG index after verifying all referenced bytes in the host's private chunk store."""
    if (
        not isinstance(agent_id, str)
        or not agent_id.strip()
        or not 1 <= chunk_size <= 16 << 20
    ):
        raise ValueError("An agent identity and bounded chunk size are required")
    schema = archive_schema_name(schema)
    path, legacy = Path(snapshot).resolve(), _base(legacy_cas_root)
    selected = _tenant_selection(expected_tenant, expected_tenants)
    allowed = _tenants(selected)
    manifest_digest = _digest(path / MANIFEST)
    manifest = verify_snapshot(path)
    source = path / _relative(database)
    report = _inspect_verified_source(
        path,
        database,
        vault,
        selected,
        require_git_history,
        manifest,
        manifest_digest,
        history_only=history_only,
    )
    if report["issues"]:
        raise StageMismatch("Source inventory has unresolved issues")
    binding = dict(
        format=1,
        manifest=manifest_digest,
        database=report["database_sha256"],
        agent=agent_id,
        tenants=sorted(allowed),
        legacy_cas_root=str(legacy),
        chunk_size=chunk_size,
    )
    with (
        closing(
            sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True)
        ) as db,
        psycopg.connect(dsn) as pg,
    ):
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        pg.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            ("artifact_archive:" + schema,),
        )
        if not pg.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname=%s", (schema,)
        ).fetchone():
            if verify_only:
                raise StageMismatch("Artifact archive is missing")
            pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            pg.execute(
                sql.SQL(
                    "CREATE TABLE {}(singleton boolean PRIMARY KEY CHECK(singleton),binding jsonb NOT NULL)"
                ).format(_table(schema, "archive_receipt"))
            )
            pg.execute(
                sql.SQL(
                    "CREATE TABLE {}(tenant text NOT NULL,agent text NOT NULL,id text NOT NULL,"
                    "descriptor jsonb NOT NULL,chunks jsonb NOT NULL,PRIMARY KEY(tenant,agent,id))"
                ).format(_table(schema, "archive_objects"))
            )
            fresh = True
        else:
            fresh = False
        _private(pg, schema)
        if not fresh:
            head = _header(pg, schema)
            if not head or any(head[0].get(k) != v for k, v in binding.items()):
                raise StageMismatch(
                    "Existing artifact archive has a different source or policy"
                )
        fingerprint = hashlib.sha256()
        counts = dict(artifacts=0, links=0, request_references=0, bytes=0)
        for item in _records(db, agent_id, allowed, legacy):
            row = item["record"]
            cas = source.parent / "artifacts" / row["storage_key"]
            entry = manifest["files"].get(str(cas.relative_to(path)))
            if (
                not entry
                or entry["sha256"] != row["sha256"]
                or entry["bytes"] != row["size_bytes"]
            ):
                raise StageMismatch(
                    "Available artifact lacks its exact fenced CAS content"
                )
            chunks = []
            calculated = hashlib.sha256()
            with cas.open("rb") as stream:
                while data := stream.read(chunk_size):
                    sha = hashlib.sha256(data).hexdigest()
                    key = artifact_chunk_key(row["tenant_id"], agent_id, sha)
                    if fresh:
                        await blobs.put_immutable(key, data)
                    if await blobs.get(key, len(data)) != data:
                        raise StageMismatch(
                            "Stored attachment bytes disagree with the original"
                        )
                    calculated.update(data)
                    chunks.append([sha, len(data)])
            if calculated.hexdigest() != row["sha256"]:
                raise StageMismatch("Fenced CAS changed during transfer")
            if fresh:
                pg.execute(
                    sql.SQL("INSERT INTO {} VALUES(%s,%s,%s,%s,%s)").format(
                        _table(schema, "archive_objects")
                    ),
                    (row["tenant_id"], agent_id, row["id"], Jsonb(item), Jsonb(chunks)),
                )
            fingerprint.update(bytes.fromhex(_hash([item, chunks])))
            counts["artifacts"] += 1
            counts["links"] += len(item["links"])
            counts["request_references"] += sum(
                o["run_id"] is not None for o in item["origins"]
            )
            counts["bytes"] += row["size_bytes"]
        binding["index_sha256"] = fingerprint.hexdigest()
        if not fresh and head[0] != binding:
            raise StageMismatch(
                "Archived chunk sequence differs from the verified object source"
            )
        if fresh:
            pg.execute(
                sql.SQL("INSERT INTO {} VALUES(true,%s)").format(
                    _table(schema, "archive_receipt")
                ),
                (Jsonb(binding),),
            )
        runs = verify_artifact_candidate(
            pg,
            db,
            schema=schema,
            manifest_sha256=manifest_digest,
            database_sha256=report["database_sha256"],
            agent_id=agent_id,
            tenants=selected,
        )
        if (
            _digest(path / MANIFEST) != manifest_digest
            or _digest(source) != binding["database"]
        ):
            raise StageMismatch("Artifact source changed during transfer")
    return dict(
        format=1,
        manifest_sha256=manifest_digest,
        index_sha256=binding["index_sha256"],
        verified=counts,
        attachment_runs=len(runs),
        runtime_activated=False,
    )
