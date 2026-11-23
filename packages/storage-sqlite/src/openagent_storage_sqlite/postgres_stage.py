"""Stage sealed SQLite history and referenced CAS bytes in an isolated PostgreSQL schema."""
from __future__ import annotations

import argparse
import base64
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
from struct import pack

import psycopg
from psycopg import sql

from .migration import MANIFEST, _digest, _relative, _tenant_selection, verify_snapshot
from .preflight import _inspect_verified_source, _tenant_cli, _vault_options

# The storage domains carried by this transfer. Product secrets and source-specific
# tables outside these domains require their own custody/migration plan.
HISTORY = (
    "operational_storage_state", "sessions", "sessions_v2", "session_runs",
    "session_messages", "session_message_parts", "tool_invocations", "resource_acl", "artifacts",
    "artifact_links", "context_snapshots", "domain_events", "activity_items",
    "search_outbox", "search_outbox_consumers", "legacy_session_changes",
    "storage_migration_state", "storage_migration_journal", "schema_migrations",
    "oa_document_run_sources", "oa_vault_read_receipts", "oa_vault_revisions",
    "oa_private_legacy_sessions", "oa_private_legacy_messages",
    "operational_resource_owners", "operational_automation_projection",
    "operational_automation_changes", "durable_operations", "durable_operation_handoffs",
)
CHUNK_BYTES = 1 << 20


class StageMismatch(ValueError):
    """The destination or source disagrees with the sealed transfer identity."""


def _wire(value):
    if isinstance(value, bytes):
        return {"blob": base64.b64encode(value).decode("ascii")}
    if isinstance(value, float):
        return {"float64": pack(">d", value).hex()}
    return value


def _table_parts(db, table):
    # All table identifiers are selected from HISTORY, never from client input.
    info = db.execute(f'PRAGMA table_xinfo("{table}")').fetchall()
    columns = [r[1] for r in info]
    definition, = db.execute("SELECT sql FROM sqlite_schema WHERE type='table' AND name=?",
                             (table,)).fetchone()
    without_rowid = db.execute("SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?",
                               (table,)).fetchone()[0]
    if without_rowid:
        primary_columns = [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]
        order = ", ".join('"' + c.replace('"', '""') + '"' for c in primary_columns)
        projection = "*"
    else:
        if any(c.casefold() in ("rowid", "_rowid_", "oid") for c in columns):
            raise StageMismatch("Shadowed SQLite row identity is unsupported")
        order, projection = "rowid", "rowid,*"
        columns = ["rowid", *columns]
    header = {"format": 1, "sql": definition, "columns": columns}
    yield json.dumps(header, ensure_ascii=True, separators=(",", ":")).encode("ascii"), 0
    chunk = bytearray()
    count = 0
    for row in db.execute(f'SELECT {projection} FROM "{table}" ORDER BY {order}'):
        line = json.dumps([_wire(item) for item in row], ensure_ascii=True,
                          allow_nan=False, separators=(",", ":")).encode("ascii") + b"\n"
        if chunk and len(chunk) + len(line) > CHUNK_BYTES:
            yield bytes(chunk), count
            chunk.clear()
            count = 0
        chunk.extend(line)
        count += 1
    if chunk:
        yield bytes(chunk), count


def _blob_parts(path):
    with path.open("rb") as stream:
        while data := stream.read(CHUNK_BYTES):
            yield data, 0


def _private_schema(pg, schema, identity, verify):
    # This schema is dedicated to one source and one migration role. Product APIs
    # never query it. Concurrency and stale retries must not interleave sources.
    if not re.fullmatch(r"oa_stage_[a-z0-9_]{1,48}", schema):
        raise ValueError("Expected a dedicated oa_stage_ schema")
    pg.execute("SET search_path TO pg_catalog")
    if not pg.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (schema,)).fetchone()[0]:
        raise StageMismatch("Another transfer owns this destination")
    owner = pg.execute("""SELECT nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                          FROM pg_namespace WHERE nspname=%s""", (schema,)).fetchone()
    if owner is None:
        if verify:
            raise StageMismatch("Missing destination")
        with pg.transaction():
            pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            pg.execute(sql.SQL("SET search_path TO pg_catalog, {}").format(sql.Identifier(schema)))
            for table, definition in (
                    ("identity", "singleton boolean PRIMARY KEY CHECK(singleton), receipt text NOT NULL"),
                    ("parts", "collection text NOT NULL, part bigint NOT NULL, rows bigint NOT NULL,"
                              " payload bytea NOT NULL, PRIMARY KEY(collection,part)")):
                pg.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(schema), sql.Identifier(table), sql.SQL(definition)))
            pg.execute(sql.SQL("INSERT INTO {}.identity VALUES(true,%s)").format(sql.Identifier(schema)),
                       (identity,))
    elif not owner[0]:
        raise StageMismatch("Dedicated destination must belong to the migration role")
    granted = pg.execute("""SELECT EXISTS(
        SELECT 1 FROM pg_namespace n, LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
        WHERE n.nspname=%s AND a.grantee<>n.nspowner
        UNION ALL SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
        LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
        WHERE n.nspname=%s AND c.relkind='r' AND (a.grantee<>c.relowner OR c.relowner<>n.nspowner))""", (schema,schema)).fetchone()[0]
    if granted:
        raise StageMismatch("Unexpected destination privileges")
    pg.execute(sql.SQL("SET search_path TO pg_catalog, {}").format(sql.Identifier(schema)))
    row = pg.execute("SELECT receipt FROM identity WHERE singleton").fetchone()
    if row != (identity,):
        raise StageMismatch("Destination bound to another snapshot or scope")


def _transfer_collection(pg, name, source, verify):
    total = 0
    pieces = 0
    digest = hashlib.sha256()
    for part, (data, count) in enumerate(source):
        if not verify:
            pg.execute("""INSERT INTO parts(collection,part,rows,payload) VALUES(%s,%s,%s,%s)
                          ON CONFLICT(collection,part) DO NOTHING""", (name, part, count, data))
        saved = pg.execute("SELECT rows,payload FROM parts WHERE collection=%s AND part=%s",
                           (name,part)).fetchone()
        if saved is None or saved[0] != count or bytes(saved[1]) != data:
            raise StageMismatch("Destination payload differs from the fenced source")
        digest.update(data)
        pieces += 1
        total += count
    extra = pg.execute("SELECT 1 FROM parts WHERE collection=%s AND part>=%s LIMIT 1",
                       (name,pieces)).fetchone()
    if extra:
        raise StageMismatch("Destination contains unexpected parts")
    return total, digest.hexdigest()


def transfer_history(snapshot, database, vault, *, expected_tenant=None, expected_tenants=None, dsn, schema,
                     verify_only=False, require_git_history=True, history_only=False):
    """Transfer and compare every included record in a sealed host-owned snapshot.

    Re-running on the same source resumes by part; existing parts are compared
    and never overwritten. No live session or runtime lease is created.
    """
    root = Path(snapshot).resolve()
    manifest_digest = _digest(root / MANIFEST)
    manifest = verify_snapshot(root)
    selected = _tenant_selection(expected_tenant, expected_tenants)
    report = _inspect_verified_source(root, database, vault, selected, require_git_history,
                                      manifest, manifest_digest, history_only=history_only)
    identity = json.dumps([2 if history_only or expected_tenants is not None else 1, report["snapshot_manifest_sha256"], str(_relative(database)),
                           str(_relative(vault)), selected], separators=(",", ":"))
    database_path = root / _relative(database)
    with closing(sqlite3.connect(database_path.as_uri() + "?mode=ro&immutable=1", uri=True)) as db, \
            psycopg.connect(dsn, autocommit=True) as pg:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
        _private_schema(pg, schema, identity, verify_only)
        collections = set()
        stats = {}
        for table in HISTORY:
            if table in tables:
                key = "table/" + table
                collections.add(key)
                stats[table], _ = _transfer_collection(pg, key, _table_parts(db, table), verify_only)

        artifacts = 0
        missing = 0
        seen = set()
        for key, digest, size, state in db.execute(
                "SELECT storage_key,sha256,size_bytes,storage_state FROM artifacts ORDER BY sha256"):
            if not re.fullmatch(r"[0-9a-f]{64}", digest) or key != f"sha256/{digest[:2]}/{digest}":
                raise StageMismatch("Unsupported artifact address")
            path = database_path.parent / "artifacts" / key
            entry = manifest["files"].get(str(path.relative_to(root)))
            if entry is None:
                if state == "available":
                    raise StageMismatch("Available artifact bytes missing from snapshot")
                missing += 1
                continue
            if entry["bytes"] != size or entry["sha256"] != digest:
                raise StageMismatch("Artifact manifest differs from its stored reference")
            if digest in seen:
                continue
            seen.add(digest)
            name = "cas/" + digest
            # An empty object is still a distinct resource in the stage.
            def blob():
                yield b"", 0
                yield from _blob_parts(path)
            _, actual = _transfer_collection(pg, name, blob(), verify_only)
            if actual != digest:
                raise StageMismatch("Artifact bytes disagree with their reference")
            collections.add(name)
            artifacts += 1
        actual = {row[0] for row in pg.execute("SELECT DISTINCT collection FROM parts")}
        if actual != collections:
            raise StageMismatch("Destination contains unexpected collections")
    return {"format": 1, "source_manifest_sha256": report["snapshot_manifest_sha256"],
            **({"vault_status": "absent"} if history_only else {}),
            "source_database_sha256": report["database_sha256"],
            "verified_rows": stats, "verified_artifact_blobs": artifacts,
            "unavailable_artifact_references": missing, "source_issues": report["issues"],
            "other_source_tables": len(tables.difference(HISTORY).difference({"sqlite_sequence"})),
            "runtime_activated": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("--database", required=True)
    parser.add_argument("--vault", required=True)
    parser.add_argument("--expected-tenant", required=True, action="append")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--verify-only", action="store_true")
    _vault_options(parser)
    args = parser.parse_args(argv)
    dsn = os.environ.get("OPENAGENT_MIGRATION_POSTGRES_DSN")
    if not dsn:
        parser.error("Set OPENAGENT_MIGRATION_POSTGRES_DSN for a private destination")
    try:
        result = transfer_history(args.snapshot, args.database, args.vault, **_tenant_cli(args.expected_tenant),
                                  schema=args.schema, dsn=dsn, verify_only=args.verify_only,
                                  require_git_history=not args.allow_unversioned_vault, history_only=args.history_only)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, psycopg.Error, subprocess.SubprocessError):
        print(json.dumps({"error": "history_transfer_rejected"}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 1 if result["source_issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
