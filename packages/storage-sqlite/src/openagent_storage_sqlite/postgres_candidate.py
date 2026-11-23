"""Materialize a private, offline runtime candidate from verified staged history."""
from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from itertools import zip_longest
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from openagent_core.contracts import ExecutionContext, PrincipalRef, TERMINAL_STATUSES, canonical_json
from . import SqliteRuntimeStore
from .migration import _digest, _relative, _tenant_selection
from .postgres_stage import StageMismatch, transfer_history
from .preflight import _tenant_cli, _vault_options


# These columns describe the public runtime's relational projection. The source
# envelope, ACL evidence, derived items and search journal stay in the private stage.
PROJECTION = {
    "sessions": ("agent id tenant authority owner_id participants title archived origin parent_id root_id "
                 "created_at updated_at last_activity_at", 'id COLLATE "C"'),
    "runs": ("agent id session_id tenant ordinal request_key digest request context worker status finished_at "
             "cancel_requested output parent_run_id", 'id COLLATE "C"'),
    "messages": ("agent id session_id session_seq run_id role author body call_id status created_at", 'id COLLATE "C"'),
    "events": ("agent seq run_id kind payload", "seq"),
    "tool_calls": ("agent run_id id invocation_id intent result error finished", 'run_id COLLATE "C",id COLLATE "C"'),
}
JSON_COLUMNS = {"request", "context", "output", "body", "payload", "intent", "result", "error"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
OFFLINE_WORKER = UUID(int=0)


def _time(ms):
    return EPOCH + timedelta(milliseconds=ms)


def _principal(raw, tenant):
    person = PrincipalRef(**json.loads(raw))
    if person.key != raw or person.tenant_id != tenant:
        raise StageMismatch("Source principal lacks an exact tenant binding")
    return person


def _source_tenant(row, selected):
    tenant = row["tenant_id"]
    if tenant not in ((selected,) if isinstance(selected, str) else selected):
        raise StageMismatch("Source row outside the sealed tenant set")
    return tenant


def _session_rows(db, agent, tenants):
    for row in db.execute("SELECT * FROM sessions_v2 ORDER BY id"):
        tenant = _source_tenant(row, tenants)
        if (row["agent_id"] != agent or row["visibility"] != "private"
                or row["status"] != "active" or row["deleted_at_ms"] is not None or row["kind"] != "chat"
                or row["parent_session_id"] is not None or row["root_session_id"] is not None
                or json.loads(row["metadata_json"]).get("runtime_contract") != 1):
            raise StageMismatch("Session needs an explicit historical projection")
        owner = _principal(row["owner_principal_id"], tenant)
        yield (agent, row["id"], tenant, owner.authority, owner.key, [owner.key], row["title"], False,
               row["origin"], None, None, _time(row["created_at_ms"]), _time(row["updated_at_ms"]),
               _time(row["last_activity_at_ms"]))


def _run_rows(db, agent, tenants):
    for row in db.execute("""SELECT r.*,s.owner_principal_id, ROW_NUMBER() OVER
            (ORDER BY r.session_id,r.ordinal) AS import_ordinal FROM session_runs r
            LEFT JOIN sessions_v2 s ON r.session_id=s.id AND r.tenant_id=s.tenant_id ORDER BY r.id"""):
        meta = json.loads(row["metadata_json"])
        tenant = _source_tenant(row, tenants)
        if (meta.get("runtime_contract") != 1 or row["owner_principal_id"] is None or row["agent_id"] != agent
                or row["status"] not in TERMINAL_STATUSES or row["finished_at_ms"] is None
                or row["input_json"] is None or meta.get("operation_continuation") or meta.get("attachments")
                or meta.get("steer_run_id") or row["delegated_parent_run_id"] or row["parent_run_id"]):
            raise StageMismatch("Run requires an unsupported execution or retention mapping")
        accepted = SqliteRuntimeStore._decode_accepted_request(row)
        owner = _principal(row["owner_principal_id"], tenant)
        if (accepted.author != owner or accepted.authority != owner or accepted.initiator != owner
                or accepted.audience != (owner,) or accepted.parent_run_id or accepted.deferred
                or accepted.delegation_id or meta["execution_context"].get("capabilities")):
            raise StageMismatch("Run has a distinct admission or execution authority")
        ctx = ExecutionContext(owner, owner, owner, row["session_id"], agent, (owner,), accepted.scopes,
                               ingress_id=accepted.ingress_id)
        if (canonical_json(ctx.snapshot()) != canonical_json(meta["execution_context"])
                or accepted.request.fingerprint(ctx) != meta.get("request_digest")):
            raise StageMismatch("Source request digest or execution context differs")
        record = SqliteRuntimeStore._record(row)
        persisted = ctx.snapshot()
        persisted.pop("capabilities")
        yield (agent, row["id"], row["session_id"], tenant, row["import_ordinal"], accepted.request.idempotency_key,
               record.request_digest, asdict(accepted.request), persisted, OFFLINE_WORKER, record.status,
               _time(row["finished_at_ms"]), record.cancel_requested, record.output, None)


def _message_rows(db, agent, tenants):
    for row in db.execute("""SELECT m.*,s.owner_principal_id,r.id AS admitted,t.tool_call_id AS invocation
                             FROM session_messages m
                             LEFT JOIN sessions_v2 s ON m.session_id=s.id AND m.tenant_id=s.tenant_id
                             LEFT JOIN session_runs r ON r.id=m.run_id AND r.tenant_id=m.tenant_id
                                 AND r.session_id=m.session_id AND r.agent_id=?
                             LEFT JOIN tool_invocations t ON t.session_run_id=m.run_id AND t.tool_call_id=m.tool_call_id
                                 AND t.session_id=m.session_id AND t.tenant_id=m.tenant_id ORDER BY m.id""", (agent,)):
        tenant = _source_tenant(row, tenants)
        has_call = row["role"] == "tool"
        if (row["visibility"] != "user_visible" or row["admitted"] is None or row["owner_principal_id"] is None
                or row["content_json"] is not None or not isinstance(row["text"], str)
                or row["role"] not in ("user", "assistant", "tool") or has_call != bool(row["tool_call_id"])
                or has_call and (row["invocation"] is None or row["status"] != "complete")):
            raise StageMismatch("Message requires an unsupported visibility, tool or content mapping")
        actor = _principal(row["author_principal_id"], tenant)
        owner = _principal(row["owner_principal_id"], tenant)
        expected_actor = owner if row["role"] == "user" else PrincipalRef(owner.authority, tenant, agent, "agent")
        if actor.kind != row["author_kind"] or actor != expected_actor:
            raise StageMismatch("Message author evidence differs")
        yield (agent, row["id"], row["session_id"], row["sequence"], row["run_id"], row["role"], actor.key,
               row["text"], row["tool_call_id"], row["status"], _time(row["created_at_ms"]))


def _event_rows(db, agent, tenants):
    for row in db.execute("""SELECT e.*,r.id AS admitted FROM domain_events e LEFT JOIN session_runs r
                             ON e.run_id=r.id AND e.session_id=r.session_id AND e.tenant_id=r.tenant_id
                             AND r.agent_id=? ORDER BY e.sequence""", (agent,)):
        _source_tenant(row, tenants)
        if (not row["admitted"] or row["resource_type"] != "run"
                or row["resource_id"] != row["run_id"]):
            raise StageMismatch("Event requires a distinct historical or reservation mapping")
        yield agent, row["sequence"], row["run_id"], row["event_type"], json.loads(row["metadata_json"])


def _tool_rows(db, agent, tenants):
    for row in db.execute("""SELECT t.*,s.owner_principal_id AS session_owner,r.id AS admitted,
            (SELECT COUNT(*) FROM session_messages m WHERE m.run_id=t.session_run_id
             AND m.session_id=t.session_id AND m.tenant_id=t.tenant_id AND m.role='tool'
             AND m.tool_call_id=t.tool_call_id) AS anchors
            FROM tool_invocations t
            LEFT JOIN sessions_v2 s ON s.id=t.session_id AND s.tenant_id=t.tenant_id
            LEFT JOIN session_runs r ON r.id=t.session_run_id AND r.session_id=t.session_id
                AND r.tenant_id=t.tenant_id AND r.agent_id=?
            ORDER BY t.session_run_id,t.tool_call_id""", (agent,)):
        _source_tenant(row, tenants)
        call, run = row["tool_call_id"], row["session_run_id"]
        ident = uuid5(NAMESPACE_URL, canonical_json([run, call]))
        if (not call or not row["admitted"] or row["visibility"] != "private" or row["root_kind"] != "session"
                or row["root_id"] != row["session_id"] or row["session_owner"] != row["owner_principal_id"]
                or row["session_owner"] is None or row["id"] != str(ident) or row["finished_at_ms"] is None
                or row["anchors"] != 1 or row["raw_envelope_schema"] != 1):
            raise StageMismatch("Tool invocation lacks a scoped terminal intent and message")
        envelope = json.loads(row["raw_envelope_json"])
        binding = envelope["binding"]
        arguments = json.loads(row["args_json"])
        result = json.loads(row["result_json"]) if row["result_json"] is not None else None
        error = json.loads(row["error_json"]) if row["error_json"] is not None else None
        status = "error" if error is not None or isinstance(result, dict) and result.get("isError") else "success"
        outcome = {"error": error} if error is not None else {"result": result}
        intent = {"call_id": call, "binding": binding, "arguments": arguments}
        if (binding.get("source_id") != row["tool_server"] or binding.get("name") != row["tool_name"]
                or row["status"] != status or row["error_json"] is not None and row["result_json"] is not None
                or canonical_json(envelope) != canonical_json({**intent, "status": status, **outcome})):
            raise StageMismatch("Tool invocation envelope differs from the retained intent or outcome")
        yield agent, run, call, ident, intent, result, error, True


def _source_scope(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")}
    restricted = ("sessions", "resource_acl", "artifacts", "artifact_links",
                  "session_message_parts", "context_snapshots", "durable_operations", "durable_operation_handoffs")
    for table in restricted:
        if table in tables and db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
            raise StageMismatch("Source requires additional runtime adapters for staged history")


def _compare(pg, table, rows, create):
    columns, order = PROJECTION[table]
    names = columns.split()
    field_list = ",".join(names)
    if create:
        with pg.cursor().copy(f"COPY {table} ({field_list}) FROM STDIN") as writer:
            count = 0
            for row in rows:
                writer.write_row(tuple(Jsonb(v) if k in JSON_COLUMNS and v is not None else v
                                       for k, v in zip(names, row, strict=True)))
                count += 1
        return count

    count = 0
    with pg.cursor(name="verify_" + table) as reader:
        reader.execute(f"SELECT {field_list} FROM {table} ORDER BY {order}")
        for expected, actual in zip_longest(rows, reader):
            if expected is None or actual is None:
                raise StageMismatch("Candidate row count differs")
            for key, source, target in zip(names, expected, actual, strict=True):
                if key in JSON_COLUMNS:
                    source, target = canonical_json(source), canonical_json(target)
                if source != target:
                    raise StageMismatch("Candidate data differs from the source projection")
            count += 1
    return count


def _schema(pg, name, receipt, verify_only):
    if not re.fullmatch(r"oa_candidate_[a-z0-9_]{1,44}", name):
        raise ValueError("Expected a dedicated oa_candidate_ schema")
    pg.execute("SET LOCAL search_path TO pg_catalog")
    # The native runtime takes this same lock before it starts a worker.
    pg.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("openagent:" + name,))
    current = pg.execute("""SELECT nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
                            FROM pg_namespace WHERE nspname=%s""", (name,)).fetchone()
    fresh = current is None
    if fresh:
        if verify_only:
            raise StageMismatch("Missing candidate")
        pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
    elif not current[0]:
        raise StageMismatch("Candidate belongs to another migration role")
    pg.execute(sql.SQL("SET LOCAL search_path TO {},pg_catalog").format(sql.Identifier(name)))
    if fresh:
        pg.execute(files("openagent_storage_postgres").joinpath("schema.sql").read_text())
        pg.execute("CREATE TABLE migration_receipt(singleton boolean PRIMARY KEY CHECK(singleton), receipt text NOT NULL)")
    mismatch = pg.execute("""SELECT EXISTS(
        SELECT 1 FROM pg_namespace n,LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
        WHERE n.nspname=%s AND a.grantee<>n.nspowner
        UNION ALL SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid,
        LATERAL aclexplode(COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's' ELSE 'r' END::"char",
                               c.relowner))) a WHERE n.nspname=%s AND c.relkind IN ('r','S')
        AND (a.grantee<>c.relowner OR c.relowner<>n.nspowner))""", (name, name)).fetchone()[0]
    if mismatch:
        raise StageMismatch("Candidate has unexpected migration privileges")
    if fresh:
        pg.execute("INSERT INTO migration_receipt VALUES(true,%s)", (receipt,))
    elif pg.execute("SELECT receipt FROM migration_receipt WHERE singleton").fetchone() != (receipt,):
        raise StageMismatch("Candidate belongs to another source or scope")
    if not fresh:
        pg.execute("LOCK TABLE workers,sessions,runs,messages,events,reservations,operations,handoffs,tool_calls IN SHARE MODE")
        for table in ("workers", "reservations", "operations", "handoffs"):
            if pg.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                raise StageMismatch("Candidate has runtime or historical state outside the offline receipt")
    return fresh


def build_runtime_candidate(snapshot, database, vault, *, expected_tenant=None, expected_tenants=None, agent_id, dsn, stage_schema,
                            candidate_schema, verify_only=False, require_git_history=True, history_only=False):
    """Build or independently re-read an atomic candidate, without runtime admission or cutover."""
    if not isinstance(agent_id, str) or not agent_id.strip():
        raise ValueError("An exact agent ID is required")
    selected = _tenant_selection(expected_tenant, expected_tenants)
    staged = transfer_history(snapshot, database, vault, expected_tenant=expected_tenant, expected_tenants=expected_tenants, dsn=dsn,
                              schema=stage_schema, verify_only=True, require_git_history=require_git_history,
                              history_only=history_only)
    if staged["source_issues"]:
        raise StageMismatch("Source inventory has unresolved issues")
    digest = staged["source_manifest_sha256"]
    receipt = canonical_json([2 if history_only or expected_tenants is not None else 1, digest, database, vault, selected, agent_id, stage_schema])
    source = Path(snapshot).resolve() / _relative(database)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True)) as db, psycopg.connect(dsn) as pg:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        _source_scope(db)
        fresh = _schema(pg, candidate_schema, receipt, verify_only)
        sources = dict(sessions=_session_rows, runs=_run_rows, messages=_message_rows, events=_event_rows, tool_calls=_tool_rows)
        counts = {}
        for table, generate in sources.items():
            if fresh:
                _compare(pg, table, generate(db, agent_id, selected), True)
            counts[table] = _compare(pg, table, generate(db, agent_id, selected), False)
        highwater = db.execute("SELECT seq FROM sqlite_sequence WHERE name='domain_events'").fetchone()
        last_event = highwater[0] if highwater else 0
        if fresh:
            pg.execute("SELECT setval(pg_get_serial_sequence('events','seq'),%s,%s)",
                       (max(last_event, 1), bool(last_event)))
            pg.execute("""SELECT setval(pg_get_serial_sequence('runs','ordinal'),
                          COALESCE(MAX(ordinal),1), COUNT(*)>0) FROM runs""")
        if pg.execute("SELECT last_value,is_called FROM events_seq_seq").fetchone() != (max(last_event, 1), bool(last_event)):
            raise StageMismatch("Event cursor high-water mark differs")
        if _digest(source) != staged["source_database_sha256"]:
            raise StageMismatch("Source file changed during projection")
    return {"format": 1, "scope": "private_terminal_runtime_candidate", "verified_rows": counts,
            **({"vault_status": "absent"} if history_only else {}),
            "source_manifest_sha256": digest, "runtime_activated": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("--expected-tenant", required=True, action="append")
    for key in ("database", "vault", "agent-id", "stage-schema", "candidate-schema"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--verify-only", action="store_true")
    _vault_options(parser)
    args = vars(parser.parse_args(argv))
    args.update(_tenant_cli(args.pop("expected_tenant")))
    dsn = os.environ.get("OPENAGENT_MIGRATION_POSTGRES_DSN")
    if not dsn:
        parser.error("Set OPENAGENT_MIGRATION_POSTGRES_DSN for a private destination")
    args["require_git_history"] = not args.pop("allow_unversioned_vault")
    try:
        report = build_runtime_candidate(**args, dsn=dsn)
    except (OSError, ValueError, KeyError, TypeError, ImportError, sqlite3.Error, psycopg.Error, subprocess.SubprocessError):
        print(json.dumps({"error": "runtime_candidate_rejected"}))
        return 2
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
