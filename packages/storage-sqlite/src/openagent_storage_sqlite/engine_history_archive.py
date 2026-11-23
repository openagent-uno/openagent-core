"""Bind archived engine runs to independently admitted canonical history."""

import hashlib
import json

from .postgres_stage import StageMismatch, _wire

TABLE = """CREATE TABLE migration_engine_runs (
    agent text NOT NULL, tenant text NOT NULL, session_id text NOT NULL, run_id text NOT NULL,
    source_rowid bigint NOT NULL, source_row_sha256 text NOT NULL, engine_ordinal integer NOT NULL,
    engine_run_sha256 text NOT NULL, PRIMARY KEY(agent,run_id), UNIQUE(source_rowid,engine_ordinal),
    FOREIGN KEY(agent,run_id) REFERENCES runs(agent,id) ON DELETE CASCADE)"""


def _unique(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise StageMismatch("Repeated field in archived engine history")
    return result


def _parse(value):
    def invalid(_):
        raise ValueError("Nonfinite JSON number")

    try:
        return json.loads(value, object_pairs_hook=_unique, parse_constant=invalid)
    except (ValueError, TypeError) as exc:
        raise StageMismatch("Invalid archived engine history") from exc


def _digest(data):
    return hashlib.sha256(data).hexdigest()


def _matches(engine, row, canonical):
    if not isinstance(engine, dict) or canonical is None:
        return False
    input_text = _parse(canonical["input_json"])
    output = _parse(canonical["output_json"])
    messages = engine.get("messages")
    if not isinstance(messages, list) or not all(isinstance(m, dict) for m in messages):
        return False
    roles = [m.get("role") for m in messages]
    if roles not in (["user", "assistant"], ["system", "user", "assistant"]):
        return False
    return (
        canonical["status"] == "success"
        and engine.get("status") == "COMPLETED"
        and canonical["session_id"] == row["session_id"] == engine.get("session_id")
        and engine.get("agent_id") == row["agent_id"]
        and engine.get("user_id") == row["user_id"]
        and isinstance(input_text, str)
        and isinstance(output, str)
        and isinstance(engine.get("input"), dict)
        and engine["input"].get("input_content") == input_text
        and messages[-2].get("content") == input_text
        and messages[-1].get("content") == output
        and engine.get("content") == output
    )


def _record_bytes(record):
    try:
        return json.dumps(
            record,
            ensure_ascii=True,
            sort_keys=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise StageMismatch("Invalid canonical engine envelope") from exc


def _matches_admitted_envelope(engine, row, canonical, agent):
    if (
        canonical is None
        or not {"raw_envelope_json", "raw_envelope_schema"} <= set(canonical.keys())
        or canonical["raw_envelope_schema"] != 1
        or not canonical["raw_envelope_json"]
    ):
        return False
    envelope = _parse(canonical["raw_envelope_json"])
    if not isinstance(envelope, dict) or _record_bytes(envelope) != _record_bytes(
        engine
    ):
        return False
    metadata = _parse(canonical["metadata_json"])
    owner = _parse(canonical["owner_principal_id"])
    context = metadata.get("execution_context") if isinstance(metadata, dict) else None
    statuses = {"COMPLETED": "success", "ERROR": "failed", "CANCELLED": "cancelled"}
    return (
        isinstance(context, dict)
        and isinstance(owner, dict)
        and type(metadata.get("runtime_contract")) is int
        and metadata["runtime_contract"] == 1
        and owner.get("tenant_id") == canonical["tenant_id"]
        and context.get("authority")
        == context.get("author")
        == context.get("initiator")
        == owner
        and context.get("audience") == [owner]
        and context.get("agent_id") == agent
        and context.get("session_id")
        == canonical["session_id"]
        == row["session_id"]
        == engine.get("session_id")
        and canonical["id"] == engine.get("run_id")
        and isinstance(engine.get("status"), str)
        and canonical["status"] == statuses.get(engine["status"])
        and isinstance(engine.get("agent_id"), str)
        and bool(engine["agent_id"])
        and engine.get("user_id") == row["user_id"]
    )


def associations(db, agent, tenants, *, canonical_envelopes=False):
    """The sealed raw stage owns all engine payloads. Candidate rows contain references and digests only."""
    tenants = (tenants,) if isinstance(tenants, str) else tenants
    required = {
        "session_id",
        "session_type",
        "agent_id",
        "user_id",
        "runs",
        "team_id",
        "workflow_id",
    }
    info = {r[1] for r in db.execute('PRAGMA table_xinfo("sessions")')}
    if not required <= info or {"rowid", "_rowid_", "oid"} & {
        name.casefold() for name in info
    }:
        raise StageMismatch("Unsupported archived engine session shape")
    layout = db.execute(
        "SELECT wr FROM pragma_table_list WHERE schema='main' AND name='sessions'"
    ).fetchone()
    if not layout or layout[0] != 0:
        raise StageMismatch("Engine archive requires an exact SQLite row identity")
    for row in db.execute(
        "SELECT _rowid_, * FROM sessions ORDER BY session_id COLLATE BINARY"
    ):
        legacy = _parse(row["runs"])
        if (
            row["session_type"] != "agent"
            or row["team_id"] is not None
            or row["workflow_id"] is not None
            or not isinstance(row["agent_id"], str)
            or not row["agent_id"]
            or not isinstance(row["user_id"], str)
            or not row["user_id"]
            or not isinstance(legacy, list)
            or not legacy
        ):
            raise StageMismatch("Engine history lacks a supported canonical binding")
        # Byte-identical representation of this row in table/sessions of the verified, private stage.
        raw = json.dumps(
            [_wire(val) for val in row],
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        )
        archived = _digest((raw + "\n").encode("ascii"))
        if canonical_envelopes:
            if not any(
                isinstance(record, dict) and record.get("agent_id") == row["agent_id"]
                for record in legacy
            ):
                raise StageMismatch("Unbound archived engine identity")
            if "agent_data" in row.keys() and row["agent_data"] is not None:
                identity = _parse(row["agent_data"])
                if (
                    not isinstance(identity, dict)
                    or identity.get("agent_id") != row["agent_id"]
                ):
                    raise StageMismatch("Conflicting archived engine identity")
        seen = set()
        for ordinal, engine in enumerate(legacy):
            run_id = engine.get("run_id") if isinstance(engine, dict) else None
            if not isinstance(run_id, str) or not run_id or run_id in seen:
                raise StageMismatch("Missing or ambiguous archived engine run")
            seen.add(run_id)
            canonical = db.execute(
                """SELECT r.*,s.owner_principal_id FROM session_runs r JOIN sessions_v2 s
                ON s.id=r.session_id AND s.tenant_id=r.tenant_id AND s.agent_id=r.agent_id
                WHERE r.id=? AND r.session_id=? AND r.agent_id=? AND s.visibility='private'
                AND s.status='active' AND s.deleted_at_ms IS NULL""",
                (run_id, row["session_id"], agent),
            ).fetchone()
            matches = (
                _matches_admitted_envelope(engine, row, canonical, agent)
                if canonical_envelopes
                else _matches(engine, row, canonical)
            )
            if (
                not matches
                or canonical["tenant_id"] not in tenants
                or canonical["finished_at_ms"] is None
            ):
                raise StageMismatch(
                    "Archived engine run differs from its canonical session, input or output"
                )
            engine_hash = _digest(_record_bytes(engine))
            yield (
                agent,
                canonical["tenant_id"],
                row["session_id"],
                run_id,
                row[0],
                archived,
                ordinal,
                engine_hash,
            )
