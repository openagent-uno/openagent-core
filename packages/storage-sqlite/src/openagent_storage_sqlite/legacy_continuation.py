"""Offline, owner-scoped admission of verified legacy turns to a canonical history window."""

from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

from openagent_core.contracts import canonical_json
from openagent_core.memory.operational.privacy_snapshots import (
    guard_snapshot_transaction,
)
from openagent_core.memory.operational.projection import (
    build_session_projection,
    loads_maybe_double,
)
from openagent_core.memory.operational.repository import (
    _fetch_legacy,
    _MESSAGE_COLUMNS,
    _message_values,
    _preserve_delivery_sequences,
    _RUN_COLUMNS,
    _run_values,
)
from openagent_core.runtime import current_run_id
from .migration import (
    MANIFEST,
    _check_transaction_tenant,
    _digest,
    _relative,
    verify_snapshot,
)


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS oa_private_legacy_sessions(
        session_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, source_agent TEXT, target_agent TEXT NOT NULL,
        principal TEXT NOT NULL, acl_version INTEGER NOT NULL, source_digest TEXT NOT NULL,
        instance TEXT NOT NULL, source_hash TEXT NOT NULL, lineage TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS oa_private_legacy_messages(
        session_id TEXT NOT NULL, message_id TEXT NOT NULL, run_id TEXT NOT NULL,
        role TEXT NOT NULL, sequence INTEGER NOT NULL, ordinal INTEGER NOT NULL, body_digest TEXT NOT NULL,
        PRIMARY KEY(session_id,message_id), FOREIGN KEY(session_id) REFERENCES oa_private_legacy_sessions(session_id))""")
    db.execute(
        "CREATE INDEX IF NOT EXISTS oa_private_history_order ON oa_private_legacy_messages(session_id,ordinal DESC,sequence DESC)"
    )


def _eligible(db, row, person):
    # The old projector namespaces a verified human as user:<handle>. A device-only,
    # bare or unknown subject needs an independent host identity repair first.
    if (
        row is None
        or row["tenant_id"] != person.tenant_id
        or row["visibility"] != "private"
        or row["deleted_at_ms"] is not None
        or row["acl_version"] != 1
        or row["owner_principal_id"] not in (person.key, "user:" + person.subject_id)
    ):
        return False
    aliases = (person.key, person.subject_id, "user:" + person.subject_id)
    return not db.execute(
        """SELECT 1 FROM resource_acl WHERE tenant_id=? AND resource_type='session'
        AND resource_id=? AND (acl_version<>? OR principal_type<>'user' OR principal_id NOT IN (?,?,?)) LIMIT 1""",
        (person.tenant_id, row["id"], row["acl_version"], *aliases),
    ).fetchone()


def _session(db, sid):
    return db.execute("SELECT * FROM sessions_v2 WHERE id=?", (sid,)).fetchone()


def _table(db, name):
    return (
        db.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _private_parents(db, row, owner):
    chain = []
    seen = set()
    while row is not None:
        key = row["id"]
        if key in seen or len(seen) >= 64 or not _eligible(db, row, owner):
            return None
        seen.add(key)
        canonical_parent = row["parent_session_id"]
        legacy = db.execute(
            "SELECT metadata FROM sessions WHERE session_id=?", (key,)
        ).fetchone()
        original_parent = None
        if legacy is not None:
            meta = loads_maybe_double(legacy[0], None)
            if not isinstance(meta, dict) or meta.get("client_id") not in (
                owner.subject_id,
                "user:" + owner.subject_id,
            ):
                return None
            original_parent = str(meta.get("parent_session_id") or "").strip() or None
            if canonical_parent is not None and canonical_parent != original_parent:
                return None
        chain.append(
            [
                key,
                row["agent_id"],
                row["owner_principal_id"],
                row["acl_version"],
                canonical_parent,
                original_parent,
            ]
        )
        parent = canonical_parent or original_parent
        if not parent:
            return _digest_values(chain)
        row = _session(db, parent)
    return None


def _digest_values(values):
    return sha256(canonical_json(values).encode("utf-8")).hexdigest()


# The message text, authorship and the original run's captured scope remain
# inseparable, even if a later compatibility writer edits only attribution.
def _body_digest(message, run):
    return _digest_values(
        [
            message["text"],
            message["author_kind"],
            message["author_principal_id"],
            message["author_device_id"],
            run["agent_id"],
            run["parent_run_id"],
            run["metadata_json"],
        ]
    )


def _expected_messages(db, row, owner):
    raw = _fetch_legacy(db, row["id"])
    if not raw or raw.get("agent_id") != row["agent_id"]:
        return None
    projection = build_session_projection(
        raw, tenant_id=owner.tenant_id, now_ms=row["created_at_ms"]
    )
    if (
        projection.malformed_error
        or projection.session["visibility"] != "private"
        or projection.session["owner_principal_id"] != "user:" + owner.subject_id
    ):
        return None
    # Source version is a whole-session projection epoch; canonical delivery can
    # advance it independently. Every original identity, envelope and audience
    # field of each individual message and its source run must still agree.
    kw = dict(
        tenant_id=owner.tenant_id,
        session_id=row["id"],
        source_version=row["source_version"],
    )
    foreign_authors = set()
    original_agent = row["agent_id"] or "openagent"
    agent_subject = (
        original_agent
        if original_agent.startswith("agent:")
        else "agent:" + original_agent
    )
    for message in projection.messages:
        if message["visibility"] != "user_visible" or message["role"] not in (
            "user",
            "assistant",
        ):
            continue
        # A model-facing user role can also be a prompt authored by this same
        # private agent. Preserve its original attribution; reject any other actor.
        author = (message["author_kind"], message["author_principal_id"])
        if author == ("agent", agent_subject):
            continue
        if message["role"] == "user" and author in (
            ("user", owner.key),
            ("user", "user:" + owner.subject_id),
        ):
            continue
        foreign_authors.add(message["run_id"])
    expected = {}
    run_ids = {item["id"] for item in projection.runs}
    raw_runs = loads_maybe_double(raw.get("runs"), [])
    for item in projection.runs:
        if item["id"] in foreign_authors:
            continue
        active = db.execute(
            "SELECT * FROM session_runs WHERE id=? AND session_id=?",
            (item["id"], row["id"]),
        ).fetchone()
        if not active:
            continue
        before = _run_values(item, **kw)
        if before["parent_run_id"] not in run_ids:
            before["parent_run_id"] = None
        if before["raw_envelope_json"] != active["raw_envelope_json"]:
            # The first format stored the complete original envelope. Its
            # successor moved nested arrays to normalized rows. Accept the old
            # format only when the entire envelope equals the original input.
            ordinal = item["ordinal"]
            if canonical_json(
                json.loads(active["raw_envelope_json"])
            ) != canonical_json(raw_runs[ordinal]):
                continue
            before["raw_envelope_json"] = active["raw_envelope_json"]
        if any(
            active[k] != before.get(k) for k in _RUN_COLUMNS if k != "source_version"
        ):
            continue
        if (
            json.loads(active["metadata_json"] or "{}").get("execution_context")
            is not None
        ):
            continue
        if (
            _table(db, "oa_document_run_sources")
            and db.execute(
                "SELECT 1 FROM oa_document_run_sources WHERE run_id=? LIMIT 1",
                (item["id"],),
            ).fetchone()
        ):
            continue
        expected[item["id"]] = active
    actual_messages = {
        r["id"]: r
        for r in db.execute(
            "SELECT * FROM session_messages WHERE session_id=?", (row["id"],)
        )
    }
    desired = {item["id"]: _message_values(item, **kw) for item in projection.messages}
    _preserve_delivery_sequences(db, row["id"], desired, actual_messages)
    messages = {}
    for item in projection.messages:
        rid = item.get("run_id")
        if (
            rid not in expected
            or item["role"] not in ("user", "assistant")
            or not isinstance(item.get("text"), str)
        ):
            continue
        active = actual_messages.get(item["id"])
        before = desired[item["id"]]
        if (
            not active
            or active["status"] != "complete"
            or active["visibility"] != "user_visible"
        ):
            continue
        if any(
            active[k] != before.get(k)
            for k in _MESSAGE_COLUMNS
            if k != "source_version"
        ):
            continue
        messages[item["id"]] = (
            rid,
            active["role"],
            active["sequence"],
            expected[rid]["ordinal"],
            _body_digest(active, expected[rid]),
        )
    return projection.source_hash, messages


def bound(db, context):
    """A persisted mapping remains usable only under the same exact personal audience."""
    p = context.authority
    if (
        p.kind != "user"
        or context.author != p
        or context.initiator != p
        or set(context.audience) != {p}
        or context.deferred
        or context.parent_run_id
        or context.delegation_id
    ):
        return False
    proof = db.execute(
        "SELECT source_agent,acl_version,instance,lineage FROM oa_private_legacy_sessions WHERE session_id=? AND tenant=? AND target_agent=? AND principal=?",
        (context.session_id, context.tenant_id, context.agent_id, p.key),
    ).fetchone()
    if not proof:
        return False
    row = _session(db, context.session_id)
    return (
        _eligible(db, row, p)
        and row["acl_version"] == proof["acl_version"]
        and row["agent_id"] == proof["source_agent"]
        and _private_parents(db, row, p) == proof["lineage"]
        and _instance(db) == proof["instance"]
    )


def admit(db, context):
    if not db.execute(
        "SELECT 1 FROM oa_private_legacy_sessions WHERE session_id=?",
        (context.session_id,),
    ).fetchone():
        return False
    if not bound(db, context):
        raise PermissionError(
            "Historical continuation no longer admits this owner and audience"
        )
    return True


HISTORY_ROWS = """
    UNION ALL
    SELECT r.id,r.ordinal,m.sequence,m.role,m.text,h.body_digest,
           m.author_kind,m.author_principal_id,m.author_device_id,r.agent_id,r.parent_run_id,r.metadata_json
      FROM oa_private_legacy_messages h JOIN session_messages m ON m.id=h.message_id
      JOIN session_runs r ON r.id=h.run_id
     WHERE h.session_id=? AND m.session_id=h.session_id AND r.session_id=h.session_id
       AND m.tenant_id=? AND r.tenant_id=m.tenant_id AND r.ordinal<? AND r.ordinal=h.ordinal
       AND m.run_id=r.id AND m.sequence=h.sequence AND m.role=h.role AND m.text IS NOT NULL
       AND m.visibility='user_visible' AND m.status='complete'
       AND json_type(r.metadata_json,'$.execution_context') IS NULL
       AND NOT EXISTS (SELECT 1 FROM oa_document_run_sources d WHERE d.run_id=h.run_id)
"""


def verify_bodies(rows):
    for row in rows:
        if (
            row["body_digest"] is not None
            and _body_digest(row, row) != row["body_digest"]
        ):
            raise PermissionError(
                "A historical message differs from its attested original"
            )


def _instance(db):
    row = db.execute(
        "SELECT db_instance_id FROM operational_storage_state WHERE singleton_id=1"
    ).fetchone()
    if not row or not row[0]:
        raise ValueError("Missing standalone installation identity")
    return row[0]


def attest_private_continuations(
    snapshot, database, target, *, owner, target_agent, approved_sessions, writer_fence
):
    """Bind a host-approved session/old-writer map on an offline clone of the sealed source."""
    if (
        current_run_id()
        or owner.kind != "user"
        or not target_agent
        or not callable(writer_fence)
    ):
        raise PermissionError(
            "A private host migration and an authenticated owner are required"
        )
    root = Path(snapshot).resolve()
    name = str(_relative(database))
    dest = Path(target).resolve(strict=True)
    receipt = _digest(root / MANIFEST)
    manifest = verify_snapshot(root)
    if name not in manifest["databases"] or dest.is_relative_to(root):
        raise ValueError("A distinct candidate is required")
    total = enabled = 0
    with (
        writer_fence() as stamp,
        closing(
            sqlite3.connect((root / name).as_uri() + "?mode=ro&immutable=1", uri=True)
        ) as src,
        closing(sqlite3.connect(dest)) as dst,
    ):
        if not isinstance(stamp, str) or not stamp:
            raise PermissionError("Host writer fence required")
        src.row_factory = dst.row_factory = sqlite3.Row
        src.execute("BEGIN")
        dst.execute("BEGIN IMMEDIATE")
        for conn in (src, dst):
            guard_snapshot_transaction(conn)
            _check_transaction_tenant(conn, owner.tenant_id)
        identity = _instance(src)
        if _instance(dst) != identity:
            raise PermissionError("Source and candidate installation differ")
        initialize(dst)
        for sid, old_agent in approved_sessions.items():
            source = _session(src, sid)
            candidate = _session(dst, sid)
            if not all(
                _eligible(c, r, owner) and r["agent_id"] == old_agent
                for c, r in [(src, source), (dst, candidate)]
            ):
                continue
            lineage = _private_parents(src, source, owner)
            if not lineage:
                continue
            if _private_parents(dst, candidate, owner) != lineage:
                raise ValueError("Candidate lineage differs from the sealed source")
            data = _expected_messages(src, source, owner)
            if not data or not data[1]:
                continue
            if _expected_messages(dst, candidate, owner) != data:
                raise ValueError("Candidate differs from sealed evidence")
            source_hash, messages = data
            binding = (
                sid,
                owner.tenant_id,
                old_agent,
                target_agent,
                owner.key,
                source["acl_version"],
                receipt,
                identity,
                source_hash,
                lineage,
            )
            dst.execute(
                "INSERT OR IGNORE INTO oa_private_legacy_sessions VALUES(?,?,?,?,?,?,?,?,?,?)",
                binding,
            )
            if (
                tuple(
                    dst.execute(
                        "SELECT * FROM oa_private_legacy_sessions WHERE session_id=?",
                        (sid,),
                    ).fetchone()
                )
                != binding
            ):
                raise ValueError(
                    "Historical session already belongs to another binding"
                )
            dst.executemany(
                "INSERT OR IGNORE INTO oa_private_legacy_messages VALUES(?,?,?,?,?,?,?)",
                [(sid, mid, *facts) for mid, facts in messages.items()],
            )
            stored = {
                v[0]: tuple(v[1:])
                for v in dst.execute(
                    "SELECT message_id,run_id,role,sequence,ordinal,body_digest FROM oa_private_legacy_messages WHERE session_id=?",
                    (sid,),
                )
            }
            if stored != messages:
                raise ValueError("Stored original messages differ from sealed evidence")
            enabled += 1
            total += len(messages)
        if _digest(root / MANIFEST) != receipt:
            raise ValueError("Source manifest changed during attestation")
        dst.commit()
    return {
        "requested_sessions": len(approved_sessions),
        "qualified_sessions": enabled,
        "verified_messages": total,
        "source_manifest_sha256": receipt,
    }
