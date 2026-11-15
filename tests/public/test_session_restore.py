"""A tombstoned session whose history was retained can be restored unchanged.

A tombstone keeps the normalized messages and tool calls until payload pruning
reclaims them, so a conversation deleted by mistake (a retention policy that
matched the wrong rows) must come back with its own transcript, its real
last-activity time, a live activity row and search upserts — never a rebuild
from a compacted compatibility buffer.

The agent DB is built from the code's own DDL and the session is tombstoned by
the real repository writer, so schema or writer drift breaks these tests.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.memory.operational.repository import (
    SessionRestoreError,
    restore_tombstoned_session,
    tombstone_session,
)
from openagent_core.memory.operational.schema import operational_schema_sql

TENANT = "installation:test"
OWNER = "user:owner"
HOUR = 3_600_000


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=2)
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _db(tmp_path: Path) -> sqlite3.Connection:
    path = tmp_path / "agent.db"
    con = _connect(path)
    con.executescript(SCHEMA_SQL)
    con.executescript(operational_schema_sql())
    con.commit()
    return con


def _conversation(con, sid: str, *, at: int, runtime_owned: bool = True, legacy: bool = False) -> None:
    metadata = {"runtime_contract": 1} if runtime_owned else {}
    con.execute(
        "INSERT INTO sessions_v2 (id, tenant_id, owner_principal_id, visibility, title, "
        "session_type, kind, origin, status, metadata_json, created_at_ms, updated_at_ms, "
        "last_activity_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, TENANT, OWNER, "private", "ci sei?", "agent", "agent", "chat", "active",
         json.dumps(metadata), at, at, at),
    )
    run = f"{sid}:run"
    con.execute(
        "INSERT INTO session_runs (id, tenant_id, session_id, ordinal, runner_kind, status, "
        "status_raw, raw_envelope_json, raw_envelope_schema, created_at_ms, finished_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (run, TENANT, sid, 0, "agent", "success", "success", "{}", 1, at, at + 2000),
    )
    for ordinal, (role, text) in enumerate((("user", "ci sei?"), ("assistant", "Sì, ci sono."))):
        con.execute(
            "INSERT INTO session_messages (id, tenant_id, session_id, run_id, sequence, ordinal, "
            "role, status, author_kind, author_principal_id, text, raw_envelope_json, "
            "raw_envelope_schema, created_at_ms, updated_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"{sid}:m{ordinal}", TENANT, sid, run, ordinal, ordinal, role, "complete",
             "user" if role == "user" else "agent", OWNER if role == "user" else "agent:main",
             text, "{}", 1, at + ordinal * 1000,
             at + ordinal * 1000),
        )
    con.execute(
        "INSERT INTO tool_invocations (id, tenant_id, owner_principal_id, root_kind, root_id, "
        "session_id, session_run_id, ordinal, tool_server, tool_name, status, status_raw, "
        "raw_envelope_json, raw_envelope_schema, created_at_ms, finished_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{sid}:t0", TENANT, OWNER, "session", sid, sid, run, 0, "srv", "tool", "success",
         "succeeded", "{}", 1, at, at),
    )
    if legacy:
        con.execute(
            "INSERT INTO sessions (session_id, session_type, runs, created_at, updated_at) "
            "VALUES (?, 'agent', '[]', ?, ?)",
            (sid, at // 1000, at // 1000),
        )


def _transcript(con, sid):
    return con.execute(
        "SELECT id, role, text FROM session_messages WHERE session_id=? ORDER BY sequence",
        (sid,),
    ).fetchall()


def test_restored_conversation_keeps_its_transcript_and_real_activity_time(tmp_path):
    con = _db(tmp_path)
    started = int(time.time() * 1000) - 48 * HOUR
    _conversation(con, "session-1", at=started)
    before = _transcript(con, "session-1")
    deleted_at = started + 30 * HOUR
    assert tombstone_session(con, "session-1", now_ms=deleted_at).deleted

    result = restore_tombstoned_session(con, "session-1", now_ms=deleted_at + HOUR)

    assert result.changed and not result.deleted
    status, deleted, last_activity = con.execute(
        "SELECT status, deleted_at_ms, last_activity_at_ms FROM sessions_v2 WHERE id='session-1'"
    ).fetchone()
    assert (status, deleted) == ("active", None)
    # The tombstone had moved it to the deletion time; the app orders chats by it.
    assert last_activity == started + 2000
    assert _transcript(con, "session-1") == before
    activity = con.execute(
        "SELECT deleted_at_ms, deleted_revision FROM activity_items "
        "WHERE resource_type='session' AND resource_id='session-1'"
    ).fetchone()
    assert activity == (None, None)


def test_restore_queues_search_upserts_after_the_tombstone_deletes(tmp_path):
    con = _db(tmp_path)
    _conversation(con, "session-1", at=int(time.time() * 1000) - HOUR)
    tombstone_session(con, "session-1")

    result = restore_tombstoned_session(con, "session-1")

    upserts = {
        (kind, source)
        for kind, source in con.execute(
            "SELECT source_kind, source_id FROM search_outbox "
            "WHERE operation='upsert' AND source_version=?",
            (result.source_version,),
        )
    }
    assert upserts == {
        ("session", "session-1"),
        ("message", "session-1:m0"),
        ("message", "session-1:m1"),
        ("tool_invocation", "session-1:t0"),
    }
    assert con.execute(
        "SELECT COUNT(*) FROM domain_events WHERE session_id='session-1' "
        "AND event_type='session.restored'"
    ).fetchone()[0] == 1


def test_restore_refuses_a_session_whose_payloads_were_pruned(tmp_path):
    con = _db(tmp_path)
    _conversation(con, "session-1", at=int(time.time() * 1000) - HOUR)
    tombstone_session(con, "session-1")
    con.execute("DELETE FROM tool_invocations WHERE session_id='session-1'")
    con.execute("DELETE FROM session_messages WHERE session_id='session-1'")

    with pytest.raises(SessionRestoreError) as refused:
        restore_tombstoned_session(con, "session-1")

    assert refused.value.reason == "payloads_pruned"
    assert con.execute(
        "SELECT status FROM sessions_v2 WHERE id='session-1'"
    ).fetchone()[0] == "deleted"


def test_restore_refuses_a_legacy_projection_without_its_source_row(tmp_path):
    con = _db(tmp_path)
    _conversation(con, "session-1", at=int(time.time() * 1000) - HOUR, runtime_owned=False)
    tombstone_session(con, "session-1")

    with pytest.raises(SessionRestoreError) as refused:
        restore_tombstoned_session(con, "session-1")

    assert refused.value.reason == "legacy_source_missing"


def test_restore_accepts_a_legacy_projection_whose_source_row_is_back(tmp_path):
    con = _db(tmp_path)
    _conversation(con, "session-1", at=int(time.time() * 1000) - HOUR, runtime_owned=False)
    tombstone_session(con, "session-1")
    con.execute(
        "INSERT INTO sessions (session_id, session_type, runs, created_at, updated_at) "
        "VALUES ('session-1', 'agent', '[]', 1, 1)"
    )

    assert restore_tombstoned_session(con, "session-1").changed


def test_restoring_a_live_session_changes_nothing(tmp_path):
    con = _db(tmp_path)
    _conversation(con, "session-1", at=int(time.time() * 1000) - HOUR)
    outbox = con.execute("SELECT COUNT(*) FROM search_outbox").fetchone()[0]

    result = restore_tombstoned_session(con, "session-1")

    assert not result.changed
    assert con.execute("SELECT COUNT(*) FROM search_outbox").fetchone()[0] == outbox
