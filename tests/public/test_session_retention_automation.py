"""Retention reaches native 1.1 automation sessions.

Native sessions have no legacy ``sessions`` row, so the legacy retention never
tombstoned support webhook turns (``automation:event:*``), scheduler runs
(``scheduler:*``) or delegated children (``childroot_*``): each turn stored its
tool payloads four times and production databases grew ~1 GB/day.

The agent DB is built from the code's own DDL (legacy ``SCHEMA_SQL`` plus the
shipped operational v2 migration) and tombstones go through the real
repository writer, so schema or writer drift breaks these tests.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from openagent_core.core import session_retention as sr
from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.memory.operational.schema import operational_schema_sql

TENANT = "installation:test"
OWNER = "user:owner"
DAY = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path, timeout=2)
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA busy_timeout=2000")
    return con


def _agent_db(path: Path) -> None:
    con = _connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA_SQL)
    con.executescript(operational_schema_sql())
    con.commit()
    con.close()


def _session(con, sid: str, *, age_ms: int, parent: str | None = None,
             run_status: str = "success", payload: int = 2000) -> None:
    at = _now_ms() - age_ms
    con.execute(
        "INSERT INTO sessions_v2 (id, tenant_id, owner_principal_id, visibility, title, "
        "session_type, kind, origin, parent_session_id, root_session_id, status, "
        "created_at_ms, updated_at_ms, last_activity_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, TENANT, OWNER, "private", sid, "agent", "chat", "event", parent, parent,
         "active", at, at, at),
    )
    run = f"{sid}:run"
    con.execute(
        "INSERT INTO session_runs (id, tenant_id, session_id, ordinal, runner_kind, status, "
        "status_raw, input_json, output_json, raw_envelope_json, raw_envelope_schema, "
        "created_at_ms, finished_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (run, TENANT, sid, 0, "agent", run_status, run_status, '{"p":"x"}', '{"o":"y"}',
         '{"raw":"z"}', 1, at, None if run_status == "running" else at),
    )
    con.execute(
        "INSERT INTO session_messages (id, tenant_id, session_id, run_id, sequence, ordinal, "
        "role, status, author_kind, author_principal_id, text, raw_envelope_json, "
        "raw_envelope_schema, created_at_ms, updated_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{sid}:m0", TENANT, sid, run, 0, 0, "tool", "complete", "system", None,
         "r" * payload, "{}", 1, at, at),
    )
    con.execute(
        "INSERT INTO tool_invocations (id, tenant_id, owner_principal_id, root_kind, root_id, "
        "session_id, session_run_id, ordinal, tool_server, tool_name, status, status_raw, "
        "result_json, raw_envelope_json, raw_envelope_schema, created_at_ms, finished_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{sid}:t0", TENANT, OWNER, "session", sid, sid, run, 0, "srv", "tool", "success",
         "succeeded", '"' + "r" * payload + '"', "{}", 1, at, at),
    )
    for index, event_type in enumerate(("run.accepted", "run.stream", "tool.invoking",
                                        "tool.completed", "run.status", "run.success")):
        con.execute(
            "INSERT INTO domain_events (event_id, tenant_id, resource_type, resource_id, "
            "session_id, run_id, event_type, occurred_at_ms, schema_version, metadata_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (f"{sid}:e{index}", TENANT, "run", run, sid, run, event_type, at, 1,
             '{"result":"' + "r" * (payload if event_type == "tool.completed" else 1) + '"}'),
        )


def _deleted(con, sid: str) -> bool:
    return con.execute("SELECT deleted_at_ms FROM sessions_v2 WHERE id=?", (sid,)).fetchone()[0] is not None


def _seed(path: Path) -> None:
    _agent_db(path)
    con = _connect(path)
    _session(con, "automation:event:support:old", age_ms=2 * DAY)
    _session(con, "automation:event:support:fresh", age_ms=3600_000)
    _session(con, "automation:event:support:inflight", age_ms=2 * DAY, run_status="running")
    _session(con, "automation:event:support:parent", age_ms=2 * DAY)
    _session(con, "childroot_abc::sub::x::1", age_ms=2 * DAY, parent="automation:event:support:parent")
    _session(con, "scheduler:digest:run1", age_ms=2 * DAY)
    _session(con, "scheduler:digest", age_ms=5 * DAY)  # synthetic per-task root
    _session(con, "peer:alice", age_ms=30 * DAY)
    _session(con, "tg:12345", age_ms=30 * DAY)
    _session(con, "automation:event:support:legacy", age_ms=2 * DAY)
    con.execute("INSERT INTO sessions (session_id, session_type, runs, created_at, updated_at) "
                "VALUES ('automation:event:support:legacy','agent','[]',?,?)",
                (int(time.time()), int(time.time())))
    con.commit()
    con.close()


def test_idle_automation_sessions_are_tombstoned_through_the_repository(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    con = _connect(db)
    result = sr.tombstone_idle_automation_sessions(con, days=1, batch_size=1, pause=0)
    assert result == {"tombstoned": 4, "skipped_locked": 0, "failed": 0}
    for sid in ("automation:event:support:old", "automation:event:support:parent",
                "childroot_abc::sub::x::1", "scheduler:digest:run1"):
        assert _deleted(con, sid), sid
    for sid in ("automation:event:support:fresh", "automation:event:support:inflight",
                "scheduler:digest", "peer:alice", "tg:12345", "automation:event:support:legacy"):
        assert not _deleted(con, sid), sid
    # The child is tombstoned before its parent: lineage never points at a
    # live child under a dead parent, and nothing is physically deleted.
    child_at, parent_at = (con.execute("SELECT deleted_at_ms FROM sessions_v2 WHERE id=?", (s,)).fetchone()[0]
                           for s in ("childroot_abc::sub::x::1", "automation:event:support:parent"))
    assert child_at <= parent_at
    # The repository journals search deletes for the session, its messages
    # and tool calls, plus a session.deleted event.
    sid = "automation:event:support:old"
    deletes = {(k, s) for k, s in con.execute(
        "SELECT source_kind, source_id FROM search_outbox WHERE operation='delete'")}
    assert {("session", sid), ("message", f"{sid}:m0"), ("tool_invocation", f"{sid}:t0")} <= deletes
    assert con.execute("SELECT count(*) FROM domain_events WHERE session_id=? AND "
                       "event_type='session.deleted'", (sid,)).fetchone()[0] == 1
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []
    # A second pass finds nothing new.
    assert sr.tombstone_idle_automation_sessions(con, days=1, pause=0)["tombstoned"] == 0


def test_full_prune_reclaims_payloads_and_bulky_events_but_keeps_lineage(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    sr.prune(str(db), days=3, max_runs=5, trim_threshold=200_000,
             settings={"automation_days": 1})
    con = _connect(db)
    sid = "automation:event:support:old"
    assert con.execute("SELECT count(*) FROM session_messages WHERE session_id=?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM tool_invocations WHERE session_id=?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT raw_envelope_json, input_json FROM session_runs WHERE session_id=?",
                       (sid,)).fetchone() == ("{}", None)
    # Events go only once the tombstone is older than the grace period.
    assert con.execute("SELECT count(*) FROM domain_events WHERE session_id=? AND "
                       "event_type='tool.completed'", (sid,)).fetchone()[0] == 1
    later = _now_ms() + 2 * DAY
    assert sr.prune_deleted_domain_events(con, days=1, now_ms=later, pause=0) > 0
    kept = {r[0] for r in con.execute("SELECT event_type FROM domain_events WHERE session_id=?", (sid,))}
    assert kept == {"run.accepted", "run.success", "session.deleted"}
    # Conversational and in-flight sessions keep every row.
    for keep in ("peer:alice", "tg:12345", "automation:event:support:inflight"):
        assert con.execute("SELECT count(*) FROM session_messages WHERE session_id=?", (keep,)).fetchone()[0] == 1
        assert con.execute("SELECT count(*) FROM domain_events WHERE session_id=?", (keep,)).fetchone()[0] == 6
    assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []


def test_a_busy_writer_is_never_blocked(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    locker = subprocess.Popen(
        [sys.executable, "-c",
         "import sqlite3,sys,time;c=sqlite3.connect(sys.argv[1],isolation_level=None);"
         "c.execute('BEGIN IMMEDIATE');print('locked',flush=True);time.sleep(30)", str(db)],
        stdout=subprocess.PIPE, text=True,
    )
    try:
        assert locker.stdout.readline().strip() == "locked"
        con = sqlite3.connect(db, timeout=0.2)
        con.execute("PRAGMA busy_timeout=200")
        started = time.monotonic()
        result = sr.tombstone_idle_automation_sessions(con, days=1, pause=0, max_lock_skips=2)
        assert time.monotonic() - started < 5
        assert result["tombstoned"] == 0 and result["skipped_locked"] == 2
        assert not con.in_transaction
    finally:
        locker.kill()
        locker.wait()
    con = _connect(db)
    assert sr.tombstone_idle_automation_sessions(con, days=1, pause=0)["tombstoned"] == 4


def test_incremental_vacuum_is_bounded_and_only_for_incremental_databases(tmp_path):
    for mode, expect_release in (("INCREMENTAL", True), ("NONE", False)):
        db = tmp_path / f"{mode}.db"
        con = sqlite3.connect(db)
        con.execute(f"PRAGMA auto_vacuum={mode}")
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE blob(x BLOB)")
        con.executemany("INSERT INTO blob VALUES (?)", [(b"x" * 4000,) for _ in range(600)])
        con.commit()
        con.execute("DELETE FROM blob")
        con.commit()
        free = con.execute("PRAGMA freelist_count").fetchone()[0]
        assert free > 500
        out = sr.reclaim_free_pages(con, str(db), step_pages=100, max_pages=300, pause=0)
        assert out["free_before"] == free
        if expect_release:
            assert out["auto_vacuum"] == 2 and 300 <= out["released"] < free
            assert out["free_after"] == free - out["released"]
            out = sr.reclaim_free_pages(con, str(db), step_pages=100, max_pages=10_000, pause=0)
            assert out["free_after"] == 0
        else:
            assert out["released"] == 0 and out["free_after"] == free


def test_without_the_repository_the_step_is_skipped(tmp_path, monkeypatch):
    db = tmp_path / "agent.db"
    _seed(db)
    monkeypatch.setattr(sr, "_load_tombstone_api", lambda: None)
    con = _connect(db)
    assert sr.tombstone_idle_automation_sessions(con, days=1, pause=0).get("unavailable") == 1
    assert not _deleted(con, "automation:event:support:old")
