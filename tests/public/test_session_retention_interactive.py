"""Retention never deletes an interactive conversation through the run-log step.

On OpenAgent 1.1 a conversation with the user also has a legacy ``sessions`` row
with ``session_type='agent'``. Deleting that row tombstones the conversation, so
only the ``days`` window (step 1) may remove it. Scheduled-run and automation
run-logs keep their transcript for ``scheduler_days``; an oversized one is trimmed
to its newest run instead of being deleted.
"""
import json
import sqlite3
import time

from openagent_core.core import session_retention as sr


def _db(tmp_path, rows):
    path = str(tmp_path / "db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE sessions(session_id TEXT PRIMARY KEY, session_type TEXT, updated_at INTEGER, runs TEXT)")
    conn.executemany("INSERT INTO sessions VALUES (?,?,?,?)", rows)
    conn.commit()
    conn.close()
    return path


def _ids(path):
    conn = sqlite3.connect(path)
    try:
        return {row[0]: row[1] for row in conn.execute("SELECT session_id, runs FROM sessions")}
    finally:
        conn.close()


def test_idle_interactive_conversation_survives_inside_the_days_window(tmp_path):
    two_days_ago = int(time.time()) - 2 * 86400
    path = _db(tmp_path, [("session-1794583121382", "agent", two_days_ago, json.dumps([{"t": "hi"}]))])

    sr.prune(path, 30, 5, 200_000)

    assert "session-1794583121382" in _ids(path)


def test_oversized_interactive_conversation_is_not_deleted(tmp_path):
    now = int(time.time())
    runs = json.dumps([{"t": "x" * 300_000}, {"t": "y" * 300_000}])
    path = _db(tmp_path, [("session-big", "agent", now, runs)])

    sr.prune(path, 30, 5, 10_000_000)

    assert "session-big" in _ids(path)


def test_oversized_recent_scheduler_run_log_is_trimmed_not_deleted(tmp_path):
    now = int(time.time())
    runs = json.dumps([{"t": "old" + "x" * 300_000}, {"t": "newest" + "y" * 300_000}])
    path = _db(tmp_path, [("scheduler:task:run", "agent", now, runs)])

    _, trimmed = sr.prune(path, 30, 5, 10_000_000)

    kept = _ids(path)
    assert "scheduler:task:run" in kept
    assert [r["t"][:6] for r in json.loads(kept["scheduler:task:run"])] == ["newest"]
    assert trimmed >= 1


def test_scheduler_run_log_past_scheduler_days_is_deleted(tmp_path):
    old = int(time.time()) - 8 * 86400
    recent = int(time.time()) - 2 * 86400
    path = _db(tmp_path, [
        ("scheduler:task:old", "agent", old, "[]"),
        ("scheduler:task:recent", "agent", recent, "[]"),
        ("automation:task:old", "agent", old, "[]"),
    ])

    sr.prune(path, 30, 5, 200_000, settings={"scheduler_days": 7})

    assert set(_ids(path)) == {"scheduler:task:recent"}


def test_scheduler_days_is_read_from_the_retention_block(tmp_path):
    cfg = tmp_path / "openagent.yaml"
    cfg.write_text("retention:\n  days: 30\n  scheduler_days: 14\n")

    assert sr.load_settings(str(cfg))["scheduler_days"] == 14
