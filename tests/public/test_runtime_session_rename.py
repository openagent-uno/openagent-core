"""Renaming a runtime-owned conversation reaches its canonical session row.

The rename endpoint writes the title into the compatibility ``sessions`` row.
Runtime admission owns everything else about a runtime-owned session, so the
projection used to drop the title and the conversation kept its first-message
name. The DB is built from the code's own DDL and projected by the real writer.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.memory.operational.repository import (
    project_legacy_session, projection_coverage, reconcile_pending,
)
from openagent_core.memory.operational.schema import operational_schema_sql

TENANT = "installation:test"
OWNER = "user:owner"
SID = "session-rename"


def _db(tmp_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(tmp_path / "agent.db", timeout=2)
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(SCHEMA_SQL)
    con.executescript(operational_schema_sql())
    con.execute(
        "INSERT INTO sessions_v2 (id, tenant_id, owner_principal_id, visibility, title, session_type, kind, "
        "origin, status, metadata_json, created_at_ms, updated_at_ms, last_activity_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (SID, TENANT, OWNER, "private", "Questa è la conversazione…", "agent", "agent", "chat", "active",
         json.dumps({"runtime_contract": 1, "legacy": {"title": "Questa è la conversazione…"}}), 1, 1, 1),
    )
    con.commit()
    return con


def _rename(con: sqlite3.Connection, title: str) -> None:
    con.execute("INSERT OR REPLACE INTO sessions (session_id, session_type, user_id, metadata, runs, created_at, "
                "updated_at) VALUES (?, 'agent', NULL, ?, '[]', 1, 2)",
                (SID, json.dumps({"client_id": "owner", "title": title})))
    project_legacy_session(con, SID)
    con.commit()


def test_a_rename_updates_the_runtime_owned_session_its_feed_row_and_search(tmp_path):
    con = _db(tmp_path)

    _rename(con, "Daily Triage")

    title, metadata, version = con.execute(
        "SELECT title, metadata_json, source_version FROM sessions_v2 WHERE id=?", (SID,)).fetchone()
    assert title == "Daily Triage"
    assert json.loads(metadata)["legacy"]["title"] == "Daily Triage"
    assert json.loads(metadata)["runtime_contract"] == 1
    assert con.execute("SELECT title FROM activity_items WHERE resource_id=? AND deleted_at_ms IS NULL",
                       (SID,)).fetchone() == ("Daily Triage",)
    assert con.execute("SELECT count(*) FROM search_outbox WHERE source_kind='session' AND source_id=? "
                       "AND source_version=?", (SID, version)).fetchone() == (1,)
    assert con.execute("SELECT count(*) FROM domain_events WHERE session_id=? AND event_type='session.renamed'",
                       (SID,)).fetchone() == (1,)


def test_the_same_title_again_changes_nothing(tmp_path):
    con = _db(tmp_path)
    _rename(con, "Daily Triage")
    version = con.execute("SELECT source_version FROM sessions_v2 WHERE id=?", (SID,)).fetchone()

    _rename(con, "Daily Triage")

    assert con.execute("SELECT source_version FROM sessions_v2 WHERE id=?", (SID,)).fetchone() == version


@pytest.mark.parametrize("metadata", [None, "null", "[]", "false", '"null"', '{"unfinished":'])
def test_metadata_without_a_title_does_not_block_runtime_history_reconciliation(tmp_path, metadata):
    con = _db(tmp_path)
    con.execute("INSERT INTO sessions(session_id,session_type,metadata,runs,created_at,updated_at) "
                "VALUES (?,'agent',?,'[]',1,2)", (SID, metadata))
    con.execute("INSERT INTO legacy_session_changes(session_id,operation,observed_at_ms) "
                "VALUES (?,'insert',1)", (SID,))
    con.commit()

    assert len(reconcile_pending(con, now_ms=3)) == 1
    assert projection_coverage(con)["complete"] is True
    assert con.execute("SELECT owner_principal_id,title FROM sessions_v2 WHERE id=?", (SID,)).fetchone() == (
        OWNER, "Questa è la conversazione…")
    assert con.execute("SELECT count(*) FROM domain_events WHERE event_type='session.renamed'").fetchone()[0] == 0


def test_double_encoded_compatibility_metadata_carries_an_explicit_rename(tmp_path):
    con = _db(tmp_path)
    con.execute("INSERT INTO sessions(session_id,session_type,metadata,runs,created_at,updated_at) "
                "VALUES (?,'agent',?,'[]',1,2)",
                (SID, json.dumps(json.dumps({"title": "Reviewed title"}))))
    project_legacy_session(con, SID)
    assert con.execute("SELECT owner_principal_id,title FROM sessions_v2 WHERE id=?", (SID,)).fetchone() == (
        OWNER, "Reviewed title")
