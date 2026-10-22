"""Recall indexes cover canonical (``sessions_v2``) conversations.

After the operational storage v2 migration the runtime writes conversations
to ``sessions_v2`` + ``session_messages``; the legacy ``sessions.runs`` blob is
only still written by a few paths. The transcript (FTS) and semantic indexes
read only the legacy table, so on a migrated host deployment every chat, event
and webhook conversation was invisible to recall and transcript search.

The agent DB here is built from the code's own DDL — the legacy schema of
``MemoryDB`` and the shipped operational v2 migration — so a schema drift in
either breaks this test rather than passing against a hand-written copy.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.memory.operational.schema import operational_schema_sql
from openagent_core.memory.semantic_index import SemanticIndex
from openagent_core.memory.transcript_index import TranscriptIndex

TENANT = "installation:test"
OWNER = "user:owner"


class _Embedder:
    """Deterministic, network-free: the vector depends only on the text."""

    model_id = "test-embedder"
    provider = "test"

    def __init__(self):
        self.calls: list[str] = []

    def embed(self, texts):
        self.calls.extend(texts)
        return [[float(len(t) % 7) + 1.0, 1.0, float(sum(map(ord, t)) % 11)] for t in texts]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _connect(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _agent_db(path: Path) -> None:
    con = _connect(path)
    con.executescript(SCHEMA_SQL)
    con.executescript(operational_schema_sql())
    con.commit()
    con.close()


def _canonical_session(con, sid: str, *, title: str, messages: list[tuple[str, str]],
                       visibility: str = "private", origin: str = "event") -> None:
    now = _now_ms()
    con.execute(
        "INSERT INTO sessions_v2 (id, tenant_id, owner_principal_id, visibility, title, "
        "session_type, kind, origin, status, created_at_ms, updated_at_ms, "
        "last_activity_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, TENANT, OWNER, visibility, title, "agent", "chat", origin, "active",
         now, now, now),
    )
    for role, text in messages:
        _append(con, sid, role, text)


def _append(con, sid: str, role: str, text: str, *, visibility: str = "user_visible") -> None:
    now = _now_ms()
    seq = con.execute(
        "SELECT COALESCE(MAX(sequence), -1) + 1 FROM session_messages WHERE session_id=?",
        (sid,)).fetchone()[0]
    con.execute(
        "INSERT INTO session_messages (id, tenant_id, session_id, sequence, ordinal, role, "
        "status, author_kind, author_principal_id, text, visibility, raw_envelope_json, "
        "raw_envelope_schema, created_at_ms, updated_at_ms) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (f"{sid}:m{seq}", TENANT, sid, seq, seq, role, "complete",
         "user" if role == "user" else "agent", OWNER, text, visibility, "{}", 1, now, now),
    )
    con.execute(
        "UPDATE sessions_v2 SET updated_at_ms=?, last_activity_at_ms=?, "
        "source_version=source_version+1 WHERE id=?", (now, now, sid))


def _legacy_session(con, sid: str, user_text: str, reply: str) -> None:
    runs = [{"created_at": int(time.time()),
             "messages": [{"role": "user", "content": user_text},
                          {"role": "assistant", "content": reply}]}]
    now = int(time.time())
    con.execute(
        "INSERT INTO sessions (session_id, session_type, metadata, runs, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?)",
        (sid, "agent", json.dumps({"title": f"legacy {sid}"}), json.dumps(runs), now, now),
    )


def _seed(path: Path) -> None:
    _agent_db(path)
    con = _connect(path)
    _canonical_session(con, "event:alpha", title="Import stops early", messages=[
        ("user", "my playlist import stops at fifty songs every time"),
        ("assistant", "Scrolling the source playlist loads the remaining songs."),
    ])
    _canonical_session(con, "event:beta", title="Refund", messages=[
        ("user", "please refund the duplicate yearly subscription charge"),
        ("assistant", "The duplicate charge has been refunded to your card."),
    ])
    # A session present in both stores: the canonical copy is authoritative.
    _canonical_session(con, "chat:both", title="Both", messages=[
        ("user", "canonical wording about the offline downloads"),
    ])
    _legacy_session(con, "chat:both", "stale legacy wording zebracorn", "legacy reply text")
    # Only in the legacy table (a pre-migration or scheduler-only session).
    _legacy_session(con, "scheduler:nightly", "nightly digest of the moderation queue",
                    "The queue has three pending reports.")
    # Never indexed: quarantined, internal-only text, already tombstoned.
    _canonical_session(con, "event:fenced", title="Fenced", visibility="quarantined",
                       messages=[("user", "quarantined conversation about kumquats")])
    _canonical_session(con, "event:internal", title="", messages=[])
    _append(con, "event:internal", "assistant", "internal scaffolding about rutabagas",
            visibility="internal")
    con.commit()
    con.close()


def _search_ids(idx: TranscriptIndex, query: str) -> set[str]:
    return {h["session_id"] for h in idx.search(query, limit=25)}


def test_transcript_index_covers_canonical_sessions(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    idx = TranscriptIndex(db, tmp_path / "transcripts.db")

    stats = idx.sync()
    assert stats.pending == 0
    assert _search_ids(idx, "playlist fifty") == {"event:alpha"}
    assert _search_ids(idx, "duplicate refunded") == {"event:beta"}
    hit = idx.search("playlist", limit=1)[0]
    assert hit["title"] == "Import stops early" and hit["origin"] == "event"
    # Legacy-only sessions are still indexed.
    assert _search_ids(idx, "moderation queue") == {"scheduler:nightly"}
    # Dual-stored session: canonical text wins, the legacy copy is not indexed.
    assert _search_ids(idx, "offline downloads") == {"chat:both"}
    assert _search_ids(idx, "zebracorn") == set()
    # Quarantined sessions and non-user-visible messages are never indexed.
    assert _search_ids(idx, "kumquats") == set()
    assert _search_ids(idx, "rutabagas") == set()

    again = idx.sync()
    assert (again.added, again.updated, again.deleted, again.messages) == (0, 0, 0, 0)

    con = _connect(db)
    _append(con, "event:alpha", "user", "also the artist radio skips tracks")
    con.commit()
    con.close()
    upd = idx.sync()
    assert upd.updated == 1 and upd.added == 0
    assert _search_ids(idx, "radio skips") == {"event:alpha"}

    # A privacy redaction rewrites one message and stamps only that row.
    con = _connect(db)
    con.execute("UPDATE session_messages SET text='[redacted]', updated_at_ms=? "
                "WHERE session_id='event:beta' AND role='user'", (_now_ms() + 5,))
    con.commit()
    con.close()
    assert idx.sync().updated == 1
    assert _search_ids(idx, "yearly subscription") == set()

    # Archive (tombstone) removes the session from the index.
    con = _connect(db)
    con.execute("UPDATE sessions_v2 SET deleted_at_ms=updated_at_ms, status='archived' "
                "WHERE id='event:alpha'")
    con.commit()
    con.close()
    gone = idx.sync()
    assert gone.deleted == 1
    assert _search_ids(idx, "playlist") == set()
    # beta, both, nightly and the (empty) internal-only session remain.
    assert idx.stats()["sessions"] == 4
    idx.close()


def test_transcript_index_upgrades_a_legacy_built_cache(tmp_path):
    """A cache file built before canonical sessions were indexed keeps working:
    the source column is added in place and canonical sessions are filled in."""
    db = tmp_path / "agent.db"
    _seed(db)
    cache = tmp_path / "transcripts.db"
    old = sqlite3.connect(cache)
    old.executescript(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);"
        "CREATE TABLE indexed_sessions (session_id TEXT PRIMARY KEY, updated_at INTEGER NOT NULL,"
        " runs_len INTEGER NOT NULL, title TEXT, origin TEXT, msg_count INTEGER NOT NULL DEFAULT 0,"
        " indexed_at REAL NOT NULL);"
    )
    old.commit()
    old.close()
    idx = TranscriptIndex(db, cache)
    idx.sync()
    assert _search_ids(idx, "playlist fifty") == {"event:alpha"}
    idx.close()


def test_semantic_index_covers_canonical_sessions(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    embedder = _Embedder()
    idx = SemanticIndex(db, index_path=tmp_path / "semantic.db", embedder=embedder,
                        index_sessions=True)

    def vectors() -> dict[str, tuple[str, str]]:
        return {r["session_id"]: (r["source"], r["title"]) for r in idx._conn.execute(
            "SELECT session_id, source, title FROM session_vectors")}

    stats = idx.sync_sessions()
    assert stats.pending == 0 and not stats.errored
    assert vectors() == {
        "event:alpha": ("canonical", "Import stops early"),
        "event:beta": ("canonical", "Refund"),
        "chat:both": ("canonical", "Both"),
        "scheduler:nightly": ("legacy", "legacy scheduler:nightly"),
    }
    digests = "\n".join(embedder.calls)
    assert "fifty songs" in digests and "moderation queue" in digests
    assert "zebracorn" not in digests  # legacy copy of a canonical session
    assert "kumquats" not in digests and "rutabagas" not in digests

    embedder.calls.clear()
    again = idx.sync_sessions()
    assert (again.embedded, again.deleted) == (0, 0) and embedder.calls == []

    con = _connect(db)
    _append(con, "event:beta", "assistant", "A confirmation email is on its way.")
    con.commit()
    con.close()
    upd = idx.sync_sessions()
    assert upd.updated == 1 and upd.embedded == 1
    assert "confirmation email" in embedder.calls[-1]

    con = _connect(db)
    con.execute("UPDATE sessions_v2 SET deleted_at_ms=updated_at_ms, status='archived' "
                "WHERE id='event:beta'")
    con.commit()
    con.close()
    gone = idx.sync_sessions()
    assert gone.deleted == 1
    assert "event:beta" not in vectors()
    hits = idx.search("refund the duplicate", scope="sessions", limit=10, min_score=-1.0)
    assert "event:beta" not in {h["session_id"] for h in hits}
    idx.close()


def test_semantic_session_budget_is_respected(tmp_path):
    db = tmp_path / "agent.db"
    _seed(db)
    idx = SemanticIndex(db, index_path=tmp_path / "semantic.db", embedder=_Embedder(),
                        index_sessions=True)
    first = idx.sync_sessions(max_items=2)
    assert first.embedded == 2 and first.pending == 2
    second = idx.sync_sessions(max_items=2)
    assert second.embedded == 2 and second.pending == 0
    idx.close()
