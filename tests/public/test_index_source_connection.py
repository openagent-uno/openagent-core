"""The transcript and semantic indexes read the agent DB through ``src``.

A namespace rewrite (``src.`` -> ``openagent_core.``) also renamed the local
source connection in ``TranscriptIndex.sync`` and ``SemanticIndex.sync_sessions``
/ ``sync_tasks``: every sync raised ``NameError: name 'openagent_core' is not
defined`` and the semantic index stopped updating on every agent.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from openagent_core.memory.semantic_index import SemanticIndex
from openagent_core.memory.transcript_index import TranscriptIndex


class _Embedder:
    model_id = "test-embedder"
    provider = "test"

    def embed(self, texts):
        return [[float(len(t) % 7), 1.0, 0.5] for t in texts]


def _agent_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE sessions (session_id TEXT PRIMARY KEY, updated_at REAL, runs TEXT, metadata TEXT)")
    con.execute("CREATE TABLE scheduled_tasks (id TEXT PRIMARY KEY, name TEXT, enabled INTEGER, updated_at REAL, prompt TEXT)")
    con.execute("INSERT INTO scheduled_tasks VALUES ('t-1','nightly-review',1,?, 'Review the support queue and summarise.')", (time.time(),))
    runs = [{"messages": [{"role": "user", "content": "the playlist import stops at fifty songs"},
                          {"role": "assistant", "content": "Thanks, scrolling loads the rest."}]}]
    con.execute("INSERT INTO sessions VALUES (?,?,?,?)", ("s-1", time.time(), json.dumps(runs), "{}"))
    con.commit()
    con.close()


def test_transcript_sync_reads_the_agent_db(tmp_path):
    db = tmp_path / "agent.db"
    _agent_db(db)
    idx = TranscriptIndex(db, tmp_path / "transcripts.db")
    stats = idx.sync()  # raised NameError before the fix
    assert stats.pending == 0


def test_semantic_session_sync_reads_the_agent_db(tmp_path):
    db = tmp_path / "agent.db"
    _agent_db(db)
    idx = SemanticIndex(db, embedder=_Embedder())
    if not idx.active:
        return  # this environment lacks the vector backend; nothing to exercise
    idx.sync_sessions()  # raised NameError before the fix
    idx.sync_tasks()
