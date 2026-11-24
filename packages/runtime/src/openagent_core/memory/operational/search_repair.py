"""Resume the one-time repair of missing canonical SQLite search intents."""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
import sqlite3
import time


_BATCH_SIZE = 128
_SOURCES = {
    "session": (
        "sessions_v2",
        """SELECT s.tenant_id,s.id AS source_id,s.source_version,s.acl_version FROM sessions_v2 AS s
           WHERE json_extract(s.metadata_json,'$.runtime_contract')=1
             AND s.deleted_at_ms IS NULL AND s.id IN ({keys})""",
    ),
    "message": (
        "session_messages",
        """SELECT m.tenant_id,m.id AS source_id,m.source_version,s.acl_version FROM session_messages AS m
           CROSS JOIN sessions_v2 AS s ON s.id=m.session_id
           WHERE json_extract(s.metadata_json,'$.runtime_contract')=1
             AND s.deleted_at_ms IS NULL AND m.id IN ({keys})""",
    ),
    "tool_invocation": (
        "tool_invocations",
        """SELECT t.tenant_id,t.id AS source_id,t.source_version,t.acl_version FROM tool_invocations AS t
           CROSS JOIN session_runs AS r ON r.id=t.session_run_id
           CROSS JOIN sessions_v2 AS s ON s.id=r.session_id
           WHERE json_extract(r.metadata_json,'$.runtime_contract')=1
             AND s.deleted_at_ms IS NULL AND t.id IN ({keys})""",
    ),
}


def initialize_runtime_search_repair(db: sqlite3.Connection) -> None:
    """Capture indexed upper bounds once. Called inside the store startup transaction."""
    db.execute("""CREATE TABLE IF NOT EXISTS runtime_search_intent_repair (
        source_kind TEXT PRIMARY KEY,
        after_id TEXT,
        through_id TEXT
    ) STRICT""")
    if db.execute("SELECT 1 FROM runtime_search_intent_repair LIMIT 1").fetchone():
        return
    for kind, (table, _) in _SOURCES.items():
        through = db.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
        db.execute(
            "INSERT INTO runtime_search_intent_repair(source_kind,through_id) VALUES(?,?)",
            (kind, through),
        )


async def runtime_search_repair_pending(source) -> bool:
    table = await (await source.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_search_intent_repair'"
    )).fetchone()
    if table is None:
        return False
    return await (await source.execute(
        "SELECT 1 FROM runtime_search_intent_repair WHERE through_id IS NOT NULL LIMIT 1"
    )).fetchone() is not None


def advance_runtime_search_repair(path: Path) -> bool:
    """Advance one bounded page, returning whether coverage is still incomplete.

    This low-priority writer gives up quickly if a live turn owns the WAL
    writer. The cursor and newly discovered intents commit together.
    """
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=0.025)) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT source_kind,after_id,through_id FROM runtime_search_intent_repair
                WHERE through_id IS NOT NULL ORDER BY source_kind LIMIT 1""").fetchone()
            if row is None:
                return False
            kind, after, through = row
            table, query = _SOURCES[kind]
            bounds = "id<=?" if after is None else "id>? AND id<=?"
            args = (through,) if after is None else (after, through)
            ids = tuple(row[0] for row in db.execute(
                f"SELECT id FROM {table} WHERE {bounds} ORDER BY id LIMIT ?",
                (*args, _BATCH_SIZE),
            ))
            if ids:
                keys = ",".join("?" for _ in ids)
                db.execute(f"""INSERT OR IGNORE INTO search_outbox
                    (tenant_id,source_kind,source_id,operation,source_version,acl_version,committed_at_ms)
                    SELECT c.tenant_id,?,c.source_id,'upsert',c.source_version,c.acl_version,?
                    FROM ({query.format(keys=keys)}) AS c
                    WHERE NOT EXISTS (SELECT 1 FROM search_outbox AS deletion
                        WHERE deletion.tenant_id=c.tenant_id AND deletion.source_kind=?
                          AND deletion.source_id=c.source_id AND deletion.operation='delete'
                          AND deletion.source_version>=c.source_version AND deletion.acl_version>=c.acl_version)
                    """, (kind, int(time.time()*1000), *ids, kind))
            db.execute("""UPDATE runtime_search_intent_repair SET after_id=?,through_id=?
                WHERE source_kind=?""", (ids[-1] if ids else after, through if len(ids) == _BATCH_SIZE else None, kind))
            pending = db.execute(
                "SELECT 1 FROM runtime_search_intent_repair WHERE through_id IS NOT NULL LIMIT 1"
            ).fetchone()
            db.commit()
            return pending is not None
    except sqlite3.OperationalError as error:
        if (getattr(error, "sqlite_errorcode", 0) & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            return True
        raise
