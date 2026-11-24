"""Transactional intents for the existing rebuildable operational search index."""
from __future__ import annotations


class SqliteOperationalSearchPrivacy:
    """Canonical Core index consumer; no product SQL or rebuild shortcut."""
    def __init__(self,runtime_store,memory_db):
        from pathlib import Path
        if Path(memory_db.db_path).resolve()!=runtime_store.path.resolve():
            raise ValueError('Privacy index must consume the canonical content database')
        self.memory_db=memory_db

    async def refresh(self,through_seq):
        from openagent_core.memory.operational.search import refresh_privacy_search
        return await refresh_privacy_search(self.memory_db,through_seq=through_seq)


def enqueue_source(db, source_kind: str, source_id: str, now_ms: int) -> None:
    """Called inside the canonical writer's transaction; never commits independently."""
    if source_kind == 'session':
        row = db.execute('SELECT tenant_id, source_version, acl_version FROM sessions_v2 WHERE id=?', (source_id,)).fetchone()
    elif source_kind == 'message':
        row = db.execute('''SELECT m.tenant_id, m.source_version, s.acl_version
            FROM session_messages m JOIN sessions_v2 s ON s.id=m.session_id WHERE m.id=?''', (source_id,)).fetchone()
    elif source_kind == 'tool_invocation':
        row = db.execute('SELECT tenant_id, source_version, acl_version FROM tool_invocations WHERE id=?', (source_id,)).fetchone()
    else:
        raise ValueError('Unknown canonical runtime search source')
    if row is None:
        raise LookupError(source_id)
    db.execute('''INSERT OR IGNORE INTO search_outbox
        (tenant_id,source_kind,source_id,operation,source_version,acl_version,committed_at_ms)
        VALUES(?,?,?,'upsert',?,?,?)''', (row[0],source_kind,source_id,row[1],row[2],now_ms))
