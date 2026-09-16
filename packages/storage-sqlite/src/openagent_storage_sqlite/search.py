"""Transactional intents for the existing rebuildable operational search index."""
from __future__ import annotations


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


def restore_runtime_search_intents(db, now_ms: int) -> None:
    """Backfill omitted intents for pre-fix public rows, without rewriting history.

    The existing outbox uniqueness makes replay idempotent. Its retained latest
    intents also let the normal index consumer rebuild after an index is lost.
    Legacy projection rows keep their existing migration and indexing path.
    """
    sources = (
        ('session', '''SELECT s.tenant_id,s.id,s.source_version,s.acl_version
            FROM sessions_v2 s WHERE json_extract(s.metadata_json,'$.runtime_contract')=1'''),
        ('message', '''SELECT m.tenant_id,m.id,m.source_version,s.acl_version
            FROM session_messages m JOIN sessions_v2 s ON s.id=m.session_id
            WHERE json_extract(s.metadata_json,'$.runtime_contract')=1'''),
        ('tool_invocation', '''SELECT t.tenant_id,t.id,t.source_version,t.acl_version
            FROM tool_invocations t JOIN session_runs r ON r.id=t.session_run_id
            WHERE json_extract(r.metadata_json,'$.runtime_contract')=1'''),
    )
    for kind, query in sources:
        db.execute('''INSERT OR IGNORE INTO search_outbox
            (tenant_id,source_kind,source_id,operation,source_version,acl_version,committed_at_ms)
            SELECT tenant_id,?,id,'upsert',source_version,acl_version,? FROM (''' + query + ')', (kind,now_ms))
