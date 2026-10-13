"""Selective privacy adapter for immutable public RuntimeStore content.

This covers the canonical *terminal* public run ledger, not legacy engine
projections, active streaming, external files, Vault/Git or search replicas.
Hosts must keep those coverage gates closed until their own adapters qualify.
"""
from __future__ import annotations

import hashlib
import json
import time

from openagent_core.content_privacy import ContentRevision, PrivacyConflict
from .content_privacy import SqliteContentPrivacyStore
from .search import enqueue_source


_FIELDS = {
    'message': ('session_messages', ('text','content_json','compressed_content', 'reasoning_content',
                                     'redacted_reasoning_content','raw_envelope_json')),
    'tool': ('tool_invocations', ('args_json','result_json','result_text','error_json','error_text','raw_envelope_json')),
    'event': ('domain_events', ('metadata_json',)),
    'output': ('session_runs', ('output_json',)),
}


def _content(row, fields):
    return json.dumps({field: row[field] for field in fields}, sort_keys=True, separators=(',', ':')).encode()


class SqliteRuntimeContentPrivacySink:
    """Host-authorized exact-revision operations on terminal public run records.

    The ledger must use the same database as the public runtime store. Capturing
    provenance and canonical revisions then shares one SQLite transaction. Run
    authority, original human messages and unrelated invocations are retained.
    Exports use only the explicit host-bound callback, never a public API response.
    A terminal public run can be captured by exact provider invocation call IDs;
    later generated assistant/compaction text is conservatively derived from it.
    """
    sink_id = 'runtime-ledger'

    def __init__(self, runtime_store, ledger: SqliteContentPrivacyStore, *, export):
        if runtime_store.path != ledger.path:
            raise ValueError('Canonical run content and provenance must share one transaction database')
        self.runtime_store, self.ledger, self.export = runtime_store, ledger, export

    async def capture(self, run_id: str, call_ids: tuple[str, ...], origins: tuple):
        """Called by a trusted host, never model-supplied IDs or scope labels."""
        from openagent_core.persistence import run_sync
        async with self.runtime_store._operation_lock:
            return await run_sync(self._capture, run_id, call_ids, origins)

    def _capture(self, run_id, call_ids, origins):
        if not origins or not call_ids or not isinstance(call_ids, tuple) or len(set(call_ids)) != len(call_ids):
            raise ValueError('Exact provider invocation identities and trusted origins are required')
        with self.runtime_store._transaction() as db:
            run = db.execute('SELECT * FROM session_runs WHERE id=?', (run_id,)).fetchone()
            if run is None or run['finished_at_ms'] is None or not json.loads(run['metadata_json']).get('runtime_contract'):
                raise PrivacyConflict('Only immutable terminal public runtime records can be captured')
            if any(origin.tenant_id != run['tenant_id'] for origin in origins):
                raise PrivacyConflict('Origin belongs to another tenant')
            tools = [db.execute('SELECT * FROM tool_invocations WHERE session_run_id=? AND tool_call_id=?',
                                (run_id, call_id)).fetchone() for call_id in call_ids]
            if any(tool is None or tool['finished_at_ms'] is None for tool in tools):
                raise PrivacyConflict('Provider invocation is absent or still active')
            anchors = db.execute("SELECT * FROM session_messages WHERE run_id=? AND role='tool'", (run_id,)).fetchall()
            selected_anchors = [r for r in anchors if r['tool_call_id'] in call_ids]
            if not selected_anchors:
                raise PrivacyConflict('Provider result has no canonical message anchor')
            first_sequence = min(r['sequence'] for r in selected_anchors)
            messages = selected_anchors + list(db.execute("SELECT * FROM session_messages WHERE run_id=? AND sequence>? AND role IN ('assistant','compaction')", (run_id,first_sequence)))
            events = []
            replay_rows = db.execute('SELECT * FROM domain_events WHERE run_id=? ORDER BY sequence', (run_id,)).fetchall()
            results = [row for row in replay_rows if row['event_type'] == 'tool.completed'
                       and json.loads(row['metadata_json']).get('call_id') in call_ids]
            if len(results) != len(call_ids):
                raise PrivacyConflict('Exact provider results have no complete replay provenance')
            first_result_sequence = min(row['sequence'] for row in results)
            for row in replay_rows:
                value = json.loads(row['metadata_json'])
                if ((row['event_type'] == 'tool.completed' and value.get('call_id') in call_ids)
                        or (row['event_type'].startswith('run.') and value.get('output') is not None)
                        or (row['sequence'] > first_result_sequence and row['event_type'] == 'run.status')
                        or (row['sequence'] > first_result_sequence and row['event_type'] == 'run.stream'
                            and value.get('kind') in {'delta','done'})):
                    events.append(row)
            records = [('tool', r) for r in tools] + [('message', r) for r in messages] + [('event', r) for r in events]
            if run['output_json'] is not None:
                records.append(('output',run))
            revisions = []
            for kind, row in records:
                fields = _FIELDS[kind][1]
                digest = hashlib.sha256(_content(row,fields)).hexdigest()
                identity = str(row['event_id']) if kind == 'event' else row['id']
                revision = ContentRevision(run['tenant_id'], kind+'/'+identity, digest, self.sink_id, digest)
                self.ledger._record_transaction(db, revision, origins)
                revisions.append(revision)
            return tuple(revisions)

    async def apply(self, request, revision):
        from openagent_core.persistence import run_sync
        async with self.runtime_store._operation_lock:
            result = await run_sync(self._apply, request, revision)
            if request.operation == 'redact' and result == 'complete':
                if not await run_sync(self._scrub):
                    return 'conflict'
        if request.operation == 'export' and result != 'conflict':
            await self.export(request, revision, result)
            return 'complete'
        return result

    def _scrub(self):
        # secure_delete scrubs replaced cells; truncate the WAL only after the
        # canonical transaction commits. A reader retaining an old snapshot
        # blocks completion and the worker retries without repeating effects.
        return self.runtime_store.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0

    def _apply(self, request, revision):
        if revision.sink_id != self.sink_id or revision.tenant_id != request.tenant_id:
            return 'conflict'
        kind, _, identity = revision.artifact_id.partition('/')
        if kind not in _FIELDS or not identity:
            return 'conflict'
        table, fields = _FIELDS[kind]
        key = 'event_id' if kind == 'event' else 'id'
        self.runtime_store.connection.execute('PRAGMA secure_delete=ON')
        with self.runtime_store._transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS runtime_privacy_erased(
                tenant TEXT NOT NULL, artifact TEXT NOT NULL, revision TEXT NOT NULL,
                PRIMARY KEY(tenant,artifact,revision))''')
            marker = db.execute('SELECT 1 FROM runtime_privacy_erased WHERE tenant=? AND artifact=? AND revision=?',
                                (revision.tenant_id, revision.artifact_id, revision.revision_id)).fetchone()
            row = db.execute(f'SELECT * FROM {table} WHERE {key}=? AND tenant_id=?', (identity,revision.tenant_id)).fetchone()
            if row is None or marker:
                return b'' if request.operation == 'export' else 'complete'
            content = _content(row, fields)
            if hashlib.sha256(content).hexdigest() != revision.content_sha256:
                return 'conflict'
            if request.operation == 'export':
                return content
            values = {field: None for field in fields}
            # Keep required JSON envelope columns valid without retaining data.
            for field in ('raw_envelope_json','metadata_json'):
                if field in values:
                    values[field] = '{"privacy_redacted":true}'
            changes = ','.join(field+'=?' for field in values)
            if kind == 'event':
                # The public event ledger is immutable during ordinary runtime
                # writes. A privacy operation is an explicit canonical-content
                # exception, performed in one exclusive transaction. Readers
                # never observe an unprotected schema; rollback restores it.
                trigger = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_domain_events_no_update'").fetchone()
                expected = "CREATE TRIGGER trg_domain_events_no_update BEFORE UPDATE ON domain_events BEGIN SELECT RAISE(ABORT, 'domain_events rows are immutable'); END"
                if trigger is None or ' '.join(trigger[0].split()) != expected:
                    return 'conflict'
                db.execute('DROP TRIGGER trg_domain_events_no_update')
                try:
                    db.execute(f'UPDATE {table} SET {changes} WHERE {key}=? AND tenant_id=?',
                               (*values.values(),identity,revision.tenant_id))
                finally:
                    db.execute(trigger[0])
            else:
                db.execute(f'UPDATE {table} SET {changes} WHERE {key}=? AND tenant_id=?',
                           (*values.values(),identity,revision.tenant_id))
            now = int(time.time()*1000)
            if kind == 'message':
                db.execute('UPDATE session_messages SET source_version=source_version+1,updated_at_ms=? WHERE id=?', (now,identity))
                enqueue_source(db,'message',identity,now)
            elif kind == 'tool':
                db.execute('UPDATE tool_invocations SET source_version=source_version+1,result_sha256=NULL,result_size_bytes=NULL WHERE id=?', (identity,))
                enqueue_source(db,'tool_invocation',identity,now)
            db.execute('INSERT INTO runtime_privacy_erased VALUES(?,?,?)',
                       (revision.tenant_id,revision.artifact_id,revision.revision_id))
            return 'complete'
