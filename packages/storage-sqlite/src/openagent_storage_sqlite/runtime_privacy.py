"""Selective privacy adapter for immutable public RuntimeStore content.

This covers the canonical *terminal* public run ledger, not legacy engine
projections, active streaming, external files, Vault/Git or search replicas.
Hosts must keep those coverage gates closed until their own adapters qualify.
"""
from __future__ import annotations

import hashlib
import base64
import json
import time

from openagent_core.content_privacy import ContentRevision, PrivacyConflict, PrivacyFence, ToolResultProvenance
from .content_privacy import SqliteContentPrivacyStore
from .privacy_json import get as json_get, replace as json_replace, encode_path
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


def _fragment_content(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


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

    def __init__(self, runtime_store, ledger: SqliteContentPrivacyStore, *, export, search_privacy=None):
        if runtime_store.path != ledger.path:
            raise ValueError('Canonical run content and provenance must share one transaction database')
        self.runtime_store, self.ledger, self.export = runtime_store, ledger, export
        self.search_privacy=search_privacy

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
        if request.operation=='redact' and result=='complete':
            from openagent_core.memory.operational.search import operational_search_path
            if operational_search_path(self.runtime_store.path).exists():
                if self.search_privacy is None:
                    return 'conflict'
                through=await run_sync(lambda:self.runtime_store.connection.execute('SELECT coalesce(max(seq),0) FROM search_outbox').fetchone()[0])
                if not await self.search_privacy.refresh(through):
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
        if kind=='fragment':
            return self._apply_fragment(request,revision)
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

    def _apply_fragment(self,request,revision):
        self.runtime_store.connection.execute('PRAGMA secure_delete=ON')
        with self.runtime_store._transaction() as db:
            node=db.execute('SELECT * FROM runtime_privacy_fragments WHERE artifact=? AND tenant=?',
                            (revision.artifact_id,revision.tenant_id)).fetchone()
            if node is None:
                return 'conflict'
            db.execute('''CREATE TABLE IF NOT EXISTS runtime_privacy_erased(
                tenant TEXT NOT NULL, artifact TEXT NOT NULL, revision TEXT NOT NULL,
                PRIMARY KEY(tenant,artifact,revision))''')
            if db.execute('SELECT 1 FROM runtime_privacy_erased WHERE tenant=? AND artifact=? AND revision=?',
                          (revision.tenant_id,revision.artifact_id,revision.revision_id)).fetchone():
                return b'' if request.operation=='export' else 'complete'
            kind=node['kind'];table=_FIELDS[kind][0];key='event_id' if kind=='event' else 'id'
            field=node['field'];path=json.loads(node['path'])
            if field not in _FIELDS[kind][1]:
                return 'conflict'
            row=db.execute(f'SELECT {field} FROM {table} WHERE {key}=? AND tenant_id=?',(node['identity'],revision.tenant_id)).fetchone()
            if row is None:
                return b'' if request.operation=='export' else 'complete'
            try:
                root=json.loads(row[0]);value=json_get(root,path)
            except (ValueError,TypeError,IndexError,KeyError):
                return 'conflict'
            content=_fragment_content(value)
            if hashlib.sha256(content).hexdigest()!=revision.content_sha256:
                return 'conflict'
            if request.operation=='export':
                return content
            replacement=json.dumps(json_replace(root,path,None),sort_keys=True,separators=(',',':'),ensure_ascii=False)
            trigger=None
            if kind=='event':
                trigger=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_domain_events_no_update'").fetchone()
                expected="CREATE TRIGGER trg_domain_events_no_update BEFORE UPDATE ON domain_events BEGIN SELECT RAISE(ABORT, 'domain_events rows are immutable'); END"
                if trigger is None or ' '.join(trigger[0].split())!=expected:
                    return 'conflict'
                db.execute('DROP TRIGGER trg_domain_events_no_update')
            try:
                db.execute(f'UPDATE {table} SET {field}=? WHERE {key}=? AND tenant_id=?',(replacement,node['identity'],revision.tenant_id))
            finally:
                if trigger: db.execute(trigger[0])
            now=int(time.time()*1000)
            if kind=='message':
                db.execute('UPDATE session_messages SET source_version=source_version+1,updated_at_ms=? WHERE id=?',(now,node['identity']))
                enqueue_source(db,'message',node['identity'],now)
            elif kind=='tool':
                db.execute('UPDATE tool_invocations SET source_version=source_version+1,result_sha256=NULL,result_size_bytes=NULL WHERE id=?',(node['identity'],))
                enqueue_source(db,'tool_invocation',node['identity'],now)
            db.execute('INSERT INTO runtime_privacy_erased VALUES(?,?,?)',(revision.tenant_id,revision.artifact_id,revision.revision_id))
            return 'complete'


class SqliteToolResultPrivacyPublisher:
    """Atomic live public-ledger provenance, explicitly installed by a host.

    This is not a legacy-engine or Vault writer. Enabling it alone never attests
    full surface coverage. The same canonical transaction writes source result,
    generation fence checks, lineage and later generated public stream records.
    """
    def __init__(self, runtime_store, ledger):
        if runtime_store.path != ledger.path:
            raise ValueError('Live provenance must use the canonical transaction database')
        if getattr(runtime_store,'_content_privacy_publication',None) is not None:
            raise ValueError('Only one canonical provenance publisher may bind a store')
        self.runtime_store, self.ledger = runtime_store, ledger
        runtime_store.connection.execute('''CREATE TABLE IF NOT EXISTS runtime_privacy_origins(
            run_id TEXT PRIMARY KEY REFERENCES session_runs(id), origins TEXT NOT NULL)''')
        runtime_store.connection.execute('''CREATE TABLE IF NOT EXISTS runtime_privacy_fragments(
            artifact TEXT PRIMARY KEY,tenant TEXT NOT NULL,kind TEXT NOT NULL,identity TEXT NOT NULL,
            field TEXT NOT NULL,path TEXT NOT NULL)''')
        runtime_store.connection.execute('''CREATE TABLE IF NOT EXISTS runtime_privacy_tool_partitions(
            invocation TEXT PRIMARY KEY,partition_json TEXT NOT NULL)''')
        runtime_store._content_privacy_publication = self

    def _origins(self, db, run_id):
        row=db.execute('SELECT origins FROM runtime_privacy_origins WHERE run_id=?',(run_id,)).fetchone()
        return tuple(self.ledger._origin(value) for value in json.loads(row[0])) if row else ()

    def _merge(self, db, run_id, origins):
        merged=set(self._origins(db,run_id)).union(origins)
        encoded=json.dumps(sorted((dict(tenant_id=o.tenant_id,source_id=o.source_id,group_id=o.group_id,
                                       generation=o.generation,subjects=o.subjects) for o in merged),
                                  key=lambda v:json.dumps(v,sort_keys=True)),sort_keys=True,separators=(',',':'))
        db.execute('INSERT INTO runtime_privacy_origins VALUES(?,?) ON CONFLICT(run_id) DO UPDATE SET origins=excluded.origins',(run_id,encoded))
        return tuple(merged)

    def inherit(self, db, run_id, session_id, parent_run_id):
        origins=set()
        if parent_run_id:
            origins.update(self._origins(db,parent_run_id))
        for row in db.execute('SELECT id FROM session_runs WHERE session_id=? AND id!=?',(session_id,run_id)):
            origins.update(self._origins(db,row[0]))
        if origins:
            self._merge(db,run_id,origins)
            # Reject admission before a model can consume revoked historical
            # context. A source/tool-only fence is too late for provider IO.
            self._guard(db,run_id)

    def _record(self,db,kind,identity,origins):
        if not origins:
            return
        table,fields=_FIELDS[kind]
        key='event_id' if kind=='event' else 'id'
        row=db.execute(f'SELECT * FROM {table} WHERE {key}=?',(identity,)).fetchone()
        digest=hashlib.sha256(_content(row,fields)).hexdigest()
        revision=ContentRevision(row['tenant_id'],kind+'/'+identity,digest,'runtime-ledger',digest)
        self.ledger._record_transaction(db,revision,tuple(origins))

    def generated(self,db,kind,identity,run_id):
        self._record(db,kind,identity,self._origins(db,run_id))

    def _guard(self,db,run_id):
        origins=self._origins(db,run_id)
        for row in db.execute('SELECT request FROM privacy_requests'):
            request=self.ledger._request(json.loads(row[0]))
            if request.operation=='redact' and any(request.matches(origin) for origin in origins):
                raise PrivacyFence('Revoked run context cannot invoke or publish derived content')

    async def origins_for_execution(self,context,run_id):
        from openagent_core.persistence import run_sync
        from dataclasses import asdict
        def read():
            db=self.runtime_store.connection
            row=db.execute('SELECT tenant_id,session_id,metadata_json FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if row is None or (row['tenant_id'],row['session_id'])!=(context.tenant_id,context.session_id):
                raise PrivacyConflict('Origin reader does not identify this exact run')
            accepted=json.loads(row['metadata_json']).get('execution_context',{})
            if accepted.get('initiator')!=asdict(context.initiator) or accepted.get('authority')!=asdict(context.authority):
                raise PrivacyConflict('Origin reader does not retain admitted authority')
            self._guard(db,run_id)
            return self._origins(db,run_id)
        async with self.runtime_store._operation_lock:
            return await run_sync(read)

    async def validate_tool_admission(self,context,run_id,*,privacy_fenced=False):
        from openagent_core.persistence import run_sync
        def validate():
            db=self.runtime_store.connection
            row=db.execute('SELECT tenant_id,session_id FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if row is None or (row['tenant_id'],row['session_id'])!=(context.tenant_id,context.session_id):
                raise PrivacyConflict('Admission context does not identify this exact run')
            self._guard(db,run_id)
            if self._origins(db,run_id) and not privacy_fenced:
                raise PrivacyFence('This capability has no attested canonical writer for protected-origin context')
        async with self.runtime_store._operation_lock:
            await run_sync(validate)

    def record_completed_tool(self,db,run_id,call_id,origins):
        self._guard(db,run_id)
        partition=origins if isinstance(origins,ToolResultProvenance) else None
        origins=partition.origins if partition else (origins or self._origins(db,run_id))
        self._merge(db,run_id,origins)
        tool=db.execute('SELECT id FROM tool_invocations WHERE session_run_id=? AND tool_call_id=?',(run_id,call_id)).fetchone()
        if partition:
            from dataclasses import asdict
            db.execute('INSERT INTO runtime_privacy_tool_partitions VALUES(?,?)',(tool['id'],json.dumps(asdict(partition),sort_keys=True)))
            self._record_partition(db,run_id,call_id,tool['id'],partition)
            return
        self._record(db,'tool',tool['id'],origins)
        for row in db.execute("SELECT id FROM session_messages WHERE run_id=? AND role='tool' AND tool_call_id=?",(run_id,call_id)):
            self._record(db,'message',row['id'],origins)
        for row in db.execute("SELECT event_id,metadata_json FROM domain_events WHERE run_id=? AND event_type='tool.completed'",(run_id,)):
            if json.loads(row['metadata_json']).get('call_id')==call_id:
                self._record(db,'event',row['event_id'],origins)

    def _fragment(self,db,kind,identity,field,path,origins):
        if not origins:
            return
        path=encode_path(path)
        table=_FIELDS[kind][0];key='event_id' if kind=='event' else 'id'
        row=db.execute(f'SELECT tenant_id,{field} FROM {table} WHERE {key}=?',(identity,)).fetchone()
        value=json_get(json.loads(row[field]),path)
        artifact='fragment/'+base64.urlsafe_b64encode(_fragment_content([kind,str(identity),field,list(path)])).decode().rstrip('=')
        digest=hashlib.sha256(_fragment_content(value)).hexdigest()
        self.ledger._record_transaction(db,ContentRevision(row['tenant_id'],artifact,digest,'runtime-ledger',digest),origins)
        db.execute('INSERT OR IGNORE INTO runtime_privacy_fragments VALUES(?,?,?,?,?,?)',
                   (artifact,row['tenant_id'],kind,str(identity),field,json.dumps(path)))

    def _record_partition(self,db,run_id,call_id,tool_id,partition):
        for fragment in partition.fragments:
            self._fragment(db,'tool',tool_id,'result_json',fragment.path,fragment.origins)
            self._fragment(db,'tool',tool_id,'raw_envelope_json',('result',)+fragment.path,fragment.origins)
            for row in db.execute("SELECT id FROM session_messages WHERE run_id=? AND role='tool' AND tool_call_id=?",(run_id,call_id)):
                self._fragment(db,'message',row['id'],'text',fragment.path,fragment.origins)
            for row in db.execute("SELECT event_id,metadata_json FROM domain_events WHERE run_id=? AND event_type='tool.completed'",(run_id,)):
                if json.loads(row['metadata_json']).get('call_id')==call_id:
                    self._fragment(db,'event',row['event_id'],'metadata_json',('result',)+fragment.path,fragment.origins)
        # Invocation arguments are one source-bound query, not a mixed response.
        for field,path in (('args_json',()),('raw_envelope_json',('arguments',))):
            self._fragment(db,'tool',tool_id,field,path,partition.origins)
        for row in db.execute("SELECT event_id,metadata_json FROM domain_events WHERE run_id=? AND event_type IN ('tool.invoking','tool.completed')",(run_id,)):
            if json.loads(row['metadata_json']).get('call_id')==call_id:
                self._fragment(db,'event',row['event_id'],'metadata_json',('arguments',),partition.origins)

    async def publish_tool_result(self,context,run_id,call_id,result,origins):
        from openagent_core.persistence import run_sync
        if isinstance(origins,ToolResultProvenance):
            origins.validate_result(result,context.tenant_id)
        async with self.runtime_store._operation_lock:
            row=self.runtime_store.connection.execute('SELECT tenant_id,session_id FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if row is None or row['tenant_id']!=context.tenant_id or row['session_id']!=context.session_id:
                raise PrivacyConflict('Publication context does not identify this exact run')
            await run_sync(self.runtime_store._finish_tool,run_id,call_id,result=result,
                           _privacy_provenance=(self,origins))
