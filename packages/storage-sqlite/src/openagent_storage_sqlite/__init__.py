"""Durable run authority on OpenAgent's existing operational SQLite tables.

No second run ledger is created. Acceptance, messages, status and replay use
sessions_v2/session_runs/session_messages/domain_events from storage v2.
"""
from __future__ import annotations
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any, Mapping
import json
import os
import sqlite3
import time
import uuid
import hashlib
import asyncio
import functools
from openagent_core.persistence import run_sync

from .search import enqueue_source, restore_runtime_search_intents

from openagent_core.contracts import (AcceptedRunRequest, PrincipalRef, ExecutionContext, IdempotencyConflict, RunEvent,
    RunRecord, RunRequest, TERMINAL_STATUSES, canonical_json)


def _serialized(method):
    @functools.wraps(method)
    async def invoke(self, *args, **kwargs):
        async with self._operation_lock:
            return await run_sync(method, self, *args, **kwargs)
    return invoke


class SqliteRuntimeStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).absolute()
        self._db: sqlite3.Connection | None = None
        self._lock_fd: int | None = None
        self._operation_lock = asyncio.Lock()

    @property
    def connection(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("Store has not been started")
        return self._db

    def _start(self) -> None:
        if self._db is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path) + '.runtime.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if os.name == 'nt':
                import msvcrt
                if os.fstat(fd).st_size == 0:
                    os.write(fd, b'0')
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise RuntimeError("Another runtime owns this database") from None
        self._lock_fd = fd
        existed = self.path.exists() and self.path.stat().st_size > 0
        try:
            self._db = sqlite3.connect(str(self.path), isolation_level=None, timeout=5, check_same_thread=False)
            self._db.row_factory = sqlite3.Row
            self._db.execute('PRAGMA foreign_keys=ON')
            if existed:
                present = {r[0] for r in self._db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not {'sessions_v2','session_runs','session_messages','domain_events'} <= present:
                    raise RuntimeError("Legacy database requires verified operational migration before starting v1")
            else:
                schema = files('openagent_core.memory.operational.sql').joinpath('operational_storage_v2.sql').read_text()
                self._db.executescript(schema)
                # Fresh embedding stores have no legacy sessions to backfill.
                # Use the final v2 indexes; the immutable legacy migration is
                # still run in full by MemoryDB when that adapter is selected.
                self._db.executescript('''
                    DROP INDEX IF EXISTS uq_tool_invocations_call_context;
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tool_invocations_session_run_call_context
                      ON tool_invocations(session_run_id, tool_call_id)
                      WHERE root_kind = 'session' AND session_run_id IS NOT NULL AND tool_call_id IS NOT NULL;
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tool_invocations_session_root_call_context
                      ON tool_invocations(root_kind, root_id, tool_call_id)
                      WHERE root_kind = 'session' AND session_run_id IS NULL AND tool_call_id IS NOT NULL;
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_tool_invocations_non_session_call_context
                      ON tool_invocations(root_kind, root_id, tool_call_id)
                      WHERE root_kind <> 'session' AND tool_call_id IS NOT NULL;
                    CREATE INDEX IF NOT EXISTS idx_session_messages_run_tool_call
                      ON session_messages(session_id,run_id,tool_call_id)
                      WHERE run_id IS NOT NULL AND tool_call_id IS NOT NULL;
                ''')
            self._db.execute('PRAGMA journal_mode=WAL')
            self._db.execute('PRAGMA synchronous=FULL')
            with self._transaction() as db:
                restore_runtime_search_intents(db, int(time.time()*1000))
        except BaseException:
            self._close()
            raise

    def _close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

    @contextmanager
    def _transaction(self):
        db = self.connection
        db.execute('BEGIN IMMEDIATE')
        try:
            yield db
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise

    @staticmethod
    def _record(row) -> RunRecord:
        metadata = json.loads(row['metadata_json'])
        return RunRecord(row['id'],row['session_id'],row['tenant_id'],row['status'],
                         metadata.get('request_digest',''), json.loads(row['output_json']) if row['output_json'] else None,
                         metadata.get('cancel_requested',False))

    def _get(self, run_id: str) -> RunRecord | None:
        row = self.connection.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone()
        if row:
            return self._record(row)
        reservation = self.connection.execute("SELECT tenant_id,session_id FROM domain_events WHERE run_id=? AND event_type='run.cancel_reserved' ORDER BY sequence LIMIT 1",(run_id,)).fetchone()
        return RunRecord(run_id,reservation['session_id'],reservation['tenant_id'],'cancelled','',cancel_requested=True) if reservation else None

    def _event(self, db, run_id: str, kind: str, payload: Mapping[str, Any]) -> RunEvent:
        row = db.execute('SELECT tenant_id,session_id,metadata_json FROM session_runs WHERE id=?',(run_id,)).fetchone()
        if row is None:
            raise LookupError(run_id)
        actor = json.loads(row['metadata_json']).get('execution_context',{}).get('author',{})
        cursor = db.execute('''INSERT INTO domain_events(event_id,tenant_id,actor_principal_type,
            actor_principal_id,resource_type,resource_id,session_id,run_id,event_type,occurred_at_ms,schema_version,metadata_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (str(uuid.uuid4()),row['tenant_id'],actor.get('kind'),
            canonical_json(actor),'run',run_id,row['session_id'],run_id,kind,int(time.time()*1000),1,canonical_json(dict(payload)))).lastrowid
        return RunEvent(cursor,run_id,kind,dict(payload))

    def _accept(self, request: RunRequest, context: ExecutionContext) -> tuple[RunRecord,bool]:
        digest=request.fingerprint(context)
        with self._transaction() as db:
            rows=db.execute('SELECT * FROM session_runs WHERE id=? OR (session_id=? AND idempotency_key=?)',
                            (request.run_id,request.session_id,request.idempotency_key)).fetchall()
            if rows:
                if len(rows)!=1 or rows[0]['id']!=request.run_id or self._record(rows[0]).request_digest!=digest:
                    raise IdempotencyConflict('Idempotency key or run ID already belongs to a different request or author')
                return self._record(rows[0]),False
            reservation=db.execute("SELECT tenant_id,session_id FROM domain_events WHERE run_id=? AND event_type='run.cancel_reserved' ORDER BY sequence LIMIT 1",(request.run_id,)).fetchone()
            if reservation and (reservation['tenant_id']!=context.tenant_id or reservation['session_id']!=request.session_id):
                raise IdempotencyConflict('Cancelled reservation belongs to another context')
            now=int(time.time()*1000)
            existing=db.execute('SELECT tenant_id,parent_session_id FROM sessions_v2 WHERE id=?',(request.session_id,)).fetchone()
            if existing and existing[0]!=context.tenant_id:
                raise PermissionError('Session belongs to another tenant')
            if not existing:
                db.execute('''INSERT INTO sessions_v2(id,tenant_id,owner_principal_id,visibility,session_type,kind,
                    status,created_at_ms,updated_at_ms,last_activity_at_ms,agent_id,metadata_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',(request.session_id,context.tenant_id,context.authority.key,
                    'shared' if len(context.audience)>1 else 'private','agent','chat','active',now,now,now,context.agent_id,canonical_json({'runtime_contract':1})))
                for recipient in context.audience:
                    if recipient == context.authority:
                        continue
                    kind = recipient.kind if recipient.kind in {'user','agent','device','system','installation','role'} else 'system'
                    for permission in ('view','search'):
                        db.execute('''INSERT INTO resource_acl(tenant_id,resource_type,resource_id,principal_type,
                            principal_id,permission,acl_version,granted_by_principal_id,granted_at_ms)
                            VALUES(?,?,?,?,?,?,?,?,?)''',(context.tenant_id,'session',request.session_id,kind,
                            recipient.key,permission,1,context.authority.key,now))
            else:
                # This acceptance is the explicit transition to the public run
                # writer. Preserve all history and host metadata while stopping
                # legacy projection from replacing newly accepted run records.
                db.execute("UPDATE sessions_v2 SET metadata_json=json_set(metadata_json,'$.runtime_contract',1) WHERE id=?",(request.session_id,))
            if context.parent_run_id:
                parent=db.execute("SELECT r.session_id,s.root_session_id FROM session_runs r JOIN sessions_v2 s ON s.id=r.session_id WHERE r.id=? AND r.tenant_id=?",(context.parent_run_id,context.tenant_id)).fetchone()
                if parent is None or parent[0]==request.session_id:
                    raise ValueError("A child requires an existing parent in the same tenant")
                if existing and existing['parent_session_id'] != parent[0]:
                    raise ValueError('An existing session cannot acquire a different parent')
                if not existing:
                    db.execute("UPDATE sessions_v2 SET parent_session_id=?,root_session_id=? WHERE id=?",(parent[0],parent[1] or parent[0],request.session_id))
            ordinal=db.execute('SELECT coalesce(max(ordinal),-1)+1 FROM session_runs WHERE session_id=?',(request.session_id,)).fetchone()[0]
            metadata={'runtime_contract':1,'request_digest':digest,'execution_context':context.snapshot(),
                      'deadline_seconds':request.deadline_seconds,'cancel_requested':bool(reservation),
                      'attachments':request.attachments,'model_ref':request.model_ref,'steer_run_id':request.steer_run_id}
            db.execute('''INSERT INTO session_runs(id,tenant_id,session_id,ordinal,idempotency_key,runner_kind,
                agent_id,status,status_raw,input_json,metadata_json,raw_envelope_json,raw_envelope_schema,created_at_ms,parent_run_id)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(request.run_id,context.tenant_id,request.session_id,ordinal,
                request.idempotency_key,'agent',context.agent_id,'queued','accepted',canonical_json(request.input),
                canonical_json(metadata),'{}',1,now,context.parent_run_id))
            if reservation:
                db.execute("UPDATE session_runs SET status='cancelled',status_raw='cancelled_before_accept',finished_at_ms=? WHERE id=?",(now,request.run_id))
            self._message(db,request.run_id,'user',request.input,context.author.key,context.author.kind,now)
            self._event(db,request.run_id,'run.accepted',{'status':'cancelled' if reservation else 'queued','request_digest':digest})
            row=db.execute('SELECT * FROM session_runs WHERE id=?',(request.run_id,)).fetchone()
            return self._record(row),not bool(reservation)

    def _message(self,db,run_id,role,text,principal,kind,now,*,tool_call_id=None,status='complete'):
        row=db.execute('SELECT tenant_id,session_id FROM session_runs WHERE id=?',(run_id,)).fetchone()
        seq=db.execute('SELECT coalesce(max(sequence),-1)+1 FROM session_messages WHERE session_id=?',(row['session_id'],)).fetchone()[0]
        ordinal=db.execute('SELECT coalesce(max(ordinal),-1)+1 FROM session_messages WHERE run_id=?',(run_id,)).fetchone()[0]
        # The physical legacy enum is narrower than PrincipalRef.kind; the
        # complete abstract author remains in the immutable envelope.
        author_kind=kind if kind in ('user','agent','system') else 'system'
        message_id = str(uuid.uuid4())
        db.execute('''INSERT INTO session_messages(id,tenant_id,session_id,run_id,sequence,ordinal,role,status,
            author_kind,author_principal_id,text,raw_envelope_json,raw_envelope_schema,created_at_ms,updated_at_ms,completed_at_ms,tool_call_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(message_id,row['tenant_id'],row['session_id'],run_id,
            seq,ordinal,role,status,author_kind,principal,text if isinstance(text,str) else canonical_json(text),
            canonical_json({'author':json.loads(principal)}),1,now,now,now if status=='complete' else None,tool_call_id))
        db.execute('UPDATE sessions_v2 SET source_version=source_version+1,updated_at_ms=?,last_activity_at_ms=? WHERE id=?', (now,now,row['session_id']))
        enqueue_source(db, 'message', message_id, now)
        enqueue_source(db, 'session', row['session_id'], now)
        # The existing typed tool resolver uses the newest visible message as
        # fallback anchor. A new reply must refresh earlier tool references.
        for tool in db.execute('SELECT id FROM tool_invocations WHERE session_run_id=?', (run_id,)).fetchall():
            db.execute('UPDATE tool_invocations SET source_version=source_version+1 WHERE id=?', (tool['id'],))
            enqueue_source(db, 'tool_invocation', tool['id'], now)
        return message_id

    def _transition(self, run_id: str, status: str, *, output: Any = None) -> RunRecord:
        with self._transaction() as db:
            row=db.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if row is None:
                raise LookupError(run_id)
            current=self._record(row)
            if current.terminal:
                return current
            if status!='running' and status not in TERMINAL_STATUSES:
                raise ValueError('Unsupported run transition')
            if status=='running' and current.status!='queued':
                raise ValueError('Run has already started')
            now=int(time.time()*1000)
            db.execute('UPDATE session_runs SET status=?,status_raw=?,output_json=?,finished_at_ms=? WHERE id=?',
                       (status,status,canonical_json(output) if output is not None else None,
                        now if status in TERMINAL_STATUSES else None,run_id))
            if status=='success' and output is not None:
                context=json.loads(row['metadata_json'])['execution_context']
                principal={'authority':context['authority']['authority'],'tenant_id':row['tenant_id'],
                           'subject_id':context['agent_id'],'kind':'agent'}
                self._message(db,run_id,'assistant',output,canonical_json(principal),'agent',now)
            self._event(db,run_id,'run.'+status,{'status':status,'output':output})
            return self._record(db.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone())

    def _request_cancel(self, run_id: str) -> RunRecord:
        with self._transaction() as db:
            row=db.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if row is None:
                raise LookupError(run_id)
            current=self._record(row)
            if current.terminal or current.cancel_requested:
                return current
            metadata=json.loads(row['metadata_json']); metadata['cancel_requested']=True
            db.execute('UPDATE session_runs SET metadata_json=? WHERE id=?',(canonical_json(metadata),run_id))
            self._event(db,run_id,'run.cancel_requested',{})
            return self._record(db.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone())

    def _reserve_cancel(self,run_id: str,context: ExecutionContext) -> RunRecord:
        """Cancel a delayed request through the existing immutable event log.

        No synthetic message, owner or session is created before acceptance.
        The first acceptance creates the run with its real author and a
        terminal cancelled state in the same transaction.
        """
        with self._transaction() as db:
            existing=db.execute('SELECT * FROM session_runs WHERE id=?',(run_id,)).fetchone()
            if existing:
                if existing['tenant_id']!=context.tenant_id or existing['session_id']!=context.session_id:
                    raise PermissionError('Run belongs to another context')
                return self._record(existing)
            reserved=db.execute("SELECT tenant_id,session_id FROM domain_events WHERE run_id=? AND event_type='run.cancel_reserved' ORDER BY sequence LIMIT 1",(run_id,)).fetchone()
            if reserved:
                if reserved['tenant_id']!=context.tenant_id or reserved['session_id']!=context.session_id:
                    raise PermissionError('Run reservation belongs to another context')
            else:
                db.execute('''INSERT INTO domain_events(event_id,tenant_id,actor_principal_type,actor_principal_id,
                    resource_type,resource_id,session_id,run_id,event_type,occurred_at_ms,schema_version,metadata_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',(str(uuid.uuid5(uuid.NAMESPACE_URL,canonical_json(['cancel',run_id]))),
                    context.tenant_id,context.initiator.kind,context.initiator.key,'run',run_id,context.session_id,
                    run_id,'run.cancel_reserved',int(time.time()*1000),1,canonical_json({'status':'cancelled','cancelled_before_accept':True})))
            return RunRecord(run_id,context.session_id,context.tenant_id,'cancelled','',cancel_requested=True)

    def _append_event(self,run_id: str,kind: str,payload: Mapping[str,Any]) -> RunEvent:
        with self._transaction() as db:
            return self._event(db,run_id,kind,payload)

    def _begin_tool(self, run_id: str, call_id: str, binding: Mapping[str, Any], arguments: Mapping[str, Any]) -> None:
        """Commit the invocation intent before allowing an external effect."""
        with self._transaction() as db:
            run = db.execute('SELECT * FROM session_runs WHERE id=?', (run_id,)).fetchone()
            if run is None or run['status'] != 'running':
                raise RuntimeError('A tool invocation requires an active run')
            if db.execute('SELECT 1 FROM tool_invocations WHERE session_run_id=? AND tool_call_id=?', (run_id,call_id)).fetchone():
                raise IdempotencyConflict('This invocation already has a durable intent; reconcile its outcome before retrying')
            session = db.execute('SELECT * FROM sessions_v2 WHERE id=?', (run['session_id'],)).fetchone()
            ordinal = db.execute("SELECT coalesce(max(ordinal),-1)+1 FROM tool_invocations WHERE root_kind = 'session' AND root_id=?", (run['session_id'],)).fetchone()[0]
            now = int(time.time()*1000)
            payload = {'call_id':call_id, 'binding':dict(binding), 'arguments':dict(arguments)}
            invocation_id = str(uuid.uuid5(uuid.NAMESPACE_URL,canonical_json([run_id,call_id])))
            db.execute('''INSERT INTO tool_invocations(id,tenant_id,owner_principal_id,visibility,
                root_kind,root_id,session_id,session_run_id,ordinal,tool_call_id,tool_server,tool_name,
                status,status_raw,args_json,raw_envelope_json,raw_envelope_schema,created_at_ms,completeness)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (invocation_id,run['tenant_id'],
                 session['owner_principal_id'],session['visibility'],'session',run['session_id'],
                 run['session_id'],run_id,ordinal,call_id,binding['source_id'],binding['name'],
                 'running','invoking',canonical_json(arguments),canonical_json(payload),1,now,'partial'))
            enqueue_source(db, 'tool_invocation', invocation_id, now)
            context=json.loads(run['metadata_json'])['execution_context']
            principal=PrincipalRef(context['authority']['authority'],run['tenant_id'],context['agent_id'],'agent')
            self._message(db,run_id,'tool','',principal.key,'agent',now,tool_call_id=call_id,status='streaming')
            self._event(db,run_id,'tool.invoking',payload)

    def _finish_tool(self, run_id: str, call_id: str, *, result: Any = None, error: Mapping[str, Any] | None = None) -> None:
        with self._transaction() as db:
            row = db.execute('SELECT * FROM tool_invocations WHERE session_run_id=? AND tool_call_id=?',(run_id,call_id)).fetchone()
            if row is None:
                raise LookupError('No durable invocation intent')
            if row['finished_at_ms'] is not None:
                raise IdempotencyConflict('The invocation already has a terminal result')
            encoded = canonical_json(result) if error is None else None
            failed = error is not None or (isinstance(result,dict) and bool(result.get('isError')))
            status = 'error' if failed else 'success'
            original=json.loads(row['raw_envelope_json'])
            payload = {'call_id':call_id,'status':status,'binding':original.get('binding',{}),'arguments':original.get('arguments',{})}
            payload['error' if error is not None else 'result'] = dict(error) if error is not None else result
            envelope = original | payload
            db.execute('''UPDATE tool_invocations SET status=?,status_raw=?,result_json=?,
                error_json=?,raw_envelope_json=?,result_sha256=?,result_size_bytes=?,
                result_complete=?,completeness=?,finished_at_ms=?,source_version=source_version+1 WHERE id=?''',
                (status,'effects_unknown' if error else status,encoded,canonical_json(error) if error else None,
                 canonical_json(envelope),hashlib.sha256(encoded.encode()).hexdigest() if encoded else None,
                 len(encoded.encode()) if encoded else None,int(error is None),'partial' if error else 'complete',
                 int(time.time()*1000),row['id']))
            enqueue_source(db, 'tool_invocation', row['id'], int(time.time()*1000))
            now=int(time.time()*1000)
            anchor=db.execute('SELECT id FROM session_messages WHERE run_id=? AND tool_call_id=? AND role=?',(run_id,call_id,'tool')).fetchone()
            if anchor is not None:
                db.execute("UPDATE session_messages SET text=?,status='complete',updated_at_ms=?,completed_at_ms=?,source_version=source_version+1 WHERE id=?",
                    (encoded if error is None else canonical_json(error),now,now,anchor['id']))
                enqueue_source(db,'message',anchor['id'],now)
            self._event(db,run_id,'tool.completed',payload)

    def _events(self,run_id: str,after: int=0) -> tuple[RunEvent,...]:
        rows=self.connection.execute('SELECT sequence,event_type,metadata_json FROM domain_events WHERE run_id=? AND sequence>? ORDER BY sequence',(run_id,after))
        return tuple(RunEvent(r[0],run_id,r[1],json.loads(r[2])) for r in rows)

    def _recover(self) -> None:
        pending=self.connection.execute("SELECT t.session_run_id,t.tool_call_id FROM tool_invocations t JOIN session_runs r ON r.id=t.session_run_id WHERE json_extract(r.metadata_json,'$.runtime_contract')=1 AND t.finished_at_ms IS NULL").fetchall()
        for row in pending:
            self._finish_tool(row[0],row[1],error={'reason':'runtime_restarted','effects':'unknown','retry_allowed':False})
        rows=self.connection.execute("SELECT id FROM session_runs WHERE json_extract(metadata_json,'$.runtime_contract')=1 AND finished_at_ms IS NULL").fetchall()
        for row in rows:
            self._transition(row[0],'interrupted',output={'reason':'runtime_restarted','effects':'unknown','retry_allowed':False})

    def _children(self,run_id: str) -> tuple[RunRecord,...]:
        rows=self.connection.execute("SELECT * FROM session_runs WHERE json_extract(metadata_json,'$.execution_context.parent_run_id')=? ORDER BY created_at_ms,id",(run_id,))
        return tuple(self._record(row) for row in rows)

    def _accepted_request(self, run_id: str) -> AcceptedRunRequest:
        row=self.connection.execute("SELECT * FROM session_runs WHERE id=? AND json_extract(metadata_json,'$.runtime_contract')=1",(run_id,)).fetchone()
        if row is None:
            raise LookupError('No durable request has been accepted for this run')
        metadata=json.loads(row['metadata_json']);context=metadata['execution_context']
        request=RunRequest(row['id'],row['session_id'],row['idempotency_key'],json.loads(row['input_json']),
            metadata.get('deadline_seconds'),tuple(metadata.get('attachments',())),
            metadata.get('model_ref'),metadata.get('steer_run_id'))
        return AcceptedRunRequest(request,PrincipalRef(**context['author']),
            PrincipalRef(**context['initiator']),PrincipalRef(**context['authority']),
            tuple(PrincipalRef(**p) for p in context['audience']),tuple(context.get('scopes',())),
            context.get('delegation_id'),context.get('ingress_id'),
            context.get('parent_run_id'),bool(context.get('deferred',False)))

    # Public asynchronous operations share one per-instance transaction lane.
    start = _serialized(_start)
    close = _serialized(_close)
    get = _serialized(_get)
    accept = _serialized(_accept)
    transition = _serialized(_transition)
    request_cancel = _serialized(_request_cancel)
    reserve_cancel = _serialized(_reserve_cancel)
    append_event = _serialized(_append_event)
    begin_tool = _serialized(_begin_tool)
    finish_tool = _serialized(_finish_tool)
    events = _serialized(_events)
    recover = _serialized(_recover)
    children = _serialized(_children)
    accepted_request = _serialized(_accepted_request)
