"""Canonical legacy engine field provenance and selective exact-revision sink.

A trusted host binds this writer through EngineContentPrivacyWriter. It consumes
framework-owned public run origins and exact provider call identities; it never
accepts origin labels from serialized model data. Historical untracked state,
extra session-state keys, provider caches and external media still require their
own qualified adapters before a full privacy request can complete.
"""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import time

from openagent_core.content_privacy import ContentRevision, PrivacyConflict
from openagent_core.memory.operational.projection import loads_maybe_double


_GENERATED_FIELDS = ('content','reasoning_content','reasoning_steps','reasoning_messages',
                     'images','videos','audio','files','response_audio','citations','references','followups')
_MESSAGE_FIELDS = ('content','compressed_content','reasoning_content','redacted_reasoning_content','tool_calls')


def _encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


def _get(root,path):
    value=root
    for component in path:
        value=value[component]
    return value


def _set(root,path,value):
    parent=_get(root,path[:-1])
    parent[path[-1]]=value


class SqliteEngineContentPrivacy:
    sink_id='engine-history'

    def __init__(self,public_store,ledger,publisher,*,export):
        if public_store.path!=ledger.path or publisher.runtime_store is not public_store:
            raise ValueError('Engine provenance must share the canonical runtime database')
        self.store,self.ledger,self.publisher,self.export=public_store,ledger,publisher,export
        public_store.connection.execute('''CREATE TABLE IF NOT EXISTS engine_privacy_nodes(
            artifact TEXT PRIMARY KEY,tenant TEXT NOT NULL,session TEXT NOT NULL,
            run_id TEXT NOT NULL,path TEXT NOT NULL)''')
        public_store.connection.execute('''CREATE TABLE IF NOT EXISTS engine_privacy_erased(
            artifact TEXT NOT NULL,revision TEXT NOT NULL,PRIMARY KEY(artifact,revision))''')

    def record_engine_session(self,transaction,session_id,source_row,context,run_id):
        main=next((row[2] for row in transaction.execute('PRAGMA database_list') if row[1]=='main'),None)
        if main is None or Path(main).resolve()!=self.store.path.resolve():
            raise PrivacyConflict('Engine writer is bound to another canonical database')
        if source_row is None or source_row.get('session_id')!=session_id:
            raise PrivacyConflict('Engine source row does not identify this exact session')
        if context is not None:
            if context.session_id!=session_id:
                raise PrivacyConflict('Engine writer cannot choose another session')
            self.publisher._guard(transaction,run_id)
        runs=loads_maybe_double(source_row.get('runs'),[])
        if not isinstance(runs,list):
            raise PrivacyConflict('Canonical engine envelope must be a run list')
        for run in runs:
            if not isinstance(run,dict):
                continue
            identity=run.get('run_id') or run.get('id')
            trusted=transaction.execute('SELECT tenant_id,session_id FROM session_runs WHERE id=?',(identity,)).fetchone()
            if trusted is None:
                continue  # No invented attribution for historical unknown runs.
            if trusted[1]!=session_id:
                raise PrivacyConflict('Engine envelope contains another session run')
            origins=self.publisher._origins(transaction,identity)
            if not origins:
                continue
            if context is not None and context.tenant_id!=trusted[0]:
                raise PrivacyConflict('Engine write crossed a tenant boundary')
            self.publisher._guard(transaction,identity)
            model_calls={}
            for row in transaction.execute('SELECT id,raw_envelope_json FROM tool_invocations WHERE session_run_id=?',(identity,)):
                binding=json.loads(row[1]).get('binding',{})
                # Only source results carrying trusted origins qualify, not any
                # model call that happens to use the same customer in its text.
                source=transaction.execute('SELECT origins FROM privacy_revisions WHERE tenant=? AND artifact=?',
                                           (trusted[0],'tool/'+row[0])).fetchone()
                if source and binding.get('provider_call_id'):
                    model_calls.setdefault(binding['provider_call_id'],set()).update(
                        self.ledger._origin(value) for value in json.loads(source[0]))
            paths=[]
            messages=run.get('messages') or []
            first_source=None
            for index,message in enumerate(messages):
                if isinstance(message,dict) and message.get('role')=='tool' and message.get('tool_call_id') in model_calls:
                    first_source=index if first_source is None else first_source
                    paths.extend((('messages',index,field),tuple(model_calls[message['tool_call_id']]))
                                 for field in _MESSAGE_FIELDS if message.get(field) is not None)
            if first_source is not None:
                for index,message in enumerate(messages):
                    if index>first_source and isinstance(message,dict) and message.get('role')=='assistant':
                        paths.extend((('messages',index,field),origins) for field in _MESSAGE_FIELDS if message.get(field) is not None)
                paths.extend(((field,),origins) for field in _GENERATED_FIELDS if run.get(field) is not None)
            for index,tool in enumerate(run.get('tools') or []):
                if isinstance(tool,dict) and tool.get('tool_call_id') in model_calls:
                    paths.extend((('tools',index,field),tuple(model_calls[tool['tool_call_id']]))
                                 for field in ('tool_args','result') if tool.get(field) is not None)
            for path,node_origins in paths:
                locator=[session_id,identity,list(path)]
                artifact=base64.urlsafe_b64encode(_encoded(locator)).decode().rstrip('=')
                digest=hashlib.sha256(_encoded(_get(run,path))).hexdigest()
                revision=ContentRevision(trusted[0],artifact,digest,self.sink_id,digest)
                self.ledger._record_transaction(transaction,revision,node_origins)
                transaction.execute('INSERT OR IGNORE INTO engine_privacy_nodes VALUES(?,?,?,?,?)',
                                    (artifact,trusted[0],session_id,identity,json.dumps(path)))

    async def apply(self,request,revision):
        from openagent_core.persistence import run_sync
        async with self.store._operation_lock:
            result=await run_sync(self._apply,request,revision)
            if request.operation=='redact' and result=='complete':
                if self.store.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]!=0:
                    return 'conflict'
        if request.operation=='export' and result!='conflict':
            await self.export(request,revision,result)
            return 'complete'
        return result

    def _apply(self,request,revision):
        if revision.sink_id!=self.sink_id or request.tenant_id!=revision.tenant_id:
            return 'conflict'
        db=self.store.connection
        db.execute('PRAGMA secure_delete=ON')
        with self.store._transaction():
            node=db.execute('SELECT * FROM engine_privacy_nodes WHERE artifact=? AND tenant=?',
                            (revision.artifact_id,revision.tenant_id)).fetchone()
            if node is None:
                return 'conflict'
            erased=db.execute('SELECT 1 FROM engine_privacy_erased WHERE artifact=? AND revision=?',
                              (revision.artifact_id,revision.revision_id)).fetchone()
            row=db.execute('SELECT runs FROM sessions WHERE session_id=?',(node['session'],)).fetchone()
            if row is None or erased:
                return b'' if request.operation=='export' else 'complete'
            runs=loads_maybe_double(row[0],[])
            run=next((run for run in runs if isinstance(run,dict) and (run.get('run_id') or run.get('id'))==node['run_id']),None)
            if run is None:
                return 'conflict'  # Compaction may hold an untracked copy.
            path=json.loads(node['path'])
            try:
                value=_get(run,path)
            except (KeyError,IndexError,TypeError):
                return 'conflict'
            encoded=_encoded(value)
            if hashlib.sha256(encoded).hexdigest()!=revision.content_sha256:
                return 'conflict'
            if request.operation=='export':
                return encoded
            _set(run,path,None)
            db.execute('UPDATE sessions SET runs=?,updated_at=? WHERE session_id=?',
                       (json.dumps(runs,ensure_ascii=False),int(time.time()),node['session']))
            # The public run's compatibility envelope is another canonical copy.
            public=db.execute('SELECT raw_envelope_json FROM session_runs WHERE id=? AND tenant_id=?',
                              (node['run_id'],revision.tenant_id)).fetchone()
            if public and public[0] and public[0]!='{}':
                envelope=json.loads(public[0])
                try:
                    copied=_get(envelope,path)
                except (KeyError,IndexError,TypeError):
                    copied=None
                if copied is not None:
                    if hashlib.sha256(_encoded(copied)).hexdigest()!=revision.content_sha256:
                        raise PrivacyConflict('Compatibility envelope changed independently')
                    _set(envelope,path,None)
                    db.execute('UPDATE session_runs SET raw_envelope_json=? WHERE id=?',
                               (json.dumps(envelope,ensure_ascii=False),node['run_id']))
            db.execute('INSERT INTO engine_privacy_erased VALUES(?,?)',(revision.artifact_id,revision.revision_id))
            return 'complete'
