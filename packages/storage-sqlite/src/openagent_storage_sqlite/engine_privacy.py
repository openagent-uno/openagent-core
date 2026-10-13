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
from .privacy_json import get as _get, replace as _replace


_GENERATED_FIELDS = ('content','reasoning_content','reasoning_steps','reasoning_messages',
                     'images','videos','audio','files','response_audio','citations','references','followups')
_MESSAGE_FIELDS = ('content','compressed_content','reasoning_content','redacted_reasoning_content','tool_calls')


def _encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


def _set(root,path,value):
    _replace(root,path,value)


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
        public_store.connection.execute('''CREATE TABLE IF NOT EXISTS engine_privacy_recaps(
            run_id TEXT PRIMARY KEY,tenant TEXT NOT NULL,session TEXT NOT NULL,origins TEXT NOT NULL)''')
        public_store.connection.execute('''CREATE TABLE IF NOT EXISTS engine_privacy_folds(
            tenant TEXT NOT NULL,session TEXT NOT NULL,source_run TEXT NOT NULL,recap TEXT NOT NULL,
            PRIMARY KEY(tenant,session,source_run,recap))''')

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
            model_partitions={}
            for row in transaction.execute('SELECT id,raw_envelope_json FROM tool_invocations WHERE session_run_id=?',(identity,)):
                binding=json.loads(row[1]).get('binding',{})
                # Only source results carrying trusted origins qualify, not any
                # model call that happens to use the same customer in its text.
                source=transaction.execute('SELECT origins FROM privacy_revisions WHERE tenant=? AND artifact=?',
                                           (trusted[0],'tool/'+row[0])).fetchone()
                if source and binding.get('provider_call_id'):
                    model_calls.setdefault(binding['provider_call_id'],set()).update(
                        self.ledger._origin(value) for value in json.loads(source[0]))
                partition=transaction.execute('SELECT partition_json FROM runtime_privacy_tool_partitions WHERE invocation=?',(row[0],)).fetchone()
                if partition and binding.get('provider_call_id'):
                    fragments=json.loads(partition[0])['fragments']
                    model_partitions[binding['provider_call_id']]=fragments
                    model_calls.setdefault(binding['provider_call_id'],set()).update(
                        self.ledger._origin(origin) for fragment in fragments for origin in fragment['origins'])
            paths=[]
            messages=run.get('messages') or []
            first_source=None
            admitted=transaction.execute('SELECT input_json,delegated_parent_run_id,metadata_json FROM session_runs WHERE id=?',(identity,)).fetchone()
            admitted_input=json.loads(admitted[0])
            agent_child=bool(admitted[1] and json.loads(admitted[2]).get('execution_context',{}).get('author',{}).get('kind')=='agent')
            # The final output belongs to this admitted run even when its only
            # protected source is inherited history/parent context.
            paths.extend(((field,),origins) for field in _GENERATED_FIELDS if run.get(field) is not None)
            if agent_child and run.get('input') is not None:
                if not isinstance(run['input'],dict) or run['input'].get('input_content')!=admitted_input:
                    raise PrivacyConflict('Child engine input does not retain the admitted agent mission')
                paths.append((('input',),origins))
            admitted_user=None
            for index,message in enumerate(messages):
                if isinstance(message,dict) and message.get('role')=='user' and message.get('content')==admitted_input:
                    admitted_user=index
            if agent_child and admitted_user is not None:
                paths.append((('messages',admitted_user,'content'),origins))
            for index,message in enumerate(messages):
                if isinstance(message,dict) and message.get('role')=='tool' and message.get('tool_call_id') in model_calls:
                    first_source=index if first_source is None else first_source
                    fragments=model_partitions.get(message['tool_call_id'])
                    for field in _MESSAGE_FIELDS:
                        if message.get(field) is None: continue
                        if fragments and field=='content':
                            paths.extend((('messages',index,field)+tuple(fragment['path']),
                                          tuple(self.ledger._origin(origin) for origin in fragment['origins']))
                                         for fragment in fragments if fragment['origins'])
                        else:
                            paths.append((('messages',index,field),tuple(model_calls[message['tool_call_id']])))
            if first_source is not None:
                for index,message in enumerate(messages):
                    if index>first_source and isinstance(message,dict) and message.get('role')=='assistant':
                        paths.extend((('messages',index,field),origins) for field in _MESSAGE_FIELDS if message.get(field) is not None)
            elif admitted_user is not None:
                for index,message in enumerate(messages):
                    if index>admitted_user and isinstance(message,dict) and message.get('role')=='assistant':
                        paths.extend((('messages',index,field),origins) for field in _MESSAGE_FIELDS if message.get(field) is not None)
            for index,tool in enumerate(run.get('tools') or []):
                if isinstance(tool,dict) and tool.get('tool_call_id') in model_calls:
                    fragments=model_partitions.get(tool['tool_call_id'])
                    for field in ('tool_args','result'):
                        if tool.get(field) is None: continue
                        if fragments and field=='result':
                            paths.extend((('tools',index,field)+tuple(fragment['path']),
                                          tuple(self.ledger._origin(origin) for origin in fragment['origins']))
                                         for fragment in fragments if fragment['origins'])
                        else:
                            paths.append((('tools',index,field),tuple(model_calls[tool['tool_call_id']])))
            for path,node_origins in paths:
                locator=[session_id,identity,list(path)]
                artifact=base64.urlsafe_b64encode(_encoded(locator)).decode().rstrip('=')
                digest=hashlib.sha256(_encoded(_get(run,path))).hexdigest()
                revision=ContentRevision(trusted[0],artifact,digest,self.sink_id,digest)
                self.ledger._record_transaction(transaction,revision,node_origins)
                transaction.execute('INSERT OR IGNORE INTO engine_privacy_nodes VALUES(?,?,?,?,?)',
                                    (artifact,trusted[0],session_id,identity,json.dumps(path)))

    def _fold_origins(self,transaction,session_id,identities):
        tenant_row=transaction.execute('SELECT tenant_id FROM sessions_v2 WHERE id=?',(session_id,)).fetchone()
        if tenant_row is None:
            raise PrivacyConflict('Compaction session has no trusted canonical identity')
        origins=set()
        for identity in identities:
            owned=transaction.execute('SELECT tenant_id,session_id FROM session_runs WHERE id=?',(identity,)).fetchone()
            if owned:
                if tuple(owned)!=(tenant_row[0],session_id):
                    raise PrivacyConflict('Compaction crossed an admitted session boundary')
                origins.update(self.publisher._origins(transaction,identity))
            else:
                recap=transaction.execute('SELECT tenant,session,origins FROM engine_privacy_recaps WHERE run_id=?',(identity,)).fetchone()
                if recap:
                    if tuple(recap[:2])!=(tenant_row[0],session_id):
                        raise PrivacyConflict('Compaction crossed a recap boundary')
                    origins.update(self.ledger._origin(value) for value in json.loads(recap[2]))
        return tenant_row[0],tuple(origins)

    def _guard_origins(self,transaction,origins):
        from openagent_core.content_privacy import PrivacyFence
        for row in transaction.execute('SELECT request FROM privacy_requests'):
            request=self.ledger._request(json.loads(row[0]))
            if request.operation=='redact' and any(request.matches(origin) for origin in origins):
                raise PrivacyFence('Revoked folded context cannot be summarized or persisted')

    def validate_compaction(self,session_id,folded_run_ids):
        with self.ledger._db() as transaction:
            _tenant,origins=self._fold_origins(transaction,session_id,folded_run_ids)
            self._guard_origins(transaction,origins)

    def record_compaction(self,transaction,session_id,runs,folded_run_ids,recap_id):
        tenant,origins=self._fold_origins(transaction,session_id,folded_run_ids)
        self._guard_origins(transaction,origins)
        if not origins:
            return
        recap=next((run for run in runs if isinstance(run,dict) and run.get('run_id')==recap_id),None)
        if recap is None or recap_id in folded_run_ids:
            raise PrivacyConflict('Compaction has no new exact recap identity')
        encoded_origins=json.dumps([dict(tenant_id=o.tenant_id,source_id=o.source_id,group_id=o.group_id,
                                         generation=o.generation,subjects=o.subjects) for o in origins])
        transaction.execute('INSERT INTO engine_privacy_recaps VALUES(?,?,?,?)',(recap_id,tenant,session_id,encoded_origins))
        parents=[]
        for identity in folded_run_ids:
            for row in transaction.execute('SELECT artifact FROM engine_privacy_nodes WHERE tenant=? AND session=? AND run_id=?',(tenant,session_id,identity)):
                latest=transaction.execute('SELECT revision FROM privacy_revisions WHERE tenant=? AND artifact=? ORDER BY rowid DESC LIMIT 1',(tenant,row[0])).fetchone()
                if latest:
                    parents.append((row[0],latest[0]))
        paths=[('content',)]
        paths.extend(('messages',index,'content') for index,value in enumerate(recap.get('messages') or [])
                     if isinstance(value,dict) and value.get('role')=='assistant' and value.get('content') is not None)
        for path in paths:
            artifact=base64.urlsafe_b64encode(_encoded([session_id,recap_id,list(path)])).decode().rstrip('=')
            digest=hashlib.sha256(_encoded(_get(recap,path))).hexdigest()
            revision=ContentRevision(tenant,artifact,digest,self.sink_id,digest)
            self.ledger._record_transaction(transaction,revision,origins,tuple(parents))
            transaction.execute('INSERT OR IGNORE INTO engine_privacy_nodes VALUES(?,?,?,?,?)',
                                (artifact,tenant,session_id,recap_id,json.dumps(path)))
        for source in folded_run_ids:
            transaction.execute('INSERT OR IGNORE INTO engine_privacy_folds VALUES(?,?,?,?)',
                                (tenant,session_id,source,recap_id))

    async def apply(self,request,revision):
        from openagent_core.persistence import run_sync
        async with self.store._operation_lock:
            result=await run_sync(self._apply,request,revision)
            if request.operation=='redact' and result=='complete':
                if not await run_sync(self._scrub):
                    return 'conflict'
        if request.operation=='export' and result!='conflict':
            await self.export(request,revision,result)
            return 'complete'
        return result

    def _scrub(self):
        return self.store.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0

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
            folded=db.execute('SELECT 1 FROM engine_privacy_folds WHERE tenant=? AND session=? AND source_run=?',
                              (revision.tenant_id,node['session'],node['run_id'])).fetchone()
            if run is None and not folded:
                return 'conflict'
            public=db.execute('SELECT raw_envelope_json FROM session_runs WHERE id=? AND tenant_id=?',
                              (node['run_id'],revision.tenant_id)).fetchone()
            envelope=json.loads(public[0]) if public and public[0] and public[0]!='{}' else None
            path=json.loads(node['path'])
            def read(value):
                try:
                    return _get(value,path) if value is not None else None
                except (KeyError,IndexError,TypeError):
                    return None
            value=read(run) if run is not None else read(envelope)
            if value is None:
                # A newer tracked revision of this exact field may already be
                # erased. Never treat a changed nonempty human value as gone.
                marker=db.execute('SELECT 1 FROM engine_privacy_erased WHERE artifact=?',(revision.artifact_id,)).fetchone()
                if marker and read(envelope) is None:
                    return b'' if request.operation=='export' else 'complete'
                return 'conflict'
            encoded=_encoded(value)
            if hashlib.sha256(encoded).hexdigest()!=revision.content_sha256:
                return 'conflict'
            if request.operation=='export':
                return encoded
            if run is not None:
                _set(run,path,None)
                db.execute('UPDATE sessions SET runs=?,updated_at=? WHERE session_id=?',
                           (json.dumps(runs,ensure_ascii=False),int(time.time()),node['session']))
            copied=read(envelope)
            if copied is not None:
                if hashlib.sha256(_encoded(copied)).hexdigest()!=revision.content_sha256:
                    raise PrivacyConflict('Compatibility envelope changed independently')
                _set(envelope,path,None)
                db.execute('UPDATE session_runs SET raw_envelope_json=? WHERE id=?',
                           (json.dumps(envelope,ensure_ascii=False),node['run_id']))
            db.execute('INSERT INTO engine_privacy_erased VALUES(?,?)',(revision.artifact_id,revision.revision_id))
            return 'complete'
