"""Atomic canonical MemoryDB journal provenance and exact fragment privacy sink."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import time

from openagent_core.content_privacy import ContentFragment,ContentRevision,ToolResultProvenance,PrivacyFence,PrivacyConflict
from openagent_core.persistence import run_sync
from .privacy_json import get,replace,encode_path


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


class SqliteSessionJournalContentPrivacy:
    """Bind as SessionJournalPrivacyWriter and as a canonical privacy sink.

    Only typed generated journal kinds have a default whole-payload origin.
    User/raw tool/command payloads require the host's trusted schema resolver.
    A model-supplied label in data never changes this classification.
    """
    sink_id='session-journal'
    _GENERATED=frozenset({'assistant/message','turn/end','error','compaction'})

    def __init__(self,runtime_store,ledger,publisher,*,export,provenance=None):
        if runtime_store.path!=ledger.path or publisher.runtime_store is not runtime_store:
            raise ValueError('Journal privacy must share the canonical transaction database')
        self.store,self.ledger,self.publisher=runtime_store,ledger,publisher
        self.export,self.provenance=export,provenance
        runtime_store.connection.execute('''CREATE TABLE IF NOT EXISTS journal_privacy_nodes(
            artifact TEXT PRIMARY KEY,tenant TEXT NOT NULL,session TEXT NOT NULL,
            seq INTEGER NOT NULL,path TEXT NOT NULL)''')
        runtime_store.connection.execute('''CREATE TABLE IF NOT EXISTS journal_privacy_erased(
            artifact TEXT NOT NULL,revision TEXT NOT NULL,PRIMARY KEY(artifact,revision))''')

    async def append_session_event(self,database_path,session_id,event_type,data,context,run_id):
        if Path(database_path).resolve()!=self.store.path.resolve() or session_id!=context.session_id:
            raise PrivacyConflict('Journal writer cannot choose a different canonical database/session')
        origins=await self.publisher.origins_for_execution(context,run_id)
        payload=json.loads(_json(data))
        if self.provenance is not None:
            plan=await self.provenance(event_type,payload,context,run_id)
        elif origins and event_type not in self._GENERATED:
            raise PrivacyFence('Raw or human journal payload requires a trusted schema partition')
        else:
            plan=ToolResultProvenance((ContentFragment((),origins),))
        if not isinstance(plan,ToolResultProvenance):
            raise PrivacyConflict('Journal schema resolver must return a typed partition')
        plan.validate_result(payload,context.tenant_id)
        async with self.store._operation_lock:
            return await run_sync(self._append,session_id,event_type,payload,context,run_id,plan)

    def _append(self,session_id,event_type,payload,context,run_id,plan):
        with self.store._transaction() as db:
            self.publisher._guard(db,run_id)
            session=db.execute('SELECT tenant_id FROM sessions_v2 WHERE id=?',(session_id,)).fetchone()
            if session is None or session[0]!=context.tenant_id:
                raise PrivacyConflict('Journal lacks the admitted tenant/session identity')
            row=db.execute('''INSERT INTO session_events(session_id,seq,ts_ms,type,data)
                VALUES(?,coalesce((SELECT max(seq) FROM session_events WHERE session_id=?),0)+1,?,?,?) RETURNING seq''',
                (session_id,session_id,int(time.time()*1000),event_type,_json(payload))).fetchone()
            seq=row[0]
            for fragment in plan.fragments:
                if not fragment.origins: continue
                path=encode_path(fragment.path)
                artifact='journal/'+base64.urlsafe_b64encode(_json([session_id,seq,list(path)]).encode()).decode().rstrip('=')
                digest=hashlib.sha256(_json(get(payload,fragment.path)).encode()).hexdigest()
                self.ledger._record_transaction(db,ContentRevision(context.tenant_id,artifact,digest,self.sink_id,digest),fragment.origins)
                db.execute('INSERT INTO journal_privacy_nodes VALUES(?,?,?,?,?)',(artifact,context.tenant_id,session_id,seq,_json(path)))
            return seq

    async def apply(self,request,revision):
        async with self.store._operation_lock:
            result=await run_sync(self._apply,request,revision)
            if request.operation=='redact' and result=='complete':
                if not await run_sync(lambda:self.store.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]==0):
                    return 'conflict'
        if request.operation=='export' and result!='conflict':
            await self.export(request,revision,result)
            return 'complete'
        return result

    def _apply(self,request,revision):
        if revision.sink_id!=self.sink_id or request.tenant_id!=revision.tenant_id:
            return 'conflict'
        self.store.connection.execute('PRAGMA secure_delete=ON')
        with self.store._transaction() as db:
            node=db.execute('SELECT * FROM journal_privacy_nodes WHERE artifact=? AND tenant=?',(revision.artifact_id,revision.tenant_id)).fetchone()
            if node is None: return 'conflict'
            marker=db.execute('SELECT 1 FROM journal_privacy_erased WHERE artifact=? AND revision=?',(revision.artifact_id,revision.revision_id)).fetchone()
            row=db.execute('SELECT data FROM session_events WHERE session_id=? AND seq=?',(node['session'],node['seq'])).fetchone()
            if marker or row is None:
                return b'' if request.operation=='export' else 'complete'
            try:
                payload=json.loads(row[0]);path=json.loads(node['path']);content=_json(get(payload,path)).encode()
            except (ValueError,KeyError,IndexError,TypeError):
                return 'conflict'
            if hashlib.sha256(content).hexdigest()!=revision.content_sha256:
                return 'conflict'
            if request.operation=='export': return content
            db.execute('UPDATE session_events SET data=? WHERE session_id=? AND seq=?',
                       (_json(replace(payload,path,None)),node['session'],node['seq']))
            db.execute('INSERT INTO journal_privacy_erased VALUES(?,?)',(revision.artifact_id,revision.revision_id))
            return 'complete'
