"""Exact session summary/state fields produced by a canonical engine run.

This is the engine storage owner's internal implementation. No host queries the
legacy schema or supplies origin labels. Unchanged human fields are preserved.
"""
from __future__ import annotations
import base64
import hashlib
import json
import time

from openagent_core.content_privacy import ContentRevision, PrivacyConflict, PrivacyFence
from openagent_core.memory.operational.projection import loads_maybe_double
from .privacy_json import get, replace


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()


def leaves(value,path=()):
    if isinstance(value,dict) and value:
        for key,child in value.items():
            yield from leaves(child,path+(key,))
    else:
        yield path,value


def read(root,path):
    try:return get(root,path)
    except (KeyError,IndexError,TypeError):return None


def numeric(value):
    if isinstance(value,dict):return all(numeric(child) for child in value.values())
    if isinstance(value,list):return all(numeric(child) for child in value)
    return value is None or type(value) in (bool,int,float)


def technical_metrics(value):
    from dataclasses import fields
    from openagent_core.core.metrics import SessionMetrics, ModelMetrics
    if not isinstance(value,dict) or set(value)-{field.name for field in fields(SessionMetrics)}:
        return False
    if not numeric({key:child for key,child in value.items() if key!='details'}):return False
    details=value.get('details') or {}
    if not isinstance(details,dict):return False
    allowed={field.name for field in fields(ModelMetrics)}
    for kind,rows in details.items():
        if not isinstance(kind,str) or len(kind)>64 or not isinstance(rows,list):return False
        for row in rows:
            if not isinstance(row,dict) or set(row)-allowed:return False
            for key,child in row.items():
                # These canonical fields identify the configured provider/model,
                # not tool-result labels. Other provider metrics stay numeric.
                if key in {'id','provider'}:
                    if child is not None and (not isinstance(child,str) or len(child)>1024):return False
                elif not numeric(child):return False
    return True


class EngineSessionState:
    def __init__(self,owner):
        self.owner=owner
        owner.store.connection.execute('''CREATE TABLE IF NOT EXISTS engine_privacy_state_nodes(
            artifact TEXT PRIMARY KEY,tenant TEXT NOT NULL,session TEXT NOT NULL,
            column_name TEXT NOT NULL,path TEXT NOT NULL)''')

    def begin(self,db,session_id,incoming,context,run_id):
        if context is None or run_id is None:return
        if context.session_id!=session_id:
            raise PrivacyConflict('Session state writer crossed an admitted session boundary')
        self.owner.publisher._guard(db,run_id)
        origins=self.owner.publisher._origins(db,run_id)
        if not origins:return
        old=db.execute('SELECT summary,session_data FROM sessions WHERE session_id=?',(session_id,)).fetchone()
        old_summary=loads_maybe_double(old[0],None) if old else None
        old_data=loads_maybe_double(old[1],{}) if old else {}
        new_summary=loads_maybe_double(incoming.get('summary'),None)
        new_data=loads_maybe_double(incoming.get('session_data'),{})
        if new_data is None:new_data={}
        if old_data is None:old_data={}
        if not isinstance(new_data,dict) or not isinstance(old_data,dict):
            raise PrivacyFence('Protected engine session data requires its canonical object schema')
        changes=[]
        if new_summary is not None and new_summary!=old_summary:
            changes.append(('summary',(),new_summary))
        for key,value in new_data.items():
            if value==old_data.get(key):continue
            if key=='session_state':
                if not isinstance(value,dict):raise PrivacyFence('Protected session state must be an object')
                if old is None and value:
                    raise PrivacyFence('First protected session state lacks a trusted persisted baseline')
                prior=old_data.get(key) or {}
                for path,child in leaves(value):
                    if child is not None and child!=read(prior,path):
                        changes.append(('session_data',(key,)+path,child))
            elif key=='session_name':
                if value is not None:changes.append(('session_data',(key,),value))
            elif key=='session_metrics' and technical_metrics(value):
                continue
            else:
                raise PrivacyFence('Protected engine session field lacks an attested writer')
        for column,path,value in changes:
            artifact='engine-state/'+base64.urlsafe_b64encode(encoded([session_id,column,list(path)])).decode().rstrip('=')
            digest=hashlib.sha256(encoded(value)).hexdigest()
            revision=ContentRevision(context.tenant_id,artifact,digest,self.owner.sink_id,digest)
            self.owner.ledger._record_transaction(db,revision,origins)
            db.execute('INSERT OR IGNORE INTO engine_privacy_state_nodes VALUES(?,?,?,?,?)',
                       (artifact,context.tenant_id,session_id,column,json.dumps(path)))

    def run_paths(self,db,tenant,session_id,run):
        state=run.get('session_state')
        if state is None:return ()
        if not isinstance(state,dict):raise PrivacyFence('Protected run session state must be an object')
        row=db.execute('SELECT session_data FROM sessions WHERE session_id=?',(session_id,)).fetchone()
        current=loads_maybe_double(row[0],{}) if row else {}
        persisted=current.get('session_state') or {}
        if state!=persisted:
            raise PrivacyFence('Run session state does not retain the canonical state baseline')
        paths=[]
        for node in db.execute("SELECT artifact,path FROM engine_privacy_state_nodes WHERE tenant=? AND session=? AND column_name='session_data'",(tenant,session_id)):
            path=tuple(json.loads(node[1]))
            if path[:1]!=('session_state',):continue
            value=read(current,path)
            if value is None:continue
            digest=hashlib.sha256(encoded(value)).hexdigest()
            origin=db.execute('SELECT origins FROM privacy_revisions WHERE tenant=? AND artifact=? AND revision=?',
                              (tenant,node[0],digest)).fetchone()
            if origin:
                paths.append((path,tuple(self.owner.ledger._origin(value) for value in json.loads(origin[0]))))
        return tuple(paths)

    def apply(self,db,request,revision):
        node=db.execute('SELECT * FROM engine_privacy_state_nodes WHERE tenant=? AND artifact=?',
                        (revision.tenant_id,revision.artifact_id)).fetchone()
        if node is None:return 'conflict'
        column=node['column_name']
        if column not in {'summary','session_data'}:raise PrivacyConflict('Invalid canonical state selector')
        row=db.execute(f'SELECT {column} FROM sessions WHERE session_id=?',(node['session'],)).fetchone()
        erased=db.execute('SELECT 1 FROM engine_privacy_erased WHERE artifact=? AND revision=?',
                          (revision.artifact_id,revision.revision_id)).fetchone()
        if row is None or erased:return b'' if request.operation=='export' else 'complete'
        root=loads_maybe_double(row[0],None)
        path=tuple(json.loads(node['path']))
        value=read(root,path)
        if value is None:
            marker=db.execute('SELECT 1 FROM engine_privacy_erased WHERE artifact=?',(revision.artifact_id,)).fetchone()
            return (b'' if request.operation=='export' else 'complete') if marker else 'conflict'
        content=encoded(value)
        if hashlib.sha256(content).hexdigest()!=revision.content_sha256:return 'conflict'
        if request.operation=='export':return content
        if path:replace(root,path,None)
        else:root=None
        db.execute(f'UPDATE sessions SET {column}=?,updated_at=? WHERE session_id=?',
                   (json.dumps(root,ensure_ascii=False) if root is not None else None,int(time.time()),node['session']))
        # This canonical v2 header is also an exact content copy. Projection
        # owns its storage representation; the sink updates only the selected
        # field and preserves ACL, identity and every unrelated header value.
        header=db.execute('SELECT metadata_json,title FROM sessions_v2 WHERE id=? AND tenant_id=?',
                          (node['session'],revision.tenant_id)).fetchone()
        if header:
            metadata=json.loads(header[0]);legacy=metadata.get('legacy_header',{})
            copied=loads_maybe_double(legacy.get(column),None)
            copied_value=read(copied,path)
            if copied_value is not None:
                if hashlib.sha256(encoded(copied_value)).hexdigest()!=revision.content_sha256:
                    raise PrivacyConflict('Canonical session header changed independently')
                if path:replace(copied,path,None)
                else:copied=None
                legacy[column]=(json.dumps(copied,ensure_ascii=False) if isinstance(legacy.get(column),str)
                                and copied is not None else copied)
                db.execute('UPDATE sessions_v2 SET metadata_json=? WHERE id=?',
                           (json.dumps(metadata,ensure_ascii=False),node['session']))
            if column=='session_data' and path==('session_name',) and not metadata.get('legacy',{}).get('title'):
                if header[1]==str(value)[:1024]:
                    db.execute('UPDATE sessions_v2 SET title=? WHERE id=?',(node['session'],node['session']))
            now=int(time.time()*1000)
            db.execute('UPDATE sessions_v2 SET source_version=source_version+1,updated_at_ms=? WHERE id=?',(now,node['session']))
            from .search import enqueue_source
            enqueue_source(db,'session',node['session'],now)
        db.execute('INSERT OR IGNORE INTO engine_privacy_erased VALUES(?,?)',(revision.artifact_id,revision.revision_id))
        return 'complete'
