"""Offline namespace adoption of one empty, quarantined legacy placeholder.

This four-row contract is separate from verified delegation adoption. A legacy
parent hint never supplies a run link, historical authority, owner or grant.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import sqlite3
from typing import Callable

from openagent_core.contracts import PrincipalRef, canonical_json
from openagent_core.memory.operational.projection import build_session_projection
from openagent_core.memory.operational.repository import _fetch_legacy
from .migration import check_storage_tenant, inspect_snapshot_identity, verify_snapshot, _digest
from .legacy_child_adoption import (
    ExactRow, legacy_row_sha256, _AUDIT, _AUDIT_SQL, _hash, _quote, _json,
    _schema, _inventory, _writer_lock, _normalized_sql,
)

_LAYOUT = {'sessions_v2': ('id', 1), 'activity_items': ('activity_id', 1),
           'domain_events': ('sequence', 1), 'search_outbox': ('seq', 1)}


@dataclass(frozen=True)
class EmptyChildHostAttestation:
    """Host authorization binds storage and an exact native parent row.

    The parent row is contextual evidence only; it does not prove delegation.
    ``storage_uid`` is host-defined and has no Kubernetes dependency.
    """
    workspace_id: str
    agent_id: str
    storage_uid: str
    fence_id: str
    db_instance_id: str
    snapshot_sha256: str
    database_sha256: str
    source_tenant: str
    target_tenant: str
    parent_session_id: str
    parent_row_sha256: str


@dataclass(frozen=True)
class EmptyChildAdoptionPlan:
    snapshot: str
    database: str
    attestation: EmptyChildHostAttestation
    child_session_id: str
    rows: tuple[ExactRow, ...]

    @property
    def sha256(self) -> str:
        return _hash(asdict(self))


def _snapshot(plan):
    a = plan.attestation
    if not isinstance(a, EmptyChildHostAttestation): raise TypeError('Empty-placeholder attestation is required')
    if any(not isinstance(v, str) or not v.strip() for v in asdict(a).values()):
        raise ValueError('Complete host attestation is required')
    if any(not re.fullmatch('[0-9a-f]{64}', v) for v in
           (a.snapshot_sha256, a.database_sha256, a.parent_row_sha256)):
        raise ValueError('Exact snapshot, database and parent-row hashes are required')
    if a.source_tenant != 'installation:' + a.db_instance_id or a.source_tenant == a.target_tenant:
        raise PermissionError('Only the attested historical installation namespace may be adopted')
    if not isinstance(plan.child_session_id, str) or not plan.child_session_id or plan.child_session_id == a.parent_session_id:
        raise ValueError('A distinct exact child ID is required')
    identity = inspect_snapshot_identity(plan.snapshot, plan.database)
    if identity != {'tenant_ids': sorted([a.source_tenant, a.target_tenant]),
                    'db_instance_id': a.db_instance_id, 'backup_sha256': a.snapshot_sha256,
                    'fence_id': a.fence_id, 'database_sha256': a.database_sha256}:
        raise PermissionError('Snapshot provenance differs from host attestation')
    return verify_snapshot(plan.snapshot)


def _selected(db, plan):
    if not isinstance(plan.rows, tuple) or len(plan.rows) != 4:
        raise ValueError('Exactly four placeholder rows are required')
    selected = {}
    for table, (pk, count) in _LAYOUT.items():
        supplied = [item for item in plan.rows if item.table == table]
        if len(supplied) != count: raise ValueError('Exactly one row per placeholder table is required')
        columns = db.execute('PRAGMA table_info(' + _quote(table) + ')').fetchall()
        if [row['name'] for row in columns if row['pk']] != [pk]: raise ValueError('Unexpected primary key')
        item = supplied[0]
        row = db.execute('SELECT * FROM ' + _quote(table) + ' WHERE ' + _quote(pk) + '=?', (item.key,)).fetchone()
        if row is None or legacy_row_sha256(row) != item.sha256: raise ValueError('Placeholder row changed')
        selected[(table, item.key)] = dict(row)
    actual = set()
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        columns = {row['name'] for row in db.execute('PRAGMA table_info(' + _quote(table) + ')')}
        if 'tenant_id' not in columns: continue
        tenants = {r[0] for r in db.execute('SELECT DISTINCT tenant_id FROM ' + _quote(table) + ' WHERE tenant_id IS NOT NULL')}
        if tenants - {plan.attestation.source_tenant, plan.attestation.target_tenant}:
            raise PermissionError('Unexpected storage tenant')
        for row in db.execute('SELECT * FROM ' + _quote(table) + ' WHERE tenant_id=?', (plan.attestation.source_tenant,)):
            if table not in _LAYOUT: raise ValueError('Additional foreign data is not an empty placeholder')
            actual.add((table, row[_LAYOUT[table][0]]))
    if actual != set(selected): raise ValueError('Foreign rows do not match the exact four-row placeholder')
    return selected


def _no_other_references(db, child_id, selected):
    """Reject typed child links across all tenants, including non-FK projections."""
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        columns = {r['name'] for r in db.execute('PRAGMA table_info(' + _quote(table) + ')')}
        clauses, params = [], []
        for column in ('session_id', 'child_session_id'):
            if column in columns: clauses.append(_quote(column) + '=?'); params.append(child_id)
        for id_column, type_column in (('resource_id','resource_type'), ('root_id','root_kind'),
                                       ('parent_id','parent_type'), ('source_id','source_kind')):
            if {id_column, type_column} <= columns:
                clauses.append('(' + _quote(id_column) + '=? AND ' + _quote(type_column) + "='session')")
                params.append(child_id)
        if table == 'sessions_v2':
            clauses.extend(('parent_session_id=?', 'root_session_id=?')); params.extend((child_id,child_id))
        if not clauses: continue
        for row in db.execute('SELECT * FROM ' + _quote(table) + ' WHERE ' + ' OR '.join(clauses), params):
            # The compatibility source and its reconciliation checkpoint are
            # retained, never promoted into canonical read access.
            if table in {'sessions', 'legacy_session_changes'} and row['session_id'] == child_id: continue
            key = row[_LAYOUT[table][0]] if table in _LAYOUT else None
            if (table, key) not in selected: raise ValueError('Empty placeholder has additional data or incoming references')
    # Incoming legacy hints are not verified lineage either. Refuse to drag a
    # second historical placeholder into this bounded operation.
    columns = {r['name'] for r in db.execute('PRAGMA table_info(sessions)')}
    if {'session_id','metadata'} <= columns:
        for row in db.execute('SELECT session_id,metadata FROM sessions WHERE session_id<>?', (child_id,)):
            if _json(row['metadata']).get('parent_session_id') == child_id:
                raise ValueError('Another legacy source claims this placeholder as parent')


def _validate_empty(db, plan, selected):
    a, child_id = plan.attestation, plan.child_session_id
    parent = db.execute('SELECT * FROM sessions_v2 WHERE id=?', (a.parent_session_id,)).fetchone()
    if (parent is None or legacy_row_sha256(parent) != a.parent_row_sha256
            or parent['tenant_id'] != a.target_tenant or parent['agent_id'] != a.agent_id
            or _json(parent['metadata_json']).get('runtime_contract') != 1
            or parent['visibility'] == 'quarantined' or parent['deleted_at_ms'] is not None
            or not parent['owner_principal_id']):
        raise PermissionError('Attested native parent row does not match')
    owner = PrincipalRef(**_json(parent['owner_principal_id']))
    if owner.key != parent['owner_principal_id'] or owner.tenant_id != a.target_tenant:
        raise PermissionError('Native parent owner is not a canonical same-tenant principal')
    child = selected.get(('sessions_v2', child_id))
    if (child is None or child['tenant_id'] != a.source_tenant or child['owner_principal_id'] is not None
            or child['visibility'] != 'quarantined' or child['parent_session_id'] is not None
            or child['root_session_id'] != child_id or child['deleted_at_ms'] is not None
            or child['origin'] != 'delegation' or _json(child['metadata_json']).get('runtime_contract') == 1):
        raise ValueError('Only an unresolved quarantined placeholder qualifies')
    legacy = _fetch_legacy(db, child_id)
    if legacy is None: raise ValueError('The exact legacy source is required')
    runs = legacy.get('runs')
    if isinstance(runs,str): runs=json.loads(runs)
    if runs not in (None, []): raise ValueError('The exact legacy source must have zero runs')
    metadata = legacy.get('metadata') or {}
    if isinstance(metadata,str): metadata=_json(metadata)
    if not isinstance(metadata,dict) or metadata.get('parent_session_id') != a.parent_session_id:
        raise ValueError('Expected legacy claimed-parent hint is missing')
    projection = build_session_projection(legacy, tenant_id=a.source_tenant, now_ms=child['updated_at_ms'])
    if (projection.runs or projection.messages or projection.tools or projection.malformed_error
            or projection.source_hash != child['legacy_source_hash']):
        raise ValueError('Legacy source is not the exact empty projection')
    for (table, _), row in selected.items():
        if table == 'sessions_v2': continue
        if table == 'search_outbox':
            if row['source_kind'] != 'session' or row['source_id'] != child_id or row['operation'] != 'upsert':
                raise ValueError('Search intent is not the exact empty session')
        elif row['session_id'] != child_id or row['resource_type'] != 'session' or row['resource_id'] != child_id:
            raise ValueError('Projection does not belong to this empty session')
        if table == 'activity_items' and (row['visibility'] != 'quarantined' or row['owner_principal_id'] is not None
                                           or row['parent_id'] is not None):
            raise ValueError('Activity ownership and unresolved ancestry must be preserved')
        if table == 'domain_events' and (row['run_id'] is not None or row['tool_invocation_id'] is not None
                                         or row['event_type'] != 'session.projected'):
            raise ValueError('Placeholder event must contain no run or tool linkage')
    _no_other_references(db, child_id, selected)
    return projection.source_hash


def plan_legacy_empty_child_adoption(snapshot, database, *, attestation, child_session_id, rows):
    """Read-only plan; the host separately authorizes storage namespace repair."""
    plan = EmptyChildAdoptionPlan(str(Path(snapshot).resolve()), database, attestation, child_session_id, tuple(rows))
    _snapshot(plan)
    with closing(sqlite3.connect((Path(plan.snapshot)/database).as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or db.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('Snapshot integrity failed')
        _validate_empty(db,plan,_selected(db,plan))
    return plan


def apply_legacy_empty_child_adoption(candidate, plan, *, authorize: Callable[[EmptyChildHostAttestation,str],bool]):
    """Change four tenant cells only, without assigning lineage or activating."""
    if authorize(plan.attestation,plan.child_session_id) is not True:
        raise PermissionError('Current host authority is required')
    manifest = _snapshot(plan)
    root, source = Path(candidate).resolve(), Path(plan.snapshot).resolve()
    if root == source or root in source.parents or source in root.parents:
        raise ValueError('Candidate must be a separate new prepared directory')
    path = root/plan.database
    if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1 or path.samefile(source/plan.database):
        raise ValueError('A separate regular candidate database is required')
    expected_files, found = set(manifest['files']), set()
    for item in root.rglob('*'):
        if item.is_symlink(): raise ValueError('Candidate contains a symbolic link')
        if item.is_file() and str(item) != str(path)+'.runtime.lock': found.add(str(item.relative_to(root)))
    pristine = found == expected_files
    if pristine:
        for name,entry in manifest['files'].items():
            value = root/name
            if value.stat().st_size != entry['bytes'] or _digest(value) != entry['sha256']:
                pristine=False;break
    with _writer_lock(path), closing(sqlite3.connect(path,isolation_level=None)) as db:
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN EXCLUSIVE')
        try:
            existing=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(_AUDIT,)).fetchone()
            if existing:
                if _normalized_sql(existing[0]) != _normalized_sql(_AUDIT_SQL):raise ValueError('Unexpected audit schema')
                saved=db.execute(f'SELECT receipt_json FROM {_AUDIT} WHERE plan_sha256=?',(plan.sha256,)).fetchone()
                if saved is None:raise ValueError('Candidate belongs to a different adoption plan')
                receipt=_json(saved[0])
                if (receipt.get('mode') != 'empty_placeholder' or receipt.get('parent_link_verified') is not False
                        or receipt.get('result_logical_sha256') != _hash({t:sorted(c.items()) for t,c in _inventory(db).items()})):
                    raise ValueError('Previously adopted empty candidate changed')
                db.execute('ROLLBACK');return receipt
            if not pristine:raise ValueError('Candidate is not the untouched prepared snapshot')
            # Revalidate accompanying files under the writer lock too. SQLite
            # alone may have created its own sidecars since pristine preflight;
            # those are not arbitrary additional product files.
            internal = {str(path)+suffix for suffix in ('.runtime.lock','-wal','-shm','-journal')}
            locked_files=set()
            for item in root.rglob('*'):
                if item.is_symlink():raise ValueError('Candidate acquired a symbolic link')
                if item.is_file() and str(item) not in internal:locked_files.add(str(item.relative_to(root)))
            if locked_files != expected_files:raise ValueError('Candidate files changed during preflight')
            for name,entry in manifest['files'].items():
                value=root/name
                if value.stat().st_size != entry['bytes'] or _digest(value) != entry['sha256']:
                    raise ValueError('Prepared candidate file changed during preflight')
            with closing(sqlite3.connect((source/plan.database).as_uri()+'?mode=ro&immutable=1',uri=True)) as original:
                original.row_factory=sqlite3.Row
                if _schema(original) != _schema(db) or _inventory(original) != _inventory(db):
                    raise ValueError('Candidate changed before exclusive migration')
            selected=_selected(db,plan);legacy_hash=_validate_empty(db,plan,selected)
            schema,expected=_schema(db),_inventory(db)
            triggers=[]
            for table in ('domain_events','search_outbox'):
                name='trg_'+table+'_no_update'
                row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=? AND tbl_name=?",(name,table)).fetchone()
                expected_sql=f"CREATE TRIGGER {name} BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT, '{table} rows are immutable'); END"
                if row is None or _normalized_sql(row[0]) != _normalized_sql(expected_sql):
                    raise ValueError('Canonical immutable-history trigger is required')
                triggers.append(row[0]);db.execute('DROP TRIGGER '+_quote(name))
            after_rows=[]
            for (table,key),before in selected.items():
                after={**before,'tenant_id':plan.attestation.target_tenant};pk=_LAYOUT[table][0]
                db.execute('UPDATE '+_quote(table)+' SET tenant_id=? WHERE '+_quote(pk)+'=?',(plan.attestation.target_tenant,key))
                old_digest,new_digest=legacy_row_sha256(before),legacy_row_sha256(after)
                expected[table][old_digest]-=1
                if not expected[table][old_digest]:del expected[table][old_digest]
                expected[table][new_digest]+=1
                after_rows.append({'table':table,'key':key,'before_sha256':old_digest,'after_sha256':new_digest})
            for sql in triggers:db.execute(sql)
            if _schema(db) != schema or _inventory(db) != expected:
                raise ValueError('Migration changed data outside four tenant cells')
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or db.execute('PRAGMA foreign_key_check').fetchall():
                raise ValueError('Candidate integrity failed')
            receipt={'format':1,'mode':'empty_placeholder','plan_sha256':plan.sha256,
                     'host_attestation':asdict(plan.attestation),
                     'parent_link':{'child_session_id':plan.child_session_id,'parent_session_id':None},
                     'claimed_parent_session_id':plan.attestation.parent_session_id,'parent_link_verified':False,
                     'root_session_id':plan.child_session_id,'rows':after_rows,
                     'legacy_projection_source_hash':legacy_hash,'activated':False,'quarantined':True,
                     'external_effects_replay':False,
                     'result_logical_sha256':_hash({t:sorted(c.items()) for t,c in expected.items()})}
            db.execute(_AUDIT_SQL);db.execute(f'INSERT INTO {_AUDIT} VALUES(?,?)',(plan.sha256,canonical_json(receipt)))
            _snapshot(plan)
            if authorize(plan.attestation,plan.child_session_id) is not True:
                raise PermissionError('Current host migration authority was revoked')
            db.execute('COMMIT')
        except BaseException:
            if db.in_transaction:db.execute('ROLLBACK')
            raise
    check_storage_tenant(path,plan.attestation.target_tenant)
    return receipt
