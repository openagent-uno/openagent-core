"""Offline adoption of one exactly attested, quarantined legacy child projection.

Only a new prepare_migration candidate is writable. This is not run admission,
identity discovery, an ownership grant, or permission to replay historical work.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Callable, Mapping

from openagent_core.contracts import PrincipalRef, canonical_json
from .migration import MANIFEST, check_storage_tenant, inspect_snapshot_identity, verify_snapshot

_LAYOUT = {'sessions_v2': ('id', 1), 'session_runs': ('id', 1),
           'session_messages': ('id', 3), 'domain_events': ('sequence', 2),
           'activity_items': ('activity_id', 1), 'search_outbox': ('seq', 5)}
_AUDIT = 'legacy_child_adoption_receipts'
_AUDIT_SQL = f'CREATE TABLE {_AUDIT}(plan_sha256 TEXT PRIMARY KEY, receipt_json TEXT NOT NULL)'


@dataclass(frozen=True)
class HostAttestation:
    """Opaque host-owned provenance, approved independently of historical work."""
    workspace_id: str
    agent_id: str
    pvc_uid: str
    fence_id: str
    db_instance_id: str
    snapshot_sha256: str
    database_sha256: str
    source_tenant: str
    target_tenant: str
    parent_context_sha256: str


@dataclass(frozen=True)
class ExactRow:
    table: str
    key: str | int
    sha256: str


@dataclass(frozen=True)
class ParentToolLink:
    parent_session_id: str
    parent_run_id: str
    child_session_id: str
    child_run_id: str
    original_child_run_id: str
    tool_call_id: str


@dataclass(frozen=True)
class LegacyChildAdoptionPlan:
    snapshot: str
    database: str
    attestation: HostAttestation
    link: ParentToolLink
    rows: tuple[ExactRow, ...]

    @property
    def sha256(self) -> str:
        return _hash(asdict(self))


def _hash(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def legacy_row_sha256(row: Mapping) -> str:
    """Version-one full SQL-row digest, including BLOB type and exact JSON text."""
    def typed(value):
        if isinstance(value, bytes): return ['blob', base64.b64encode(value).decode()]
        if value is None: return ['null', None]
        if isinstance(value, int): return ['integer', value]
        if isinstance(value, float): return ['real', value]
        if isinstance(value, str): return ['text', value]
        raise ValueError('Unsupported SQL value')
    return _hash({key: typed(value) for key, value in dict(row).items()})


def _quote(value): return '"' + value.replace('"', '""') + '"'


def _json(value):
    result = json.loads(value or '{}')
    if not isinstance(result, dict): raise ValueError('An object envelope is required')
    return result


def _schema(db):
    return tuple(tuple(row) for row in db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' "
        "AND name<>? ORDER BY type,name", (_AUDIT,)))


def _inventory(db):
    result = {}
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name<>?", (_AUDIT,)):
        result[table] = Counter(legacy_row_sha256(row) for row in db.execute('SELECT * FROM ' + _quote(table)))
    return result


def _snapshot(plan):
    a = plan.attestation
    for value in asdict(a).values():
        if not isinstance(value, str) or not value.strip(): raise ValueError('Complete host attestation is required')
    for value in (a.snapshot_sha256, a.database_sha256, a.parent_context_sha256):
        if not re.fullmatch('[0-9a-f]{64}', value): raise ValueError('Expected SHA-256 is required')
    if a.source_tenant == a.target_tenant: raise ValueError('Distinct historical and target tenants are required')
    root = Path(plan.snapshot)
    identity = inspect_snapshot_identity(root, plan.database)
    if identity != {'tenant_ids': sorted([a.source_tenant, a.target_tenant]),
                    'db_instance_id': a.db_instance_id, 'backup_sha256': a.snapshot_sha256,
                    'fence_id': a.fence_id, 'database_sha256': a.database_sha256}:
        raise PermissionError('Snapshot provenance does not match the host attestation')
    return verify_snapshot(root)


def _selected(db, plan):
    if not isinstance(plan.rows, tuple) or len(plan.rows) != 13: raise ValueError('Exactly thirteen rows are required')
    selected = {}
    for table, (pk, count) in _LAYOUT.items():
        supplied = [row for row in plan.rows if row.table == table]
        if len(supplied) != count or len({row.key for row in supplied}) != count:
            raise ValueError('The six-table cohort must be exact and distinct')
        columns = db.execute('PRAGMA table_info(' + _quote(table) + ')').fetchall()
        if [r['name'] for r in columns if r['pk']] != [pk]: raise ValueError('Unexpected cohort primary key')
        for item in supplied:
            row = db.execute('SELECT * FROM ' + _quote(table) + ' WHERE ' + _quote(pk) + '=?', (item.key,)).fetchone()
            if row is None or legacy_row_sha256(row) != item.sha256: raise ValueError('A cohort row changed')
            selected[(table, item.key)] = dict(row)
    # Unknown tenants or additional foreign rows are never silently adopted.
    actual = set()
    for (table,) in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        columns = {r['name'] for r in db.execute('PRAGMA table_info(' + _quote(table) + ')')}
        if 'tenant_id' not in columns: continue
        tenants = {r[0] for r in db.execute('SELECT DISTINCT tenant_id FROM ' + _quote(table) + ' WHERE tenant_id IS NOT NULL')}
        if tenants - {plan.attestation.source_tenant, plan.attestation.target_tenant}:
            raise PermissionError('Unexpected storage tenant')
        rows = db.execute('SELECT * FROM ' + _quote(table) + ' WHERE tenant_id=?', (plan.attestation.source_tenant,))
        for row in rows:
            if table not in _LAYOUT: raise ValueError('Foreign rows outside the attested cohort')
            actual.add((table, row[_LAYOUT[table][0]]))
    if actual != set(selected): raise ValueError('Foreign cohort does not match the thirteen selected rows')
    return selected


def _lineage(db, plan, selected):
    a, link = plan.attestation, plan.link
    parent = db.execute('SELECT * FROM sessions_v2 WHERE id=?', (link.parent_session_id,)).fetchone()
    run = db.execute('SELECT * FROM session_runs WHERE id=?', (link.parent_run_id,)).fetchone()
    if (parent is None or run is None or parent['tenant_id'] != a.target_tenant
            or run['tenant_id'] != a.target_tenant or run['session_id'] != parent['id']
            or parent['agent_id'] != a.agent_id or run['agent_id'] != a.agent_id):
        raise PermissionError('Canonical parent boundary does not match')
    metadata = _json(run['metadata_json']); context = metadata.get('execution_context')
    if (metadata.get('runtime_contract') != 1 or _json(parent['metadata_json']).get('runtime_contract') != 1
            or not isinstance(context, dict) or _hash(context) != a.parent_context_sha256):
        raise PermissionError('Canonical parent execution provenance does not match')
    authority = PrincipalRef(**context['authority'])
    if (authority.key != parent['owner_principal_id'] or authority.tenant_id != a.target_tenant
            or context.get('session_id') != parent['id'] or context.get('agent_id') != a.agent_id
            or not context.get('audience')
            or any(p.get('tenant_id') != a.target_tenant for p in context['audience'])):
        raise PermissionError('Historical parent authority, scope or audience is unproven')
    tools = _json(run['raw_envelope_json']).get('tools')
    if not isinstance(tools, list) or len(tools) > 4096: raise ValueError('Canonical parent tool envelope is required')
    matches = []
    for tool in tools:
        if not isinstance(tool, dict) or tool.get('tool_name') != 'delegate_task_to_member': continue
        if tool.get('child_session_id') == link.child_session_id or tool.get('tool_call_id') == link.tool_call_id:
            matches.append(tool)
    if len(matches) != 1: raise ValueError('The canonical parent tool linkage is ambiguous or missing')
    tool = matches[0]
    arguments = tool.get('tool_args')
    if not isinstance(arguments, dict): raise ValueError('Canonical delegation arguments are required')
    child = selected[('sessions_v2', link.child_session_id)]
    child_run = selected[('session_runs', link.child_run_id)]
    if (tool.get('tool_call_id') != link.tool_call_id or tool.get('child_session_id') != child['id']
            or tool.get('child_run_id') != link.original_child_run_id
            or arguments.get('member_id') != child['agent_id'] or child_run['agent_id'] != child['agent_id']
            or child['origin'] != 'delegation' or child['owner_principal_id'] is not None
            or child['visibility'] != 'quarantined' or child['parent_session_id'] is not None
            or child['root_session_id'] not in (None, child['id'])
            or child_run['session_id'] != child['id'] or child_run['parent_run_id'] is not None
            or child_run['delegated_parent_run_id'] is not None
            or _json(child_run['metadata_json']).get('runtime_contract') == 1
            or _json(child['metadata_json']).get('runtime_contract') == 1
            or _json(child_run['metadata_json']).get('parent_session_id') != parent['id']):
        raise ValueError('Legacy projection and parent tool evidence disagree')
    raw = _json(child_run['raw_envelope_json'])
    if raw.get('run_id', raw.get('id')) != link.original_child_run_id:
        raise ValueError('Original legacy run ID is not proven by its envelope')
    if _json(child_run['metadata_json']).get('origin') != 'delegation':
        raise ValueError('Legacy child origin is unproven')
    message_ids = {row['id'] for (table, _), row in selected.items() if table == 'session_messages'}
    for (table, _), row in selected.items():
        if table == 'session_runs' and row['session_id'] != child['id']: raise ValueError('Unrelated run')
        if table == 'session_messages' and (row['session_id'] != child['id'] or row['run_id'] != child_run['id']):
            raise ValueError('Unrelated message')
        if table in {'domain_events', 'activity_items'} and (
                row['session_id'] != child['id'] or row['resource_type'] != 'session' or row['resource_id'] != child['id']):
            raise ValueError('Unrelated projection')
        if table == 'activity_items' and (row['owner_principal_id'] is not None or row['visibility'] != 'quarantined'):
            raise ValueError('Activity quarantine must be preserved')
        if table == 'search_outbox' and not (
                (row['source_kind'] == 'session' and row['source_id'] == child['id'])
                or (row['source_kind'] == 'message' and row['source_id'] in message_ids)):
            raise ValueError('Unrelated search intent')
    return parent['root_session_id'] or parent['id']


def plan_legacy_child_adoption(snapshot, database, *, attestation, link, rows):
    """Verify a host-supplied exact cohort against a coherent read-only snapshot."""
    plan = LegacyChildAdoptionPlan(str(Path(snapshot).resolve()), database, attestation, link, tuple(rows))
    _snapshot(plan)
    with closing(sqlite3.connect((Path(plan.snapshot) / database).as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or db.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('Snapshot integrity failed')
        _lineage(db, plan, _selected(db, plan))
    return plan


@contextmanager
def _writer_lock(path):
    fd = os.open(str(path) + '.runtime.lock', os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        if os.name == 'nt':
            import msvcrt
            if os.fstat(fd).st_size == 0: os.write(fd, b'0')
            os.lseek(fd, 0, os.SEEK_SET); msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally: os.close(fd)


def _normalized_sql(sql): return re.sub(r'\s+', ' ', sql).strip().rstrip(';').lower()


def apply_legacy_child_adoption(candidate, plan, *, authorize: Callable[[HostAttestation, ParentToolLink], bool]):
    """Apply under a current host authorization and writer fence; never activate.

    ``authorize`` is trusted host code, not a JSON flag or historical principal.
    It must verify that the exact PVC/snapshot mapping and durable fence remain
    authorized. The unchanged tenant guard remains mandatory after this action.
    """
    if authorize(plan.attestation, plan.link) is not True: raise PermissionError('Current host migration authority is required')
    manifest = _snapshot(plan)
    root, source = Path(candidate).resolve(), Path(plan.snapshot).resolve()
    if root == source or root in source.parents or source in root.parents:
        raise ValueError('Only a separate new candidate directory may be adopted')
    path = root / plan.database
    if not path.is_file() or path.is_symlink() or path.stat().st_nlink != 1 or path.samefile(source / plan.database):
        raise ValueError('A separate regular candidate database is required')
    # This code and the public reader guards ship together. No host flag can
    # convert a quarantined historical row into a readable session.
    from openagent_core.workspace_administration import require_session_not_quarantined
    try: require_session_not_quarantined({'visibility': 'quarantined'})
    except PermissionError: pass
    else: raise PermissionError('Quarantine reader boundary is unavailable')
    # SQLite may create its own WAL/SHM when a WAL-mode backup is opened.
    # Verify pristine files before opening it; then recheck logical content
    # against the immutable source under the exclusive transaction.
    expected_files = set(manifest['files'])
    found = set()
    for item in root.rglob('*'):
        if item.is_symlink(): raise ValueError('Candidate contains a symbolic link')
        if item.is_file() and str(item) != str(path) + '.runtime.lock': found.add(str(item.relative_to(root)))
    pristine = found == expected_files
    if pristine:
        for name, entry in manifest['files'].items():
            value = root / name
            if value.stat().st_size != entry['bytes'] or hashlib.sha256(value.read_bytes()).hexdigest() != entry['sha256']:
                pristine = False; break
    with _writer_lock(path), closing(sqlite3.connect(path, isolation_level=None)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON'); db.execute('PRAGMA synchronous=FULL'); db.execute('BEGIN EXCLUSIVE')
        try:
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (_AUDIT,)).fetchone()
            if existing:
                if _normalized_sql(existing[0]) != _normalized_sql(_AUDIT_SQL): raise ValueError('Unexpected migration audit schema')
                saved = db.execute(f'SELECT receipt_json FROM {_AUDIT} WHERE plan_sha256=?', (plan.sha256,)).fetchone()
                if saved is None: raise ValueError('Candidate belongs to another adoption plan')
                receipt = _json(saved[0])
                if receipt['result_logical_sha256'] != _hash({t: sorted(c.items()) for t, c in _inventory(db).items()}):
                    raise ValueError('Previously adopted candidate changed')
                db.execute('ROLLBACK'); return receipt
            if not pristine:
                raise ValueError('Candidate is not the untouched prepared snapshot')
            with closing(sqlite3.connect((source / plan.database).as_uri() + '?mode=ro&immutable=1', uri=True)) as original:
                original.row_factory = sqlite3.Row
                if _schema(original) != _schema(db) or _inventory(original) != _inventory(db):
                    raise ValueError('Candidate changed before the exclusive migration transaction')
            selected = _selected(db, plan); root_id = _lineage(db, plan, selected)
            schema, expected = _schema(db), _inventory(db)
            # No outgoing/incoming reference may be dragged across namespaces.
            for table in ('sessions_v2', 'session_runs'):
                if table == 'sessions_v2':
                    refs = db.execute('SELECT id FROM sessions_v2 WHERE parent_session_id=? OR root_session_id=?',
                                      (plan.link.child_session_id, plan.link.child_session_id)).fetchall()
                    if any(row['id'] != plan.link.child_session_id for row in refs): raise ValueError('External child session reference')
                else:
                    refs = db.execute('SELECT id FROM session_runs WHERE delegated_parent_run_id=? OR parent_run_id=?',
                                      (plan.link.child_run_id, plan.link.child_run_id)).fetchall()
                    if refs: raise ValueError('External child run reference')
            triggers = []
            for table in ('domain_events', 'search_outbox'):
                name = 'trg_' + table + '_no_update'
                row = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=? AND tbl_name=?", (name, table)).fetchone()
                expected_sql = f"CREATE TRIGGER {name} BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT, '{table} rows are immutable'); END"
                if row is None or _normalized_sql(row[0]) != _normalized_sql(expected_sql):
                    raise ValueError('Canonical immutable-history trigger is required')
                triggers.append(row[0]); db.execute('DROP TRIGGER ' + _quote(name))
            db.execute('PRAGMA defer_foreign_keys=ON')
            after_rows = []
            for (table, key), before in selected.items():
                changes = {'tenant_id': plan.attestation.target_tenant}
                if table == 'sessions_v2': changes.update(parent_session_id=plan.link.parent_session_id, root_session_id=root_id)
                if table == 'session_runs': changes['delegated_parent_run_id'] = plan.link.parent_run_id
                after = {**before, **changes}
                pk = _LAYOUT[table][0]
                db.execute('UPDATE ' + _quote(table) + ' SET ' + ','.join(_quote(k) + '=?' for k in changes)
                           + ' WHERE ' + _quote(pk) + '=?', (*changes.values(), key))
                old_digest, new_digest = legacy_row_sha256(before), legacy_row_sha256(after)
                expected[table][old_digest] -= 1
                if not expected[table][old_digest]: del expected[table][old_digest]
                expected[table][new_digest] += 1
                after_rows.append({'table': table, 'key': key, 'before_sha256': old_digest, 'after_sha256': new_digest})
            for sql in triggers: db.execute(sql)
            if _schema(db) != schema or _inventory(db) != expected: raise ValueError('Migration changed unrelated schema or data')
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or db.execute('PRAGMA foreign_key_check').fetchall():
                raise ValueError('Candidate integrity failed')
            receipt = {'format': 1, 'plan_sha256': plan.sha256, 'host_attestation': asdict(plan.attestation),
                       'parent_link': asdict(plan.link), 'rows': after_rows, 'activated': False,
                       'legacy_projection_source_hash': selected[('sessions_v2', plan.link.child_session_id)]['legacy_source_hash'],
                       'root_session_id': root_id,
                       'quarantined': True, 'external_effects_replay': False,
                       'result_logical_sha256': _hash({t: sorted(c.items()) for t, c in expected.items()})}
            db.execute(_AUDIT_SQL)
            db.execute(f'INSERT INTO {_AUDIT} VALUES(?,?)', (plan.sha256, canonical_json(receipt)))
            _snapshot(plan)
            if authorize(plan.attestation, plan.link) is not True:
                raise PermissionError('Current host migration authority was revoked')
            db.execute('COMMIT')
        except BaseException:
            if db.in_transaction: db.execute('ROLLBACK')
            raise
    check_storage_tenant(path, plan.attestation.target_tenant)
    return receipt
