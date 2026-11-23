"""Prepare and verify a data migration while the product fences all writers.

The product owns quiescing ingress, agents, automation and external effects.
This module never stops processes, switches deployments or replays work.
"""
from __future__ import annotations
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
from typing import Any
from openagent_core.memory.operational.privacy_snapshots import guard_snapshot_transaction

MANIFEST = '.openagent-state-manifest.json'


def _relative(value: str) -> Path:
    path=Path(value)
    if path.is_absolute() or not path.parts or '..' in path.parts:
        raise ValueError('State paths must be relative and remain in the state root')
    return path


def _digest(path: Path) -> str:
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1<<20),b''):h.update(chunk)
    return h.hexdigest()


def _empty_destination(path: Path) -> None:
    if path.exists():
        raise FileExistsError('Migration destinations must be new directories')
    path.mkdir(parents=True,mode=0o700)


def _require_absent_vault(root: Path, declared: str) -> str:
    relative = _relative(declared)
    if relative.parts[0] == MANIFEST:
        raise ValueError("Vault path collides with the snapshot manifest")
    current = root
    for part in relative.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return str(relative)
        if current == root / relative or not stat.S_ISDIR(mode):
            raise ValueError("History-only migration requires an absent vault at the declared path")
    raise ValueError("Invalid vault path")


def _tenant_selection(expected_tenant=None, expected_tenants=None):
    if expected_tenants is None:
        if not isinstance(expected_tenant, str) or not expected_tenant.strip():
            raise ValueError("A host-attested tenant is required")
        return expected_tenant
    if (expected_tenant is not None or not isinstance(expected_tenants, (list, tuple))
            or len(expected_tenants) < 2 or any(not isinstance(t, str) or not t.strip() for t in expected_tenants)
            or len(set(expected_tenants)) != len(expected_tenants)):
        raise ValueError("Expected distinct host-attested tenants")
    return tuple(sorted(expected_tenants))


def _check_exact_tenants(db, expected):
    found = set()
    for (name,) in db.execute("SELECT name FROM sqlite_schema WHERE type='table'"):
        quoted = '"' + name.replace('"', '""') + '"'
        if any(col[1] == "tenant_id" for col in db.execute("PRAGMA table_info(" + quoted + ")")):
            for (tenant,) in db.execute("SELECT DISTINCT tenant_id FROM " + quoted):
                if not isinstance(tenant, str) or not tenant.strip():
                    raise PermissionError("Unbound tenant in a declared storage domain")
                found.add(tenant)
    if found != set(expected):
        raise PermissionError("Canonical tenant inventory differs from the exact host attestation")


def _check_snapshot_attestations(root: Path, manifest: dict) -> None:
    if manifest.get("format") == 1 and "vault" not in manifest and "tenant_domains" not in manifest:
        return
    if (manifest.get("format") != 2 or not isinstance(manifest.get("fence_id"), str)
            or not manifest["fence_id"].strip() or not ("vault" in manifest or "tenant_domains" in manifest)):
        raise ValueError("Invalid sealed snapshot attestation")
    if "vault" in manifest:
        receipt = manifest["vault"]
        if (not isinstance(receipt, dict) or set(receipt) != {"status", "path"}
                or receipt["status"] != "absent" or not isinstance(receipt["path"], str)
                or _require_absent_vault(root, receipt["path"]) != receipt["path"]):
            raise ValueError("Invalid history-only vault receipt")
    if "tenant_domains" in manifest:
        domains = manifest["tenant_domains"]
        if not isinstance(domains, dict) or not domains or not set(domains).issubset(manifest["databases"]):
            raise ValueError("Invalid canonical database tenant attestation")
        for tenants in domains.values():
            if list(_tenant_selection(expected_tenants=tenants)) != tenants:
                raise ValueError("Noncanonical tenant attestation")


def _snapshot(source: Path, destination: Path, databases: tuple[str,...], fence_id: str,
              absent_vault: str | None = None, expected_tenants_by_database=None) -> dict[str,Any]:
    database_paths={_relative(p) for p in databases}
    domains = {}
    if expected_tenants_by_database is not None:
        if not isinstance(expected_tenants_by_database, dict) or not expected_tenants_by_database:
            raise ValueError("Expected a database to tenant inventory mapping")
        for name, tenants in expected_tenants_by_database.items():
            if name not in databases or str(_relative(name)) != name:
                raise ValueError("Tenant attestation requires an exact declared database")
            domains[name] = list(_tenant_selection(expected_tenants=tenants))
    if not database_paths:raise ValueError('At least one canonical database must be declared')
    # The product's quiesce receipt is not permission to create untracked copies
    # of a protected cohort. Reject before copying any accompanying state files.
    for relative in database_paths:
        with closing(sqlite3.connect((source/relative).as_uri()+'?mode=ro',uri=True)) as original:
            original.execute('BEGIN')
            guard_snapshot_transaction(original)
    if absent_vault is not None:
        absent_vault = _require_absent_vault(source, absent_vault)
    _empty_destination(destination)
    excluded={Path(str(p)+suffix) for p in database_paths for suffix in ('-wal','-shm','-journal','.runtime.lock')}
    entries={}
    for path in sorted(source.rglob('*')):
        relative=path.relative_to(source)
        if path.is_symlink():raise ValueError('Product must explicitly map symlinked state: '+str(relative))
        if not path.is_file() or relative in excluded or relative==Path(MANIFEST):continue
        target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        mode=path.stat().st_mode&0o777
        if relative in database_paths:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as original,closing(sqlite3.connect(target)) as backup:
                original.execute('BEGIN')
                guard_snapshot_transaction(original)
                original.backup(backup)
                if str(relative) in domains:
                    _check_exact_tenants(backup, domains[str(relative)])
                if backup.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                    raise ValueError('SQLite backup integrity verification failed')
            kind='sqlite'
        else:
            with path.open('rb') as stream:
                if stream.read(16)==b'SQLite format 3\x00':
                    raise ValueError('Declare every canonical SQLite database: '+str(relative))
            before=_digest(path);shutil.copyfile(path,target)
            if _digest(target)!=before or _digest(path)!=before:
                raise RuntimeError('State changed while the product claimed it was quiesced')
            kind='file'
        os.chmod(target,0o600)
        entries[str(relative)]={'sha256':_digest(target),'bytes':target.stat().st_size,'mode':mode,'kind':kind}
    if not {str(p) for p in database_paths}<=entries.keys():
        raise FileNotFoundError('A declared canonical database is missing')
    manifest={'format':1,'fence_id':fence_id,'databases':list(databases),'files':entries,
              'external_effects_replay':False,'sqlite_wal':'Included by SQLite backup API; WAL files must not be overlaid'}
    if absent_vault is not None:
        manifest.update(format=2, vault={"status": "absent", "path": absent_vault})
    if domains:
        manifest.update(format=2, tenant_domains=domains)
    _check_snapshot_attestations(source, manifest)
    (destination/MANIFEST).write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    os.chmod(destination/MANIFEST,0o600)
    return manifest


def verify_snapshot(snapshot: str|Path) -> dict[str,Any]:
    root=Path(snapshot).resolve()
    if not root.is_dir():raise FileNotFoundError(root)
    manifest=json.loads((root/MANIFEST).read_text())
    if manifest.get('format') not in (1, 2) or manifest.get('external_effects_replay') is not False:
        raise ValueError('Unsupported state manifest')
    _check_snapshot_attestations(root, manifest)
    actual=set()
    for path in root.rglob('*'):
        if path.is_symlink():raise ValueError('Snapshot contains symbolic links')
        if path.is_file() and path.relative_to(root)!=Path(MANIFEST):actual.add(str(path.relative_to(root)))
    if actual!=set(manifest['files']):raise ValueError('Snapshot files do not match manifest')
    for name,expected in manifest['files'].items():
        path=root/_relative(name)
        if path.stat().st_size!=expected['bytes'] or _digest(path)!=expected['sha256']:
            raise ValueError('Snapshot checksum mismatch: '+name)
    return manifest


def verify_migration_restore(snapshot: str | Path, destination: str | Path, *,
                             expected_manifest_sha256: str) -> dict[str, Any]:
    """Compare an offline rollback tree with the host's independently retained receipt."""
    root, target = Path(snapshot).resolve(), Path(destination).resolve()
    if not target.is_dir() or root == target or root in target.parents or target in root.parents:
        raise ValueError("Restore verification requires a separate offline directory")
    if _digest(root / MANIFEST) != expected_manifest_sha256:
        raise ValueError("Source manifest differs from the host receipt")
    manifest = verify_snapshot(root)
    actual = set()
    for path in target.rglob("*"):
        if path.is_symlink():
            raise ValueError("Restored tree contains symbolic links")
        if path.is_dir():
            continue
        name = str(path.relative_to(target))
        entry = manifest["files"].get(name)
        if (not path.is_file() or entry is None or path.stat().st_size != entry["bytes"]
                or stat.S_IMODE(path.stat().st_mode) != entry["mode"] or _digest(path) != entry["sha256"]):
            raise ValueError("Restored files differ from the host receipt")
        actual.add(name)
    if actual != set(manifest["files"]):
        raise ValueError("Restored files differ from the host receipt")
    _check_snapshot_attestations(target, manifest)
    if _digest(root / MANIFEST) != expected_manifest_sha256:
        raise ValueError("Source manifest changed during restore verification")
    return {"manifest_sha256": expected_manifest_sha256, "verified_files": len(actual),
            "vault": manifest.get("vault"), "activated": False}


def restore_to_new_directory(snapshot: str|Path,destination: str|Path, *,
                             expected_manifest_sha256: str | None = None) -> dict[str,Any]:
    """Restore to a new writer location; never overwrite an active deployment."""
    root=Path(snapshot).resolve();target=Path(destination).resolve()
    if target == root or root in target.parents or target in root.parents:
        raise ValueError("Restore requires a separate directory tree")
    receipt = _digest(root / MANIFEST)
    if expected_manifest_sha256 is not None and expected_manifest_sha256 != receipt:
        raise ValueError("Source manifest differs from the host receipt")
    manifest=verify_snapshot(root)
    for name in manifest['databases']:
        with closing(sqlite3.connect((root/_relative(name)).as_uri()+'?mode=ro&immutable=1',uri=True)) as source:
            source.execute('BEGIN')
            guard_snapshot_transaction(source)
    _empty_destination(target)
    for name,entry in manifest['files'].items():
        output=target/_relative(name);output.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        shutil.copyfile(root/name,output)
        os.chmod(output,entry['mode']&0o777)
    if manifest["format"] == 2 or expected_manifest_sha256 is not None:
        verify_migration_restore(root, target, expected_manifest_sha256=receipt)
    return manifest


def inspect_snapshot_identity(snapshot: str|Path, database: str) -> dict[str,Any]:
    """Inspect canonical tenant IDs without rewriting historical ownership.

    The product attests which workspace/agent/PVC supplied this verified
    backup. These opaque storage tenants are not inferred from a client body
    and are not replaced with whichever user currently opens the product.
    """
    root=Path(snapshot).resolve()
    manifest=verify_snapshot(root)
    relative=str(_relative(database))
    if relative not in manifest['databases'] or manifest['files'][relative]['kind']!='sqlite':
        raise ValueError('Identity inspection requires a declared canonical database')
    tenants=set();instance=None
    with closing(sqlite3.connect((root/relative).as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        tables=[row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            quoted='"'+table.replace('"','""')+'"'
            columns={row[1] for row in db.execute('PRAGMA table_info('+quoted+')')}
            if 'tenant_id' in columns:
                tenants.update(str(row[0]) for row in db.execute('SELECT DISTINCT tenant_id FROM '+quoted+' WHERE tenant_id IS NOT NULL') if row[0])
        if 'operational_storage_state' in tables:
            row=db.execute('SELECT db_instance_id FROM operational_storage_state WHERE singleton_id=1').fetchone()
            instance=str(row[0]) if row is not None else None
        # An empty installation can still have an established canonical
        # namespace that will be reused by its legacy projection adapter.
        if not tenants:
            if 'network' in tables:
                row=db.execute("SELECT network_id FROM network WHERE network_id IS NOT NULL AND network_id<>'' LIMIT 1").fetchone()
                if row:tenants.add(str(row[0]))
            if not tenants and instance:tenants.add('installation:'+instance)
    verify_snapshot(root)
    return {'tenant_ids':sorted(tenants),'db_instance_id':instance,
            'backup_sha256':_digest(root/MANIFEST),'fence_id':manifest['fence_id'],
            'database_sha256':manifest['files'][relative]['sha256']}


def check_storage_tenant(database: str|Path, expected_tenant: str) -> None:
    """Fail closed when an existing canonical store belongs to another tenant.

    A fresh empty store may use the host's selected namespace. Existing opaque
    tenant IDs require an attested host mapping, never a silent new namespace.
    This reads the WAL through SQLite and never rewrites ownership.
    """
    path=Path(database).absolute()
    if not isinstance(expected_tenant,str) or not expected_tenant.strip():raise ValueError('Expected tenant is required')
    if not path.exists():return
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        db.execute('BEGIN')
        _check_transaction_tenant(db, expected_tenant)


def _check_transaction_tenant(db, expected_tenant: str) -> None:
    tables=[row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    foreign=False
    for table in tables:
        quoted='"'+table.replace('"','""')+'"'
        if 'tenant_id' not in {row[1] for row in db.execute('PRAGMA table_info('+quoted+')')}:continue
        tenants={str(row[0]) for row in db.execute('SELECT DISTINCT tenant_id FROM '+quoted+' WHERE tenant_id IS NOT NULL') if row[0]}
        if tenants-{expected_tenant}:
            foreign=True
    if foreign and not _verified_legacy_projection_domain(db, tables, expected_tenant):
        raise PermissionError('Canonical storage tenant requires an attested host identity mapping')


def _verified_legacy_projection_domain(db, tables, expected_tenant):
    """Recognize exact compatibility projections beside an admitted runtime.

    A local installation namespace is not an additional runtime tenant grant.
    Its session, run, message and tool rows must still match their retained
    legacy source byte-for-byte, and its derived rows must reference those
    verified resources. Unknown tables and unrelated rows remain fail closed.
    """
    from openagent_core.memory.operational.repository import verify_session_projection

    required={'sessions','sessions_v2','session_runs','session_messages',
              'tool_invocations','operational_storage_state'}
    if not required.issubset(tables):return False
    row=db.execute('SELECT db_instance_id FROM operational_storage_state WHERE singleton_id=1').fetchone()
    if row is None or not isinstance(row[0],str) or not row[0]:return False
    legacy='installation:'+row[0]
    if 'network' in tables:
        network=db.execute('SELECT network_id FROM network LIMIT 1').fetchone()
        if network and isinstance(network[0],str) and network[0].strip():legacy=network[0].strip()
    if legacy==expected_tenant:return False
    # An existing legacy-only store still requires its normal host attestation.
    # A row in an arbitrary auxiliary table cannot establish a runtime tenant.
    if not db.execute("SELECT 1 FROM session_runs WHERE tenant_id=? "
        "AND json_extract(metadata_json,'$.runtime_contract')=1 "
        "AND json_extract(metadata_json,'$.execution_context.authority.tenant_id')=? LIMIT 1",
        (expected_tenant,expected_tenant)).fetchone():return False
    sessions=set()
    for identity,metadata in db.execute('SELECT id,metadata_json FROM sessions_v2 WHERE tenant_id=?',(legacy,)):
        if json.loads(metadata).get('runtime_contract')==1:return False
        verified=verify_session_projection(db,identity,include_deleted=True)
        if not verified.matches:return False
        sessions.add(identity)
    if not sessions:return False
    resources={'session':sessions,'message':set(),'tool_invocation':set(),'run':set()}
    for table,kind in [('session_runs','run'),('session_messages','message'),('tool_invocations','tool_invocation')]:
        for identity,session in db.execute('SELECT id,session_id FROM '+table+' WHERE tenant_id=?',(legacy,)):
            if session not in sessions:return False
            resources[kind].add(identity)
    # Verification above compares the complete nested row sets, including
    # canonical metadata and contents. No unbacked contract=0 row is admitted.
    rules={
        'sessions_v2':('id',lambda row:row[0] in sessions),
        'session_runs':('id',lambda row:row[0] in resources['run']),
        'session_messages':('id',lambda row:row[0] in resources['message']),
        'tool_invocations':('id',lambda row:row[0] in resources['tool_invocation']),
        'domain_events':('resource_type,resource_id,session_id,event_type',
            lambda row:row[0]=='session' and row[1] in sessions and row[2]==row[1]
                and row[3] in {'session.projected','session.deleted'}),
        'activity_items':('kind,resource_type,resource_id,session_id',
            lambda row:row[0] in {'chat','delegated_session'} and row[1]=='session'
                and row[2] in sessions and row[3]==row[2]),
        'search_outbox':('source_kind,source_id',
            lambda row:row[0] in resources and row[0]!='run' and row[1] in resources[row[0]]),
    }
    for table in tables:
        quoted='"'+table.replace('"','""')+'"'
        if 'tenant_id' not in {row[1] for row in db.execute('PRAGMA table_info('+quoted+')')}:continue
        tenants={str(row[0]) for row in db.execute('SELECT DISTINCT tenant_id FROM '+quoted+' WHERE tenant_id IS NOT NULL') if row[0]}
        if not tenants-{expected_tenant}:continue
        if tenants-{expected_tenant,legacy} or table not in rules:return False
        columns,valid=rules[table]
        if any(not valid(row) for row in db.execute('SELECT '+columns+' FROM '+quoted+' WHERE tenant_id=?',(legacy,))):return False
    return True


async def prepare_migration(source: str|Path,backup: str|Path,candidate: str|Path,*,databases: tuple[str,...],quiesce,migrate,
                            absent_vault: str | None = None, expected_tenants_by_database=None):
    """Fence via a trusted product context manager, back up, then migrate a copy.

    `quiesce()` yields a durable host fence ID only after all old writers and
    ingress are stopped. It must remain held through backup and migration.
    `migrate(candidate)` is the host's selected module/schema migration. The
    product reconciles deliveries and explicitly activates one candidate writer
    afterwards. A successful preparation never activates it automatically.
    """
    source=Path(source).resolve();backup=Path(backup).resolve();candidate=Path(candidate).resolve()
    if not source.is_dir():raise NotADirectoryError(source)
    paths=(source,backup,candidate)
    if len(set(paths))!=3 or any(a in b.parents for a in paths for b in paths if a!=b):
        raise ValueError('Source, backup and candidate must be separate directory trees')
    async with quiesce() as fence_id:
        if not isinstance(fence_id,str) or not fence_id.strip():
            raise PermissionError('The product must provide a durable quiescence fence ID')
        manifest=_snapshot(source,backup,databases,fence_id,absent_vault,expected_tenants_by_database)
        receipt = _digest(backup / MANIFEST)
        verify_snapshot(backup)
        restore_to_new_directory(backup,candidate)
        result=await migrate(candidate)
        verify_snapshot(backup)
        if _digest(backup / MANIFEST) != receipt:
            raise ValueError("Source manifest changed during migration")
        _check_snapshot_attestations(source, manifest)
        return {'manifest_sha256': receipt, 'fence_id':fence_id,'backup':str(backup),'candidate':str(candidate),
                'source_files':len(manifest['files']),'migration':result,'activated':False}
