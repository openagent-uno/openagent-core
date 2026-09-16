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
from typing import Any

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


def _snapshot(source: Path, destination: Path, databases: tuple[str,...], fence_id: str) -> dict[str,Any]:
    _empty_destination(destination)
    database_paths={_relative(p) for p in databases}
    if not database_paths:raise ValueError('At least one canonical database must be declared')
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
                original.backup(backup)
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
    (destination/MANIFEST).write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    os.chmod(destination/MANIFEST,0o600)
    return manifest


def verify_snapshot(snapshot: str|Path) -> dict[str,Any]:
    root=Path(snapshot).resolve()
    if not root.is_dir():raise FileNotFoundError(root)
    manifest=json.loads((root/MANIFEST).read_text())
    if manifest.get('format')!=1 or manifest.get('external_effects_replay') is not False:
        raise ValueError('Unsupported state manifest')
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


def restore_to_new_directory(snapshot: str|Path,destination: str|Path) -> dict[str,Any]:
    """Restore to a new writer location; never overwrite an active deployment."""
    root=Path(snapshot).resolve();target=Path(destination).resolve()
    manifest=verify_snapshot(root)
    _empty_destination(target)
    for name,entry in manifest['files'].items():
        output=target/_relative(name);output.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        shutil.copyfile(root/name,output)
        os.chmod(output,entry['mode']&0o777)
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
        tables=[row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            quoted='"'+table.replace('"','""')+'"'
            if 'tenant_id' not in {row[1] for row in db.execute('PRAGMA table_info('+quoted+')')}:continue
            tenants={str(row[0]) for row in db.execute('SELECT DISTINCT tenant_id FROM '+quoted+' WHERE tenant_id IS NOT NULL') if row[0]}
            if tenants-{expected_tenant}:
                raise PermissionError('Canonical storage tenant requires an attested host identity mapping')


async def prepare_migration(source: str|Path,backup: str|Path,candidate: str|Path,*,databases: tuple[str,...],quiesce,migrate):
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
        manifest=_snapshot(source,backup,databases,fence_id)
        verify_snapshot(backup)
        restore_to_new_directory(backup,candidate)
        result=await migrate(candidate)
        verify_snapshot(backup)
        return {'fence_id':fence_id,'backup':str(backup),'candidate':str(candidate),
                'source_files':len(manifest['files']),'migration':result,'activated':False}
