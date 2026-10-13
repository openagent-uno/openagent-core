import contextlib
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import aiosqlite

from openagent_core.content_privacy import ContentOrigin,ContentRevision,PrivacyFence
from openagent_core.memory.operational.schema import _verified_backup
from openagent_core.memory.operational.privacy_snapshots import checked_snapshot_source
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
from openagent_storage_sqlite.migration import prepare_migration,restore_to_new_directory,MANIFEST


class SnapshotPrivacy(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_memorydb_boot_completes_checked_backup_and_first_ddl(self):
        from openagent_core.engine import MemoryDB
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);path=root/'fresh.sqlite'
            memory=MemoryDB(str(path))
            try:
                await memory.connect()
                with sqlite3.connect(path) as database:
                    self.assertEqual(database.execute('PRAGMA integrity_check').fetchone()[0],'ok')
                    self.assertIsNotNone(database.execute("SELECT 1 FROM sqlite_master WHERE name='operational_storage_state'").fetchone())
                snapshots=list((root/'backups').glob('*.db'))
                self.assertEqual(len(snapshots),1)
                with sqlite3.connect(snapshots[0]) as snapshot:
                    self.assertEqual(snapshot.execute('PRAGMA integrity_check').fetchone()[0],'ok')
            finally:
                await memory.close()

    async def test_preddl_backup_refuses_protected_cohort_before_creating_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);path=root/'state.sqlite'
            ledger=SqliteContentPrivacyStore(path)
            await ledger.record(ContentRevision('tenant','source','1','sink',hashlib.sha256(b'private').hexdigest()),
                                origins=(ContentOrigin('tenant','shopify/app','shop',1,nonpersonal=True),))
            before=path.read_bytes()
            async with aiosqlite.connect(path) as source:
                with self.assertRaises(PrivacyFence):
                    await _verified_backup(source,path,app_version='fixture')
            self.assertFalse((root/'backups').exists())
            self.assertEqual(before,path.read_bytes())

    async def test_checked_backup_pins_old_read_snapshot_against_later_protected_writer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);path=root/'state.sqlite'
            ledger=SqliteContentPrivacyStore(path)
            with sqlite3.connect(path) as writer:
                writer.execute('PRAGMA journal_mode=WAL')
                writer.execute('CREATE TABLE canary(value TEXT)');writer.execute("INSERT INTO canary VALUES('human before')")
            async with checked_snapshot_source(path) as snapshot:
                await ledger.record(ContentRevision('tenant','source','1','sink',hashlib.sha256(b'private').hexdigest()),
                                    origins=(ContentOrigin('tenant','shopify/app','shop',1,('customer:23',)),))
                with sqlite3.connect(path) as writer:writer.execute("UPDATE canary SET value='private after admission'")
                with sqlite3.connect(root/'copy.sqlite') as destination:await snapshot.backup(destination)
            with sqlite3.connect(root/'copy.sqlite') as copy:
                self.assertEqual(copy.execute('SELECT value FROM canary').fetchone()[0],'human before')
                self.assertEqual(copy.execute('SELECT count(*) FROM privacy_revisions').fetchone()[0],0)
            async with aiosqlite.connect(path) as source:
                with self.assertRaises(PrivacyFence):await _verified_backup(source,path,app_version='fixture')

    async def test_migration_and_restore_refuse_unmanaged_protected_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'source';source.mkdir();path=source/'state.sqlite'
            ledger=SqliteContentPrivacyStore(path)
            @contextlib.asynccontextmanager
            async def quiesce():yield 'host-fence'
            called=[]
            async def migrate(_):called.append('migration')
            await prepare_migration(source,root/'old-backup',root/'old-candidate',databases=('state.sqlite',),quiesce=quiesce,migrate=migrate)
            origin=ContentOrigin('tenant','shopify/app','shop',1,('customer:23',))
            revision=ContentRevision('tenant','source','1','sink',hashlib.sha256(b'private').hexdigest())
            await ledger.record(revision,origins=(origin,))
            called.clear()
            with self.assertRaises(PrivacyFence):
                await prepare_migration(source,root/'backup',root/'candidate',databases=('state.sqlite',),quiesce=quiesce,migrate=migrate)
            self.assertFalse((root/'backup').exists());self.assertFalse((root/'candidate').exists());self.assertEqual(called,[])
            # Simulate a checksummed older protected snapshot, created before this guard.
            old=root/'old-backup';old_ledger=SqliteContentPrivacyStore(old/'state.sqlite')
            await old_ledger.record(revision,origins=(origin,))
            manifest=json.loads((old/MANIFEST).read_text());entry=manifest['files']['state.sqlite']
            entry.update(bytes=(old/'state.sqlite').stat().st_size,sha256=hashlib.sha256((old/'state.sqlite').read_bytes()).hexdigest())
            (old/MANIFEST).write_text(json.dumps(manifest))
            with self.assertRaises(PrivacyFence):restore_to_new_directory(old,root/'restored')
            self.assertFalse((root/'restored').exists())
