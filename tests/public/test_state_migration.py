import contextlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from openagent_storage_sqlite.migration import prepare_migration,restore_to_new_directory,verify_snapshot


class StateMigration(unittest.IsolatedAsyncioTestCase):
    async def test_fenced_wal_snapshot_migration_and_restore_preserve_data_and_keys(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'source';source.mkdir()
            writer=sqlite3.connect(source/'state.sqlite3')
            writer.execute('PRAGMA journal_mode=WAL');writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('CREATE TABLE definitions(id TEXT PRIMARY KEY,author TEXT,digest TEXT,timezone TEXT)')
            writer.execute('INSERT INTO definitions VALUES(?,?,?,?)',('unchanged-id','unknown-historical','digest-v1','Europe/Rome'));writer.commit()
            self.assertTrue(Path(str(source/'state.sqlite3')+'-wal').exists())
            (source/'identity.key').write_bytes(b'fixture-secret-must-not-appear-in-manifest')
            (source/'vault').mkdir();(source/'vault'/'note.md').write_text('Persistent [[linked-note]]')
            fenced=[]
            @contextlib.asynccontextmanager
            async def quiesce():
                fenced.append(True)
                try:yield 'host-fence-123'
                finally:fenced.append(False)
            async def migrate(candidate):
                self.assertEqual(fenced,[True])
                with sqlite3.connect(candidate/'state.sqlite3') as connection:
                    connection.execute('ALTER TABLE definitions ADD COLUMN delegation_id TEXT')
                return {'schema':'new','grant_status':'requires_explicit_authorization'}
            try:
                result=await prepare_migration(source,root/'backup',root/'candidate',databases=('state.sqlite3',),quiesce=quiesce,migrate=migrate)
                self.assertFalse(result['activated']);self.assertEqual(fenced,[True,False])
                manifest=verify_snapshot(root/'backup')
                self.assertNotIn('fixture-secret',str(manifest))
                self.assertEqual((root/'candidate'/'identity.key').read_bytes(),(source/'identity.key').read_bytes())
                with sqlite3.connect(root/'candidate'/'state.sqlite3') as candidate:
                    row=candidate.execute('SELECT id,author,digest,timezone,delegation_id FROM definitions').fetchone()
                    self.assertEqual(row,('unchanged-id','unknown-historical','digest-v1','Europe/Rome',None))
                    candidate.execute("UPDATE definitions SET digest='new-writes'")
                # Rollback is a coherent restored location, never restarting
                # old software against the candidate's changed files.
                restore_to_new_directory(root/'backup',root/'restored')
                with sqlite3.connect(root/'restored'/'state.sqlite3') as restored:
                    self.assertEqual(restored.execute('SELECT digest FROM definitions').fetchone()[0],'digest-v1')
                with self.assertRaises(FileExistsError):restore_to_new_directory(root/'backup',root/'candidate')
                self.assertEqual(writer.execute('SELECT digest FROM definitions').fetchone()[0],'digest-v1')
                (root/'backup'/'vault'/'note.md').write_text('tampered')
                with self.assertRaises(ValueError):verify_snapshot(root/'backup')
            finally:writer.close()

    async def test_snapshot_identity_preserves_historical_tenant_and_author(self):
        from openagent_storage_sqlite.migration import inspect_snapshot_identity
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'source';source.mkdir()
            with sqlite3.connect(source/'state.sqlite3') as db:
                db.execute('CREATE TABLE sessions_v2(id TEXT,tenant_id TEXT,owner_principal_id TEXT)')
                db.execute('INSERT INTO sessions_v2 VALUES(?,?,?)',('session','legacy-network','unresolved-author'))
            @contextlib.asynccontextmanager
            async def quiesce():yield 'verified-pvc-fence'
            async def migrate(_):return None
            await prepare_migration(source,root/'backup',root/'candidate',databases=('state.sqlite3',),quiesce=quiesce,migrate=migrate)
            before=(root/'backup'/'state.sqlite3').read_bytes()
            identity=inspect_snapshot_identity(root/'backup','state.sqlite3')
            from openagent_storage_sqlite.migration import check_storage_tenant
            check_storage_tenant(root/'candidate'/'state.sqlite3','legacy-network')
            with self.assertRaises(PermissionError):check_storage_tenant(root/'candidate'/'state.sqlite3','current-workspace')
            self.assertEqual(identity['tenant_ids'],['legacy-network'])
            self.assertEqual(identity['fence_id'],'verified-pvc-fence')
            self.assertEqual((root/'backup'/'state.sqlite3').read_bytes(),before)
            with sqlite3.connect(root/'candidate'/'state.sqlite3') as db:
                self.assertEqual(db.execute('SELECT * FROM sessions_v2').fetchone(),('session','legacy-network','unresolved-author'))
            with self.assertRaises(ValueError):inspect_snapshot_identity(root/'backup','../state.sqlite3')

    async def test_no_fence_means_no_backup_or_migration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'source').mkdir()
            @contextlib.asynccontextmanager
            async def quiesce():yield None
            async def migrate(_):raise AssertionError('must not run')
            with self.assertRaises(PermissionError):
                await prepare_migration(root/'source',root/'backup',root/'candidate',databases=('state.sqlite3',),quiesce=quiesce,migrate=migrate)
            self.assertFalse((root/'backup').exists());self.assertFalse((root/'candidate').exists())


if __name__=='__main__':unittest.main()
