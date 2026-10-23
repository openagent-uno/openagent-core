"""A legacy projection and an admitted runtime may share one physical store."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from openagent_core import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.engine import MemoryDB
from openagent_core.memory.store.sqlite.sqlite import SqliteDb
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.core._run_state.agent import RunOutput, RunInput
from openagent_core.core._run_state.base import RunStatus
from openagent_core.models.providers.message import Message
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import check_storage_tenant
from openagent_core.memory.operational.repository import load_v2_legacy_session, verify_session_projection


class StorageTenantDomains(unittest.IsolatedAsyncioTestCase):
    async def populated(self, root, *, canonical=True):
        path = root / 'state.sqlite3'
        check_storage_tenant(path, 'workspace')
        memory = MemoryDB(str(path))
        await memory.connect()
        store = SqliteRuntimeStore(path)
        await store.start()
        try:
            check_storage_tenant(path, 'workspace')
            if canonical:
                actor = PrincipalRef('host', 'workspace', 'alice', 'user')
                context = ExecutionContext(actor, actor, actor, 'admitted', 'agent', (actor,))
                await store.accept(RunRequest('admitted-run', 'admitted', 'first', 'Synthetic input'), context)
            provider = SqliteDb(db_file=str(path), session_table='sessions')
            try:
                run = RunOutput(run_id='legacy-run', agent_id='agent', session_id='legacy',
                    input=RunInput(input_content='Synthetic legacy question'), content='Synthetic answer',
                    messages=[Message(role='user', content='Synthetic legacy question'),
                              Message(role='assistant', content='Synthetic answer')], status=RunStatus.completed)
                self.assertIsNotNone(provider.upsert_session(AgentSession(session_id='legacy', agent_id='agent',
                    user_id='original-author', runs=[run], created_at=1, updated_at=2)))
            finally:
                provider.close()
            await memory.upsert_session('legacy', title='Synthetic legacy session')
        finally:
            await store.close()
            await memory.close()
        return path

    async def test_real_public_legacy_and_canonical_writes_can_restart_without_rewriting_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                tenants = {row[0] for row in db.execute('SELECT DISTINCT tenant_id FROM sessions_v2')}
                instance = db.execute('SELECT db_instance_id FROM operational_storage_state').fetchone()[0]
                self.assertEqual(tenants, {'workspace', 'installation:' + instance})
                self.assertEqual(db.execute("SELECT COUNT(*) FROM session_runs WHERE tenant_id='workspace' AND json_extract(metadata_json,'$.runtime_contract')=1").fetchone()[0], 1)
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            check_storage_tenant(path, 'workspace')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'other-workspace')

    async def test_installation_shaped_canonical_runtime_is_not_a_legacy_projection(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                tenant = 'installation:' + db.execute('SELECT db_instance_id FROM operational_storage_state').fetchone()[0]
            store = SqliteRuntimeStore(path)
            await store.start()
            try:
                actor = PrincipalRef('other-host', tenant, 'mallory', 'user')
                context = ExecutionContext(actor, actor, actor, 'foreign', 'agent', (actor,))
                await store.accept(RunRequest('foreign-run', 'foreign', 'foreign', 'Other canonical tenant'), context)
            finally:
                await store.close()
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_unbacked_contract_zero_projection_is_denied(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                db.execute("DELETE FROM sessions WHERE session_id='legacy'")
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_changed_projected_message_is_denied(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                db.execute("UPDATE session_messages SET text='Unbacked content' WHERE session_id='legacy'")
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_unknown_foreign_table_is_denied_even_for_the_local_installation_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                tenant = 'installation:' + db.execute('SELECT db_instance_id FROM operational_storage_state').fetchone()[0]
                db.execute('CREATE TABLE unrelated_records(id TEXT,tenant_id TEXT)')
                db.execute('INSERT INTO unrelated_records VALUES(?,?)', ('foreign', tenant))
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_unrelated_derived_row_is_denied(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                tenant = 'installation:' + db.execute('SELECT db_instance_id FROM operational_storage_state').fetchone()[0]
                db.execute("INSERT INTO search_outbox(tenant_id,source_kind,source_id,operation,source_version,acl_version,committed_at_ms) VALUES(?,?,?,'upsert',1,1,1)",
                           (tenant, 'session', 'not-the-verified-legacy-session'))
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_changed_database_identity_is_denied(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary))
            with sqlite3.connect(path) as db:
                db.execute("UPDATE operational_storage_state SET db_instance_id='another-database' WHERE singleton_id=1")
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_legacy_only_store_still_requires_its_existing_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.populated(Path(temporary), canonical=False)
            with sqlite3.connect(path) as db:
                tenant = 'installation:' + db.execute('SELECT db_instance_id FROM operational_storage_state').fetchone()[0]
            check_storage_tenant(path, tenant)
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def deleted(self, root):
        path = await self.populated(root)
        memory = MemoryDB(str(path))
        await memory.connect()
        try:
            await memory.delete_session('legacy')
        finally:
            await memory.close()
        return path

    async def test_public_legacy_delete_retains_history_and_can_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.deleted(Path(temporary))
            with sqlite3.connect(path) as db:
                self.assertIsNone(db.execute("SELECT 1 FROM sessions WHERE session_id='legacy'").fetchone())
                self.assertEqual(db.execute("SELECT status FROM sessions_v2 WHERE id='legacy'").fetchone()[0], 'deleted')
                self.assertEqual(db.execute("SELECT COUNT(*) FROM session_runs WHERE session_id='legacy'").fetchone()[0], 1)
                self.assertIsNone(load_v2_legacy_session(db, 'legacy'))
                self.assertFalse(verify_session_projection(db, 'legacy').matches)
                verified = verify_session_projection(db, 'legacy', include_deleted=True)
                self.assertTrue(verified.matches)
                self.assertFalse(verified.eligible_for_v2)  # Never resurrect a deleted session.
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            check_storage_tenant(path, 'workspace')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)

    async def test_deleted_projection_requires_the_normal_deletion_event(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.deleted(Path(temporary))
            with sqlite3.connect(path) as db:
                db.execute("DELETE FROM domain_events WHERE session_id='legacy' AND event_type='session.deleted'")
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_changed_retained_deleted_content_is_not_reauthenticated(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = await self.deleted(Path(temporary))
            with sqlite3.connect(path) as db:
                db.execute("UPDATE session_messages SET text='Unbacked retained content' WHERE session_id='legacy'")
            with self.assertRaises(PermissionError):
                check_storage_tenant(path, 'workspace')

    async def test_forged_or_incomplete_tombstone_proof_is_denied(self):
        mutations = [
            'forged-event-time',
            'forged-event-version',
            "DELETE FROM legacy_session_changes WHERE session_id='legacy' AND operation='delete'",
            "DELETE FROM activity_items WHERE session_id='legacy'",
            "DELETE FROM search_outbox WHERE source_kind='session' AND source_id='legacy' AND operation='delete'",
            "UPDATE sessions_v2 SET legacy_source_hash='unbacked' WHERE id='legacy'",
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                path = await self.deleted(Path(temporary))
                with sqlite3.connect(path) as db:
                    if mutation.startswith('forged-event-'):
                        # Normal UPDATE is already blocked by the append-only
                        # event trigger. Forge an imported event in this isolated
                        # negative fixture without weakening that trigger.
                        cursor = db.execute("SELECT * FROM domain_events WHERE session_id='legacy' AND event_type='session.deleted'")
                        columns = [item[0] for item in cursor.description]
                        values = list(cursor.fetchone())
                        if mutation == 'forged-event-time':
                            values[columns.index('occurred_at_ms')] += 1
                        else:
                            values[columns.index('metadata_json')] = json.dumps({'source_version': 999})
                        db.execute("DELETE FROM domain_events WHERE session_id='legacy' AND event_type='session.deleted'")
                        db.execute('INSERT INTO domain_events (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in columns) + ')', values)
                    else:
                        db.execute(mutation)
                with self.assertRaises(PermissionError):
                    check_storage_tenant(path, 'workspace')


if __name__ == '__main__':
    unittest.main()
