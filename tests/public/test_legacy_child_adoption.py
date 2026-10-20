"""Synthetic legacy writers, exact copied cohort, and actual public read guards."""
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.contracts import PrincipalRef, ExecutionContext, RunRequest, SessionRef, canonical_json
from openagent_core.engine import MemoryDB
from openagent_core.workspace_administration import SessionInspection
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.legacy_child_adoption import (
    HostAttestation, ExactRow, ParentToolLink, legacy_row_sha256,
    plan_legacy_child_adoption, apply_legacy_child_adoption,
)
from openagent_storage_sqlite.migration import prepare_migration, inspect_snapshot_identity, check_storage_tenant
from openagent_core.memory.operational.repository import project_legacy_session, project_legacy_session_async

LAYOUT = {'sessions_v2': ('id', 1), 'session_runs': ('id', 1), 'session_messages': ('id', 3),
          'domain_events': ('sequence', 2), 'activity_items': ('activity_id', 1), 'search_outbox': ('seq', 5)}


def digest(value): return hashlib.sha256(canonical_json(value).encode()).hexdigest()


class Allow:
    async def authorize(self, *args, **kwargs): return True


class LegacyChildAdoption(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.source = self.root/'source'; self.source.mkdir()
        self.path = self.source/'state.sqlite'
        db = MemoryDB(str(self.path)); await db.connect()
        store = SqliteRuntimeStore(self.path); await store.start()
        owner = PrincipalRef('host', 'workspace', 'owner')
        self.context = ExecutionContext(owner, owner, owner, 'parent', 'agent', (owner,), scopes=('projectId:project',))
        await store.accept(RunRequest('parent-run', 'parent', 'parent-request', 'Synthetic parent'), self.context)
        await store.transition('parent-run', 'success', output={'text': 'synthetic'})
        # First legacy projection is empty; the next one adds exactly one run
        # and three messages, reproducing the two events/five outbox intents.
        await db.upsert_session('child', parent_session_id='parent', origin='delegation', kind='worker')
        connection = await db._ensure_connected()
        envelope = {'run_id': 'original-child-run', 'created_at': 1,
                    'metadata': {'parent_session_id': 'parent', 'origin': 'delegation'}, 'messages': [
            {'id': 'one', 'role': 'user', 'content': 'Keep original user bytes'},
            {'id': 'two', 'role': 'assistant', 'content': 'Keep original response bytes'},
            {'id': 'three', 'role': 'user', 'content': 'Keep unrelated author attribution'}]}
        await connection.execute('UPDATE sessions SET agent_id=?,runs=? WHERE session_id=?',
                                 ('worker', json.dumps([envelope]), 'child'))
        await project_legacy_session_async(connection, 'child'); await connection.commit()
        await connection.execute('UPDATE session_runs SET raw_envelope_json=? WHERE id=?',
            (canonical_json({'tools': [{'tool_name': 'delegate_task_to_member', 'tool_call_id': 'delegation-call',
                'tool_args': {'member_id': 'worker'}, 'child_session_id': 'child', 'child_run_id': 'original-child-run'}]}), 'parent-run'))
        await connection.commit(); await store.close(); await db.close()
        with sqlite3.connect(self.path) as c:
            c.row_factory = sqlite3.Row
            row = c.execute("SELECT db_instance_id FROM operational_storage_state WHERE singleton_id=1").fetchone()
            self.foreign = 'installation:' + row[0]
            self.child_run = c.execute("SELECT id FROM session_runs WHERE session_id='child'").fetchone()[0]
        @asynccontextmanager
        async def quiesce(): yield 'host-fence'
        async def noop(_): return None
        # MemoryDB's first bootstrap preserves its own pre-DDL backup. Keep
        # that fixture outside the product state tree: prepare_migration must
        # continue rejecting undeclared embedded SQLite copies.
        import shutil
        if (self.source/'backups').exists(): shutil.move(self.source/'backups', self.root/'preserved-fixture-backups')
        self.backup, self.candidate = self.root/'backup', self.root/'candidate'
        await prepare_migration(self.source, self.backup, self.candidate,
                                databases=('state.sqlite',), quiesce=quiesce, migrate=noop)
        identity = inspect_snapshot_identity(self.backup, 'state.sqlite')
        self.attestation = HostAttestation('workspace-host', 'agent', 'pvc-unique', 'host-fence',
            identity['db_instance_id'], identity['backup_sha256'], identity['database_sha256'],
            self.foreign, 'workspace', digest(self.context.snapshot()))
        self.link = ParentToolLink('parent', 'parent-run', 'child', self.child_run,
                                  'original-child-run', 'delegation-call')
        self.rows = self.read_rows()

    def read_rows(self):
        rows = []
        with sqlite3.connect((self.backup/'state.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True) as db:
            db.row_factory = sqlite3.Row
            for table, (pk, count) in LAYOUT.items():
                values = db.execute('SELECT * FROM '+table+' WHERE tenant_id=?', (self.foreign,)).fetchall()
                self.assertEqual(count, len(values), table)
                rows.extend(ExactRow(table, row[pk], legacy_row_sha256(row)) for row in values)
        return tuple(rows)

    def plan(self, **kw):
        return plan_legacy_child_adoption(self.backup, 'state.sqlite', attestation=kw.get('attestation', self.attestation),
                                         link=kw.get('link', self.link), rows=kw.get('rows', self.rows))

    def apply(self, plan=None):
        return apply_legacy_child_adoption(self.candidate, plan or self.plan(), authorize=lambda *_: True)

    async def test_exact_copy_adoption_preserves_history_keys_author_acl_quarantine_and_triggers(self):
        before = (self.backup/'state.sqlite').read_bytes(); receipt = self.apply()
        self.assertEqual(13, len(receipt['rows'])); self.assertFalse(receipt['activated'])
        check_storage_tenant(self.candidate/'state.sqlite', 'workspace')
        self.assertEqual(before, (self.backup/'state.sqlite').read_bytes())
        with sqlite3.connect(self.candidate/'state.sqlite') as c:
            c.row_factory = sqlite3.Row
            child = dict(c.execute("SELECT * FROM sessions_v2 WHERE id='child'").fetchone())
            self.assertEqual(('workspace', None, 'quarantined', 'parent', 'parent'),
                tuple(child[k] for k in ('tenant_id','owner_principal_id','visibility','parent_session_id','root_session_id')))
            self.assertNotEqual(1, json.loads(child['metadata_json']).get('runtime_contract'))
            run = c.execute('SELECT parent_run_id,delegated_parent_run_id FROM session_runs WHERE id=?', (self.child_run,)).fetchone()
            self.assertEqual((None, 'parent-run'), tuple(run))
            for table in ('domain_events','search_outbox'):
                with self.assertRaises(sqlite3.IntegrityError): c.execute('UPDATE '+table+' SET tenant_id=tenant_id')
            self.assertEqual([], c.execute('PRAGMA foreign_key_check').fetchall())
        self.assertEqual(receipt, self.apply())

    async def test_no_current_authority_no_source_or_candidate_change(self):
        plan = self.plan(); before = (self.candidate/'state.sqlite').read_bytes()
        with self.assertRaises(PermissionError):
            apply_legacy_child_adoption(self.candidate, plan, authorize=lambda *_: False)
        self.assertEqual(before, (self.candidate/'state.sqlite').read_bytes())
        with self.assertRaises(ValueError):
            apply_legacy_child_adoption(self.backup, plan, authorize=lambda *_: True)

    async def test_wrong_host_snapshot_cohort_or_parent_rejected(self):
        for field, value in [('pvc_uid','another-pvc'), ('fence_id','different'), ('db_instance_id','other'),
                             ('database_sha256','0'*64), ('parent_context_sha256','0'*64), ('agent_id','other')]:
            with self.subTest(field=field):
                if field == 'pvc_uid':
                    # PVC ownership is deliberately verified by current trusted
                    # host code, not guessed from the SQLite file.
                    plan = self.plan(attestation=replace(self.attestation, **{field:value}))
                    with self.assertRaises(PermissionError):
                        apply_legacy_child_adoption(self.candidate, plan,
                            authorize=lambda a, _: a.pvc_uid == self.attestation.pvc_uid)
                else:
                    with self.assertRaises((PermissionError,ValueError)): self.plan(attestation=replace(self.attestation, **{field:value}))
        for rows in (self.rows[:-1], self.rows[:-1]+(self.rows[0],),
                     (replace(self.rows[0],sha256='0'*64),)+self.rows[1:]):
            with self.assertRaises(ValueError): self.plan(rows=rows)
        with self.assertRaises((ValueError,PermissionError,KeyError)):
            self.plan(link=replace(self.link, parent_run_id='absent'))

    async def test_revocation_during_migration_rolls_back_before_commit(self):
        plan = self.plan(); before = (self.candidate/'state.sqlite').read_bytes()
        decisions = iter((True, False))
        with self.assertRaises(PermissionError):
            apply_legacy_child_adoption(self.candidate, plan, authorize=lambda *_: next(decisions))
        self.assertEqual(before, (self.candidate/'state.sqlite').read_bytes())
        with sqlite3.connect(self.candidate/'state.sqlite') as c:
            self.assertIsNone(c.execute("SELECT name FROM sqlite_master WHERE name='legacy_child_adoption_receipts'").fetchone())

    async def test_candidate_drift_and_hardlink_rejected(self):
        plan = self.plan()
        with sqlite3.connect(self.candidate/'state.sqlite') as c:
            c.execute("UPDATE sessions_v2 SET title='changed' WHERE id='parent'")
        with self.assertRaises(ValueError): self.apply(plan)
        import os
        other = self.root/'hardlinked'; other.mkdir()
        os.link(self.backup/'state.sqlite', other/'state.sqlite')
        with self.assertRaises(ValueError):
            apply_legacy_child_adoption(other, plan, authorize=lambda *_: True)

    async def test_rollback_on_unrelated_trigger_and_preserve_trigger_schema(self):
        # A trigger that mutates an unrelated record must not widen the contract.
        with sqlite3.connect(self.source/'state.sqlite') as c:
            c.execute("CREATE TRIGGER unrelated_mutation AFTER UPDATE OF tenant_id ON sessions_v2 BEGIN UPDATE sessions_v2 SET title='side effect' WHERE id='parent'; END")
        # The prepared immutable snapshot does not contain it: exact candidate
        # hash checks reject drift before any controlled immutable trigger drop.
        with sqlite3.connect(self.candidate/'state.sqlite') as c:
            c.execute("CREATE TRIGGER unrelated_mutation AFTER UPDATE OF tenant_id ON sessions_v2 BEGIN UPDATE sessions_v2 SET title='side effect' WHERE id='parent'; END")
        before = (self.candidate/'state.sqlite').read_bytes()
        with self.assertRaises(ValueError): self.apply()
        self.assertEqual(before,(self.candidate/'state.sqlite').read_bytes())

    async def test_actual_public_readers_deny_quarantine_even_with_allow_all_policy(self):
        self.apply(); path = self.candidate/'state.sqlite'
        store = SqliteRuntimeStore(path); await store.start()
        try:
            child_context = replace(self.context, agent_id='worker')
            reference = SessionRef('host','workspace','worker','child')
            with self.assertRaises(PermissionError):
                await store.accept(RunRequest('must-not-run','child','new-request','No replay'),child_context)
            cancel_context = replace(child_context,session_id='child')
            for call in (store.read_session(reference,child_context,limit=10),store.get(self.child_run),store.events(self.child_run),
                         store.transition(self.child_run,'success'),store.request_cancel(self.child_run),
                         store.reserve_cancel(self.child_run,cancel_context),store.reserve_cancel('unknown-run',cancel_context),
                         store.append_event(self.child_run,'must-not-publish',{}),
                         store.begin_tool(self.child_run,'must-not-invoke',{},{}),
                         store.finish_tool(self.child_run,'must-not-finish',error={})):
                with self.assertRaises(PermissionError): await call
            self.assertEqual([], (await store.list_sessions(child_context,include_archived=True,limit=10))['sessions'])
            self.assertEqual([], (await store.search_sessions(child_context,query='Keep',limit=10))['sessions'])
            self.assertEqual((), await store.children('parent-run'))
            import aiosqlite
            async with aiosqlite.connect(path) as connection:
                connection.row_factory=aiosqlite.Row
                class Adapter:
                    async def _ensure_connected(self): return connection
                inspection = SessionInspection(Adapter(),Allow())
                with self.assertRaises(PermissionError): await inspection.authorize(self.context,'child')
                self.assertEqual({'ancestries':{}},await inspection.ancestries('workspace',['child']))
        finally: await store.close()

    async def test_reconciliation_and_restart_preserve_adoption_and_changed_legacy_fails(self):
        self.apply(); path = self.candidate/'state.sqlite'
        with sqlite3.connect(path) as c:
            before = c.execute("SELECT tenant_id,parent_session_id,root_session_id,metadata_json FROM sessions_v2 WHERE id='child'").fetchone()
            first = project_legacy_session(c,'child'); second = project_legacy_session(c,'child')
            self.assertFalse(first.changed); self.assertFalse(second.changed)
            self.assertEqual(before,c.execute("SELECT tenant_id,parent_session_id,root_session_id,metadata_json FROM sessions_v2 WHERE id='child'").fetchone())
            c.commit()
        store=SqliteRuntimeStore(path);await store.start();await store.close()
        check_storage_tenant(path,'workspace')
        with sqlite3.connect(path) as c:
            c.execute("UPDATE sessions SET metadata=? WHERE session_id='child'", (json.dumps({'origin':'delegation','parent_session_id':'parent','changed':True}),))
            with self.assertRaises(RuntimeError): project_legacy_session(c,'child')


if __name__ == '__main__': unittest.main()
