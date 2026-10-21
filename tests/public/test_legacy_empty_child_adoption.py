"""Synthetic empty-placeholder migration; no historical delegation is inferred."""
from contextlib import asynccontextmanager, closing
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from openagent_core.contracts import PrincipalRef, ExecutionContext, RunRequest, SessionRef
from openagent_core.engine import MemoryDB
from openagent_core.workspace_administration import SessionInspection
from openagent_core.memory.operational.repository import project_legacy_session
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import prepare_migration, inspect_snapshot_identity, check_storage_tenant
from openagent_storage_sqlite.legacy_child_adoption import ExactRow, legacy_row_sha256
from openagent_storage_sqlite.legacy_empty_child_adoption import (
    EmptyChildHostAttestation, plan_legacy_empty_child_adoption, apply_legacy_empty_child_adoption,
)

LAYOUT={'sessions_v2':'id','activity_items':'activity_id','domain_events':'sequence','search_outbox':'seq'}


class Allow:
    async def authorize(self,*args,**kwargs): return True


class EmptyLegacyChildAdoption(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.path=self.root/'fixture.sqlite';self.serial=0
        db=MemoryDB(str(self.path));await db.connect()
        store=SqliteRuntimeStore(self.path);await store.start()
        owner=PrincipalRef('host','workspace','owner')
        self.context=ExecutionContext(owner,owner,owner,'parent','agent',(owner,))
        await store.accept(RunRequest('parent-run','parent','parent-request','Synthetic parent'),self.context)
        await store.transition('parent-run','success',output={'text':'Synthetic result'})
        # The actual historical writer produces one unresolved session and its
        # three empty projection records. There is deliberately no child run,
        # canonical delegation tool or authority context for this placeholder.
        await db.upsert_session('empty-child',parent_session_id='parent',origin='delegation',kind='worker')
        await store.close();await db.close()

    async def prepare(self,mutate=None):
        self.serial+=1;case=self.root/('case-'+str(self.serial));source=case/'source';source.mkdir(parents=True)
        path=source/'state.sqlite'
        with closing(sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True)) as original, closing(sqlite3.connect(path)) as copy:
            original.backup(copy)
        if mutate:
            with closing(sqlite3.connect(path)) as db:
                db.row_factory=sqlite3.Row;mutate(db);db.commit()
        @asynccontextmanager
        async def quiesce():yield 'host-fence'
        async def noop(_):return None
        backup,candidate=case/'backup',case/'candidate'
        await prepare_migration(source,backup,candidate,databases=('state.sqlite',),quiesce=quiesce,migrate=noop)
        identity=inspect_snapshot_identity(backup,'state.sqlite');foreign='installation:'+identity['db_instance_id']
        rows=[]
        with closing(sqlite3.connect((backup/'state.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            db.row_factory=sqlite3.Row
            parent=db.execute("SELECT * FROM sessions_v2 WHERE id='parent'").fetchone()
            for table,pk in LAYOUT.items():
                for row in db.execute('SELECT * FROM '+table+' WHERE tenant_id=?',(foreign,)):
                    rows.append(ExactRow(table,row[pk],legacy_row_sha256(row)))
            attestation=EmptyChildHostAttestation('workspace-host','agent','host-storage-uid','host-fence',
                identity['db_instance_id'],identity['backup_sha256'],identity['database_sha256'],foreign,'workspace',
                'parent',legacy_row_sha256(parent))
        return backup,candidate,attestation,tuple(rows)

    def plan(self,values,**changes):
        backup,_,attestation,rows=values
        return plan_legacy_empty_child_adoption(backup,'state.sqlite',
            attestation=changes.get('attestation',attestation),child_session_id='empty-child',rows=changes.get('rows',rows))

    def apply(self,values,plan=None,authorize=lambda *_:True):
        return apply_legacy_empty_child_adoption(values[1],plan or self.plan(values),authorize=authorize)

    async def test_only_four_tenant_cells_change_and_no_lineage_or_native_admission(self):
        values=await self.prepare();plan=self.plan(values)
        before=(values[0]/'state.sqlite').read_bytes();receipt=self.apply(values,plan)
        self.assertEqual('empty_placeholder',receipt['mode']);self.assertEqual(4,len(receipt['rows']))
        self.assertFalse(receipt['parent_link_verified']);self.assertIsNone(receipt['parent_link']['parent_session_id'])
        self.assertEqual('parent',receipt['claimed_parent_session_id']);self.assertFalse(receipt['activated'])
        with closing(sqlite3.connect((values[0]/'state.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as old, closing(sqlite3.connect(values[1]/'state.sqlite')) as new:
            old.row_factory=new.row_factory=sqlite3.Row
            for selected in receipt['rows']:
                table,pk=selected['table'],LAYOUT[selected['table']]
                previous=dict(old.execute('SELECT * FROM '+table+' WHERE '+pk+'=?',(selected['key'],)).fetchone())
                current=dict(new.execute('SELECT * FROM '+table+' WHERE '+pk+'=?',(selected['key'],)).fetchone())
                self.assertEqual({**previous,'tenant_id':'workspace'},current)
            child=dict(new.execute("SELECT * FROM sessions_v2 WHERE id='empty-child'").fetchone())
            self.assertEqual((None,'empty-child',None,'quarantined'),tuple(child[k] for k in ('parent_session_id','root_session_id','owner_principal_id','visibility')))
            self.assertNotEqual(1,json.loads(child['metadata_json']).get('runtime_contract'))
            self.assertEqual([],new.execute('PRAGMA foreign_key_check').fetchall())
            for table in ('domain_events','search_outbox'):
                with self.assertRaises(sqlite3.IntegrityError):new.execute('UPDATE '+table+' SET tenant_id=tenant_id')
        self.assertEqual(receipt,self.apply(values,plan));self.assertEqual(before,(values[0]/'state.sqlite').read_bytes())
        check_storage_tenant(values[1]/'state.sqlite','workspace')

    async def test_current_host_authority_and_second_check_revocation(self):
        values=await self.prepare();plan=self.plan(values);before=(values[1]/'state.sqlite').read_bytes()
        with self.assertRaises(PermissionError):self.apply(values,plan,authorize=lambda *_:False)
        answers=iter((True,False))
        with self.assertRaises(PermissionError):self.apply(values,plan,authorize=lambda *_:next(answers))
        self.assertEqual(before,(values[1]/'state.sqlite').read_bytes())

    async def test_exact_hashes_parent_boundary_and_installation_namespace(self):
        values=await self.prepare();a=values[2]
        for changed in (replace(a,parent_row_sha256='0'*64),replace(a,agent_id='other'),
                        replace(a,parent_session_id='missing'),replace(a,source_tenant='arbitrary-legacy'),
                        replace(a,database_sha256='0'*64),replace(a,fence_id='other')):
            with self.subTest(attestation=changed):
                with self.assertRaises((ValueError,PermissionError)):self.plan(values,attestation=changed)
        for rows in (values[3][:-1],values[3]+(values[3][0],),(replace(values[3][0],sha256='0'*64),)+values[3][1:]):
            with self.assertRaises(ValueError):self.plan(values,rows=rows)

    async def test_legacy_runs_or_changed_projection_are_not_empty(self):
        for runs in ('[{"run_id":"hidden","messages":[]}]','{}','"not-a-list"'):
            with self.subTest(runs=runs):
                values=await self.prepare(lambda db:db.execute("UPDATE sessions SET runs=? WHERE session_id='empty-child'",(runs,)))
                with self.assertRaises(ValueError):self.plan(values)
        values=await self.prepare(lambda db:db.execute("UPDATE sessions_v2 SET legacy_source_hash=? WHERE id='empty-child'",('0'*64,)))
        with self.assertRaises(ValueError):self.plan(values)

    async def test_cross_tenant_typed_references_and_foreign_rows_fail_closed(self):
        # Generic typed references have no session FK, so test all-tenant
        # emptiness independently of PRAGMA foreign_key_check.
        for table,column in (('extra_copy','session_id'),('incoming_tool','child_session_id')):
            def add(db):
                db.execute('CREATE TABLE '+table+'(id TEXT PRIMARY KEY,tenant_id TEXT,'+column+' TEXT)')
                db.execute('INSERT INTO '+table+' VALUES(?,?,?)',('extra','workspace','empty-child'))
            values=await self.prepare(add)
            with self.assertRaises(ValueError):self.plan(values)
        def foreign(db):
            db.execute('CREATE TABLE additional_foreign(id TEXT PRIMARY KEY,tenant_id TEXT)')
            tenant=db.execute("SELECT tenant_id FROM sessions_v2 WHERE id='empty-child'").fetchone()[0]
            db.execute('INSERT INTO additional_foreign VALUES(?,?)',('extra',tenant))
        values=await self.prepare(foreign)
        with self.assertRaises(ValueError):self.plan(values)

    async def test_acl_artifact_links_and_incoming_lineage_are_rejected(self):
        def acl(db):
            db.execute("INSERT INTO resource_acl(tenant_id,resource_type,resource_id,principal_type,principal_id,permission,acl_version,granted_at_ms) VALUES('workspace','session','empty-child','user','viewer','view',1,1)")
        def artifact(db):
            db.execute("INSERT INTO artifacts(id,tenant_id,owner_principal_id,visibility,direction,kind,storage_key,sha256,size_bytes,retention_class,created_at_ms,updated_at_ms) VALUES('artifact','workspace','owner','private','input','file','key',?,0,'normal',1,1)",('0'*64,))
            db.execute("INSERT INTO artifact_links(id,tenant_id,artifact_id,resource_type,resource_id,relation,created_at_ms) VALUES('link','workspace','artifact','session','empty-child','input',1)")
        def incoming(db):
            db.execute("UPDATE sessions_v2 SET parent_session_id='empty-child',root_session_id='empty-child' WHERE id='parent'")
        for add in (acl,artifact,incoming):
            values=await self.prepare(add)
            with self.assertRaises(ValueError):self.plan(values)

    async def test_reconcile_keeps_hint_unverified_after_parent_updates_or_archival(self):
        values=await self.prepare();self.apply(values);path=values[1]/'state.sqlite'
        with closing(sqlite3.connect(path)) as db:
            db.execute('PRAGMA foreign_keys=ON')
            self.assertFalse(project_legacy_session(db,'empty-child').changed)
            db.execute("UPDATE sessions_v2 SET title='A legitimate newer conversation',source_version=source_version+1 WHERE id='parent'")
            self.assertFalse(project_legacy_session(db,'empty-child').changed)
            db.commit()
        # Use the supported deletion boundary. A raw DELETE invokes optional
        # UI-table cascade triggers that this storage-only fixture does not
        # initialize; it is not the public session archival contract.
        store=SqliteRuntimeStore(path);await store.start()
        try:
            await store.archive_session(SessionRef('host','workspace','agent','parent'),self.context)
            self.assertEqual([], (await store.list_sessions(self.context,include_archived=False,limit=10))['sessions'])
        finally:await store.close()
        with closing(sqlite3.connect(path)) as db:
            self.assertIsNotNone(db.execute("SELECT deleted_at_ms FROM sessions_v2 WHERE id='parent'").fetchone()[0])
            self.assertFalse(project_legacy_session(db,'empty-child').changed)
            db.commit()
            child=db.execute("SELECT parent_session_id,root_session_id,owner_principal_id,visibility FROM sessions_v2 WHERE id='empty-child'").fetchone()
            self.assertEqual((None,'empty-child',None,'quarantined'),tuple(child))
            db.execute("UPDATE sessions SET metadata=? WHERE session_id='empty-child'",(json.dumps({'parent_session_id':'different','origin':'delegation'}),))
            with self.assertRaises(RuntimeError):project_legacy_session(db,'empty-child')

    async def test_changed_adopted_row_or_missing_receipt_does_not_reproject(self):
        for mutation in ("UPDATE sessions_v2 SET title='changed' WHERE id='empty-child'", "DELETE FROM legacy_child_adoption_receipts"):
            values=await self.prepare();self.apply(values)
            with closing(sqlite3.connect(values[1]/'state.sqlite')) as db:
                db.execute(mutation)
                with self.assertRaises(RuntimeError):project_legacy_session(db,'empty-child')

    async def test_public_quarantine_guards_still_deny_read_and_admission_after_restart(self):
        values=await self.prepare();self.apply(values);path=values[1]/'state.sqlite'
        store=SqliteRuntimeStore(path);await store.start();await store.close();await store.start()
        try:
            with closing(sqlite3.connect(path)) as db:
                agent=db.execute("SELECT agent_id FROM sessions_v2 WHERE id='empty-child'").fetchone()[0]
            context=replace(self.context,session_id='empty-child',agent_id=agent or 'worker')
            with self.assertRaises(PermissionError):await store.accept(RunRequest('not-admitted','empty-child','new','No provider'),context)
            reference=SessionRef('host','workspace',context.agent_id,'empty-child')
            # The projected historical agent can be NULL; matching the real
            # store boundary may therefore fail as missing, never return data.
            with self.assertRaises((PermissionError,LookupError)):await store.read_session(reference,context,limit=10)
            self.assertEqual([], (await store.list_sessions(context,include_archived=True,limit=10))['sessions'])
            self.assertEqual([], (await store.search_sessions(context,query='empty',limit=10))['sessions'])
            import aiosqlite
            async with aiosqlite.connect(path) as connection:
                connection.row_factory=aiosqlite.Row
                class Adapter:
                    async def _ensure_connected(self):return connection
                with self.assertRaises(PermissionError):await SessionInspection(Adapter(),Allow()).authorize(self.context,'empty-child')
        finally:await store.close()

    async def test_old_thirteen_row_api_remains_strict(self):
        from openagent_storage_sqlite.legacy_child_adoption import HostAttestation,ParentToolLink,plan_legacy_child_adoption,_hash
        values=await self.prepare();a=values[2]
        original=HostAttestation(a.workspace_id,a.agent_id,a.storage_uid,a.fence_id,a.db_instance_id,
            a.snapshot_sha256,a.database_sha256,a.source_tenant,a.target_tenant,_hash(self.context.snapshot()))
        with self.assertRaises(ValueError):
            plan_legacy_child_adoption(values[0],'state.sqlite',attestation=original,
                link=ParentToolLink('parent','parent-run','empty-child','missing','missing','missing'),rows=values[3])


if __name__=='__main__':unittest.main()
