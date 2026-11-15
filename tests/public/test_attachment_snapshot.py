"""A just-published attachment is visible to the session check that follows it.

The upload commits on a dedicated WAL connection. During a run the shared
MemoryDB connection can sit inside an implicit transaction (see
``MemoryDB._heal_stale_shared_transaction``); a read there sees the snapshot
from before the upload, so ``send_file_to_user`` refused a correctly linked
artifact with "The host publisher did not return a session-linked artifact".
"""
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core import ExecutionContext, PrincipalRef, RunRequest


class PublishedAttachmentVisibility(unittest.IsolatedAsyncioTestCase):
    async def test_session_check_sees_an_upload_committed_after_the_shared_snapshot(self):
        from openagent_core.artifacts import (ArtifactRepository, find_session_attachments,
                                              session_output_reference)
        from openagent_core.engine import MemoryDB
        from openagent_storage_sqlite import SqliteRuntimeStore

        principal = PrincipalRef('host', 'tenant', 'alice')
        context = ExecutionContext(principal, principal, principal, 'session', 'agent', (principal,))
        with TemporaryDirectory() as temp:
            root = Path(temp)
            db = MemoryDB(str(root / 'state.sqlite'))
            await db.connect()
            store = SqliteRuntimeStore(root / 'state.sqlite')
            await store.start()
            try:
                await store.accept(RunRequest('run', 'session', 'idempotency', 'Send report'), context)

                async def allow(*args, **kwargs):
                    return True

                repository = ArtifactRepository(db, SimpleNamespace(authorize=allow))
                shared = await db._ensure_connected()
                await shared.execute('BEGIN')
                await (await shared.execute('SELECT COUNT(*) FROM artifacts')).fetchone()

                ref = await repository.upload(context, b'qa screenshot', filename='qa.png',
                                              mime_type='image/png', session_id='session')
                verified = await session_output_reference(db, tenant_id=context.tenant_id,
                                                          session_id='session', value=ref)
                listed = await find_session_attachments(db, tenant_id=context.tenant_id,
                                                        session_id='session', query='qa')

                self.assertIsNotNone(verified)
                self.assertEqual(ref['artifact_id'], verified['artifact_id'])
                self.assertEqual(['qa.png'], [a['filename'] for a in listed['attachments']])
                self.assertTrue(shared.in_transaction, 'the caller transaction is left alone')
                await shared.rollback()
            finally:
                await store.close()
                await db.close()
