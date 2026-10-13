import asyncio
from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import threading
import unittest

from openagent_core.content_privacy import (ContentOrigin, ContentRevision, PrivacyRequest,
    PrivacyConflict, PrivacyFence)
from openagent_core.privacy_files import ManagedFilePin, ManagedFilePrivacySink
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore, REQUIRED_CONTENT_SURFACES


class Privacy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = SqliteContentPrivacyStore(self.root/'privacy.sqlite')
        self.origin = ContentOrigin('tenant', 'shopify', 'group', 10, ('customer-1',))
        self.request = PrivacyRequest('event', 'tenant', 'shopify', 'group', 10, 'redact', ('customer-1',))
        self.revision = ContentRevision('tenant', 'result', 'v1', 'sink', hashlib.sha256(b'secret').hexdigest())

    async def test_origin_propagates_transitively_and_preserves_unrelated_content(self):
        await self.store.record(self.revision, origins=(self.origin,))
        derived = replace(self.revision, artifact_id='child-summary', revision_id='v2')
        await self.store.record(derived, parents=(('result', 'v1'),))
        unrelated = replace(self.revision, artifact_id='human-note')
        await self.store.record(unrelated)
        newer = replace(self.revision, artifact_id='new-grant')
        await self.store.record(newer, origins=(replace(self.origin, generation=11),))
        other_tenant = replace(self.revision, tenant_id='other', artifact_id='other-tenant')
        await self.store.record(other_tenant, origins=(replace(self.origin, tenant_id='other'),))
        self.assertEqual((await self.store.enqueue(self.request)).remaining_revisions, 2)
        calls = []
        class Sink:
            sink_id = 'sink'
            async def apply(self, request, revision):
                calls.append(revision.artifact_id)
                return 'complete'
        final = await self.store.process('event', (Sink(),), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(final.status, 'complete')
        self.assertEqual(set(calls), {'result', 'child-summary'})
        self.assertEqual((await self.store.enqueue(self.request)).status, 'complete')

    async def test_revocation_fences_new_writes_including_descendants_after_restart(self):
        await self.store.record(self.revision, origins=(self.origin,))
        await self.store.enqueue(self.request)
        store = SqliteContentPrivacyStore(self.store.path)
        with self.assertRaises(PrivacyFence):
            await store.record(self.revision, origins=(self.origin,))
        with self.assertRaises(PrivacyFence):
            await store.record(replace(self.revision, artifact_id='late-tool-result'), origins=(self.origin,))
        with self.assertRaises(PrivacyFence):
            await store.record(replace(self.revision, artifact_id='late-compaction'), parents=(('result', 'v1'),))
        await store.record(replace(self.revision, artifact_id='reauthorized'), origins=(replace(self.origin, generation=11),))

    async def test_external_publication_serializes_with_revocation_and_has_durable_recovery(self):
        entered, release = threading.Event(), threading.Event()
        path = self.root/'canonical.txt'
        def publish():
            entered.set()
            if not release.wait(3):
                raise RuntimeError('test publication timeout')
            path.write_text('secret')
        with self.assertRaises(PrivacyConflict):
            await self.store.publish(self.revision,publish=publish,origins=(self.origin,))
        self.assertFalse(entered.is_set())
        await self.store.record(self.revision,origins=(self.origin,))
        other_store=SqliteContentPrivacyStore(self.store.path)
        publication = asyncio.create_task(self.store.publish(self.revision,publish=publish,origins=(self.origin,)))
        self.assertTrue(await asyncio.to_thread(entered.wait,3))
        revocation = asyncio.create_task(other_store.enqueue(self.request))
        try:
            await asyncio.sleep(.05)
            self.assertFalse(revocation.done())
        finally:
            release.set()
        await publication
        self.assertEqual((await revocation).remaining_revisions,1)
        self.assertEqual(path.read_text(),'secret')
        callbacks=[]
        with self.assertRaises(PrivacyFence):
            await self.store.publish(self.revision,publish=lambda:callbacks.append('unsafe'),origins=(self.origin,))
        self.assertEqual(callbacks,[])

    async def test_external_writer_failure_does_not_erase_preexisting_recovery_metadata(self):
        await self.store.record(self.revision,origins=(self.origin,))
        path=self.root/'canonical.txt'
        def partial():
            path.write_text('secret')
            raise RuntimeError('simulated crash after canonical write')
        with self.assertRaises(RuntimeError):
            await self.store.publish(self.revision,publish=partial,origins=(self.origin,))
        restarted=SqliteContentPrivacyStore(self.store.path)
        self.assertEqual((await restarted.enqueue(self.request)).remaining_revisions,1)
        self.assertEqual(path.read_text(),'secret')

    async def test_subject_source_group_and_tenant_are_exact(self):
        for i, origin in enumerate((replace(self.origin, subjects=('customer-2',)),
                                   replace(self.origin, group_id='another-shop'),
                                   replace(self.origin, source_id='another-app'))):
            await self.store.record(replace(self.revision, artifact_id=str(i)), origins=(origin,))
        self.assertEqual((await self.store.enqueue(self.request)).remaining_revisions, 0)
        with self.assertRaises(PrivacyConflict):
            await self.store.record(self.revision, origins=(replace(self.origin, tenant_id='another'),))
        with self.assertRaises(PrivacyConflict):
            await self.store.record(self.revision, parents=(('untracked', 'v1'),))

    async def test_replayed_request_and_revision_metadata_are_immutable(self):
        await self.store.record(self.revision, origins=(self.origin,))
        with self.assertRaises(PrivacyConflict):
            await self.store.record(self.revision, origins=())
        await self.store.enqueue(self.request)
        with self.assertRaises(PrivacyConflict):
            await self.store.enqueue(replace(self.request, group_id='another-shop'))

    async def test_missing_coverage_cannot_complete_even_with_zero_tracked_targets(self):
        await self.store.enqueue(self.request)
        result = await self.store.process('event', (), covered_surfaces=frozenset({'transcripts'}))
        self.assertEqual((result.status, result.reason), ('blocked', 'coverage_incomplete'))
        self.assertEqual((await SqliteContentPrivacyStore(self.store.path).progress('event')).status, 'blocked')
        with self.assertRaises(PrivacyConflict):
            SqliteContentPrivacyStore(self.store.path, required_surfaces={'transcripts'})

    async def test_sink_failure_retries_without_repeating_completed_revisions(self):
        await self.store.record(self.revision, origins=(self.origin,))
        await self.store.record(replace(self.revision, artifact_id='result2'), origins=(self.origin,))
        await self.store.enqueue(self.request)
        calls = []
        class Sink:
            sink_id = 'sink'
            fail = True
            async def apply(self, request, revision):
                calls.append(revision.artifact_id)
                if revision.artifact_id == 'result2' and self.fail:
                    raise RuntimeError('PRIVATE CONTENT MUST NOT ENTER RECEIPT')
                return 'complete'
        sink = Sink()
        result = await self.store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual((result.status, result.reason, result.completed_revisions), ('blocked', 'sink_failed', 1))
        sink.fail = False
        result = await SqliteContentPrivacyStore(self.store.path).process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(result.status, 'complete')
        self.assertEqual(calls.count('result'), 1)
        self.assertNotIn(b'PRIVATE CONTENT', self.store.path.read_bytes())

    async def test_worker_lease_and_expired_worker_cannot_complete_successor(self):
        clock = [10_000]
        store = SqliteContentPrivacyStore(self.store.path, lease_ms=1000, clock=lambda: clock[0])
        await store.record(self.revision, origins=(self.origin,))
        await store.enqueue(self.request)
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        class Sink:
            sink_id = 'sink'
            async def apply(self, request, revision):
                calls.append(revision.artifact_id)
                entered.set()
                await release.wait()
                return 'complete'
        sink = Sink()
        worker = asyncio.create_task(store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES))
        await entered.wait()
        other = await store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(other.status, 'processing')
        self.assertEqual(len(calls), 1)
        clock[0] += 1001
        release.set()
        first = await worker
        self.assertEqual(first.status, 'processing')
        final = await store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(final.status, 'complete')
        self.assertEqual(len(calls), 2)

    async def test_real_file_export_redaction_and_human_revision_conflict(self):
        managed = self.root/'managed'
        managed.mkdir()
        (managed/'provider-result').write_bytes(b'secret')
        (managed/'human-note').write_bytes(b'unrelated human text')
        exports = []
        async def deliver(request, revision, content):
            exports.append((request.request_id, content))
        sink = ManagedFilePrivacySink('sink', managed, (ManagedFilePin('result', 'v1', 'provider-result', wholly_attributed=True),),
                                      writer_lock=asyncio.Lock(), export=deliver)
        await self.store.record(self.revision, origins=(self.origin,))
        export = replace(self.request, request_id='export-event', operation='export')
        await self.store.enqueue(export)
        result = await self.store.process('export-event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(result.status, 'complete')
        self.assertEqual(exports, [('export-event', b'secret')])
        self.assertNotIn(b'secret', self.store.path.read_bytes())
        (managed/'provider-result').write_bytes(b'human revised content')
        await self.store.enqueue(self.request)
        result = await self.store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual((result.status, result.reason), ('blocked', 'revision_conflict'))
        self.assertEqual((managed/'provider-result').read_bytes(), b'human revised content')
        (managed/'provider-result').write_bytes(b'secret')
        result = await self.store.process('event', (sink,), covered_surfaces=REQUIRED_CONTENT_SURFACES)
        self.assertEqual(result.status, 'complete')
        self.assertFalse((managed/'provider-result').exists())
        self.assertEqual((managed/'human-note').read_bytes(), b'unrelated human text')

    async def test_managed_file_sink_refuses_escape_and_symlinks(self):
        managed = self.root/'managed'
        managed.mkdir()
        outside = self.root/'outside'
        outside.write_bytes(b'secret')
        (managed/'link').symlink_to(outside)
        async def deliver(*args): pass
        with self.assertRaises(ValueError):
            ManagedFilePrivacySink('sink', managed, (ManagedFilePin('result', 'v1', '../outside'),),
                                   writer_lock=asyncio.Lock(), export=deliver)
        sink = ManagedFilePrivacySink('sink', managed, (ManagedFilePin('result', 'v1', 'link', wholly_attributed=True),),
                                      writer_lock=asyncio.Lock(), export=deliver)
        self.assertEqual(await sink.apply(self.request, self.revision), 'conflict')
        self.assertEqual(outside.read_bytes(), b'secret')
