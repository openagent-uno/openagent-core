"""Real temporary file/SQLite privacy jobs; no persistent product data."""
import asyncio
import hashlib
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.content_privacy import ContentOrigin, ContentRevision, PrivacyRequest
from openagent_core.privacy_files import ManagedFilePin, ManagedFilePrivacySink
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore


class ManagedFileDurability(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.managed = self.root / 'managed'
        self.parent = self.managed / 'nested'
        self.parent.mkdir(parents=True)
        self.path = self.parent / 'provider-result'
        self.path.write_bytes(b'protected fixture')
        self.human = self.managed / 'human-note'
        self.human.write_bytes(b'unrelated human revision')
        self.request = PrivacyRequest('event', 'tenant', 'provider/app', 'group', 10, 'redact', ('customer-1',))
        self.revision = ContentRevision('tenant', 'result', 'v1', 'files', hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.exports = []
        async def export(request, revision, data):
            self.exports.append(data)
        self.sink = ManagedFilePrivacySink('files', self.managed,
            (ManagedFilePin('result', 'v1', 'nested/provider-result', wholly_attributed=True),),
            writer_lock=asyncio.Lock(), export=export)

    @unittest.skipIf(os.name == 'nt', 'POSIX directory fsync qualification')
    async def test_actual_parent_is_synced_after_unlink_before_completion(self):
        actual_fsync = os.fsync
        synced = []
        def inspect_fsync(descriptor):
            info = os.fstat(descriptor)
            if stat.S_ISDIR(info.st_mode):
                self.assertFalse(self.path.exists())
                synced.append((info.st_dev, info.st_ino))
            return actual_fsync(descriptor)
        with patch('openagent_core.privacy_files.os.fsync', side_effect=inspect_fsync):
            self.assertEqual(await self.sink.apply(self.request, self.revision), 'complete')
        parent = self.parent.stat()
        self.assertEqual(synced, [(parent.st_dev, parent.st_ino)])
        self.assertEqual(self.human.read_bytes(), b'unrelated human revision')

    @unittest.skipIf(os.name == 'nt', 'POSIX durable job/retry qualification')
    async def test_failed_directory_sync_blocks_durable_receipt_and_retry_syncs_missing_path(self):
        store = SqliteContentPrivacyStore(self.root / 'privacy.sqlite', required_surfaces=frozenset({'files'}))
        origin = ContentOrigin('tenant', 'provider/app', 'group', 10, ('customer-1',))
        await store.record(self.revision, origins=(origin,))
        await store.enqueue(self.request)
        with patch.object(self.sink, '_sync_directory', side_effect=OSError('synthetic sync failure')):
            progress = await store.process('event', (self.sink,), covered_surfaces=frozenset({'files'}))
        self.assertEqual((progress.status, progress.reason, progress.remaining_revisions), ('blocked', 'sink_failed', 1))
        self.assertFalse(self.path.exists())
        restarted = SqliteContentPrivacyStore(store.path, required_surfaces=frozenset({'files'}))
        self.assertEqual((await restarted.progress('event')).status, 'blocked')
        actual_sync = self.sink._sync_directory
        synced = []
        def retry_sync(parent):
            self.assertFalse(self.path.exists())
            synced.append(parent)
            actual_sync(parent)
        with patch.object(self.sink, '_sync_directory', side_effect=retry_sync):
            progress = await restarted.process('event', (self.sink,), covered_surfaces=frozenset({'files'}))
        self.assertEqual((progress.status, progress.remaining_revisions), ('complete', 0))
        self.assertEqual(synced, [self.parent])
        self.assertEqual(self.human.read_bytes(), b'unrelated human revision')

    async def test_unqualified_windows_erase_does_not_delete_or_claim_completion(self):
        with patch('openagent_core.privacy_files.os.name', 'nt'):
            self.assertEqual(await self.sink.apply(self.request, self.revision), 'conflict')
        self.assertEqual(self.path.read_bytes(), b'protected fixture')
        self.assertEqual(self.human.read_bytes(), b'unrelated human revision')


if __name__ == '__main__':
    unittest.main()
