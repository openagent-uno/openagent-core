import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.content_privacy import ContentOrigin, ContentRevision, PrivacyConflict, PrivacyFence, PrivacyRequest
from openagent_core.mcp.servers.attachments.privacy import ManagedAttachmentSnapshots
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore


class ManagedAttachments(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.copies=self.root/'snapshots'
        self.copies.mkdir()
        self.ledger=SqliteContentPrivacyStore(self.root/'metadata.sqlite')
        self.exports=[]
        self.origin=ContentOrigin('tenant','shopify','shop',3,('a',))
        self.request=PrivacyRequest('event','tenant','shopify','shop',3,'redact',('a',))
        self.data=b'Customer A protected bytes\r\n'
        self.revision=ContentRevision('tenant','report-a','v1','attachments',hashlib.sha256(self.data).hexdigest())
        self.writer=self.new_writer()

    def new_writer(self):
        async def export(request,revision,data):
            self.exports.append((request,revision,data))
        return ManagedAttachmentSnapshots('attachments',self.copies,self.ledger,export=export)

    async def write(self,revision=None,data=None,origin=None):
        return await self.writer.write(revision or self.revision,self.data if data is None else data,
            origins=(origin or self.origin,),wholly_attributed=True)

    async def test_restart_exact_export_and_selective_redaction_preserve_siblings(self):
        path=await self.write()
        other=b'Customer B and Gmail unrelated bytes'
        other_revision=ContentRevision('tenant','report-b','v1','attachments',hashlib.sha256(other).hexdigest())
        other_origin=ContentOrigin('tenant','shopify','shop',3,('b',))
        sibling=await self.write(other_revision,other,other_origin)
        human=self.root/'human.txt'
        human.write_bytes(b'Human data')
        restarted=self.new_writer()
        export=PrivacyRequest('export','tenant','shopify','shop',3,'export',('a',))
        self.assertEqual(await restarted.apply(export,self.revision),'complete')
        self.assertEqual(self.exports[-1][2],self.data)
        self.assertTrue(path.exists())
        await self.ledger.enqueue(self.request)
        self.assertEqual(await restarted.apply(self.request,self.revision),'complete')
        self.assertFalse(path.exists())
        self.assertEqual(sibling.read_bytes(),other)
        self.assertEqual(human.read_bytes(),b'Human data')
        self.assertEqual(await restarted.apply(self.request,self.revision),'complete')
        with self.assertRaises(PrivacyFence):
            await self.write()
        self.assertFalse(path.exists())

    async def test_interruption_after_payload_fsync_is_recoverable_from_durable_pin(self):
        with patch('openagent_core.mcp.servers.attachments.privacy.os.replace',side_effect=OSError('fixture crash')):
            with self.assertRaises(OSError):
                await self.write()
        pending=list(self.copies.glob('*.pending'))
        self.assertEqual(len(pending),1)
        self.assertEqual(pending[0].read_bytes(),self.data)
        self.assertEqual((await self.ledger.enqueue(self.request)).remaining_revisions,1)
        self.assertEqual(await self.new_writer().apply(self.request,self.revision),'complete')
        self.assertFalse(pending[0].exists())

    async def test_changed_copy_and_partial_stage_conflict_without_deleting_human_edits(self):
        path=await self.write()
        path.write_bytes(b'Human changed the managed file')
        self.assertEqual(await self.writer.apply(self.request,self.revision),'conflict')
        self.assertEqual(path.read_bytes(),b'Human changed the managed file')
        path.write_bytes(self.data)
        pending=path.with_suffix('.pending')
        pending.write_bytes(b'partial')
        self.assertEqual(await self.writer.apply(self.request,self.revision),'conflict')
        self.assertEqual(path.read_bytes(),self.data)
        self.assertEqual(pending.read_bytes(),b'partial')

    async def test_revoked_before_write_never_creates_payload_and_model_mixed_bytes_refused(self):
        await self.ledger.enqueue(self.request)
        with self.assertRaises(PrivacyFence):
            await self.write()
        self.assertEqual(list(self.copies.glob('*.pending')),[])
        self.assertEqual(list(self.copies.glob('*.bin')),[])
        with self.assertRaises(PrivacyConflict):
            await self.writer.write(self.revision,self.data,origins=(self.origin,))

    async def test_pin_tamper_and_symlink_never_expand_cleanup_scope(self):
        path=await self.write()
        pin=next(self.copies.glob('*.pin.json'))
        original=pin.read_bytes()
        pin.write_bytes(b'{}')
        self.assertEqual(await self.writer.apply(self.request,self.revision),'conflict')
        pin.write_bytes(original)
        human=self.root/'human.txt'
        human.write_bytes(self.data)
        path.unlink()
        path.symlink_to(human)
        self.assertEqual(await self.writer.apply(self.request,self.revision),'conflict')
        self.assertTrue(path.is_symlink())
        self.assertEqual(human.read_bytes(),self.data)

    async def test_parent_lineage_fences_derived_snapshot_and_uncovered_surface_stays_blocked(self):
        await self.write()
        derived=b'Derived customer summary'
        revision=ContentRevision('tenant','derived','v1','attachments',hashlib.sha256(derived).hexdigest())
        await self.writer.write(revision,derived,parents=(('report-a','v1'),),wholly_attributed=True)
        progress=await self.ledger.enqueue(self.request)
        self.assertEqual(progress.remaining_revisions,2)
        final=await self.ledger.process('event',(self.writer,),covered_surfaces=frozenset({'attachments'}))
        self.assertEqual(final.status,'blocked')
        self.assertEqual(final.remaining_revisions,2)
        self.assertEqual(len(list(self.copies.glob('*.bin'))),2)
