import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from openagent_core.content_privacy import ContentOrigin, ContentRevision, PrivacyRequest, PrivacyFence
from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore
from openagent_storage_sqlite.privacy_exports import EncryptedPrivacyExports


class Cipher:
    def __init__(self):
        self.key = AESGCM.generate_key(bit_length=256)

    def encrypt(self, aad, value):
        import os
        nonce = os.urandom(12)
        return nonce + AESGCM(self.key).encrypt(nonce, value, aad)

    def decrypt(self, aad, value):
        return AESGCM(self.key).decrypt(value[:12], value[12:], aad)


class Exports(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = SqliteContentPrivacyStore(self.root / 'canonical.sqlite', required_surfaces={'test-source'})
        self.cipher = Cipher()
        self.allowed = True
        async def authorize(request, actor):
            return self.allowed and request.tenant_id == 'tenant' and actor == 'current-owner'
        self.authorize = authorize
        self.spool = EncryptedPrivacyExports(self.root / 'exports.sqlite', self.ledger,
                                            cipher=self.cipher, authorize=authorize)
        self.source = ContentRevision('tenant', 'source', 'r1', 'test-source', hashlib.sha256(b'PRIVATE CUSTOMER A').hexdigest())
        self.origin = ContentOrigin('tenant', 'shopify/app', 'shop', 1, ('customer:23',))
        await self.ledger.record(self.source, origins=(self.origin,))
        self.request = PrivacyRequest('export', 'tenant', 'shopify/app', 'shop', 1, 'export', ('customer:23',))
        await self.ledger.enqueue(self.request)

    async def finish_export(self):
        spool = self.spool
        class Source:
            sink_id = 'test-source'
            async def apply(self, request, revision):
                await spool.export(request, revision, b'PRIVATE CUSTOMER A')
                return 'complete'
        progress = await self.ledger.process(self.request.request_id, (Source(),), covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status, 'complete')
        self.assertTrue(await self.spool.seal(self.request))

    async def test_restart_encryption_current_acl_and_delivery_are_separate_from_staging(self):
        self.assertFalse(await self.spool.seal(self.request))
        await self.finish_export()
        self.assertNotIn(b'PRIVATE CUSTOMER A', (self.root / 'exports.sqlite').read_bytes())
        self.assertFalse(await self.spool.delivered('export'))
        with self.assertRaises(PermissionError):
            await self.spool.download('export', 'other-owner')
        restored = EncryptedPrivacyExports(self.spool.path, self.ledger, cipher=self.cipher, authorize=self.authorize)
        bundle = await restored.download('export', 'current-owner')
        packet = json.loads(bundle.payload)
        self.assertEqual(base64.b64decode(packet['parts'][0]['content_base64']), b'PRIVATE CUSTOMER A')
        with self.assertRaises(PermissionError):
            await restored.acknowledge('export', 'current-owner', 'wrong-token')
        self.allowed = False
        with self.assertRaises(PermissionError):
            await restored.acknowledge('export', 'current-owner', bundle.delivery_token)
        self.allowed = True
        await restored.acknowledge('export', 'current-owner', bundle.delivery_token)
        self.assertTrue(await restored.delivered('export'))

    async def test_retry_dedup_and_later_redaction_revoke_prior_export_copies(self):
        await self.finish_export()
        await self.spool.export(self.request, self.source, b'PRIVATE CUSTOMER A')
        bundle = await self.spool.download('export', 'current-owner')
        self.assertEqual(len(json.loads(bundle.payload)['parts']), 1)
        redaction = PrivacyRequest('erase', 'tenant', 'shopify/app', 'shop', 1, 'redact', ('customer:23',))
        await self.ledger.enqueue(redaction)
        with self.assertRaises(PrivacyFence):
            await self.spool.download('export', 'current-owner')
        class Source:
            sink_id = 'test-source'
            async def apply(self, request, revision):
                return 'complete'
        progress = await self.ledger.process('erase', (Source(), self.spool), covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status, 'complete')
        with self.assertRaises(PermissionError):
            await self.spool.download('export', 'current-owner')
        self.assertFalse(await self.spool.delivered('export'))

    async def test_revoked_source_cannot_create_an_untracked_export_copy(self):
        await self.ledger.enqueue(PrivacyRequest('erase', 'tenant', 'shopify/app', 'shop', 1, 'redact'))
        with self.assertRaises(PrivacyFence):
            await self.spool.export(self.request, self.source, b'PRIVATE CUSTOMER A')
        self.assertNotIn(b'PRIVATE CUSTOMER A', self.spool.path.read_bytes())

    async def test_subsequent_data_request_includes_prior_encrypted_copies_and_is_redactable(self):
        await self.finish_export()
        request=PrivacyRequest('second-export','tenant','shopify/app','shop',1,'export',('customer:23',))
        await self.ledger.enqueue(request)
        spool=self.spool
        class Source:
            sink_id='test-source'
            async def apply(self,request,revision):
                if request.operation=='export':
                    await spool.export(request,revision,b'PRIVATE CUSTOMER A')
                return 'complete'
        progress=await self.ledger.process(request.request_id,(Source(),self.spool),covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status,'complete')
        await self.spool.seal(request)
        bundle=await self.spool.download(request.request_id,'current-owner')
        self.assertEqual(len(json.loads(bundle.payload)['parts']),2)
        redact=PrivacyRequest('later-erasure','tenant','shopify/app','shop',1,'redact',('customer:23',))
        await self.ledger.enqueue(redact)
        progress=await self.ledger.process(redact.request_id,(Source(),self.spool),covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status,'complete')
        self.assertNotIn(b'PRIVATE CUSTOMER A',self.spool.path.read_bytes())
        with self.assertRaises(PermissionError):
            await self.spool.download(request.request_id,'current-owner')

    async def test_selective_redaction_preserves_other_customer_export_and_rejects_wrong_payload(self):
        await self.finish_export()
        other = ContentRevision('tenant', 'other', 'r1', 'test-source', hashlib.sha256(b'OTHER CUSTOMER B').hexdigest())
        await self.ledger.record(other, origins=(ContentOrigin('tenant', 'shopify/app', 'shop', 1, ('customer:24',)),))
        request = PrivacyRequest('export-other', 'tenant', 'shopify/app', 'shop', 1, 'export', ('customer:24',))
        await self.ledger.enqueue(request)
        spool = self.spool
        class Source:
            sink_id = 'test-source'
            async def apply(self, request, revision):
                if request.operation == 'export':
                    await spool.export(request, revision, b'OTHER CUSTOMER B')
                return 'complete'
        progress = await self.ledger.process('export-other', (Source(),), covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status, 'complete')
        await self.spool.seal(request)
        from openagent_core.content_privacy import PrivacyConflict
        with self.assertRaises(PrivacyConflict):
            await self.spool.export(request, other, b'PRIVATE CUSTOMER A')
        await self.ledger.enqueue(PrivacyRequest('erase-a', 'tenant', 'shopify/app', 'shop', 1, 'redact', ('customer:23',)))
        progress = await self.ledger.process('erase-a', (Source(), self.spool), covered_surfaces=frozenset({'test-source'}))
        self.assertEqual(progress.status, 'complete')
        bundle = await self.spool.download('export-other', 'current-owner')
        self.assertEqual(base64.b64decode(json.loads(bundle.payload)['parts'][0]['content_base64']), b'OTHER CUSTOMER B')
