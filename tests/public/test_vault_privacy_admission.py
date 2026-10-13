"""Real copies remain untouched when an unqualified writer is fenced."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from openagent_core.content_privacy import ContentOrigin, PrivacyConflict, PrivacyFence
from openagent_core.memory.vault.service import VaultService
from openagent_core.mcp.servers.attachments.adapters import build_runtime_toolkit
from openagent_core.runtime import execution_scope


class VaultPrivacyAdmission(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.vault = self.root / 'vault'
        self.vault.mkdir()
        self.note = self.vault / 'human.md'
        self.note.write_bytes(b'Human and Gmail fixture\r\n')
        self.service = VaultService(self.vault, self.root / 'index.db', git_enabled=True)
        self.addAsyncCleanup(self.service.close)
        self.context = SimpleNamespace(tenant_id='tenant', session_id='session')
        self.origin = ContentOrigin('tenant','shopify','shop',1,('customer-a',))
        self.publisher = SimpleNamespace(origins_for_execution=self.origins)
        self.runtime = SimpleNamespace(service=lambda key: self.publisher)
        self.observed = []

    async def origins(self, context, run_id):
        self.observed.append((context, run_id))
        self.assertIs(context, self.context)
        self.assertEqual(run_id, 'admitted-run')
        return (self.origin,)

    async def test_all_direct_mutation_paths_fence_before_git_index_or_file_io(self):
        operations = (
            lambda: self.service.write_note('human.md','Shopify secret', origin={'kind':'human'},validate=False),
            lambda: self.service.delete_note('human.md'),
            lambda: self.service.move('human.md','renamed.md'),
            lambda: self.service.restore_to('HEAD'),
            lambda: self.service.reset_to('HEAD'),
            lambda: self.service.commit_paths(['human.md'],'masked origin'),
            lambda: self.service.autocommit(),
            lambda: self.service.regenerate_derived(),
            lambda: self.service.doctor(apply=True),
            lambda: self.service.maintenance(),
            lambda: self.service.init_taxonomy(),
        )
        with execution_scope(self.runtime,self.context,'admitted-run'):
            for operation in operations:
                with self.assertRaises(PrivacyFence):
                    await operation()
        self.assertEqual(len(self.observed),len(operations))
        self.assertEqual(self.note.read_bytes(),b'Human and Gmail fixture\r\n')
        self.assertEqual(list(self.vault.iterdir()),[self.note])
        self.assertFalse((self.root/'index.db').exists())

    async def test_human_management_does_not_inherit_recent_audit_origin(self):
        # The temporal registry is not even consulted; no run means no taint.
        with patch('openagent_core.memory.vault.vault_origin.recent_origin',side_effect=AssertionError('temporal attribution')):
            self.service.git_enabled=False
            result=await self.service.write_note('human.md','Human updated\n',validate=False)
        self.assertTrue(result['ok'])
        self.assertEqual(self.note.read_text(),'Human updated\n')
        self.assertEqual(self.observed,[])

    async def test_invalid_verified_getter_is_not_downgraded_to_human(self):
        async def invalid(*args):
            raise PrivacyConflict('wrong recipient fixture')
        self.publisher.origins_for_execution=invalid
        with execution_scope(self.runtime,self.context,'admitted-run'):
            with self.assertRaises(PrivacyConflict):
                await self.service.delete_note('human.md')
        self.assertTrue(self.note.exists())

    async def test_attachment_does_not_publish_untracked_marker_in_protected_run(self):
        toolkit=build_runtime_toolkit()
        entry=toolkit.async_functions['send_file_to_user'].entrypoint
        with execution_scope(self.runtime,self.context,'admitted-run'):
            with self.assertRaises(PrivacyFence):
                await entry(str(self.note))
        self.assertEqual(self.note.read_bytes(),b'Human and Gmail fixture\r\n')
        ordinary=await entry(str(self.note))
        self.assertTrue(ordinary['ok'])
        self.assertIn(str(self.note),ordinary['marker'])
