from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.administration import ManagementContext
from openagent_core.contracts import PrincipalRef
from openagent_core.vault_administration import VaultAdministration


class Policy:
    def __init__(self): self.revoked = False; self.write = True
    async def authorize(self, context, action, resource, **kwargs):
        return not self.revoked and context.tenant_id == 'tenant' and resource.resource_id == 'agent' and (self.write or action == 'vault.read')


class VaultAdministrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.vault = self.root / 'vault'
        self.policy = Policy()
        self.service = VaultAdministration(self.vault, self.policy, index_path=self.root / 'index.db')
        self.context = ManagementContext(PrincipalRef('host', 'tenant', 'alice'), 'agent')
        self.addAsyncCleanup(self.service.close)

    async def call(self, operation, **kwargs):
        return await self.service.execute(self.context, operation, **kwargs)

    async def write(self, path, content):
        return await self.call('write', path=path, body={'content': content})

    async def test_crud_graph_search_rename_history_and_restore_keep_provenance(self):
        first = await self.write('sources/alpha.md', '# Alpha\nAn orchid fact.\n')
        self.assertTrue(first['ok']); self.assertTrue(first['commit'])
        second = await self.write('sources/beta.md', '# Beta\n[[sources/alpha|alias]]\n')
        self.assertTrue(second['ok'])
        self.assertEqual(len((await self.call('notes'))['notes']), 2)
        graph = await self.call('graph')
        self.assertEqual(graph['edges'], [{'source': 'sources/beta.md', 'target': 'sources/alpha.md'}])
        self.assertEqual(len((await self.call('search', query={'q': 'orchid'}))['results']), 1)
        self.assertEqual(len((await self.call('search/files', query={'q': 'alpha'}))['results']), 1)
        match = await self.call('search/in-file', query={'path': 'sources/alpha.md', 'q': 'orch.d', 'regex': 'true'})
        self.assertEqual(match['matches'][0]['line'], 2)
        moved = await self.call('move', body={'from': 'sources/alpha.md', 'to': 'sources/renamed.md'})
        self.assertEqual(moved['links_rewritten'], 1)
        self.assertIn('sources/renamed', (await self.call('read', path='sources/beta.md'))['content'])
        commits = (await self.call('history'))['commits']
        self.assertGreaterEqual(len(commits), 3)
        detail = await self.call('commit', query={'hash': first['commit']})
        self.assertIn('orchid', str(detail))
        self.assertIn('alice', str(commits))
        restored = await self.call('restore', body={'hash': second['commit']})
        self.assertTrue(restored['ok']); self.assertTrue((self.vault / 'sources/alpha.md').is_file())
        self.assertGreater(len((await self.call('history'))['commits']), len(commits))
        await self.call('delete', path='sources/beta.md')
        self.assertFalse((self.vault / 'sources/beta.md').exists())

    async def test_quality_failure_never_bypasses_service_and_readonly_blocks_writes(self):
        blocked = await self.write('knowledge/large.md', '\n'.join('line' for _ in range(500)))
        self.assertFalse(blocked['ok']); self.assertTrue(blocked['blocked'])
        self.assertFalse((self.vault / 'knowledge/large.md').exists())
        with patch.object(self.service._service(), '_enforce_write', side_effect=RuntimeError('validator unavailable')):
            with self.assertRaises(RuntimeError): await self.write('knowledge/new.md', '# Keep editor text')
        self.assertFalse((self.vault / 'knowledge/new.md').exists())
        self.policy.write = False
        with self.assertRaises(PermissionError): await self.write('sources/new.md', 'no')
        self.assertEqual((await self.call('notes'))['notes'], [])

    async def test_paths_symlinks_foreign_tenant_and_late_revocation(self):
        self.vault.mkdir(); outside = self.root / 'secret.md'; outside.write_text('PRIVATE SENTINEL')
        (self.vault / 'linked.md').symlink_to(outside)
        for path in ('../secret.md', '/tmp/note.md', '.git/config.md', 'linked.md'):
            with self.subTest(path=path):
                with self.assertRaises(ValueError): await self.call('read', path=path)
        self.assertEqual((await self.call('notes'))['notes'], [])
        self.assertEqual((await self.call('search', query={'q': 'PRIVATE'}))['results'], [])
        self.assertNotIn('linked.md', str(await self.call('gate')))
        with self.assertRaises(ValueError): await self.call('doctor', query={'apply': 'true'})
        self.assertEqual(outside.read_text(), 'PRIVATE SENTINEL')
        wrong = ManagementContext(PrincipalRef('host', 'foreign', 'alice'), 'agent')
        with self.assertRaises(PermissionError): await self.service.execute(wrong, 'notes')
        await self.write('sources/read.md', '# Read')
        original = self.service._read
        def revoke(path):
            result = original(path); self.policy.revoked = True; return result
        with patch.object(self.service, '_read', side_effect=revoke):
            with self.assertRaises(PermissionError): await self.call('read', path='sources/read.md')

    async def test_maintenance_and_explicit_reset_contract(self):
        created = await self.call('init'); self.assertTrue(created['created']); self.assertTrue(created['commit'])
        self.assertIsInstance(await self.call('stats'), dict)
        self.assertIsInstance(await self.call('doctor', query={'apply': 'false'}), dict)
        self.assertIsInstance(await self.call('derived'), dict)
        self.assertIsInstance(await self.call('index/sync', query={'force': 'true'}), dict)
        before = await self.write('sources/state.md', 'one')
        await self.write('sources/state.md', 'two')
        with self.assertRaises(ValueError): await self.call('reset', body={'hash': before['commit']})
        self.assertEqual((await self.call('read', path='sources/state.md'))['content'], 'two')
        result = await self.call('reset', body={'hash': before['commit'], 'confirm': True})
        self.assertTrue(result['ok'])
        self.assertEqual((await self.call('read', path='sources/state.md'))['content'], 'one')

    async def test_regex_deadline_and_result_bounds(self):
        await self.write('sources/regex.md', 'a' * 12000 + '!')
        with self.assertRaisesRegex(ValueError, 'deadline'):
            await self.call('search/in-file', query={'path': 'sources/regex.md', 'q': '(a+)+$', 'regex': 'true'})
        result = await self.call('search/in-file', query={'path': 'sources/regex.md', 'q': 'a', 'regex': 'true'})
        self.assertEqual(result['count'], 1000)
        self.assertTrue(result['matches'][0]['text_truncated'])
        self.assertEqual(len(result['matches'][0]['text']), 4096)


if __name__ == '__main__': unittest.main()
