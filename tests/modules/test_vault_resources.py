"""Run against an installed module wheel outside the source checkout."""
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from openagent_modules import module_assets


class InstalledVault(unittest.IsolatedAsyncioTestCase):
    def test_manifest_and_no_runtime_dependency_tree(self):
        assets = module_assets('vault')
        self.assertFalse((assets/'node_modules').exists())
        for item in json.loads((assets/'manifest.json').read_text())['artifacts']:
            data=(assets/item['file']).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(),item['sha256'])
            self.assertEqual(len(data),item['bytes'])
        with self.assertRaises(LookupError):
            module_assets('../vault')

    async def test_write_patch_read_quality_and_local_trash_over_stdio(self):
        assets=module_assets('vault')
        with tempfile.TemporaryDirectory() as root:
            params=StdioServerParameters(command=shutil.which('node'),args=[str(assets/'dist/server.mjs')],
                cwd=root,env={'OPENAGENT_VAULT_PATH':root,'OPENAGENT_VAULT_VALIDATE_WRITES':'1'})
            async with stdio_client(params) as (read,write):
                async with ClientSession(read,write) as session:
                    await session.initialize()
                    names={tool.name for tool in (await session.list_tools()).tools}
                    self.assertTrue({'write_note','patch_note','read_note','move_note'}<=names)
                    result=await session.call_tool('write_note',{'path':'Concepts/catalog.md','content':'# Catalog\n\nThe exact destination is retained.\n'})
                    self.assertFalse(result.isError,result)
                    note=Path(root,'Concepts/catalog.md').read_text()
                    self.assertTrue(note.startswith('---\n'),note)
                    self.assertIn('created:',note)
                    failed=await session.call_tool('patch_note',{'path':'Concepts/catalog.md','oldString':'does not exist','newString':'x'})
                    self.assertTrue(failed.isError)
                    self.assertEqual(Path(root,'Concepts/catalog.md').read_text(),note)
                    patched=await session.call_tool('patch_note',{'path':'Concepts/catalog.md','oldString':'destination','newString':'tool destination'})
                    self.assertFalse(patched.isError,patched)
                    read_result=await session.call_tool('read_note',{'path':'Concepts/catalog.md'})
                    self.assertIn('exact tool destination',read_result.content[0].text)
                    denied=await session.call_tool('delete_note',{'path':'Concepts/catalog.md','confirmPath':'other.md','trashMode':'local'})
                    self.assertTrue(denied.isError)
                    self.assertTrue(Path(root,'Concepts/catalog.md').exists())
                    trashed=await session.call_tool('delete_note',{'path':'Concepts/catalog.md','confirmPath':'Concepts/catalog.md','trashMode':'local'})
                    self.assertFalse(trashed.isError,trashed)
                    self.assertTrue(Path(root,'.trash/Concepts/catalog.md').exists())


if __name__ == '__main__':
    unittest.main()
