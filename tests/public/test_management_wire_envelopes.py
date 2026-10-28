"""Exact public SDK protocol envelopes; no provider, network or user DB."""
from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any
import unittest

from mcp import types
from mcp.server.fastmcp import FastMCP
from mcp.shared.memory import create_connected_server_and_client_session

from openagent_core.mcp.management_adapters import ManagementToolkit, _TOOLS, build_model_toolkit
from openagent_core.mcp.pool import MCPPool
from openagent_core.mcp.servers.model_manager import server as models
from openagent_core.mcp.servers.tool_search.adapters import _invoke_registered_tool


class ManagementWireEnvelopes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name) / 'never-opened.sqlite3'
        self.owned = []

    async def asyncTearDown(self):
        for toolkit in reversed(self.owned):
            await toolkit.aclose()
        self.assertFalse(self.path.exists())
        self.temp.cleanup()

    async def canonical(self, sdk, name, arguments):
        async with create_connected_server_and_client_session(sdk) as client:
            result = await client.call_tool(name, arguments)
            return result.model_dump(mode='json', by_alias=True, exclude_none=False)

    async def candidate(self, toolkit, name, arguments):
        # No payload accessor or fixture normalization: compare the whole SDK
        # envelope, including null fields, errors, media and extension metadata.
        result = await toolkit.async_functions[name].entrypoint(**arguments)
        return result.mcp_result

    async def model_toolkit(self):
        toolkit = await build_model_toolkit(env={'OPENAGENT_DB_PATH': str(self.path)})
        self.owned.append(toolkit)
        return toolkit

    async def synthetic_toolkit(self, handler):
        sdk = FastMCP('wire-fixture', log_level='ERROR')
        module = SimpleNamespace(mcp=sdk)
        for name in _TOOLS['budget-manager']:
            sdk.add_tool(handler, name=name, structured_output=False)
            setattr(module, name, handler)
        toolkit = ManagementToolkit('budget-manager', module, {'OPENAGENT_DB_PATH': str(self.path)})
        await toolkit.prepare(module)
        self.owned.append(toolkit)
        return toolkit, sdk

    async def test_static_list_keeps_content_structured_result_and_null_metadata(self):
        toolkit = await self.model_toolkit()
        expected = await self.canonical(models.mcp, 'list_supported_frameworks', {})
        actual = await self.candidate(toolkit, 'list_supported_frameworks', {})
        self.assertEqual(expected, actual)
        self.assertEqual(actual['structuredContent'], {'result': ['api-based']})
        self.assertIsNone(actual['_meta'])
        self.assertFalse(actual['isError'])
        self.assertIsNone(toolkit._db)

    async def test_real_broker_returns_the_same_complete_sdk_envelope(self):
        pool = MCPPool.from_config([{'builtin': 'model-manager', 'in_process': True,
            'env': {'OPENAGENT_DB_PATH': str(self.path)}}], include_defaults=False)
        try:
            await pool.connect_all()
            expected = await self.canonical(models.mcp, 'list_supported_frameworks', {})
            actual = await _invoke_registered_tool(pool, 'model-manager', 'list_supported_frameworks', {})
            self.assertEqual(expected, actual)
            self.assertIsInstance(actual, dict)
        finally:
            await pool.close_all()

    async def test_missing_and_invalid_arguments_keep_protocol_errors(self):
        toolkit = await self.model_toolkit()
        for arguments in ({}, {'model_id': 'not-an-integer'}):
            expected = await self.canonical(models.mcp, 'get_model', arguments)
            actual = await self.candidate(toolkit, 'get_model', arguments)
            self.assertEqual(expected, actual)
            self.assertTrue(actual['isError'])
            self.assertIn('Error executing tool get_model:', actual['content'][0]['text'])
        self.assertIsNone(toolkit._db)

    async def test_real_handler_error_keeps_sdk_message_and_error_flag(self):
        toolkit = await self.model_toolkit()
        arguments = {'name': 'synthetic-no-provider-key'}
        expected = await self.canonical(models.mcp, 'add_provider', arguments)
        actual = await self.candidate(toolkit, 'add_provider', arguments)
        self.assertEqual(expected, actual)
        self.assertTrue(actual['isError'])
        self.assertIn('api-based providers require an api_key', actual['content'][0]['text'])
        self.assertIsNone(toolkit._db)

    async def test_sdk_converts_unstructured_nested_values(self):
        async def value() -> dict[str, Any]:
            return {'nested': {'kept': [None, False, 2.5]}, 'unicode': 'città'}
        toolkit, sdk = await self.synthetic_toolkit(value)
        expected = await self.canonical(sdk, 'list_budgets', {})
        actual = await self.candidate(toolkit, 'list_budgets', {})
        self.assertEqual(expected, actual)
        self.assertEqual(json.loads(actual['content'][0]['text']), await value())

    async def test_media_resource_links_structured_content_and_metadata_are_intact(self):
        async def value() -> types.CallToolResult:
            return types.CallToolResult(
                content=[types.TextContent(type='text', text='synthetic', _meta={'block': 'kept'}),
                         types.ImageContent(type='image', data='AA==', mimeType='image/png'),
                         types.ResourceLink(type='resource_link', name='fixture', uri='https://example.invalid/file')],
                structuredContent={'nested': [1, None, False]}, isError=True,
                _meta={'trace': 'synthetic', 'nested': {'kept': True}},
                future_extension={'preserved': [None, 1]},
            )
        toolkit, sdk = await self.synthetic_toolkit(value)
        expected = await self.canonical(sdk, 'list_budgets', {})
        actual = await self.candidate(toolkit, 'list_budgets', {})
        self.assertEqual(expected, actual)
        self.assertEqual(actual['content'][1]['data'], 'AA==')
        self.assertEqual(actual['_meta']['nested'], {'kept': True})
        self.assertEqual(actual['future_extension'], {'preserved': [None, 1]})


if __name__ == '__main__':
    unittest.main()
