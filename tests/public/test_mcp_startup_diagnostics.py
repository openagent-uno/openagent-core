"""Startup recovery uses the SDK's closed-stream signal and stays observable."""
import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from mcp.shared.exceptions import McpError
from mcp.types import CONNECTION_CLOSED, ErrorData

from openagent_core.mcp.pool import MCPPool, _ServerSpec


class LegacyToolkit:
    """A previously swallowed startup failure, with a real MCP SDK error."""
    def __init__(self, error=None, ready=False):
        self.error = error
        self.functions = {'read_owned': object()} if ready else {}
        self.session = SimpleNamespace(initialize=self.initialize)
        self._initialized = ready
        self.tasks = []
        self.closed = False

    async def __aenter__(self):
        self.tasks.append(asyncio.current_task())
        return self

    async def __aexit__(self, *exc):
        self.tasks.append(asyncio.current_task())
        self.closed = True

    async def initialize(self):
        if self.error is not None:
            raise self.error

    async def build_tools(self):
        pass


class StartupDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def connect(self, error):
        spec = _ServerSpec('owned-fixture', command=['never-spawned'])
        pool = MCPPool([spec])
        toolkits = []
        async def build(spec):
            toolkit = LegacyToolkit(error, ready=bool(toolkits))
            toolkits.append(toolkit)
            return await pool._safe_enter(toolkit, spec)
        with patch.object(pool, '_build_and_enter_toolkit', build), \
                patch('openagent_core.mcp.pool._DORMANT_RECOVERY_BACKOFF', 0):
            await pool.connect_all()
        self.addAsyncCleanup(pool.close_all)
        return pool, toolkits

    async def test_sdk_connection_closed_respawns_once_without_replaying_any_tool(self):
        pool, toolkits = await self.connect(McpError(ErrorData(
            code=CONNECTION_CLOSED, message='Connection closed')))
        self.assertEqual(2, len(toolkits))
        self.assertIs(pool.toolkit_by_name('owned-fixture'), toolkits[1])
        self.assertEqual([{'mcp_name':'owned-fixture','state':'ready','tool_count':1}], pool.connection_status())
        await pool.close_all()
        # The detached dead supervisor drains on its original owning task.
        for _ in range(10):
            if toolkits[0].closed:
                break
            await asyncio.sleep(0)
        self.assertTrue(all(t.closed for t in toolkits))
        self.assertTrue(all(len(set(t.tasks)) == 1 for t in toolkits))

    async def test_wrapped_closed_sdk_session_also_respawns(self):
        error = ExceptionGroup('transport', [McpError(ErrorData(
            code=CONNECTION_CLOSED, message='Connection closed'))])
        pool, toolkits = await self.connect(error)
        self.assertEqual(2, len(toolkits))
        self.assertEqual('ready', pool.connection_status()[0]['state'])

    async def test_auth_failure_stays_dormant_and_diagnostics_never_publish_error_text(self):
        pool, toolkits = await self.connect(McpError(ErrorData(
            code=-32602, message='Unauthorized Bearer must-stay-private', data={'token':'secret-data'})))
        self.assertEqual(1, len(toolkits))
        snapshot = pool.connection_status()
        self.assertEqual([{'mcp_name':'owned-fixture','state':'dormant','tool_count':0,
            'failure':{'code':'initialization_failed','phase':'session.initialize'}}], snapshot)
        self.assertNotIn('must-stay-private', json.dumps(snapshot))
        self.assertNotIn('secret-data', json.dumps(snapshot))

    async def test_failed_partial_entry_is_in_snapshot_without_a_registered_toolkit(self):
        pool = MCPPool([_ServerSpec('failed-fixture')])
        class FailedEntry:
            async def __aenter__(self):
                raise FileNotFoundError('/private/credential-path')
            async def __aexit__(self, *exc):
                raise AssertionError('failed entry never belonged to the exit stack')
        self.assertIsNone(await pool._safe_enter(FailedEntry(), pool.specs[0]))
        self.assertIsNone(pool.toolkit_by_name('failed-fixture'))
        self.assertEqual([{'mcp_name':'failed-fixture','state':'unavailable','tool_count':0,
            'failure':{'code':'process_not_found','phase':'connect'}}], pool.connection_status())
        self.assertNotIn('credential-path', json.dumps(pool.connection_status()))

    async def test_respawn_failure_is_unavailable_instead_of_silent_absence(self):
        spec = _ServerSpec('failed-respawn', command=['never-spawned'])
        pool = MCPPool([spec])
        dead = LegacyToolkit(McpError(ErrorData(code=CONNECTION_CLOSED, message='Connection closed')))
        attempts = 0
        class MissingEntry:
            async def __aenter__(self):
                raise FileNotFoundError('/missing/private-path')
            async def __aexit__(self, *exc):
                return False
        async def build(spec):
            nonlocal attempts
            attempts += 1
            return await pool._safe_enter(dead if attempts == 1 else MissingEntry(), spec)
        with patch.object(pool, '_build_and_enter_toolkit', build), \
                patch('openagent_core.mcp.pool._DORMANT_RECOVERY_BACKOFF', 0):
            await pool.connect_all()
        self.addAsyncCleanup(pool.close_all)
        self.assertEqual(2, attempts)
        self.assertIsNone(pool.toolkit_by_name(spec.name))
        self.assertEqual([{'mcp_name':spec.name,'state':'unavailable','tool_count':0,
            'failure':{'code':'process_not_found','phase':'connect'}}], pool.connection_status())

    async def test_cancelled_recovery_propagates_without_starting_a_replacement(self):
        pool = MCPPool([])
        entered = asyncio.Event()
        async def initialize():
            entered.set()
            await asyncio.Event().wait()
        toolkit = SimpleNamespace(functions={}, initialize=initialize)
        task = asyncio.create_task(pool._recover_dormant_toolkit(toolkit, _ServerSpec('cancelled')))
        await entered.wait()
        task.cancel('owner shutdown')
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual([], pool._toolkit_supervisors)
