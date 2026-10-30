"""Bounded MCP App outcome retention must never replay an uncertain mutation."""
from __future__ import annotations

import asyncio
import gc
import unittest
import weakref
from dataclasses import replace

from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp_apps import McpAppService
from test_mcp_apps_resources import Policy, Source


class McpAppMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        self.source = Source()
        principal = PrincipalRef('test', 'tenant', 'owner')
        self.context = ExecutionContext(principal, principal, principal, 'session', 'agent', (principal,))
        self.catalog.register('left', self.source, self.source, target_label='left')
        self.tool = next(tool for tool in await self.catalog.discover(self.context) if tool.name == 'show')
        self.service = McpAppService(self.catalog)

    async def asyncTearDown(self):
        await self.service.shutdown()

    async def open_view(self):
        return (await self.service.open(self.tool.tool_ref, self.context))['handle']

    async def call(self, handle, request_id, **arguments):
        return await self.service.request(handle, 'tools/call',
            {'name': 'increment', 'arguments': arguments}, self.context, request_id=request_id)

    async def test_service_wide_byte_cache_evicts_without_reexecuting_mutation(self):
        self.service = McpAppService(self.catalog, max_result_cache_bytes=4000)
        first, second = await self.open_view(), await self.open_view()
        async def large_result(name, arguments, context):
            self.source.calls.append((name, arguments))
            return {'text': '\u00e8' * 600, 'sequence': len(self.source.calls)}
        self.source.call_tool = large_result
        self.assertEqual((await self.call(first, 'old'))['sequence'], 1)
        latest = await self.call(second, 'new')
        self.assertEqual(latest['sequence'], 2)
        self.assertLessEqual(self.service._result_bytes, 4000)
        self.assertEqual(self.service._result_bytes, sum(map(len, self.service._results.values())))
        self.assertTrue(all(isinstance(value, bytes) for value in self.service._results.values()))
        with self.assertRaisesRegex(CapabilityUnavailable, 'outcome expired.*inspect'):
            await self.call(first, 'old')
        with self.assertRaisesRegex(ValueError, 'changed arguments'):
            await self.call(first, 'old', changed=True)
        self.assertEqual(len(self.source.calls), 2)
        # Replays decode isolated caller-owned values rather than shared mappings.
        latest['sequence'] = 999
        self.assertEqual((await self.call(second, 'new'))['sequence'], 2)
        self.assertTrue(all(record.task is None for view in self.service.sessions.values()
            for record in view.requests.values()))

    async def test_pending_duplicate_joins_at_active_limit_and_only_dispatches_once(self):
        self.service = McpAppService(self.catalog, max_active_requests=1)
        first, second = await self.open_view(), await self.open_view()
        entered, release = asyncio.Event(), asyncio.Event()
        async def waiting(name, arguments, context):
            self.source.calls.append((name, arguments))
            entered.set()
            await release.wait()
            return {'count': len(self.source.calls)}
        self.source.call_tool = waiting
        original = asyncio.create_task(self.call(first, 'same'))
        await asyncio.wait_for(entered.wait(), 5)
        duplicate = asyncio.create_task(self.call(first, 'same'))
        try:
            with self.assertRaisesRegex(RuntimeError, 'active request limit'):
                await self.call(second, 'other')
            self.assertEqual(self.service._request_count, 1)
            release.set()
            self.assertEqual(await original, await duplicate)
            self.assertEqual(len(self.source.calls), 1)
            self.assertEqual(self.service._pending_tasks, set())
        finally:
            release.set()
            await asyncio.gather(original, duplicate, return_exceptions=True)

    async def test_concurrent_original_and_pending_duplicate_replies_survive_cache_eviction(self):
        self.service = McpAppService(self.catalog, max_result_cache_bytes=900)
        handle = await self.open_view()
        entered, release = asyncio.Event(), asyncio.Event()
        async def waiting(name, arguments, context):
            self.source.calls.append((name, arguments))
            if len(self.source.calls) == 3:
                entered.set()
            await release.wait()
            return {'sequence': arguments['sequence'], 'text': 'x' * 700}
        self.source.call_tool = waiting
        originals = [asyncio.create_task(self.call(handle, str(index), sequence=index)) for index in range(3)]
        await asyncio.wait_for(entered.wait(), 5)
        duplicate = asyncio.create_task(self.call(handle, '0', sequence=0))
        try:
            await asyncio.sleep(0)
            release.set()
            # All three providers complete before any waiter consumes a result.
            # The shared replay cache holds just one result, never the whole cohort.
            results = await asyncio.gather(*originals, duplicate)
            self.assertEqual([result['sequence'] for result in results], [0, 1, 2, 0])
            self.assertEqual(len(self.source.calls), 3)
            self.assertEqual(len(self.service._results), 1)
            with self.assertRaisesRegex(CapabilityUnavailable, 'outcome expired'):
                await self.call(handle, '0', sequence=0)
            self.assertEqual(len(self.source.calls), 3)
        finally:
            release.set()
            await asyncio.gather(*originals, duplicate, return_exceptions=True)

    async def test_disconnected_pending_duplicate_does_not_cancel_original_operation(self):
        self.service = McpAppService(self.catalog, max_active_requests=1)
        handle = await self.open_view()
        entered, release = asyncio.Event(), asyncio.Event()
        async def waiting(name, arguments, context):
            self.source.calls.append((name, arguments))
            entered.set()
            await release.wait()
            return {'completed': True}
        self.source.call_tool = waiting
        original = asyncio.create_task(self.call(handle, 'same'))
        await asyncio.wait_for(entered.wait(), 5)
        duplicate = asyncio.create_task(self.call(handle, 'same'))
        try:
            await asyncio.sleep(0)
            record = self.service.sessions[handle].requests['same']
            self.assertEqual(record.waiters, 2)
            duplicate.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await duplicate
            self.assertEqual(record.waiters, 1)
            self.assertIsNotNone(record.task)
            self.assertFalse(record.task.done())
            release.set()
            self.assertEqual(await original, {'completed': True})
            self.assertEqual(await self.call(handle, 'same'), {'completed': True})
            self.assertEqual(len(self.source.calls), 1)
            self.assertEqual(self.service._pending_tasks, set())
        finally:
            release.set()
            await asyncio.gather(original, duplicate, return_exceptions=True)

    async def test_result_limit_counts_ascii_escaped_transport_bytes(self):
        handle = await self.open_view()
        async def unicode_result(name, arguments, context):
            self.source.calls.append((name, arguments))
            # Four MiB in UTF-8, but twelve MiB with the transport's JSON escapes.
            return {'text': '\u00e9' * (2 * 1024 * 1024)}
        self.source.call_tool = unicode_result
        with self.assertRaisesRegex(ValueError, 'result too large'):
            await self.call(handle, 'unicode')
        with self.assertRaisesRegex(ValueError, 'result too large'):
            await self.call(handle, 'unicode')
        self.assertEqual(len(self.source.calls), 1)
        self.assertEqual(self.service._result_bytes, 0)
        self.assertIsNone(self.service.sessions[handle].requests['unicode'].task)

    async def test_completed_payload_holds_work_slot_until_final_authorization_finishes(self):
        self.service = McpAppService(self.catalog, max_active_requests=1, max_result_cache_bytes=16)
        first = await self.open_view()
        other_context = replace(self.context, session_id='other-session')
        second = (await self.service.open(self.tool.tool_ref, other_context))['handle']
        blocked, release = asyncio.Event(), asyncio.Event()
        original_policy = self.policy.authorize
        async def waiting_policy(context, action, resource, *, audience=()):
            record = self.service.sessions[first].requests.get('large')
            if context.session_id == self.context.session_id and action == 'tool.call' and record and record.outcome == 'result':
                blocked.set()
                await release.wait()
            return await original_policy(context, action, resource, audience=audience)
        async def large_result(name, arguments, context):
            self.source.calls.append((name, arguments))
            return {'text': 'x' * (1024 * 1024)}
        self.policy.authorize = waiting_policy
        self.source.call_tool = large_result
        original = asyncio.create_task(self.call(first, 'large'))
        try:
            await asyncio.wait_for(blocked.wait(), 5)
            self.assertIsNone(self.service.sessions[first].requests['large'].task)
            self.assertEqual(self.service._result_bytes, 0)
            self.assertEqual(len(self.service._pending_tasks), 1)
            self.assertTrue(next(iter(self.service._pending_tasks)).done())
            with self.assertRaisesRegex(RuntimeError, 'active request limit'):
                await self.service.request(second, 'tools/call', {'name': 'increment'},
                    other_context, request_id='blocked-by-delivery')
            self.assertEqual(len(self.source.calls), 1)
            release.set()
            self.assertEqual(len((await original)['text']), 1024 * 1024)
            self.assertEqual(self.service._pending_tasks, set())
            await self.service.request(second, 'tools/call', {'name': 'increment'},
                other_context, request_id='now-admitted')
            self.assertEqual(len(self.source.calls), 2)
        finally:
            release.set()
            self.policy.authorize = original_policy
            await asyncio.gather(original, return_exceptions=True)

    async def test_total_tombstone_limit_applies_across_views_and_close_frees_entries(self):
        self.service = McpAppService(self.catalog, max_requests=2, max_total_requests=2)
        first, second = await self.open_view(), await self.open_view()
        await self.call(first, 'one')
        await self.call(second, 'two')
        with self.assertRaisesRegex(RuntimeError, 'service request limit'):
            await self.call(second, 'three')
        self.assertEqual(len(self.source.calls), 2)
        # Existing IDs can still replay when the ledger is full.
        await self.call(first, 'one')
        self.assertEqual(len(self.source.calls), 2)
        await self.service.close(first, self.context)
        self.assertEqual(self.service._request_count, 1)
        self.assertFalse(any(key[0] == first for key in self.service._results))
        await self.call(second, 'three')
        self.assertEqual(len(self.source.calls), 3)
        with self.assertRaisesRegex(RuntimeError, 'App request limit'):
            await self.call(second, 'four')

    async def test_large_errors_keep_bounded_terminal_strings_without_tracebacks(self):
        handle = await self.open_view()
        retained = []
        class ProviderLocal:
            pass
        async def failing(name, arguments, context):
            marker = ProviderLocal()
            retained.append(weakref.ref(marker))
            self.source.calls.append((name, {}))
            huge_local = 'private-provider-detail-' + 'x' * (2 * 1024 * 1024)
            raise ValueError(huge_local)
        self.source.call_tool = failing
        for index in range(5):
            with self.assertRaises(ValueError) as caught:
                await self.call(handle, str(index), payload='p' * (1024 * 1024))
            self.assertLessEqual(len(str(caught.exception)), 512)
        gc.collect()
        self.assertTrue(all(reference() is None for reference in retained))
        self.assertEqual(self.service._pending_tasks, set())
        self.assertEqual(self.service._result_bytes, 0)
        for record in self.service.sessions[handle].requests.values():
            self.assertIsNone(record.task)
            self.assertEqual(record.outcome, 'error')
            self.assertEqual(record.error_type, 'ValueError')
            self.assertLessEqual(len(record.error_message), 512)
        with self.assertRaises(ValueError):
            await self.call(handle, '0', payload='p' * (1024 * 1024))
        self.assertEqual(len(self.source.calls), 5)

    async def test_cancelled_request_remains_tombstoned_and_never_dispatches_again(self):
        handle = await self.open_view()
        entered = asyncio.Event()
        async def waiting(name, arguments, context):
            self.source.calls.append((name, arguments))
            entered.set()
            await asyncio.Future()
        self.source.call_tool = waiting
        original = asyncio.create_task(self.call(handle, 'cancelled'))
        await asyncio.wait_for(entered.wait(), 5)
        self.assertTrue(await self.service.cancel(handle, 'cancelled', self.context))
        with self.assertRaises(asyncio.CancelledError):
            await original
        with self.assertRaises(asyncio.CancelledError):
            await self.call(handle, 'cancelled')
        self.assertFalse(await self.service.cancel(handle, 'cancelled', self.context))
        self.assertEqual(len(self.source.calls), 1)
        self.assertIsNone(self.service.sessions[handle].requests['cancelled'].task)

    async def test_cancel_before_provider_task_starts_also_releases_task_reference(self):
        handle = await self.open_view()
        original = asyncio.create_task(self.call(handle, 'before-start'))
        await asyncio.sleep(0)
        self.assertTrue(await self.service.cancel(handle, 'before-start', self.context))
        with self.assertRaises(asyncio.CancelledError):
            await original
        with self.assertRaises(asyncio.CancelledError):
            await self.call(handle, 'before-start')
        self.assertEqual(self.source.calls, [])
        self.assertEqual(self.service._pending_tasks, set())
        self.assertIsNone(self.service.sessions[handle].requests['before-start'].task)

    async def test_shutdown_drains_retired_operations_and_never_caches_late_results(self):
        self.service = McpAppService(self.catalog, max_active_requests=1)
        handle = await self.open_view()
        entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def late_provider(name, arguments, context):
            entered.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
            return {'late': 'x' * 10000}
        self.source.call_tool = late_provider
        original = asyncio.create_task(self.call(handle, 'retired'))
        await asyncio.wait_for(entered.wait(), 5)
        await self.service.close(handle, self.context)
        await asyncio.wait_for(cancelled.wait(), 5)
        self.assertEqual(len(self.service._pending_tasks), 1)
        shutdown = asyncio.create_task(self.service.shutdown())
        try:
            await asyncio.sleep(0)
            self.assertFalse(shutdown.done())
            release.set()
            await asyncio.wait_for(shutdown, 5)
            with self.assertRaises(CapabilityUnavailable):
                await original
            self.assertEqual(self.service._pending_tasks, set())
            self.assertEqual(self.service._request_count, 0)
            self.assertEqual(self.service._results, {})
            self.assertEqual(self.service._result_bytes, 0)
        finally:
            release.set()
            await asyncio.gather(original, shutdown, return_exceptions=True)
