"""Memory publication uses current identities and the entire audience."""
from __future__ import annotations

from dataclasses import replace
import inspect
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from openagent_core import PrincipalRef, ExecutionContext, Runtime, RuntimeSettings, RuntimeServices, RunRequest
from openagent_core.memory_access import (
    HistoryAccess, CanonicalHistorySearch, memory_resource,
    authorized_memory_hits, search_authorized_history,
)
from openagent_core.runtime import execution_scope
from openagent_storage_sqlite import SqliteRuntimeStore


class AudiencePolicy:
    def __init__(self):
        self.readers = {}
        self.calls = []

    async def authorize(self, context, action, resource, *, audience=()):
        self.calls.append((action, resource.kind, resource.resource_id, audience))
        if action not in {'memory.read', 'memory.publish'}:
            return False
        return resource.tenant_id == context.tenant_id and all(
            principal in self.readers.get((resource.kind, resource.resource_id), ())
            for principal in (context.initiator, *audience)
        )


class MemoryPrivacy(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.alice = PrincipalRef('external-host', 'tenant', 'alice')
        self.bob = PrincipalRef('external-host', 'tenant', 'bob')
        self.context = ExecutionContext(self.alice, self.alice, self.alice, 'current', 'agent', (self.alice, self.bob))
        self.policy = AudiencePolicy()
        self.store = SqliteRuntimeStore(self.path / 'state.sqlite3')
        self.runtime = Runtime(RuntimeSettings('agent', self.path), RuntimeServices(self.store, object(), self.policy, memory_access=object()))
        await self.store.start()

    async def asyncTearDown(self):
        await self.store.close()
        self.tmp.cleanup()

    async def test_canonical_audience_filter_precedes_pagination_and_revokes_live(self):
        from openagent_core.memory.db import MemoryDB
        for session, owner, audience in (
            ('private-alice', self.alice, (self.alice,)),
            ('private-bob', self.bob, (self.bob,)),
            ('shared', self.alice, (self.alice, self.bob)),
            ('unresolved-history', PrincipalRef('old', 'tenant', 'unknown'), ()),
            ('foreign', PrincipalRef('external-host', 'other', 'alice'), ()),
        ):
            context = ExecutionContext(owner, owner, owner, session, 'agent', audience or (owner,))
            await self.store.accept(RunRequest(session+'-run', session, session, 'orchid '+session), context)
            self.policy.readers[('session', session)] = set(audience)
        db = MemoryDB(str(self.store.path))
        await db.connect()
        try:
            async def resolve(principal, context):
                return HistoryAccess.from_principal(principal)
            service = CanonicalHistorySearch(db, resolve)
            self.runtime.services = replace(self.runtime.services, memory_access=service)
            shared = await search_authorized_history(self.runtime, self.context,
                query='orchid', scopes=['chats'], limit=1, offset=0, session_id=None)
            self.assertTrue(shared['ok'], shared)
            self.assertEqual([hit['target']['session_id'] for hit in shared['hits']], ['shared'])
            self.assertFalse(shared['has_more'], shared)
            self.assertIsNone(shared['next_offset'])
            self.assertEqual(set(shared['index']), {'state', 'complete'})
            private_context = replace(self.context, audience=(self.alice,))
            private = await search_authorized_history(self.runtime, private_context,
                query='orchid', scopes=['chats'], limit=25, offset=0, session_id=None)
            self.assertEqual({hit['target']['session_id'] for hit in private['hits']}, {'private-alice', 'shared'})
            # Revoke the canonical grant, while leaving the coarse runtime policy unchanged.
            self.store.connection.execute("DELETE FROM resource_acl WHERE resource_id='shared' AND principal_id=?", (self.bob.key,))
            revoked = await search_authorized_history(self.runtime, self.context,
                query='orchid', scopes=['chats'], limit=1, offset=0, session_id=None)
            self.assertEqual(revoked['hits'], [])
            self.assertFalse(revoked['has_more'])
        finally:
            await db.close()

    async def test_auto_recall_filters_before_formatting_preserves_readonly_note_discipline(self):
        from openagent_core.core import agent
        hits = [
            {'kind': 'session', 'session_id': 'private', 'title': 'PRIVATE HISTORY SENTINEL', 'score': .99},
            {'kind': 'note', 'path': 'notes/shared.md', 'title': 'Shared note', 'updated': '2026-09-16', 'score': .8},
            {'kind': 'skill', 'name': 'shared-playbook', 'score': .9},
            {'kind': 'session', 'session_id': 'unknown', 'title': 'HISTORICAL UNKNOWN SENTINEL'},
        ]
        self.policy.readers = {
            ('session', 'private'): {self.alice},
            ('vault-note', 'notes/shared.md'): {self.alice, self.bob},
            ('skill', 'shared-playbook'): {self.alice, self.bob},
        }
        with execution_scope(self.runtime, self.context, 'run'), \
             patch.object(agent, '_recall_enabled', return_value=True), \
             patch.object(agent, '_recall_candidates', return_value=hits):
            result = await agent._with_recall(object(), 'current', 'query', 'original message')
            self.assertIn('UNVERIFIED', result)
            self.assertIn('LEAD to check against current state', result)
            self.assertIn('vault_read_note', result)
            self.assertIn('notes/shared.md', result)
            self.assertIn('skill_view shared-playbook', result)
            self.assertIn('original message', result)
            self.assertNotIn('SENTINEL', result)
            self.assertTrue(all(action in {'memory.read','memory.publish'} for action, *_ in self.policy.calls))
            # Read-only memory needs no write permission. A live revocation takes effect next retrieval.
            self.policy.readers.clear()
            self.assertEqual(await agent._with_recall(object(), 'current', 'query', 'original'), 'original')

    async def test_no_service_or_wrong_session_never_searches_unscoped_history(self):
        from openagent_core.core import agent
        self.runtime.services = replace(self.runtime.services, memory_access=None)
        with patch.object(agent, '_recall_enabled', return_value=True), \
             patch.object(agent, '_recall_candidates') as gather:
            self.assertEqual(await agent._with_recall(object(), 'current', 'query', 'text'), 'text')
            with execution_scope(self.runtime, self.context, 'run'):
                self.assertEqual(await agent._with_recall(object(), 'current', 'query', 'text'), 'text')
                self.runtime.services = replace(self.runtime.services, memory_access=object())
                self.assertEqual(await agent._with_recall(object(), 'other-session', 'query', 'text'), 'text')
            gather.assert_not_called()

    async def test_actual_history_tool_uses_abstract_context_and_hides_revoked_counts(self):
        from openagent_core.mcp.servers.memory_search.adapters import build_runtime_toolkit
        hit = {'target': {'kind': 'chat_message', 'session_id': 'private', 'message_id': 'm'}, 'snippet': 'PRIVATE SENTINEL'}
        calls = []
        async def search(context, **query):
            calls.append(context)
            return {'ok': True, 'hits': [hit], 'has_more': True, 'next_offset': 6,
                'index': {'state': 'ready', 'complete': True, 'documents': 100, 'pending': 12, 'indexed_seq': 99}}
        self.runtime.services = replace(self.runtime.services, memory_access=SimpleNamespace(search_history=search))
        self.policy.readers[('session','private')] = {self.alice}
        toolkit = build_runtime_toolkit(SimpleNamespace())
        tool = toolkit.async_functions['search_past_conversations'].entrypoint
        self.assertFalse({'principal','user','tenant_id','audience'} & set(inspect.signature(tool).parameters))
        self.assertFalse((await tool('orchid'))['ok'])
        with execution_scope(self.runtime, self.context, 'run'):
            result = await tool('orchid')
        self.assertEqual(calls, [self.context])
        self.assertEqual(result['hits'], [])
        self.assertFalse(result['has_more'])
        self.assertIsNone(result['next_offset'])
        rendered = json.dumps(result)
        for hidden in ('SENTINEL', 'documents', 'pending', 'indexed_seq'):
            self.assertNotIn(hidden, rendered)
        self.assertFalse(result['index']['complete'])

    async def test_unknown_or_uncontained_resource_references_fail_closed(self):
        for hit in (
            {'kind':'note','path':'../private.md'}, {'kind':'note','path':'/private.md'},
            {'kind':'note','path':'C:\\private.md'}, {'kind':'note','path':'notes/./private.md'},
            {'kind':'note','path':'notes/ok.md','tenant_id':'foreign'},
            {'kind':'task','task_id':'unknown'}, {'target':{'kind':'chat','session_id':''}},
        ):
            self.assertIsNone(memory_resource(hit, 'tenant'), hit)
        self.assertEqual(await authorized_memory_hits(self.runtime, self.context, [None, 'unsafe']), [])


if __name__ == '__main__':
    unittest.main()
