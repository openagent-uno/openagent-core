"""Public history survives interrupted writers; ancestry follows canonical links."""
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from openagent_core import ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.administration import ManagementContext
from openagent_core.engine import MemoryDB
from openagent_core.session_transcript import expand_run_messages
from openagent_core.workspace_administration import SessionInspection
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    denied = False
    async def authorize(self, context, action, resource, *, audience=()):
        return not self.denied and context.authority.subject_id == 'alice'


class DurablePublicHistory(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.path = Path(self.temporary.name)
        self.db = MemoryDB(str(self.path / 'state.sqlite'))
        await self.db.connect()
        self.store = SqliteRuntimeStore(self.path / 'state.sqlite')
        await self.store.start()
        self.alice = PrincipalRef('host', 'tenant', 'alice', 'user')
        self.context = ExecutionContext(self.alice, self.alice, self.alice, 'parent', 'agent', (self.alice,))
        self.management = ManagementContext(self.alice, 'agent')
        self.policy = Policy()
        self.inspection = SessionInspection(self.db, self.policy)

    async def asyncTearDown(self):
        await self.store.close()
        await self.db.close()
        self.temporary.cleanup()

    async def test_reopen_recovers_accepted_input_completed_and_partial_tools_without_legacy_mirror(self):
        await self.store.accept(RunRequest('accepted', 'parent', 'accepted', 'Original input', attachments=({'file_id': 'host-file'},)), self.context)
        await self.store.transition('accepted', 'running')
        await self.store.append_event('accepted', 'run.stream', {'kind': 'delta', 'text': 'Before tool'})
        await self.store.begin_tool('accepted', 'done-call', {'source_id': 'fixture', 'name': 'read_fixture'}, {'name': 'synthetic'})
        await self.store.finish_tool('accepted', 'done-call', result={'value': 'completed'})
        await self.store.append_event('accepted', 'run.stream', {'kind': 'delta', 'text': 'After tool'})
        await self.store.begin_tool('accepted', 'pending-call', {'source_id': 'fixture', 'name': 'pending_fixture'}, {})
        await self.store.close(); await self.db.close()
        self.db = MemoryDB(str(self.path / 'state.sqlite')); await self.db.connect()
        self.store = SqliteRuntimeStore(self.path / 'state.sqlite'); await self.store.start()
        await self.store.recover()
        self.inspection = SessionInspection(self.db, self.policy)
        self.assertEqual('Original input', (await self.store.accepted_request('accepted')).request.input)
        self.assertEqual('interrupted', (await self.store.get('accepted')).status)
        before = await self.store.events('accepted')
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(['user', 'assistant', 'tool', 'assistant', 'tool'], [m['role'] for m in result['messages']])
        self.assertEqual('Original input', result['messages'][0]['text'])
        self.assertEqual('accepted', result['messages'][0]['runId'])
        self.assertEqual([{'file_id': 'host-file'}], result['messages'][0]['attachments'])
        tools = [m['toolInfo'] for m in result['messages'] if m['role'] == 'tool']
        self.assertEqual(['done-call', 'pending-call'], [t['tool_call_id'] for t in tools])
        self.assertFalse(tools[0]['tool_call_error'])
        self.assertTrue(tools[1]['tool_call_error'])
        self.assertIn('runtime_restarted', str(tools[1]['result']))
        self.assertEqual(before, await self.store.events('accepted'))  # GET never writes/replays.
        self.policy.denied = True
        with self.assertRaises(PermissionError):
            await self.inspection.transcript(self.management, 'parent')

    def test_cancelled_legacy_turn_keeps_original_partial_answer_and_completed_tool(self):
        run = {'run_id': 'cancelled', 'status': 'CANCELLED', 'messages': [
            {'role': 'user', 'content': 'Original'}, {'role': 'assistant', 'content': 'Partial'},
            {'role': 'tool', 'content': 'Completed result', 'tool_call_id': 'call', 'name': 'read_fixture'}],
            'tools': [{'tool_call_id': 'call', 'tool_name': 'read_fixture', 'result': 'Completed result'}]}
        result = expand_run_messages(run, timestamp=1, msg_counter=[0])
        self.assertEqual(['Original', 'Partial', 'Completed result'], [m['text'] for m in result])

    async def test_ordinary_spawn_chat_origin_has_exact_ancestry_and_rejects_wrong_root(self):
        async def execute(request, context, runtime):
            if request.run_id == 'parent-run':
                await runtime.spawn(RunRequest('child-run', 'child', 'child', 'Task'), context)
            return 'Synthetic answer'
        runtime = Runtime(RuntimeSettings('agent', self.path), RuntimeServices(self.store, SimpleNamespace(execute=execute), self.policy))
        await runtime.start()
        try:
            await runtime.submit(RunRequest('parent-run', 'parent', 'parent', 'Delegate'), self.context)
            await runtime.wait('parent-run', self.context)
            await self.db.upsert_session('child', title='Child', parent_session_id='parent', origin='chat')
            self.assertEqual({'ancestries': {'child': ['parent']}}, await self.inspection.ancestries('tenant', ['child']))
            self.assertEqual({'ancestries': {}}, await self.inspection.ancestries('other', ['child']))
            conn = await self.db._ensure_connected()
            await self.store.accept(RunRequest('unrelated-run', 'unrelated', 'unrelated', 'Other'), replace(self.context, session_id='unrelated'))
            await conn.execute("UPDATE sessions_v2 SET root_session_id='unrelated' WHERE id='child'"); await conn.commit()
            self.assertEqual({'ancestries': {}}, await self.inspection.ancestries('tenant', ['child']))
        finally:
            await runtime.close()

    async def test_union_is_ordered_limited_and_exactly_deduplicates_provider_mirror(self):
        for name in ('first', 'second', 'third'):
            await self.store.accept(RunRequest(name, 'parent', name, name), self.context)
        await self.db.upsert_session('parent', title='History')
        conn = await self.db._ensure_connected()
        rows = await (await conn.execute("SELECT created_at_ms FROM session_runs WHERE id='second'")).fetchone()
        mirror = [{'run_id': 'second', 'created_at': rows[0]/1000, 'messages': [
            {'role': 'user', 'content': 'Provider prepared'}, {'role': 'assistant', 'content': 'Provider detail'}]}]
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'", (json.dumps(mirror),)); await conn.commit()
        result = await self.inspection.transcript(self.management, 'parent', limit=2)
        self.assertEqual(['second', 'Provider detail', 'third'], [m['text'] for m in result['messages']])
        self.assertEqual(['second', 'second', 'third'], [m['runId'] for m in result['messages']])

    async def test_journal_status_and_tool_events_do_not_duplicate_done_text_or_tool(self):
        await self.store.accept(RunRequest('stream', 'parent', 'stream', 'Input'), self.context)
        await self.store.transition('stream', 'running')
        await self.store.append_event('stream', 'run.stream', {'kind': 'delta', 'text': 'Before'})
        await self.store.begin_tool('stream', 'internal-uuid', {'source_id': 'fixture', 'name': 'read_fixture', 'provider_call_id': 'toolu_provider'}, {})
        await self.store.append_event('stream', 'run.status', {'text': json.dumps({'tool_name': 'read_fixture', 'tool_call_id': 'toolu_provider', 'result': "{'value': 'Result'}"})})
        await self.store.finish_tool('stream', 'internal-uuid', result={'value': 'Result'})
        await self.store.append_event('stream', 'run.stream', {'kind': 'delta', 'text': 'After'})
        await self.store.append_event('stream', 'run.status', {'text': json.dumps({'tool_name': 'read_fixture', 'tool_call_id': 'toolu_provider', 'result': None, 'tool_call_error': False})})
        await self.store.append_event('stream', 'run.stream', {'kind': 'done', 'text': 'BeforeAfter'})
        await self.store.transition('stream', 'success', output='BeforeAfter')
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(['Input', 'Before', json.dumps({'value': 'Result'}), 'After'], [m['text'] for m in result['messages']])
        self.assertEqual(['stream'] * 4, [m['runId'] for m in result['messages']])
        self.assertEqual('toolu_provider', result['messages'][2]['toolInfo']['tool_call_id'])
        self.assertEqual({'value': 'Result'}, result['messages'][2]['toolInfo']['result'])

    async def test_same_session_other_agent_or_authority_is_not_unioned(self):
        await self.store.accept(RunRequest('visible', 'parent', 'visible', 'Visible'), self.context)
        with self.assertRaises(PermissionError):
            await self.store.accept(RunRequest('other-agent', 'parent', 'other-agent', 'PRIVATE AGENT'), replace(self.context, agent_id='other'))
        foreign = PrincipalRef('other-host', 'tenant', 'alice', 'user')
        await self.store.accept(RunRequest('other-authority', 'parent', 'other-authority', 'PRIVATE AUTHORITY'), replace(self.context, authority=foreign))
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(['Visible'], [m['text'] for m in result['messages']])
        with self.assertRaises(PermissionError):
            await self.inspection.transcript(ManagementContext(replace(self.alice, tenant_id='other'), 'agent'), 'parent')

    async def test_late_provider_started_status_never_erases_canonical_tool_error(self):
        await self.store.accept(RunRequest('failed-tool', 'parent', 'failed-tool', 'Input'), self.context)
        await self.store.transition('failed-tool', 'running')
        await self.store.begin_tool('failed-tool', 'internal', {'source_id':'fixture','name':'read_fixture','provider_call_id':'toolu_error'}, {})
        await self.store.finish_tool('failed-tool', 'internal', error={'reason':'synthetic failure'})
        await self.store.append_event('failed-tool', 'run.status', {'text':json.dumps({'tool_name':'read_fixture','tool_call_id':'toolu_error','result':None,'tool_call_error':False})})
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(2, len(result['messages']))
        self.assertTrue(result['messages'][1]['toolInfo']['tool_call_error'])
        self.assertEqual({'reason':'synthetic failure'}, result['messages'][1]['toolInfo']['result'])

    async def test_wrapped_legacy_alias_requires_exact_source_envelope(self):
        rid = 'run:parent:provider'
        await self.store.accept(RunRequest(rid, 'parent', 'wrapped', 'Canonical original'), self.context)
        await self.db.upsert_session('parent', title='Wrapped')
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'", (json.dumps([{'run_id':'provider','messages':[{'role':'user','content':'Provider prepared'}]}]),))
        await conn.commit()
        # The ID prefix alone is not an attestation: both exact distinct runs
        # remain until the historical writer's raw envelope binds the alias.
        unbound = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(2, len(unbound['messages']))
        await conn.execute('UPDATE session_runs SET raw_envelope_json=? WHERE id=?', (json.dumps({'run_id':'provider'}),rid)); await conn.commit()
        bound = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(1, len(bound['messages']))
        self.assertEqual('Canonical original', bound['messages'][0]['text'])

    async def test_cancelled_accepted_turn_has_no_synthetic_assistant_and_legacy_fallback_remains(self):
        await self.store.accept(RunRequest('cancelled', 'parent', 'cancelled', 'Accepted original'), self.context)
        await self.store.transition('cancelled', 'cancelled', output={'reason': 'Cancelled diagnostic'})
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(['user'], [m['role'] for m in result['messages']])
        self.assertEqual('Accepted original', result['messages'][0]['text'])
        await self.db.upsert_session('parent', title='Legacy')
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'", (json.dumps([{'run_id':'legacy-only', 'messages':[{'role':'user','content':'Legacy original'}]}]),)); await conn.commit()
        legacy = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual('Legacy original', legacy['messages'][0]['text'])
        self.assertNotIn('runId', legacy['messages'][0])

    async def test_reauthorization_before_publication_and_ancestry_cycles_fail_closed(self):
        await self.store.accept(RunRequest('parent', 'parent', 'parent', 'Original'), self.context)
        original = self.inspection._transcript_runs
        async def revoke(*args):
            rows = await original(*args)
            self.policy.denied = True
            return rows
        self.inspection._transcript_runs = revoke
        with self.assertRaises(PermissionError):
            await self.inspection.transcript(self.management, 'parent')
        self.policy.denied = False
        child_context = self.context.child(session_id='child', run_id='parent', agent=PrincipalRef('host','tenant','agent','agent'))
        await self.store.accept(RunRequest('child-run', 'child', 'child', 'Task'), child_context)
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE sessions_v2 SET parent_session_id='child',root_session_id='parent' WHERE id='parent'"); await conn.commit()
        self.assertEqual({'ancestries': {}}, await self.inspection.ancestries('tenant', ['child']))

    async def test_canonical_only_media_is_path_free_fresh_and_preserves_authors(self):
        from openagent_core.artifacts import ArtifactRepository
        bob = PrincipalRef('host', 'tenant', 'bob', 'user')
        await self.store.accept(RunRequest('media', 'parent', 'media', 'Original with media'), replace(self.context, author=bob))
        ref = await ArtifactRepository(self.db, self.policy).upload(self.context, b'synthetic png', filename='result.png', mime_type='image/png', session_id='parent')
        await self.store.append_event('media', 'run.stream', {'kind':'delta', 'text':'Published'})
        await self.store.append_event('media', 'run.stream', {'kind':'done', 'text':'Published',
            'parts':[{'kind':'text','text':'Published'}, {'kind':'attachment','attachment':ref}]})
        await self.store.transition('media', 'success', output='Published')
        result = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual(bob.key, result['messages'][0]['author']['handle'])
        answer = result['messages'][1]
        self.assertEqual('Published', answer['text'])
        self.assertEqual('media', answer['runId'])
        self.assertEqual(ref['artifact_id'], answer['attachments'][0]['artifact_id'])
        self.assertEqual('result.png', answer['attachments'][0]['filename'])
        self.assertNotIn('path', answer['attachments'][0])
        conn = await self.db._ensure_connected()
        await conn.execute('UPDATE artifacts SET deleted_at_ms=created_at_ms+1 WHERE id=?', (ref['artifact_id'],)); await conn.commit()
        denied = await self.inspection.transcript(self.management, 'parent')
        self.assertEqual([], denied['messages'][1]['attachments'])

    async def test_equal_admission_timestamps_use_canonical_ordinal(self):
        for name in ('first', 'second', 'third'):
            await self.store.accept(RunRequest(name, 'parent', name, name), self.context)
        await self.db.upsert_session('parent', title='History')
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE session_runs SET created_at_ms=1000 WHERE session_id='parent'")
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'", (json.dumps([{'run_id':'second','created_at':1,'messages':[{'role':'user','content':'prepared'}]}]),)); await conn.commit()
        result = await self.inspection.transcript(self.management, 'parent', limit=2)
        self.assertEqual(['second','third'], [m['text'] for m in result['messages']])


if __name__ == '__main__':
    unittest.main()
