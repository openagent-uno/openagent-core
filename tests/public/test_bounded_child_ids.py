"""Real delegation handler keeps bounded IDs and exact durable parent metadata."""
import asyncio
from types import SimpleNamespace
import re

import pytest

from openagent_core.core.child_session import mint_child_session_id
from openagent_core.mcp.servers.delegation import handlers


@pytest.mark.parametrize('parent', ['ses_' + 'a' * 32, '根 session/' + 'é' * 2000])
@pytest.mark.parametrize('model', ['claude qa inference:claude-sonnet-4-6', 'Fornitore 日本語/à:modello Ω', 'x' * 4000])
def test_delegation_ids_are_bounded_unique_and_do_not_accumulate_parent(parent, model):
    root = parent
    seen = set()
    for _ in range(32):
        child = mint_child_session_id('delegation', {'parent_session_id': parent, 'model_id': model})
        assert len(child.encode()) <= 128 and re.fullmatch(r'[A-Za-z0-9_.:@-]+', child)
        assert child not in seen
        if root.startswith('ses_'):
            assert child.startswith(root + '::sub::')
        seen.add(child)
        parent = child


def test_real_delegate_handler_nested_three_levels_preserves_parent_model_and_owner():
    async def run():
        rows = {}
        routed = []
        calls = []
        model = 'provider 日本語 / account:' + ('long model ' * 100).rstrip()
        root = 'ses_' + 'a' * 32

        class DB:
            async def get_session(self, sid):
                return rows.get(sid, {'client_id': 'authorized-owner'})
            async def upsert_session(self, sid, **kwargs):
                rows[sid] = kwargs

        db = DB()

        class Agent:
            name = 'QA'
            model = SimpleNamespace(build_override_model=lambda label: routed.append(label) or label)
            async def run(self, *, message, session_id, model_override, **kwargs):
                assert model_override == model
                calls.append(session_id)
                depth = int(message)
                if depth:
                    tokens = handlers.install_context(session_id=session_id, pool=None, db=db, dispatcher=None,
                                                      agent=self, owner_handle='authorized-owner')
                    try:
                        result = await handlers.delegate_task(str(depth - 1), model_id=model)
                    finally:
                        handlers.reset_context(tokens)
                    assert result['status'] == 'ok', result
                return 'leaf'
            async def release_session(self, *_args, **_kwargs): pass

        agent = Agent()
        tokens = handlers.install_context(session_id=root, pool=None, db=db, dispatcher=None, agent=agent,
                                          owner_handle='authorized-owner')
        try:
            result = await handlers.delegate_task('2', model_id=model)
        finally:
            handlers.reset_context(tokens)
        assert result['status'] == 'ok', result
        assert len(calls) == 3 and len(set(calls)) == 3
        assert routed == [model] * 3
        for index, child in enumerate(calls):
            assert len(child.encode()) <= 128 and child.startswith(root + '::sub::')
            assert rows[child]['parent_session_id'] == (root if index == 0 else calls[index-1])
            assert rows[child]['client_id'] == 'authorized-owner'
            assert rows[child]['model'] == model
    asyncio.run(run())
