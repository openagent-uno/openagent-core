from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from openagent_core.engine import ModelDispatcher


class ModelPins(unittest.IsolatedAsyncioTestCase):
    async def test_unavailable_and_budget_excluded_pins_are_preserved(self):
        dispatcher=ModelDispatcher([])
        dispatcher._db=SimpleNamespace(get_session_pin=AsyncMock(return_value='selected'),unpin_session=AsyncMock())
        dispatcher._configured_enabled_catalog=lambda: [SimpleNamespace(runtime_id='alternative')]
        with self.assertRaises(LookupError):await dispatcher._resolve_entry_model('session')
        dispatcher._configured_enabled_catalog=lambda: [SimpleNamespace(runtime_id='selected')]
        dispatcher._budget_guard=SimpleNamespace(filter_catalog=lambda entries: [])
        with self.assertRaises(PermissionError):await dispatcher._resolve_entry_model('session')
        dispatcher._db.unpin_session.assert_not_awaited()
        dispatcher._budget_guard=None
        decision=await dispatcher._resolve_entry_model('session')
        self.assertEqual(decision.primary_model,'selected')

    async def test_unreadable_pin_cannot_select_another_provider(self):
        dispatcher=ModelDispatcher([])
        dispatcher._db=SimpleNamespace(get_session_pin=AsyncMock(side_effect=OSError('db unavailable')))
        with self.assertRaises(RuntimeError):await dispatcher._resolve_entry_model('session')


if __name__=='__main__':unittest.main()
