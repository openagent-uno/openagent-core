import unittest

import aiosqlite

from openagent_core.memory.message_parts import _wait_for_message


class AdmittedMessageProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_replay_selects_its_own_message_even_after_later_turns(self):
        async with aiosqlite.connect(":memory:") as db:
            await db.execute("CREATE TABLE session_messages (id, tenant_id, session_id, run_id, "
                             "role, sequence INTEGER, ordinal INTEGER, created_at_ms, visibility)")
            await db.executemany("INSERT INTO session_messages VALUES (?,?,?,?,?,?,0,1,'user_visible')", [
                ("old", "owner", "chat", "a", "assistant", 1),
                ("next", "owner", "chat", "b", "assistant", 2),
                ("sibling", "owner", "other", "a", "assistant", 3),
                ("foreign", "peer", "chat", "a", "assistant", 4),
                ("question", "owner", "chat", "a", "user", 5),
            ])
            async def lookup(run, role="assistant"):
                return await _wait_for_message(db, "chat", role, 10, timeout_seconds=0, admitted_run=run)

            self.assertEqual((await lookup(("owner", "a")))[0], "old")
            self.assertEqual((await lookup(("owner", "b")))[0], "next")
            self.assertEqual((await lookup(("owner", "a"), "user"))[0], "question")
            self.assertIsNone(await lookup(("owner", "missing")))
            self.assertIsNone(await lookup(("stranger", "a")))
            self.assertIsNone(await lookup(None))
