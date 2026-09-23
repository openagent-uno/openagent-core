from pathlib import Path
import tempfile
import time
import unittest

from openagent_core.engine import MemoryDB


class DeletedSessionRecreation(unittest.IsolatedAsyncioTestCase):
    async def test_recreated_channel_session_clears_runtime_tombstone(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "state.sqlite3"
            memory = MemoryDB(str(database))
            await memory.connect()
            self.addAsyncCleanup(memory.close)

            session_id = "tg:stable-user"
            await memory.upsert_session(session_id, client_id="__bridge_telegram")
            await memory._conn.execute(
                "UPDATE sessions_v2 SET metadata_json="
                "json_set(metadata_json,'$.runtime_contract',1) WHERE id=?",
                (session_id,),
            )
            await memory._conn.commit()

            await memory.delete_session(session_id)
            deleted = await (
                await memory._conn.execute(
                    "SELECT status,deleted_at_ms FROM sessions_v2 WHERE id=?",
                    (session_id,),
                )
            ).fetchone()
            self.assertEqual(deleted[0], "deleted")
            self.assertIsNotNone(deleted[1])

            # Stable channel ids are reused after /clear. Recreating the
            # compatibility source must begin a clean active generation.
            await memory.upsert_session(session_id, client_id="__bridge_telegram")
            recreated = await (
                await memory._conn.execute(
                    "SELECT status,deleted_at_ms,owner_principal_id "
                    "FROM sessions_v2 WHERE id=?",
                    (session_id,),
                )
            ).fetchone()
            self.assertNotEqual(recreated[0], "deleted")
            self.assertIsNone(recreated[1])
            self.assertEqual(recreated[2], "user:__bridge_telegram")

    async def test_compatibility_upsert_does_not_restore_archived_session(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "state.sqlite3"
            memory = MemoryDB(str(database))
            await memory.connect()
            self.addAsyncCleanup(memory.close)

            session_id = "tg:archived-user"
            await memory.upsert_session(session_id, client_id="__bridge_telegram")
            archived_at_ms = int(time.time() * 1000)
            await memory._conn.execute(
                "UPDATE sessions_v2 SET metadata_json="
                "json_set(metadata_json,'$.runtime_contract',1),"
                "status='archived', deleted_at_ms=? WHERE id=?",
                (archived_at_ms, session_id),
            )
            await memory._conn.commit()

            # Ordinary provider buffer updates must preserve an explicit
            # archive. Only a deleted generation may be recreated.
            await memory.upsert_session(session_id, client_id="__bridge_telegram")
            archived = await (
                await memory._conn.execute(
                    "SELECT status,deleted_at_ms FROM sessions_v2 WHERE id=?",
                    (session_id,),
                )
            ).fetchone()
            self.assertEqual(archived[0], "archived")
            self.assertEqual(archived[1], archived_at_ms)


if __name__ == "__main__":
    unittest.main()
