"""Search sync drops consumed delete history but never the outbox head.

Before this, outbox compaction kept the latest consumed row of every source
forever, delete rows included, so every tombstoned message/tool/session left
one row behind; on long-lived agents that was ~90% of the outbox.
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import time
import unittest


async def _journal(db) -> list[tuple[int, str, str, str]]:
    rows = await (
        await db._conn.execute(
            "SELECT seq, source_kind, source_id, operation FROM search_outbox ORDER BY seq"
        )
    ).fetchall()
    return [(int(row[0]), str(row[1]), str(row[2]), str(row[3])) for row in rows]


def _deleted_sources(rows) -> set[tuple[str, str]]:
    return {(kind, source_id) for _seq, kind, source_id, operation in rows if operation == "delete"}


class SearchOutboxDeleteCompaction(unittest.IsolatedAsyncioTestCase):
    async def test_consumed_deletes_drop_their_source_history_but_never_the_head(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.memory.operational.search import sync_operational_search

        with TemporaryDirectory(prefix="openagent-outbox-delete-") as directory:
            db = MemoryDB(str(Path(directory) / "openagent.db"))
            await db.connect()
            try:
                now = time.time()
                for session_id in ("gone-chat", "kept-chat", "head-chat"):
                    await db.upsert_session(session_id, client_id="alice", title=session_id)
                    runs = [
                        {
                            "run_id": f"{session_id}-run",
                            "status": "COMPLETED",
                            "created_at": now,
                            "messages": [
                                {"id": f"{session_id}-message", "role": "user", "content": session_id}
                            ],
                        }
                    ]
                    await db._conn.execute(
                        "UPDATE sessions SET runs=?, updated_at=? WHERE session_id=?",
                        (json.dumps(runs), int(now) + 1, session_id),
                    )
                    await db._project_operational_session(session_id)
                    await db._conn.commit()
                first = await sync_operational_search(db, limit=1000)
                self.assertTrue(first.ready)
                kept = await _journal(db)

                # A second durable consumer that has not applied the deletes yet.
                await db._conn.execute(
                    "INSERT INTO search_outbox_consumers "
                    "(consumer_id, last_seq, index_generation, updated_at_ms) "
                    "VALUES ('slow-consumer', ?, 'external', ?)",
                    (first.seq, int(now * 1000)),
                )
                await db._conn.commit()
                await db.delete_session("gone-chat")
                await db.delete_session("head-chat")
                before = await _journal(db)
                deleted = _deleted_sources(before)
                head = before[-1]
                # Session + message of both chats; the head is head-chat's last delete.
                self.assertEqual(len(deleted), 4, before)
                self.assertEqual(head[3], "delete", head)
                kept = [row for row in kept if (row[1], row[2]) not in deleted]
                self.assertEqual({row[2] for row in kept if row[1] == "session"}, {"kept-chat"}, kept)

                held = await sync_operational_search(db, limit=1000)
                self.assertTrue(held.ready)
                self.assertEqual(held.seq, head[0])
                # Nothing above MIN(consumer last_seq) is touched.
                self.assertEqual(_deleted_sources(await _journal(db)), deleted)

                await db._conn.execute(
                    "UPDATE search_outbox_consumers SET last_seq=? WHERE consumer_id='slow-consumer'",
                    (head[0],),
                )
                await db._conn.commit()
                # limit=1 bounds each pass to one source; the head is never removable.
                for expected in (3, 2, 1, 1):
                    status = await sync_operational_search(db, limit=1)
                    self.assertTrue(status.ready, status)
                    self.assertEqual(status.generation, first.generation, status)
                    rows = await _journal(db)
                    self.assertEqual(len(_deleted_sources(rows)), expected, rows)
                    self.assertEqual(rows[-1], head, rows)

                # The deleted chats' history is gone; live sources and the head stay.
                self.assertEqual(await _journal(db), kept + [head])
                # last_indexed_seq == MAX(seq): no rewind, so no generation rebuild.
                final = await sync_operational_search(db, limit=1000)
                self.assertTrue(final.ready)
                self.assertEqual(final.generation, first.generation)
                self.assertEqual(final.seq, head[0])
            finally:
                await db.close()


if __name__ == "__main__":
    unittest.main()
