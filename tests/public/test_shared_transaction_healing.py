"""A swallowed failed write must not pin the shared connection's WAL snapshot.

Reproduces a production incident: a DML statement on MemoryDB's shared
connection failed on a lock, the caller swallowed the error without rolling
back, and the connection stayed inside the implicit transaction. The next read
pinned a snapshot, the WAL grew from 5MB to 126MB over four hours, and every
later write on that connection was refused until the process restarted.
"""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import sqlite3
import time
import unittest


_INSERT = (
    "INSERT INTO vault_save_reminders (session_id, turn_count, created_at, updated_at) "
    "VALUES (?, 1, 0, 0)"
)


async def _wedge_shared_connection(db, path):
    """Fail a DML on the shared connection without a rollback.

    Returns the external writer, whose lock is already released.
    """
    conn = await db._ensure_connected()
    await conn.execute("PRAGMA busy_timeout = 50")
    holder = sqlite3.connect(path, isolation_level=None)
    holder.execute("BEGIN IMMEDIATE")
    try:
        await conn.execute(_INSERT, ("wedge",))
    except sqlite3.OperationalError:
        pass  # Expected: the lock is held; the caller "forgets" to roll back.
    holder.execute("ROLLBACK")
    return holder


class SharedTransactionHealing(unittest.IsolatedAsyncioTestCase):
    def test_budget_is_read_at_runtime_and_bounded(self):
        from openagent_core.memory.db import shared_txn_max_seconds

        with patch.dict("os.environ", {"OPENAGENT_SHARED_TXN_MAX_SECONDS": ""}):
            self.assertEqual(shared_txn_max_seconds(), 300.0)
        with patch.dict("os.environ", {"OPENAGENT_SHARED_TXN_MAX_SECONDS": "abc"}):
            self.assertEqual(shared_txn_max_seconds(), 300.0)
        with patch.dict("os.environ", {"OPENAGENT_SHARED_TXN_MAX_SECONDS": "1"}):
            self.assertEqual(shared_txn_max_seconds(), 5.0)
        with patch.dict("os.environ", {"OPENAGENT_SHARED_TXN_MAX_SECONDS": "42"}):
            self.assertEqual(shared_txn_max_seconds(), 42.0)

    async def test_vault_reminder_failed_write_rolls_back(self):
        from openagent_core.memory.db import MemoryDB
        from openagent_core.learning import vault_reminder

        with TemporaryDirectory(prefix="openagent-wedge-reminder-") as directory:
            path = Path(directory) / "openagent.db"
            db = MemoryDB(str(path))
            await db.connect()
            holder = None
            try:
                conn = await db._ensure_connected()
                await conn.execute("PRAGMA busy_timeout = 50")
                holder = sqlite3.connect(path, isolation_level=None)
                holder.execute("BEGIN IMMEDIATE")
                self.assertEqual(await vault_reminder._bump_turn_count(db, "s-1"), 0)
                holder.execute("ROLLBACK")
                self.assertFalse(conn.in_transaction, "failed write left the shared connection in a transaction")
                # Another writer advances the database; the shared connection can still write.
                holder.execute(_INSERT, ("other",))
                self.assertEqual(await vault_reminder._bump_turn_count(db, "s-1"), 1)
                busy, frames, copied = holder.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                self.assertEqual(copied, frames, (busy, frames, copied))
            finally:
                if holder is not None:
                    holder.close()
                await db.close()

    async def test_orphaned_shared_transaction_is_closed_after_the_budget(self):
        from openagent_core.memory.db import MemoryDB

        with TemporaryDirectory(prefix="openagent-wedge-heal-") as directory:
            path = Path(directory) / "openagent.db"
            db = MemoryDB(str(path))
            await db.connect()
            holder = None
            try:
                holder = await _wedge_shared_connection(db, path)
                conn = db._conn
                self.assertTrue(conn.in_transaction, "reproduction did not leave a transaction open")
                # The snapshot is pinned and another writer moves on: the checkpoint stalls.
                cursor = await conn.execute("SELECT count(*) FROM vault_save_reminders")
                await cursor.fetchone()
                await cursor.close()
                holder.execute(_INSERT, ("other",))
                busy, frames, copied = holder.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                self.assertLess(copied, frames, "WAL was not pinned; reproduction is not faithful")

                # The first pass records when the transaction was seen; under budget nothing changes.
                await db._ensure_connected()
                self.assertTrue(conn.in_transaction)
                # Past the budget the orphaned transaction is closed.
                db._shared_txn_seen_since = time.monotonic() - 10_000
                await db._ensure_connected()
                self.assertFalse(conn.in_transaction)
                busy, frames, copied = holder.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                self.assertEqual(copied, frames, (busy, frames, copied))
                await conn.execute("PRAGMA busy_timeout = 2000")
                await conn.execute(_INSERT, ("after",))
                await conn.commit()
            finally:
                if holder is not None:
                    holder.close()
                await db.close()

    async def test_borrowed_host_transaction_is_never_closed(self):
        import aiosqlite
        from openagent_core.memory.db import MemoryDB

        with TemporaryDirectory(prefix="openagent-borrowed-txn-") as directory:
            path = Path(directory) / "openagent.db"
            owner = MemoryDB(str(path))
            await owner.connect()
            await owner.close()
            async with aiosqlite.connect(path) as host:
                await host.execute("BEGIN IMMEDIATE")
                await host.execute(_INSERT, ("host",))
                borrowed = MemoryDB.from_connection(host, db_path=str(path))
                borrowed._shared_txn_seen_since = time.monotonic() - 10_000
                await borrowed._ensure_connected()
                self.assertTrue(host.in_transaction)
                await host.rollback()


if __name__ == "__main__":
    unittest.main()
