"""Writes through a borrowed transaction join it instead of locking against it.

A host that opens ``BEGIN IMMEDIATE`` and hands the connection to
``MemoryDB.from_connection`` (``AutomationRepository.definition_store`` does)
already holds the write lock. Registry writes that open their own short write
connection then waited on that lock and failed with "database is locked".
"""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import aiosqlite


class BorrowedTransactionWrites(unittest.IsolatedAsyncioTestCase):
    async def _borrowed(self, path):
        from openagent_core.memory.db import MemoryDB

        connection = await aiosqlite.connect(path, timeout=0.2)
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA busy_timeout = 200")
        await connection.execute("BEGIN IMMEDIATE")
        return connection, MemoryDB.from_connection(connection, db_path=str(path))

    async def test_mcp_registry_writes_join_the_borrowed_transaction(self):
        from openagent_core.memory.db import MemoryDB

        with TemporaryDirectory(prefix="openagent-borrowed-") as directory:
            path = Path(directory) / "openagent.db"
            owner = MemoryDB(str(path))
            await owner.connect()
            await owner.close()

            connection, borrowed = await self._borrowed(path)
            row = await borrowed.upsert_mcp("notes", kind="custom", url="https://example.test/mcp", source="host")
            self.assertEqual("notes", row["name"])
            await borrowed.set_mcp_enabled("notes", False)
            self.assertFalse((await borrowed.get_mcp("notes"))["enabled"])
            # The owner decides: a rollback leaves nothing behind.
            await connection.rollback()
            self.assertIsNone(await borrowed.get_mcp("notes"))
            await connection.close()

            connection, borrowed = await self._borrowed(path)
            await borrowed.upsert_mcp("notes", kind="custom", url="https://example.test/mcp", source="host")
            await connection.commit()
            await connection.close()
            reader = MemoryDB(str(path))
            await reader.connect()
            try:
                self.assertTrue((await reader.get_mcp("notes"))["enabled"])
                connection, borrowed = await self._borrowed(path)
                await borrowed.delete_mcp("notes")
                await connection.commit()
                await connection.close()
                self.assertIsNone(await reader.get_mcp("notes"))
            finally:
                await reader.close()

    async def test_an_owned_database_still_writes_on_its_own_connection(self):
        from openagent_core.memory.db import MemoryDB

        with TemporaryDirectory(prefix="openagent-owned-") as directory:
            db = MemoryDB(str(Path(directory) / "openagent.db"))
            await db.connect()
            try:
                await db.upsert_mcp("notes", kind="custom", url="https://example.test/mcp", source="host")
                self.assertEqual("https://example.test/mcp", (await db.get_mcp("notes"))["url"])
            finally:
                await db.close()


if __name__ == "__main__":
    unittest.main()
