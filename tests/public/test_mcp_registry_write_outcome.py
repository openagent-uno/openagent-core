"""MCP registry writes report their real outcome under concurrent readers."""
import asyncio
import tempfile
import unittest
from pathlib import Path

from openagent_core.memory.db import MemoryDB


class RegistryWriteOutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = MemoryDB(str(Path(self.tmp.name) / "state.sqlite"))
        await self.db.connect()
        for name in ("alpha", "beta", "gamma"):
            await self.db.upsert_mcp(name, kind="custom", url=f"https://{name}.example/mcp")

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def _open_sibling_write(self):
        # A sibling coroutine (a running turn numbering its messages with
        # INSERT ... RETURNING) holds an unfinished write statement on the
        # shared connection: a COMMIT there fails with "SQL statements in
        # progress" until that statement is reset.
        conn = self.db._conn
        await conn.execute("CREATE TABLE IF NOT EXISTS sibling_rows (a INTEGER)")
        await conn.executemany("INSERT INTO sibling_rows VALUES (?)", [(1,), (2,), (3,)])
        await conn.commit()
        cursor = await conn.execute("INSERT INTO sibling_rows SELECT a FROM sibling_rows RETURNING a")
        self.assertIsNotNone(await cursor.fetchone())
        return cursor

    async def _finish_sibling(self, cursor, delay=0.3):
        await asyncio.sleep(delay)
        await cursor.close()
        await self.db._conn.commit()

    async def test_registration_during_a_sibling_write_reports_the_real_outcome(self):
        cursor = await self._open_sibling_write()
        finisher = asyncio.create_task(self._finish_sibling(cursor))
        row = await self.db.upsert_mcp(
            "staff", kind="custom", url="https://staff.example/mcp",
            headers={"Authorization": "Bearer ${secret:OPENAGENT_MCP_SECRET_STAFF}"},
        )
        await finisher
        self.assertEqual("staff", row["name"])
        self.assertEqual(row, await self.db.get_mcp("staff"))

    async def test_enable_and_delete_during_a_sibling_write_succeed(self):
        cursor = await self._open_sibling_write()
        finisher = asyncio.create_task(self._finish_sibling(cursor))
        await self.db.set_mcp_enabled("beta", False)
        await self.db.delete_mcp("gamma")
        await finisher
        self.assertFalse((await self.db.get_mcp("beta"))["enabled"])
        self.assertIsNone(await self.db.get_mcp("gamma"))

    async def _open_reader(self):
        # A sibling coroutine iterating a multi-row result on the shared
        # connection: its statement stays in progress between fetches.
        cursor = await self.db._conn.execute("SELECT name FROM mcps ORDER BY name")
        self.assertIsNotNone(await cursor.fetchone())
        return cursor

    async def test_upsert_succeeds_and_returns_the_committed_row_while_a_reader_is_open(self):
        cursor = await self._open_reader()
        try:
            row = await self.db.upsert_mcp(
                "delta", kind="custom", url="https://delta.example/mcp",
                headers={"Authorization": "Bearer ${secret:OPENAGENT_MCP_SECRET_DELTA}"},
            )
        finally:
            await cursor.close()
        self.assertEqual("delta", row["name"])
        self.assertEqual("https://delta.example/mcp", row["url"])
        self.assertEqual({"Authorization": "Bearer ${secret:OPENAGENT_MCP_SECRET_DELTA}"}, row["headers"])
        self.assertEqual(row, await self.db.get_mcp("delta"))

    async def test_enable_and_delete_succeed_while_a_reader_is_open(self):
        cursor = await self._open_reader()
        try:
            await self.db.set_mcp_enabled("beta", False)
            await self.db.delete_mcp("gamma")
        finally:
            await cursor.close()
        self.assertFalse((await self.db.get_mcp("beta"))["enabled"])
        self.assertIsNone(await self.db.get_mcp("gamma"))

    async def test_a_failed_registry_write_leaves_nothing_behind(self):
        with self.assertRaises(ValueError):
            await self.db.upsert_mcp("", kind="custom", url="https://x.example/mcp")
        self.assertEqual(["alpha", "beta", "gamma"], [r["name"] for r in await self.db.list_mcps()])


if __name__ == "__main__":
    unittest.main()
