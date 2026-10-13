"""Real SQLite snapshot with the writable FlushFileBuffers handle contract."""
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import aiosqlite

from openagent_core.memory.operational.schema import _verified_backup


class OperationalWindowsBackup(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_flush_opens_writable_handle_and_preserves_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.sqlite3"
            async with aiosqlite.connect(path) as source:
                await source.execute("CREATE TABLE canary(id TEXT PRIMARY KEY, value TEXT)")
                await source.execute("INSERT INTO canary VALUES('human', 'unchanged')")
                await source.commit()
                modes = []
                original = Path.open
                def portable_windows_contract(file, mode="r", *args, **kwargs):
                    if file.name.endswith(".db.tmp"):
                        modes.append(mode)
                        if mode == "rb":
                            raise OSError("Windows snapshot fsync requires a writable handle")
                    return original(file, mode, *args, **kwargs)
                with patch.object(Path, "open", portable_windows_contract):
                    evidence = await _verified_backup(source, path, app_version="fixture")
                self.assertEqual(modes, ["r+b"])
                with sqlite3.connect(evidence.database_path) as restored:
                    self.assertEqual(restored.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                    self.assertEqual(restored.execute("SELECT value FROM canary WHERE id='human'").fetchone()[0], "unchanged")
                row = await (await source.execute("SELECT value FROM canary WHERE id='human'")).fetchone()
                self.assertEqual(row[0], "unchanged")
