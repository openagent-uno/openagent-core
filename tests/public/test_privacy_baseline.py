"""Read-only legacy inventory before a durable new canonical privacy cohort."""
from pathlib import Path
import sqlite3
import tempfile
import unittest

from openagent_storage_sqlite.content_privacy import SqliteContentPrivacyStore


class PrivacyBaseline(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / 'state.sqlite'

    async def test_new_cohort_survives_restart_without_claiming_existing_legacy_history(self):
        ledger = SqliteContentPrivacyStore(self.path)
        self.assertTrue(await ledger.qualify_empty_history())
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE sessions(session_id TEXT, runs TEXT)')
            db.execute("INSERT INTO sessions VALUES('future-session','[]')")
        self.assertTrue(await SqliteContentPrivacyStore(self.path).qualify_empty_history())

    async def test_old_history_is_preserved_and_cannot_be_auto_reclassified_after_deletion(self):
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE sessions(session_id TEXT, runs TEXT)')
            db.execute("INSERT INTO sessions VALUES('historical','PRIVATE UNKNOWN HISTORY')")
        ledger = SqliteContentPrivacyStore(self.path)
        self.assertFalse(await ledger.qualify_empty_history())
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT runs FROM sessions').fetchone()[0], 'PRIVATE UNKNOWN HISTORY')
            db.execute('DELETE FROM sessions')  # Only this disposable fixture.
        self.assertFalse(await SqliteContentPrivacyStore(self.path).qualify_empty_history())

    async def test_old_snapshot_blocks_new_cohort_without_modifying_backup_bytes(self):
        backups = self.root / 'backups'
        backups.mkdir()
        backup = backups / 'old.db'
        with sqlite3.connect(backup) as db:
            db.execute('CREATE TABLE session_messages(content TEXT)')
            db.execute("INSERT INTO session_messages VALUES('UNKNOWN OLD COPY')")
        original = backup.read_bytes()
        self.assertFalse(await SqliteContentPrivacyStore(self.path).qualify_empty_history())
        self.assertEqual(backup.read_bytes(), original)

    async def test_empty_bootstrap_snapshot_is_allowed_but_unreadable_and_symlink_backups_are_not(self):
        backups = self.root / 'backups'
        backups.mkdir()
        with sqlite3.connect(backups / 'initial.db') as db:
            db.execute('CREATE TABLE sessions(session_id TEXT)')
        self.assertTrue(await SqliteContentPrivacyStore(self.path).qualify_empty_history())
        other = self.root / 'other.sqlite'
        (backups / 'broken.db').write_bytes(b'not a SQLite database')
        self.assertFalse(await SqliteContentPrivacyStore(other).qualify_empty_history())
        (backups / 'broken.db').unlink()
        (backups / 'linked.db').symlink_to(backups / 'initial.db')
        self.assertFalse(await SqliteContentPrivacyStore(self.root / 'third.sqlite').qualify_empty_history())
