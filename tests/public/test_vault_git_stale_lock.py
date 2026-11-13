"""An abandoned index.lock must not silently stop vault versioning."""
import asyncio
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest

from openagent_core.memory.vault.gitrepo import VaultGit
from openagent_core.memory.vault.service import VaultService


class VaultGitStaleLock(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        git_bin = shutil.which("git")
        if not git_bin:
            self.skipTest("Actual Git is required")
        self.git = VaultGit(self.root / "vault", git_bin=git_bin)
        self.git._gitconfig = self.root / "private.gitconfig"
        self.assertTrue(self.git.ensure_repo())
        self.lock = self.git.vault_root / ".git" / "index.lock"

    def _lock(self, age_s):
        self.lock.write_bytes(b"")
        then = time.time() - age_s
        os.utime(self.lock, (then, then))

    def test_abandoned_lock_is_removed_and_the_change_committed(self):
        (self.git.vault_root / "note.md").write_text("Note\n")
        self._lock(3600)
        with self.assertLogs("openagent_core.memory.vault.gitrepo", "WARNING") as logs:
            head = self.git.commit(["note.md"], "Fixture", ["Origin: test"])
        self.assertTrue(head)
        self.assertFalse(self.lock.exists())
        self.assertIn("stale index.lock", "\n".join(logs.output))
        (self.git.vault_root / "other.md").write_text("Other\n")
        self._lock(3600)
        self.assertTrue(self.git.commit_all("Sweep", ["Origin: test"]))

    def test_recent_lock_is_kept_and_the_failure_is_reported(self):
        (self.git.vault_root / "note.md").write_text("Note\n")
        self._lock(5)
        with self.assertLogs("openagent_core.memory.vault.gitrepo", "WARNING") as logs:
            self.assertIsNone(self.git.commit(["note.md"], "Fixture", ["Origin: test"]))
        self.assertTrue(self.lock.exists())
        self.assertIn("git add failed", "\n".join(logs.output))


class ValidateNoteSeesFreshNotes(unittest.TestCase):
    def test_link_to_a_note_written_by_another_writer_resolves(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "vault"
            root.mkdir()
            service = VaultService(root, Path(temp) / "index.db", git_enabled=False)

            async def scenario():
                await service.sync()
                # Another writer adds the target after the index was built.
                (root / "target.md").write_text("---\ntitle: Target\n---\nBody\n")
                result = await service.validate_note("source.md", "---\ntitle: Source\n---\nSee [[target]].\n")
                await service.close()
                return result

            result = asyncio.run(scenario())
            self.assertFalse([i for i in result["issues"] if i["rule"] == "broken_link"], result)


if __name__ == "__main__":
    unittest.main()
