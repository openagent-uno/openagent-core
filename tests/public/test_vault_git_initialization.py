"""Real Git acceptance and initialization failure boundaries (no overlays)."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.memory.vault.gitrepo import VaultGit, _redact_url


class VaultGitInitialization(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git_bin = shutil.which("git")
        if not self.git_bin:
            self.skipTest("Actual Git is required")
        self.git = VaultGit(self.root / "vault", git_bin=self.git_bin)
        self.git._gitconfig = self.root / "private.gitconfig"

    def test_real_repository_commits_and_history_without_recursive_initialization(self):
        self.assertTrue(self.git.ensure_repo())
        self.assertTrue(self.git._repo_ready)
        self.assertEqual(len(self.git.log()), 1)
        (self.git.vault_root / "human.md").write_text("Human note\n")
        self.assertTrue(self.git.commit(["human.md"], "Human fixture", ["Origin: human"]))
        self.assertTrue(self.git.ensure_repo())
        self.assertEqual(len(self.git.log()), 2)

    def test_init_failure_never_recurses_and_hides_credentials(self):
        response = subprocess.CompletedProcess([], 9, "", "https://user:fixture-secret@invalid.example/")
        with patch.object(self.git, "_git", return_value=response) as command:
            with self.assertLogs("openagent_core.memory.vault.gitrepo", "WARNING") as captured:
                self.assertFalse(self.git.ensure_repo())
        self.assertEqual(command.call_count, 2)
        self.assertFalse(self.git._repo_ready)
        self.assertNotIn("fixture-secret", str(captured.output))

    def test_false_success_without_repository_never_publishes_readiness(self):
        with patch.object(self.git, "_git", return_value=subprocess.CompletedProcess([], 0, "", "")) as command:
            self.assertFalse(self.git.ensure_repo())
        self.assertEqual(command.call_count, 1)
        self.assertFalse(self.git._repo_ready)

    def test_config_failure_keeps_repository_not_readiness(self):
        original = self.git._git
        def run(*args, **kwargs):
            if args[0] == "config":
                return subprocess.CompletedProcess([], 8, "", "fixture-secret")
            return original(*args, **kwargs)
        with patch.object(self.git, "_git", side_effect=run):
            self.assertFalse(self.git.ensure_repo())
        self.assertTrue(self.git.is_repo())
        self.assertFalse(self.git._repo_ready)

    def test_legacy_init_fallback_has_real_commits(self):
        original = self.git._git
        def run(*args, **kwargs):
            if args == ("init", "-b", "main"):
                return subprocess.CompletedProcess([], 129, "", "")
            return original(*args, **kwargs)
        with patch.object(self.git, "_git", side_effect=run):
            self.assertTrue(self.git.ensure_repo())
        self.assertEqual(len(self.git.log()), 1)

    def test_exception_and_url_logs_do_not_expose_userinfo(self):
        with patch.object(self.git, "_git", side_effect=RuntimeError("fixture-secret")):
            with self.assertLogs("openagent_core.memory.vault.gitrepo", "WARNING") as captured:
                self.assertFalse(self.git.ensure_repo())
        self.assertFalse(self.git._repo_ready)
        self.assertNotIn("fixture-secret", str(captured.output))
        self.assertEqual(_redact_url("https://user:fixture-secret@invalid.example/"), "https://***@invalid.example/")
