"""Frozen modules use importable wheel code, not a copied source tree."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from openagent_core.mcp.builtins import resolve_builtin_entry


class FrozenModuleResolution(unittest.TestCase):
    def test_python_mcp_resolves_from_archive_without_source_directory(self):
        with tempfile.TemporaryDirectory() as root:
            bundle = Path(root)
            with patch("openagent_core.mcp.builtins.is_frozen", return_value=True), \
                 patch("openagent_core.mcp.builtins.bundle_dir", return_value=bundle):
                spec = resolve_builtin_entry("budget-manager", env={"OPENAGENT_DB_PATH": str(bundle / "state.sqlite3")})
            self.assertFalse((bundle / "src").exists())
            self.assertEqual(spec["command"], [sys.executable, "_mcp-server", "budget-manager"])
            self.assertEqual(spec["_cwd"], str(bundle))
            self.assertEqual(spec["env"]["OPENAGENT_DB_PATH"], str(bundle / "state.sqlite3"))
            self.assertEqual(spec["_trusted_module"], "budget-manager")

    def test_missing_frozen_module_fails_before_a_subprocess_is_launched(self):
        with tempfile.TemporaryDirectory() as root:
            with patch("openagent_core.mcp.builtins.is_frozen", return_value=True), \
                 patch("openagent_core.mcp.builtins.bundle_dir", return_value=Path(root)), \
                 patch("importlib.util.find_spec", return_value=None) as find:
                with self.assertRaisesRegex(FileNotFoundError, "missing from the frozen distribution"):
                    resolve_builtin_entry("budget-manager")
                find.assert_called_once_with("openagent_core.mcp.servers.budget_manager.server")


if __name__ == "__main__":
    unittest.main()
