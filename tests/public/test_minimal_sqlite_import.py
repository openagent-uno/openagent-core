"""The synchronous store must work without optional engine dependencies."""
import os
from pathlib import Path
import subprocess
import sys


def test_minimal_store_and_privacy_guard_without_site_packages():
    root = Path(__file__).resolve().parents[2]
    script = """
import sqlite3, sys
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.migration import _verified_legacy_projection_domain
from openagent_core.memory.operational.privacy_snapshots import guard_snapshot_transaction
from openagent_core.content_privacy import PrivacyFence
connection = sqlite3.connect(':memory:')
guard_snapshot_transaction(connection)
assert not _verified_legacy_projection_domain(connection, [], 'tenant')
connection.execute('CREATE TABLE privacy_revisions(origins TEXT)')
connection.execute('INSERT INTO privacy_revisions VALUES (?)', ('["protected"]',))
try:
    guard_snapshot_transaction(connection)
except PrivacyFence:
    pass
else:
    raise AssertionError('protected snapshots must remain fenced')
assert 'aiosqlite' not in sys.modules
assert 'openagent_core.memory.db' not in sys.modules
"""
    environment = {**os.environ, 'PYTHONPATH': os.pathsep.join(str(root / path) for path in (
        'packages/runtime/src', 'packages/storage-sqlite/src'))}
    result = subprocess.run([sys.executable, '-B', '-S', '-c', script],
                            env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
