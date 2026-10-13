"""Canonical SQLite snapshot admission; protected cohorts need a managed sink."""
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from openagent_core.content_privacy import PrivacyFence


def guard_snapshot_transaction(connection):
    """Called inside the snapshot's stable read transaction, never product SQL."""
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='privacy_revisions'").fetchone():
        if connection.execute("SELECT 1 FROM privacy_revisions WHERE origins!='[]' LIMIT 1").fetchone():
            raise PrivacyFence('Protected state snapshots require a managed provenance and erasure adapter')


def guard_unscoped_history_transaction(connection, session_id, serialized=None):
    """Unscoped legacy writers cannot silently bypass an opted-in cohort."""
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    protected = 'engine_privacy_nodes' in tables and connection.execute(
        'SELECT 1 FROM engine_privacy_nodes WHERE session=? LIMIT 1', (session_id,)).fetchone()
    if not protected and serialized and 'runtime_privacy_origins' in tables:
        for run in serialized.get('runs') or ():
            if isinstance(run, dict) and connection.execute(
                    "SELECT 1 FROM runtime_privacy_origins WHERE run_id=? AND origins!='[]' LIMIT 1",
                    (run.get('run_id'),)).fetchone():
                protected = True
                break
    if protected:
        raise PrivacyFence('Protected history writes require an admitted canonical privacy writer')


@asynccontextmanager
async def checked_snapshot_source(path):
    """Pin the checked source snapshot before any destination/copy is created."""
    async with aiosqlite.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as source:
        await source.execute('BEGIN')
        try:
            exists=await (await source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='privacy_revisions'")).fetchone()
            if exists and await (await source.execute("SELECT 1 FROM privacy_revisions WHERE origins!='[]' LIMIT 1")).fetchone():
                raise PrivacyFence('Protected state snapshots require a managed provenance and erasure adapter')
            yield source
        finally:
            await source.rollback()
