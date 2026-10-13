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
