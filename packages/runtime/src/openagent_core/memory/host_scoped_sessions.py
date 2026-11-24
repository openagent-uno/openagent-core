"""Declare SQLite sessions whose projections require a live host authorization."""

import sqlalchemy as sa

metadata = sa.MetaData()
host_scoped_sessions = sa.Table(
    "oa_host_scoped_sessions", metadata, sa.Column("session_id", sa.Text, primary_key=True),
)


async def initialize_host_scoped_sessions(connection):
    """Create the registry in the canonical SQLite database under the host's transaction."""
    if connection.dialect.name != "sqlite":
        raise ValueError("The legacy projection registry requires the canonical SQLite source")
    await connection.run_sync(metadata.create_all)
    sources = await connection.exec_driver_sql(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('sessions_v2','search_outbox')"
    )
    if sources.scalar_one() == 2:
        # Registering an already indexed session must also schedule its cached children for eviction.
        await connection.exec_driver_sql(
            """CREATE TRIGGER IF NOT EXISTS oa_host_scoped_search_rebuild
               AFTER INSERT ON oa_host_scoped_sessions BEGIN
                 INSERT OR IGNORE INTO search_outbox(
                     tenant_id, source_kind, source_id, operation, source_version, acl_version, committed_at_ms)
                   SELECT tenant_id, 'session', id, 'rebuild', source_version, acl_version,
                          CAST(strftime('%s','now') AS INTEGER) * 1000
                     FROM sessions_v2 WHERE id=NEW.session_id;
               END"""
        )
