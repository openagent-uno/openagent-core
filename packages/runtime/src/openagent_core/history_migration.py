"""Public, bounded history migration over the existing operational store.

Hosts run this against a fenced candidate before admitting new work. Legacy
envelopes remain available; this service never assigns historical authors,
promotes incomplete projections, starts agents or replays external effects.
"""
from __future__ import annotations


class HistoryMigration:
    def __init__(self, memory):
        self.memory=memory

    async def advance(self, *, batch_size: int=250):
        if not 1<=batch_size<=1000:raise ValueError('History migration batch must be between 1 and 1000')
        connection=self.memory._conn
        if connection is None:raise RuntimeError('The host must start its memory storage before migrating history')
        from .memory.operational.repository import reconcile_pending_async,backfill_batch_async,projection_coverage_async
        try:
            pending=await reconcile_pending_async(connection,limit=batch_size,worker_id='host-history-migration')
            writes,backfill_complete=await backfill_batch_async(connection,limit=batch_size)
            await connection.commit()
            coverage=await projection_coverage_async(connection)
            return {**coverage,'backfill_complete':backfill_complete,'processed':len(pending)+len(writes)}
        except BaseException:
            await connection.rollback()
            raise

    async def verify(self, session_id: str):
        from .memory.operational.repository import verify_session_projection
        connection=self.memory._conn
        if connection is None:raise RuntimeError('Memory storage has not started')
        # The repository verifier performs exact envelope/author/ID comparisons
        # on the owning SQLite worker, never on the event loop.
        return await connection._execute(verify_session_projection,connection._conn,session_id)
