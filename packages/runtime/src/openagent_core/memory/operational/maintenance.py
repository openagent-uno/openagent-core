"""Incremental, instance-owned maintenance of operational history and search."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import logging
from pathlib import Path
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OperationalMaintenanceStatus:
    history_ready: bool
    search: dict[str, Any]
    failed: bool = False


async def _drain(awaitable: Any) -> Any:
    operation = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not operation.cancelled():
            try:
                operation.result()
            except Exception:
                pass
        raise


def _index_stamp(path: Path) -> tuple:
    stamps = []
    for candidate in (path, Path(str(path) + '-wal')):
        try:
            value = candidate.stat()
            stamps.append((value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns))
        except FileNotFoundError:
            stamps.append(None)
    return tuple(stamps)


async def _pending(conn: Any, through_seq: int) -> bool:
    row = await (await conn.execute(
        'SELECT EXISTS(SELECT 1 FROM legacy_session_changes '
        'WHERE processed_at_ms IS NULL) '
        'OR EXISTS(SELECT 1 FROM operational_automation_changes '
        'WHERE processed_at_ms IS NULL) '
        'OR EXISTS(SELECT 1 FROM search_outbox WHERE seq>?)',
        (through_seq,),
    )).fetchone()
    return bool(row[0])


async def maintain_operational_storage(
    db: Any,
    on_status: Callable[[OperationalMaintenanceStatus], Awaitable[None] | None],
    *,
    poll_interval: float = 0.5,
    audit_interval: float = 60.0,
) -> None:
    """Drain durable changes and audit unchanged derived state periodically.

    Empty polls use indexed journal/outbox probes and file metadata, rather
    than scanning the complete history and FTS corpus. Explicit search/status
    APIs retain their full validation. A changed/deleted index is audited on
    the next poll; the periodic audit also covers unchanged file timestamps.
    Cancellation drains in-flight SQLite work before closing owned storage.
    """
    if poll_interval <= 0 or audit_interval <= 0:
        raise ValueError('Maintenance intervals must be positive')
    import aiosqlite
    from openagent_core.configuration import sqlite_busy_timeout_ms, sqlite_busy_timeout_s
    from .repository import backfill_batch_async, projection_coverage_async, reconcile_pending_async
    from .automation import project_automation_pending
    from .search import operational_search_path, operational_search_status, sync_operational_search

    shared = await db._ensure_connected()
    raw_path = str(db.db_path)
    owns_connection = raw_path != ':memory:' and not raw_path.startswith('file::memory:')
    conn = shared
    backoff = 1.0
    last_status = None
    stamp = None
    audit_at = 0.0
    failed = False
    index_path = operational_search_path(raw_path)

    async def publish(status: OperationalMaintenanceStatus) -> None:
        result = on_status(OperationalMaintenanceStatus(status.history_ready, dict(status.search), status.failed))
        if inspect.isawaitable(result):
            await result

    try:
        if owns_connection:
            conn = aiosqlite.connect(raw_path, timeout=sqlite_busy_timeout_s())
            await _drain(conn)
            conn.row_factory = aiosqlite.Row
            for pragma in (f'busy_timeout={sqlite_busy_timeout_ms()}', 'journal_mode=WAL', 'synchronous=FULL', 'foreign_keys=ON'):
                await conn.execute('PRAGMA ' + pragma)
        while True:
            try:
                if (last_status is not None
                        and last_status.search.get('ready') and time.monotonic() < audit_at
                        and _index_stamp(index_path) == stamp
                        and not await _pending(conn, int(last_status.search.get('seq') or 0))):
                    await publish(last_status)
                    await asyncio.sleep(poll_interval)
                    continue
                while True:
                    writes = await reconcile_pending_async(conn, limit=100, worker_id='operational-maintenance')
                    await conn.commit()
                    coverage = await projection_coverage_async(conn)
                    if int(coverage['pending_sessions']) == 0:
                        break
                    if not writes:
                        raise RuntimeError('Session reconciliation made no progress')
                    await asyncio.sleep(0)
                while True:
                    _, complete = await backfill_batch_async(conn, limit=100)
                    await conn.commit()
                    if complete:
                        break
                    await asyncio.sleep(0)
                while True:
                    _, complete = await project_automation_pending(conn, limit=100)
                    await conn.commit()
                    if complete:
                        break
                    await asyncio.sleep(0)
                coverage = await projection_coverage_async(conn)
                while True:
                    search = await operational_search_status(db)
                    head = int((await (await conn.execute('SELECT COALESCE(MAX(seq),0) FROM search_outbox')).fetchone())[0])
                    if bool(search.get('ready')) and int(search.get('seq') or 0) >= head:
                        break
                    status = await sync_operational_search(db, limit=1000, source_conn=conn)
                    search = dict(status.__dict__)
                    if status.ready and status.seq >= head:
                        break
                    await asyncio.sleep(0)
                last_status = OperationalMaintenanceStatus(bool(coverage['complete']), dict(search))
                stamp = _index_stamp(index_path)
                audit_at = time.monotonic() + audit_interval
                failed = False
                backoff = 1.0
                await publish(last_status)
                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                last_status = None
                await publish(OperationalMaintenanceStatus(False, {
                    'ready': False, 'state': 'degraded', 'generation': 'unavailable',
                    'seq': 0, 'documents': 0, 'pending': 0, 'indexed_through_ms': None,
                }, failed=True))
                if not failed:
                    logger.error('Operational maintenance failed (details suppressed)')
                failed = True
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2)
    finally:
        if owns_connection and conn is not shared:
            await _drain(conn.close())
