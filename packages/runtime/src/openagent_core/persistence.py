"""Run synchronous database operations without blocking their async writers."""
from __future__ import annotations
import asyncio
from functools import partial


async def run_sync(operation, /, *args, **kwargs):
    """Drain a started operation before cancellation releases its ownership.

    SQLite may wait on a transaction whose commit needs the event loop. The
    worker receives a copy of the verified runtime context via to_thread.
    Cancellation cannot kill a Python thread, so it must wait for that worker
    before closing storage or recording the final interrupted run state.
    """
    return await complete_before_cancelling(asyncio.to_thread(partial(operation, *args, **kwargs)))


async def complete_before_cancelling(operation):
    """Finish an admitted critical section before propagating cancellation."""
    task = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()
        raise
