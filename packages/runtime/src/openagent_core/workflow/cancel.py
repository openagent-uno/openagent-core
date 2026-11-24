"""The "completely stop an in-flight workflow run" hand-off.

Stopping a run is a two-party affair: the *requester* (the desktop app via
``POST /api/workflows/{id}/stop``, or the agent via the workflow-manager MCP's
``stop_workflow``) can only record the intent; the main-process ``Scheduler``
is the only party holding the ``asyncio.Task`` that actually drives the run.
The hand-off is a row flagged ``status='cancelling'`` — never IPC — exactly
like ``workflow_run_requests`` carries a *start* request the other way. The
scheduler's ``_drain_cancellations`` scans for the flag every couple of
seconds, hard-cancels the task it owns, and the run's own cancellation handler
finalizes the row ``cancelled``. A flag with no live task (the process that
owned it crashed) is finalized directly by the same drain.

This module exists because there are two requesters and one correct way to
write that flag. The MCP subprocess holds a raw ``aiosqlite`` connection and
the gateway holds a ``MemoryDB``; both reduce to a connection, so these take
one rather than picking a side — and neither gets to re-derive the SQL. The
guard below is the whole reason it must not be re-derived by hand.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import aiosqlite


async def flag_workflow_runs_cancelling(
    conn: aiosqlite.Connection,
    *,
    workflow_id: str,
    run_id: str | None = None,
) -> list[str]:
    """Cancel a queued reservation or flag a started run under the same workflow.

    The guarded update returns only rows changed by this request. Terminal
    runs and repeated stops are never reported as newly cancelled.
    """
    target = " AND id=?" if run_id is not None else ""
    params = (workflow_id, run_id) if run_id is not None else (workflow_id,)
    cursor = await conn.execute(
        "UPDATE workflow_runs SET status=CASE WHEN status='queued' THEN 'cancelled' ELSE 'cancelling' END, "
        "finished_at=CASE WHEN status='queued' THEN CAST(strftime('%s','now') AS REAL) ELSE finished_at END "
        "WHERE workflow_id=? AND status IN ('queued','starting','running')" + target + " RETURNING id",
        params,
    )
    changed = [row["id"] for row in await cursor.fetchall()]
    await conn.commit()
    return changed


async def await_runs_terminal(
    conn: aiosqlite.Connection,
    run_ids: list[str],
    *,
    timeout_s: int,
) -> list[dict[str, Any]]:
    """Poll ``workflow_runs`` until each id leaves ``running`` / ``cancelling``.

    The scheduler's drain is the writer — in another process for the MCP, in
    another task for the gateway — so a fresh SELECT each pass is what sees its
    latest committed status (WAL); this is how ``run_workflow``'s wait-poller
    observes completion too. Returns ``[{id, status}]`` for every requested id,
    with whatever status it holds when the deadline passes, so a caller that
    waited too little reports the truth rather than an assumption.
    """
    if not run_ids:
        return []
    deadline = time.monotonic() + max(1, timeout_s)
    placeholders = ",".join("?" for _ in run_ids)
    while True:
        cursor = await conn.execute(
            f"SELECT id, status FROM workflow_runs WHERE id IN ({placeholders})",
            run_ids,
        )
        statuses = {r["id"]: r["status"] for r in await cursor.fetchall()}
        pending = [
            rid for rid in run_ids
            if statuses.get(rid) in ("running", "cancelling")
        ]
        if not pending or time.monotonic() >= deadline:
            return [
                {"id": rid, "status": statuses.get(rid, "unknown")}
                for rid in run_ids
            ]
        await asyncio.sleep(0.4)
