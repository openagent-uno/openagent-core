#!/usr/bin/env python3
"""Session retention for OpenAgent's SQLite store — keeps openagent.db bounded.

Without this the `sessions` table grows without limit: the scheduler writes a
new session per task run and the per-thread webhook binding keeps every
delivery's transcript, so the DB climbed to ~2GB and every model run reloaded
megabytes of history (the token/disk "history bloat").

Two knobs, both from openagent.yaml `retention:` (safe defaults if absent):
  * days      — delete sessions whose last activity is older than this
                (conversational sessions tg:/peer:/agent-brid are EXEMPT so
                the bot keeps its chat memory). Bounds infinite growth.
  * max_runs  — trim any session's `runs` transcript to the last N entries, so
                one chatty thread can't balloon *within* the window.

The binding stays per-thread (each thread keeps its own session) — this only
prunes OLD stuff, exactly the intended design.

Tombstoned v2 sessions lose their message/tool rows and their runs' JSON
payloads (the run rows stay for lineage), and the search outbox drops the
history of sources whose latest row is a delete every consumer has applied.

Native OpenAgent 1.1 sessions have no legacy row, so nothing above ever
tombstones them: support webhook turns (``automation:event:*``), scheduler
runs (``scheduler:*``) and delegated children (``childroot_*``) lived forever
and grew production DBs by ~1 GB/day (2026-10-22). Step 0 tombstones those
once idle for ``automation_days`` through the 1.1 repository API
(``tombstone_session``, so the search outbox gets its delete rows and the
operational index stays consistent); the payload steps below then reclaim
them. Domain events of
long-tombstoned sessions lose their bulky run/tool telemetry (session.* and
run outcome/cancel lineage stay). Freed pages are returned to the filesystem
with a bounded ``incremental_vacuum`` when the DB uses auto_vacuum=INCREMENTAL.
Step 0 needs ``openagent_core`` importable (run with the 1.1 venv python);
under a bare ``python3`` it is skipped and logged.

Every write is BATCHED: a single 1.8GB UPDATE OOM-killed the process; 200-row
batches with per-batch commits stay flat in memory. Safe to run against the
live DB (WAL, one writer at a time; busy_timeout 2 s, a locked session is
skipped and retried next cycle rather than blocking the agent writer). The
recurring job never runs an online VACUUM: rewriting every live page needs up
to twice the content in free disk and holds the only writer for the whole
rewrite, which filled volumes and starved inbound writes. Freed pages are
reused in place (or released in bounded incremental steps); full compaction
is the explicit, maintenance-only ``--vacuum`` mode.

The module is deliberately stdlib-only (PyYAML is used only when present; the
1.1 repository writer is imported lazily for step 0) so the same file runs
both imported as ``openagent_core.core.session_retention`` and as a
standalone script under a bare system ``python3``.

Usage:
  session_retention.py                 # one-shot prune, then exit
  session_retention.py --daemon        # prune now, then every interval_hours
  session_retention.py --vacuum        # one-shot prune + VACUUM; stop writers first
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import os
import shutil
import sqlite3
import time

DEFAULTS = {"days": 3, "max_runs": 5, "interval_hours": 6, "trim_threshold_bytes": 200_000,
            # Ceiling on what a trimmed transcript may still weigh. ~400KB is
            # roughly 100k tokens at the runtime's 4-chars-per-token estimate —
            # under the fold threshold, so a session this size can still compact
            # its way back down instead of being stuck above the model's window.
            "max_bytes": 400_000,
            # Native 1.1 automation sessions are tombstoned after this many
            # days idle; their bulky domain events go this many days after
            # the tombstone.
            "automation_days": 1,
            # Optional hourly policy; zero preserves the legacy day setting.
            "automation_hours": 0,
            # -1 keeps the legacy extra automation_days grace period.
            "domain_event_grace_hours": -1,
            "interval_minutes": 0,
            # Sessions tombstoned per batch (one transaction each) and the
            # per-cycle ceiling, so a backlog drains over a few cycles.
            "automation_batch": 50,
            "automation_max_per_cycle": 2000,
            # Bounded incremental_vacuum: pages per step and per cycle, and
            # the WAL size at which a cycle stops relocating pages.
            "vacuum_step_pages": 1024,
            "vacuum_max_pages_per_cycle": 131072,
            "vacuum_max_wal_bytes": 256 * 1024 * 1024}
# sessions whose id starts with one of these are conversational (Telegram, peer
# network, agent bridges) — keep them regardless of age.
EXEMPT_PREFIXES = ("tg:", "peer:", "agent-brid")
# Native 1.1 sessions owned by automation, never by a person (GLOB patterns:
# '_' is literal). Conversational ids (peer:/tg:/chat) never match.
AUTOMATION_GLOBS = ("automation:event:*", "scheduler:*", "childroot_*")
# Bulky per-run telemetry. session.*, run.accepted/success/failed and
# run.cancel_* stay: they are the lineage of a deleted session.
PRUNABLE_EVENT_TYPES = ("run.stream", "tool.completed", "tool.invoking",
                        "run.status", "run.profile", "run.prompt")


def _parse_retention_block(text: str) -> dict:
    """Minimal parser for the ``retention:`` block — no PyYAML needed, so the
    script runs under the pod's bare system python too. Reads the indented
    ``key: value`` lines until the block dedents."""
    out: dict = {}
    in_block = False
    for line in text.splitlines():
        if line.strip() == "retention:":
            in_block = True
            continue
        if in_block:
            if line.strip() and not line[0].isspace():  # dedent → block ended
                break
            body = line.split("#", 1)[0].strip()
            if ":" in body:
                key, val = body.split(":", 1)
                out[key.strip()] = val.strip()
    return out


def load_settings(yaml_path: str) -> dict:
    settings = dict(DEFAULTS)
    try:
        with open(yaml_path) as fh:
            text = fh.read()
    except FileNotFoundError:
        return settings
    try:
        import yaml  # PyYAML if present (agent venv)
        raw = (yaml.safe_load(text) or {}).get("retention") or {}
    except Exception:
        raw = _parse_retention_block(text)  # bare-python fallback
    for key in DEFAULTS:
        if key in raw:
            try:
                settings[key] = int(raw[key])
            except (TypeError, ValueError):
                pass
    return settings


def prune_deleted_payloads(conn: sqlite3.Connection, *, batch_size: int = 200) -> dict[str, int]:
    """Reclaim payloads only after the canonical session is tombstoned.

    Keep the session/run identities and search deletion journal. They carry
    lineage and allow consumers to observe deletions. Never infer deletion
    merely from a missing legacy row: native v2 sessions need not have one.
    """
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"sessions_v2", "sessions"} <= tables:
        return {}
    candidates = conn.execute(
        "SELECT id FROM sessions_v2 s WHERE deleted_at_ms IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id)"
    ).fetchall()
    counts = {}
    inactive = inactive_session_predicate(conn, "s.id")
    for table in ("tool_invocations", "session_messages"):
        if table not in tables:
            continue
        count = 0
        for (sid,) in candidates:
            while True:
                # Recheck the tombstone within the write statement so a session
                # restored since candidate selection cannot lose its payloads.
                with conn:
                    cur = conn.execute(
                        f"DELETE FROM {table} WHERE rowid IN ("
                        f"SELECT p.rowid FROM {table} p JOIN sessions_v2 s ON s.id=p.session_id "
                        "WHERE s.id=? AND s.deleted_at_ms IS NOT NULL "
                        "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id) "
                        f"AND {inactive} LIMIT ?)",
                        (sid, max(1, batch_size)),
                    )
                count += cur.rowcount
                if cur.rowcount < batch_size:
                    break
                # Hand the single WAL writer back to the agent between batches.
                time.sleep(0.05)
        counts[table] = count
    return counts


RUN_PAYLOAD_COLUMNS = ("input_json", "output_json", "metrics_json", "legacy_raw_json", "raw_envelope_json")


def prune_deleted_run_payloads(conn: sqlite3.Connection, *, batch_size: int = 200,
                               pause: float = 0.05) -> int:
    """Strip the heavy JSON of runs whose canonical session is tombstoned.

    Same predicate as prune_deleted_payloads, plus no in-flight work on the
    session. The row stays — id, ordinal, status, model, timestamps and
    metadata carry lineage — only the payloads go: the nullable ones become
    NULL and raw_envelope_json (NOT NULL, must be an object) becomes '{}'.
    Nothing rebuilds a tombstoned session from these columns
    (load_v2_legacy_session reads only deleted_at_ms IS NULL).
    """
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"sessions_v2", "sessions", "session_runs"} <= tables:
        return 0
    columns = {r[1] for r in conn.execute("PRAGMA table_info(session_runs)")}
    if not set(RUN_PAYLOAD_COLUMNS) <= columns:
        return 0
    inactive = inactive_session_predicate(conn, "s.id")
    count = 0
    while True:
        with conn:
            cur = conn.execute(
                "UPDATE session_runs SET input_json=NULL, output_json=NULL, metrics_json=NULL, "
                "legacy_raw_json=NULL, raw_envelope_json='{}' WHERE rowid IN ("
                "SELECT r.rowid FROM session_runs r JOIN sessions_v2 s ON s.id=r.session_id "
                "WHERE s.deleted_at_ms IS NOT NULL "
                "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id) "
                f"AND {inactive} "
                "AND (r.input_json IS NOT NULL OR r.output_json IS NOT NULL OR r.metrics_json IS NOT NULL "
                "OR r.legacy_raw_json IS NOT NULL OR r.raw_envelope_json!='{}') LIMIT ?)",
                (max(1, batch_size),),
            )
        count += cur.rowcount
        if cur.rowcount < batch_size:
            return count
        # Hand the single WAL writer back to the agent between batches.
        time.sleep(pause)


def prune_consumed_outbox_deletes(conn: sqlite3.Connection, *, batch_size: int = 500,
                                  pause: float = 0.05) -> int:
    """Drop the outbox history of sources whose latest row is a consumed delete.

    OpenAgent's own compaction keeps the latest consumed row per source
    forever, so every message/tool/session ever tombstoned leaves a `delete`
    row behind (270k of 297k rows on one production agent, 2026-10-15). Every durable
    consumer has applied it, and replaying a delete onto a fresh index
    generation is a no-op, so the source's whole history can go.

    Kept: anything above MIN(search_outbox_consumers.last_seq), and the
    MAX(seq) head — the index treats last_indexed_seq > MAX(seq) as a rewound
    database and rebuilds. Both bounds are evaluated inside the DELETE, under
    the write lock. A batch takes whole sources, never half a history.
    """
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"search_outbox", "search_outbox_consumers"} <= tables:
        return 0
    count = 0
    while True:
        with conn:
            cur = conn.execute(
                "DELETE FROM search_outbox WHERE seq IN ("
                "SELECT r.seq FROM ("
                "SELECT d.tenant_id, d.source_kind, d.source_id, d.seq FROM search_outbox d "
                "WHERE d.operation='delete' "
                "AND d.seq<=(SELECT MIN(last_seq) FROM search_outbox_consumers) "
                "AND d.seq<(SELECT MAX(seq) FROM search_outbox) "
                "AND NOT EXISTS (SELECT 1 FROM search_outbox n WHERE n.tenant_id=d.tenant_id "
                "AND n.source_kind=d.source_kind AND n.source_id=d.source_id AND n.seq>d.seq) "
                "LIMIT ?) latest "
                "JOIN search_outbox r ON r.tenant_id=latest.tenant_id "
                "AND r.source_kind=latest.source_kind AND r.source_id=latest.source_id "
                "AND r.seq<=latest.seq)",
                (max(1, batch_size),),
            )
        count += cur.rowcount
        if cur.rowcount == 0:
            return count
        time.sleep(pause)


def inactive_session_predicate(conn: sqlite3.Connection, session_column: str = "sessions.session_id") -> str:
    """Never delete or trim transcripts still owned by durable in-flight work."""
    predicates = []
    for table in ("task_runs", "workflow_runs", "event_deliveries", "session_runs"):
        columns = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        if {"session_id", "status"} <= columns:
            predicates.append(
                f"NOT EXISTS (SELECT 1 FROM {table} active "
                f"WHERE active.session_id={session_column} AND active.status IN "
                "('running','cancelling','pending','queued','received'))"
            )
    return " AND ".join(predicates) or "1=1"


def _load_tombstone_api():
    """The 1.1 repository writer, or None under a python without the venv."""
    try:
        from openagent_core.memory.operational.repository import tombstone_session
    except ImportError:
        return None
    return tombstone_session


def _idle_automation_predicate(conn: sqlite3.Connection, cutoff_ms: int) -> tuple[str, tuple]:
    """Eligibility of a native 1.1 automation session for a tombstone.

    Idle since ``cutoff_ms`` (session row and every run), no legacy row (the
    legacy path above owns those), no in-flight durable work, and no live
    child: a parent is tombstoned only after all its children, so delegation
    lineage never points at a live child under a dead parent. Bare
    ``scheduler:<task>`` ids are synthetic per-task roots that later firings
    hang new children on, so they are never tombstoned here.
    """
    globs = " OR ".join("s.id GLOB ?" for _ in AUTOMATION_GLOBS)
    sql = (
        f"s.deleted_at_ms IS NULL AND ({globs}) "
        "AND NOT (s.id GLOB 'scheduler:*' AND instr(substr(s.id, 11), ':') = 0) "
        "AND s.last_activity_at_ms < ? AND s.updated_at_ms < ? "
        "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id) "
        "AND NOT EXISTS (SELECT 1 FROM session_runs r WHERE r.session_id=s.id "
        "AND (r.finished_at_ms IS NULL OR r.finished_at_ms >= ? OR r.created_at_ms >= ?)) "
        "AND NOT EXISTS (SELECT 1 FROM sessions_v2 c WHERE c.deleted_at_ms IS NULL AND c.id<>s.id "
        "AND (c.parent_session_id=s.id OR c.root_session_id=s.id)) "
        f"AND {inactive_session_predicate(conn, 's.id')}"
    )
    return sql, (*AUTOMATION_GLOBS, cutoff_ms, cutoff_ms, cutoff_ms, cutoff_ms)


def tombstone_idle_automation_sessions(conn: sqlite3.Connection, *, days: int, hours: int = 0, batch_size: int = 50,
                                       max_per_cycle: int = 2000, pause: float = 0.05,
                                       max_lock_skips: int = 5, tombstone=None,
                                       now_ms: int | None = None) -> dict[str, int]:
    """Step 0: tombstone idle native 1.1 automation sessions.

    One BEGIN IMMEDIATE transaction per session through the 1.1 repository
    API, which also journals the search-outbox deletes for the session, its
    messages and tool calls plus a ``session.deleted`` event. Eligibility is
    re-checked inside the write transaction. A busy writer (busy_timeout)
    means the session is skipped this cycle: the agent always wins the lock,
    and after ``max_lock_skips`` busy waits the step yields until next cycle.
    """
    counts = {"tombstoned": 0, "skipped_locked": 0, "failed": 0}
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"sessions_v2", "sessions", "session_runs", "legacy_session_changes"} <= tables:
        return counts
    tombstone = tombstone or _load_tombstone_api()
    if tombstone is None:
        counts["unavailable"] = 1
        return counts
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff_ms = now - (max(1, int(hours)) * 3_600_000 if hours > 0 else int(days) * 86_400_000)
    where, params = _idle_automation_predicate(conn, cutoff_ms)
    if conn.in_transaction:
        conn.commit()
    seen: set[str] = set()
    while (counts["tombstoned"] + counts["skipped_locked"] + counts["failed"] < max_per_cycle
           and counts["skipped_locked"] < max_lock_skips):
        ids = [r[0] for r in conn.execute(
            f"SELECT s.id FROM sessions_v2 s WHERE {where} ORDER BY s.last_activity_at_ms LIMIT ?",
            (*params, max(1, batch_size) + len(seen)),
        ) if r[0] not in seen][:max(1, batch_size)]
        if not ids:
            break
        for sid in ids:
            if counts["skipped_locked"] >= max_lock_skips:
                break
            seen.add(sid)
            try:
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                counts["skipped_locked"] += 1
                continue
            try:
                still = conn.execute(
                    f"SELECT 1 FROM sessions_v2 s WHERE s.id=? AND {where}", (sid, *params)
                ).fetchone()
                if still is not None:
                    tombstone(conn, sid)
                    counts["tombstoned"] += 1
                conn.commit()
            except sqlite3.OperationalError as exc:
                conn.rollback()
                if "locked" in str(exc) or "busy" in str(exc):
                    counts["skipped_locked"] += 1
                else:
                    counts["failed"] += 1
                    print(f"[retention] tombstone failed {sid[:40]}: {type(exc).__name__}: {exc}", flush=True)
            except Exception as exc:  # one bad session must not stop the cycle
                conn.rollback()
                counts["failed"] += 1
                print(f"[retention] tombstone failed {sid[:40]}: {type(exc).__name__}: {exc}", flush=True)
            # Back-to-back write transactions starve a busy-handler writer
            # (measured: an agent-like writer waited 8.8 s); leave it a gap.
            time.sleep(pause)
    return counts


def prune_deleted_domain_events(conn: sqlite3.Connection, *, days: int, hours: int = -1, batch_size: int = 500,
                                pause: float = 0.05, now_ms: int | None = None) -> int:
    """Drop the bulky run/tool telemetry of sessions tombstoned > ``days`` ago.

    ``tool.completed`` repeats the full tool result in its metadata, so it is
    the fourth copy of every payload. Lineage events (session.*,
    run.accepted/success/failed, run.cancel_*) are kept. Restored sessions
    (legacy row back) and sessions with in-flight work are left alone.
    """
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"sessions_v2", "sessions", "domain_events"} <= tables:
        return 0
    now = int(time.time() * 1000) if now_ms is None else int(now_ms)
    cutoff_ms = now - (int(hours) * 3_600_000 if hours >= 0 else int(days) * 86_400_000)
    types = ",".join("?" for _ in PRUNABLE_EVENT_TYPES)
    inactive = inactive_session_predicate(conn, "s.id")
    count = 0
    while True:
        with conn:
            cur = conn.execute(
                "DELETE FROM domain_events WHERE sequence IN ("
                "SELECT e.sequence FROM sessions_v2 s JOIN domain_events e "
                "ON e.tenant_id=s.tenant_id AND e.session_id=s.id "
                "WHERE s.deleted_at_ms IS NOT NULL AND s.deleted_at_ms < ? "
                "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id) "
                f"AND {inactive} AND e.event_type IN ({types}) LIMIT ?)",
                (cutoff_ms, *PRUNABLE_EVENT_TYPES, max(1, batch_size)),
            )
        count += cur.rowcount
        if cur.rowcount < batch_size:
            return count
        time.sleep(pause)


def reclaim_free_pages(conn: sqlite3.Connection, db_path: str, *, step_pages: int = 1024,
                       max_pages: int = 131072, max_wal_bytes: int = 256 * 1024 * 1024,
                       pause: float = 0.05) -> dict[str, int]:
    """Return free pages to the filesystem with a bounded incremental_vacuum.

    Only with auto_vacuum=INCREMENTAL (2); otherwise free pages are simply
    reused. Small steps, each its own short write transaction, a PASSIVE
    checkpoint between steps so relocated pages leave the WAL, and a stop as
    soon as the writer is busy or the WAL outgrows ``max_wal_bytes``.
    """
    if conn.in_transaction:
        conn.commit()
    mode = conn.execute("PRAGMA auto_vacuum").fetchone()[0]
    free_before = conn.execute("PRAGMA freelist_count").fetchone()[0]
    out = {"auto_vacuum": int(mode), "free_before": int(free_before), "released": 0}
    if mode != 2:
        out["free_after"] = int(free_before)
        return out
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    released = 0
    while released < max_pages:
        free = conn.execute("PRAGMA freelist_count").fetchone()[0]
        if free <= 0:
            break
        step = min(step_pages, free, max_pages - released)
        try:
            conn.execute(f"PRAGMA incremental_vacuum({int(step)})").fetchall()
            if conn.in_transaction:
                conn.commit()
        except sqlite3.OperationalError as exc:
            if conn.in_transaction:
                conn.rollback()
            out["stopped"] = f"{type(exc).__name__}: {exc}"[:80]
            break
        released += free - conn.execute("PRAGMA freelist_count").fetchone()[0]
        checkpoint = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        # PASSIVE deliberately leaves an allocated high-water file. Once its
        # frames are copied the next writer can reuse it: file size alone must
        # not strand free pages forever. Bound only outstanding frame bytes.
        pending_bytes = max(0, checkpoint[1] - checkpoint[2]) * page_size
        out["pending_wal_bytes"] = int(pending_bytes)
        if pending_bytes > max_wal_bytes:
            out["stopped"] = "wal_limit"
            break
        time.sleep(pause)
    out["released"] = int(released)
    out["free_after"] = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return out


def prune(db_path: str, days: int, max_runs: int, trim_threshold: int,
          max_bytes: int = DEFAULTS["max_bytes"], *, vacuum: bool = False,
          settings: dict | None = None) -> tuple[int, int]:
    opts = dict(DEFAULTS, **(settings or {}))
    conn = sqlite3.connect(db_path, timeout=2)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        cur = conn.cursor()
        cur.execute("PRAGMA busy_timeout=2000")

        # 0) native 1.1 automation sessions: tombstone once idle (see module doc).
        auto = tombstone_idle_automation_sessions(
            conn, days=opts["automation_days"], hours=opts["automation_hours"], batch_size=opts["automation_batch"],
            max_per_cycle=opts["automation_max_per_cycle"],
        )
        if auto.get("unavailable"):
            print("[retention] automation_tombstones skipped: openagent_core not importable "
                  "(run with the OpenAgent 1.1 environment python)", flush=True)
        else:
            print(f"[retention] automation_tombstones={auto['tombstoned']} "
                  f"skipped_locked={auto['skipped_locked']} failed={auto['failed']} "
                  f"automation_days={opts['automation_days']}", flush=True)
            if auto['failed']:
                raise sqlite3.OperationalError("native automation tombstones failed; retry required")
        cutoff = f"strftime('%s','now','-{int(days)} days')"
        exempt = " AND ".join(f"session_id NOT LIKE '{p}%'" for p in EXEMPT_PREFIXES)

        inactive = inactive_session_predicate(conn)

        # 1) delete sessions past the retention window — batched to bound memory.
        deleted = 0
        while True:
            cur.execute(
                "DELETE FROM sessions WHERE session_id IN ("
                f"  SELECT session_id FROM sessions WHERE updated_at < {cutoff} AND {exempt} AND {inactive} LIMIT 200)"
            )
            batch = cur.rowcount
            conn.commit()
            deleted += batch
            if batch == 0:
                break

        # 2) trim over-long transcripts — by COUNT first, then by SIZE.
        #
        # Counting runs alone leaves the worst offenders untouched: a session of 3
        # runs is under any sane `max_runs`, and if each run is a 60k-token tool
        # result it is still a quarter-million-token transcript. Those sessions
        # cannot recover on their own either — once the history passes the model's
        # window, the fold that would shrink it is itself too big to send, so every
        # later turn fails the same way. Measured on one production agent: 28 sessions past 150k
        # tokens, the largest at 553k across 16 runs, all of them below max_runs.
        #
        # `max_bytes` is a separate knob from `trim_threshold` on purpose: the
        # threshold decides WHICH sessions to look at, and is routinely set low to
        # cast a wide net. Reusing it as the target too would mean a low threshold
        # silently trimmed every matched session down to a single run.
        # The newest run is never dropped — it is the turn the session is mid-way
        # through.
        cur.execute(f"SELECT session_id FROM sessions WHERE length(runs) > ? AND {inactive}", (trim_threshold,))
        ids = [row[0] for row in cur.fetchall()]
        trimmed = 0
        for sid in ids:
            cur.execute("SELECT runs FROM sessions WHERE session_id=?", (sid,))
            row = cur.fetchone()
            if row is None:
                continue
            raw = row[0]
            try:
                runs = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(runs, list) or not runs:
                continue
            kept = runs[-max_runs:] if len(runs) > max_runs else list(runs)
            while len(kept) > 1 and len(json.dumps(kept)) > max_bytes:
                kept.pop(0)
            if len(kept) == len(runs):
                continue
            cur.execute(
                f"UPDATE sessions SET runs=? WHERE session_id=? AND runs=? AND {inactive}",
                (json.dumps(kept), sid, raw),
            )
            conn.commit()
            trimmed += 1

        # 2b) scheduler short-retention (added 2026-07-14 after the WAL-lock outage):
        # a completed cron run needs no transcript. Prune scheduler/agent run-logs
        # hard (1 day), and drop ANY oversized run-log immediately regardless of age
        # — a single 3-4MB `runs` blob monopolised the one WAL writer and starved
        # add_event_delivery (busy_timeout breach) → the support pipeline stalled.
        sched_cut = "strftime('%s','now','-1 days')"
        while True:
            cur.execute(
                "DELETE FROM sessions WHERE session_id IN ("
                "  SELECT session_id FROM sessions"
                "  WHERE (session_id LIKE 'scheduler:%' OR session_type='agent')"
                f"    AND (updated_at < {sched_cut} OR length(coalesce(runs,'')) > 500000)"
                f"    AND {exempt} AND {inactive} LIMIT 200)"
            )
            b = cur.rowcount
            conn.commit()
            deleted += b
            if b == 0:
                break

        reclaimed = prune_deleted_payloads(conn)
        stripped = prune_deleted_run_payloads(conn)
        outbox = prune_consumed_outbox_deletes(conn)
        events = prune_deleted_domain_events(conn, days=opts["automation_days"], hours=opts["domain_event_grace_hours"])
        print(f"[retention] deleted_payloads={reclaimed} stripped_run_payloads={stripped} "
              f"outbox_deleted={outbox} domain_events_deleted={events}", flush=True)
        # PASSIVE does not wait for readers while holding the writer lock. File
        # size is a high-water mark; free pages remain available for reuse...
        cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        # ...unless the DB is auto_vacuum=INCREMENTAL: then hand a bounded
        # number of them back to the filesystem.
        if not vacuum:
            pages = reclaim_free_pages(
                conn, db_path, step_pages=opts["vacuum_step_pages"],
                max_pages=opts["vacuum_max_pages_per_cycle"],
                max_wal_bytes=opts["vacuum_max_wal_bytes"],
            )
            print(f"[retention] incremental_vacuum auto_vacuum={pages['auto_vacuum']} "
                  f"free_before={pages['free_before']} released={pages['released']} "
                  f"free_after={pages['free_after']}"
                  + (f" stopped={pages['stopped']}" if pages.get("stopped") else ""), flush=True)
        # Full compaction is explicit maintenance, never a recurring online job:
        # rewriting every live page can fill the PVC or starve inbound writers.
        if vacuum:
            pages = cur.execute("PRAGMA page_count").fetchone()[0]
            free_pages = cur.execute("PRAGMA freelist_count").fetchone()[0]
            page_size = cur.execute("PRAGMA page_size").fetchone()[0]
            required = 2 * (pages - free_pages) * page_size + 64 * 1024 * 1024
            if shutil.disk_usage(os.path.dirname(os.path.abspath(db_path))).free < required:
                raise RuntimeError("insufficient headroom for explicit VACUUM")
            cur.execute("VACUUM")
            cur.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return deleted, trimmed
    finally:
        conn.close()


def run_once(db_path: str, yaml_path: str, *, vacuum: bool = False) -> dict:
    settings = load_settings(yaml_path)
    native_available = _load_tombstone_api() is not None
    deleted, trimmed = prune(
        db_path, settings["days"], settings["max_runs"],
        settings["trim_threshold_bytes"], settings["max_bytes"], vacuum=vacuum,
        settings=settings,
    )
    print(
        f"[retention] deleted={deleted} trimmed={trimmed} "
        f"days={settings['days']} max_runs={settings['max_runs']}",
        flush=True,
    )
    # A size threshold cannot diagnose retention. Publish an atomic health
    # receipt only after every pruning/reclaim step has completed successfully.
    state_path = os.path.join(os.path.dirname(os.path.abspath(db_path)), "session-retention.json")
    import tempfile
    now = time.time()
    try:
        with open(state_path) as previous:
            prior_state = json.load(previous)
    except (OSError, ValueError):
        prior_state = {}
    with closing(sqlite3.connect("file:" + os.path.abspath(db_path) + "?mode=ro", uri=True, timeout=2)) as health:
        idle_ms = (settings["automation_hours"] * 3_600_000 if settings["automation_hours"] > 0
                   else settings["automation_days"] * 86_400_000)
        where, params = _idle_automation_predicate(health, int(now * 1000) - idle_ms)
        eligible = health.execute("SELECT count(*) FROM sessions_v2 s WHERE " + where, params).fetchone()[0]
        storage = {key: health.execute("PRAGMA " + key).fetchone()[0]
                   for key in ("page_count", "page_size", "freelist_count", "auto_vacuum")}
    state = {"checked_at": now, "last_success_at": now if native_available else prior_state.get("last_success_at", 0),
             "state": "ok" if native_available else "degraded", "native_retention_available": native_available,
             "settings": settings,
             "db_bytes": os.path.getsize(db_path), "deleted": deleted, "trimmed": trimmed,
             "storage": storage, "automation_eligible": eligible,
             "backlog_since": (prior_state.get("backlog_since") or now) if eligible else None}
    fd, temporary = tempfile.mkstemp(prefix=".retention-", dir=os.path.dirname(state_path))
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(state, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, state_path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return settings


def main() -> None:
    ap = argparse.ArgumentParser(description="Prune OpenAgent session history.")
    ap.add_argument("--db", default="/data/agent/openagent.db")
    ap.add_argument("--config", default="/data/agent/openagent.yaml")
    ap.add_argument("--vacuum", action="store_true", help="explicit compaction; stop agent writers first")
    ap.add_argument("--daemon", action="store_true", help="loop every interval_hours")
    args = ap.parse_args()

    if args.daemon and args.vacuum:
        ap.error("--vacuum is maintenance-only and cannot be combined with --daemon")
    if not args.daemon:
        run_once(args.db, args.config, vacuum=args.vacuum)
        return

    while True:
        settings = load_settings(args.config)
        try:
            run_once(args.db, args.config)
        except (sqlite3.Error, OSError) as exc:
            print(f"[retention] deferred: {type(exc).__name__}: {exc}", flush=True)
            time.sleep(60)
            continue
        interval = (max(5, settings["interval_minutes"]) * 60 if settings["interval_minutes"] > 0
                    else max(1, settings["interval_hours"]) * 3600)
        time.sleep(interval)


if __name__ == "__main__":
    main()
