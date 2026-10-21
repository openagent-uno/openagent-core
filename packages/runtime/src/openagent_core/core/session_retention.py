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

Every write is BATCHED: a single 1.8GB UPDATE OOM-killed the process; 200-row
batches with per-batch commits stay flat in memory. Safe to run against the
live DB (WAL, one writer at a time). The recurring job never runs an online
VACUUM: rewriting every live page needs up to twice the content in free disk
and holds the only writer for the whole rewrite, which filled volumes and
starved inbound writes. Freed pages are reused in place; full compaction is
the explicit, maintenance-only ``--vacuum`` mode.

The module is deliberately stdlib-only (PyYAML is used only when present) so
the same file runs both imported as ``openagent_core.core.session_retention``
and as a standalone script under a bare system ``python3``.

Usage:
  session_retention.py                 # one-shot prune, then exit
  session_retention.py --daemon        # prune now, then every interval_hours
  session_retention.py --vacuum        # one-shot prune + VACUUM; stop writers first
"""
from __future__ import annotations

import argparse
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
            "max_bytes": 400_000}
# sessions whose id starts with one of these are conversational (Telegram, peer
# network, agent bridges) — keep them regardless of age.
EXEMPT_PREFIXES = ("tg:", "peer:", "agent-brid")


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
                        "AND NOT EXISTS (SELECT 1 FROM sessions l WHERE l.session_id=s.id) LIMIT ?)",
                        (sid, max(1, batch_size)),
                    )
                count += cur.rowcount
                if cur.rowcount < batch_size:
                    break
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


def prune(db_path: str, days: int, max_runs: int, trim_threshold: int,
          max_bytes: int = DEFAULTS["max_bytes"], *, vacuum: bool = False) -> tuple[int, int]:
    conn = sqlite3.connect(db_path, timeout=2)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        cur = conn.cursor()
        cur.execute("PRAGMA busy_timeout=2000")
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
        print(f"[retention] deleted_payloads={reclaimed} stripped_run_payloads={stripped} "
              f"outbox_deleted={outbox}", flush=True)
        # PASSIVE does not wait for readers while holding the writer lock. File
        # size is a high-water mark; free pages remain available for reuse.
        cur.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
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
    deleted, trimmed = prune(
        db_path, settings["days"], settings["max_runs"],
        settings["trim_threshold_bytes"], settings["max_bytes"], vacuum=vacuum,
    )
    print(
        f"[retention] deleted={deleted} trimmed={trimmed} "
        f"days={settings['days']} max_runs={settings['max_runs']}",
        flush=True,
    )
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
        time.sleep(max(1, settings["interval_hours"]) * 3600)


if __name__ == "__main__":
    main()
