"""Durable provenance graph and leased privacy jobs, storing metadata only.

The host must supply every canonical sink and attest instrumentation coverage.
This adapter does not turn untracked session/Vault/filesystem writes into tracked
writes. It never declares completion when any required surface is uncovered.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
import asyncio
import json
import inspect
import itertools
import os
import sqlite3
import time
import uuid

from openagent_core.persistence import run_sync

from openagent_core.content_privacy import (ContentOrigin, ContentRevision, PrivacyRequest,
    PrivacyProgress, PrivacyConflict, PrivacyFence)


REQUIRED_CONTENT_SURFACES = frozenset({"transcripts", "compaction", "children", "vault",
    "attachments", "files", "search", "run-replay", "session-journal"})


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class SqliteContentPrivacyStore:
    """A product-local metadata sidecar; canonical content remains in its sink.

    The explicit required-surface set is immutable for this ledger and must
    include every channel enabled by the host, including custom external sinks.
    The default includes all persistent Core content channels. SQLite transactions
    serialize revocation with publication, and lease tokens fence worker retries.
    """
    def __init__(self, path: str | Path, *, required_surfaces=REQUIRED_CONTENT_SURFACES,
                 lease_ms: int = 60_000, clock=None):
        self.path = Path(path).absolute()
        self.required_surfaces = frozenset(required_surfaces)
        if not self.required_surfaces or not all(isinstance(s, str) and s for s in self.required_surfaces):
            raise ValueError("Explicit content coverage is required")
        if lease_ms < 1000:
            raise ValueError("Privacy worker lease must be at least one second")
        self.lease_ms = lease_ms
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise ValueError("Privacy metadata must be host-owned")
        descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600) if not self.path.exists() else None
        if descriptor is not None:
            os.close(descriptor)
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS privacy_configuration(id INTEGER PRIMARY KEY CHECK(id=1),surfaces TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS privacy_revisions(
                    tenant TEXT NOT NULL, artifact TEXT NOT NULL, revision TEXT NOT NULL,
                    sink TEXT NOT NULL, digest TEXT NOT NULL, origins TEXT NOT NULL,
                    parents TEXT NOT NULL, PRIMARY KEY(tenant,artifact,revision));
                CREATE TABLE IF NOT EXISTS privacy_requests(
                    id TEXT PRIMARY KEY, request TEXT NOT NULL, status TEXT NOT NULL,
                    lease TEXT, lease_until INTEGER NOT NULL DEFAULT 0, reason TEXT);
                CREATE TABLE IF NOT EXISTS privacy_targets(
                    request_id TEXT NOT NULL REFERENCES privacy_requests(id),
                    tenant TEXT NOT NULL, artifact TEXT NOT NULL, revision TEXT NOT NULL,
                    complete INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(request_id,tenant,artifact,revision));
            ''')
            surfaces = _json(sorted(self.required_surfaces))
            old = db.execute('SELECT surfaces FROM privacy_configuration WHERE id=1').fetchone()
            if old and old[0] != surfaces:
                raise PrivacyConflict("Required privacy coverage cannot change silently")
            db.execute('INSERT OR IGNORE INTO privacy_configuration VALUES(1,?)', (surfaces,))

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA secure_delete=ON')
        try:
            yield db
        finally:
            db.close()

    async def qualify_empty_history(self) -> bool:
        """Admit a new canonical cohort without claiming legacy reconciliation.

        Reads the known Core history and pre-DDL snapshot surfaces. Existing
        untracked history or an unreadable snapshot fails closed and is retained.
        A durable admitted cohort survives restart; hosts must still qualify
        their real writers and fences before declaring coverage.
        """
        return await run_sync(self._qualify_empty_history)

    @staticmethod
    def _history_is_empty(db):
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        history = frozenset({'sessions', 'sessions_v2', 'session_runs', 'session_messages',
            'session_message_parts', 'session_events', 'runs', 'task_runs', 'memories',
            'tool_invocations', 'domain_events', 'artifacts', 'artifact_links', 'context_snapshots',
            'activity_items', 'search_documents', 'search_chunks', 'query_snapshots',
            'query_snapshot_items', 'query_snapshot_hits', 'ui_views', 'ui_view_revisions',
            'ui_data_state', 'ui_action_runs', 'ui_message_links'})
        for name in tables:
            quoted = '"' + name.replace('"', '""') + '"'
            columns = {row[1] for row in db.execute('PRAGMA table_info(' + quoted + ')')}
            # The public legacy store supports a configurable session table
            # name. Identify its schema rather than trusting a fixed name.
            if (name in history or {'session_id', 'runs'} <= columns) and db.execute(
                    'SELECT 1 FROM ' + quoted + ' LIMIT 1').fetchone():
                return False
        return True

    def _qualify_empty_history(self):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('''CREATE TABLE IF NOT EXISTS privacy_history_baseline(
                    id INTEGER PRIMARY KEY CHECK(id=1),admitted INTEGER NOT NULL CHECK(admitted IN (0,1)))''')
                previous = db.execute('SELECT admitted FROM privacy_history_baseline WHERE id=1').fetchone()
                if previous:
                    db.rollback()
                    return bool(previous[0])
                admitted = self._history_is_empty(db)
                backups = self.path.parent / 'backups'
                if backups.is_symlink():
                    admitted = False
                elif backups.exists():
                    paths = tuple(itertools.islice(backups.iterdir(), 257))
                    if len(paths) > 256:
                        admitted = False
                    for path in paths:
                        if not admitted:
                            break
                        if path.is_symlink():
                            admitted = False
                        elif path.name.endswith(('.db', '.db.tmp')):
                            try:
                                with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as snapshot:
                                    admitted = self._history_is_empty(snapshot)
                            except sqlite3.Error:
                                admitted = False
                db.execute('INSERT INTO privacy_history_baseline VALUES(1,?)', (int(admitted),))
                db.commit()
                return admitted
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _origin(value):
        return ContentOrigin(**{**value, 'subjects': tuple(value['subjects'])})

    @staticmethod
    def _request(value):
        return PrivacyRequest(**{**value, 'subjects': tuple(value['subjects'])})

    async def record(self, revision, *, origins=(), parents=()):
        await run_sync(self._record, revision, origins, parents)

    def _record(self, revision, origins, parents):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._record_transaction(db, revision, origins, parents)
            db.commit()

    async def publish(self, revision, *, publish, origins=(), parents=()):
        if not callable(publish) or inspect.iscoroutinefunction(publish):
            raise ValueError("A synchronous canonical publication callback is required")
        await run_sync(self._publish, revision, origins, parents, publish)

    def _publish(self, revision, origins, parents, publish):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT 1 FROM privacy_revisions WHERE tenant=? AND artifact=? AND revision=?',
                             (revision.tenant_id, revision.artifact_id, revision.revision_id)).fetchone()
            if not old:
                raise PrivacyConflict("External publication requires a durable recorded recovery intent")
            self._record_transaction(db, revision, origins, parents)
            result = publish()
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                raise ValueError("Canonical publication cannot return an awaitable")
            db.commit()

    def _record_transaction(self, db, revision, origins=(), parents=()):
        if not isinstance(revision, ContentRevision) or not isinstance(origins, tuple) or not isinstance(parents, tuple):
            raise ValueError("Typed immutable provenance required")
        if len(origins) > 4096 or len(parents) > 4096 or any(not isinstance(o, ContentOrigin) for o in origins):
            raise ValueError("Bounded provenance required")
        inherited = set(origins)
        for parent in parents:
            if not isinstance(parent, tuple) or len(parent) != 2 or parent == (revision.artifact_id, revision.revision_id):
                raise ValueError("Invalid parent revision")
            row = db.execute('SELECT origins FROM privacy_revisions WHERE tenant=? AND artifact=? AND revision=?',
                             (revision.tenant_id, *parent)).fetchone()
            if not row:
                raise PrivacyConflict("Parent revision is untracked or belongs to another tenant")
            inherited.update(self._origin(o) for o in json.loads(row[0]))
        if any(origin.tenant_id != revision.tenant_id for origin in inherited):
            raise PrivacyConflict("Cross-tenant provenance is forbidden")
        encoded = _json(sorted((asdict(o) for o in inherited), key=_json))
        parent_json = _json(sorted(set(parents)))
        for row in db.execute('SELECT request FROM privacy_requests'):
            request = self._request(json.loads(row[0]))
            if request.operation == 'redact' and any(request.matches(o) for o in inherited):
                raise PrivacyFence("Revoked origin cannot publish derived content")
        old = db.execute('SELECT sink,digest,origins,parents FROM privacy_revisions WHERE tenant=? AND artifact=? AND revision=?',
                         (revision.tenant_id, revision.artifact_id, revision.revision_id)).fetchone()
        if old:
            if (old[0],old[1],old[2],old[3]) != (revision.sink_id, revision.content_sha256, encoded, parent_json):
                raise PrivacyConflict("Revision metadata is immutable")
            return
        db.execute('INSERT INTO privacy_revisions VALUES(?,?,?,?,?,?,?)',
                   (revision.tenant_id, revision.artifact_id, revision.revision_id,
                    revision.sink_id, revision.content_sha256, encoded, parent_json))
    async def enqueue(self, request):
        return await run_sync(self._enqueue, request)

    def _enqueue(self, request):
        if not isinstance(request, PrivacyRequest):
            raise ValueError("A typed provider-authenticated privacy request is required")
        encoded = _json(asdict(request))
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT request FROM privacy_requests WHERE id=?', (request.request_id,)).fetchone()
            if old:
                if old[0] != encoded:
                    raise PrivacyConflict("Request identity was reused for another scope")
            else:
                db.execute("INSERT INTO privacy_requests(id,request,status) VALUES(?,?,'pending')", (request.request_id, encoded))
                for row in db.execute('SELECT * FROM privacy_revisions WHERE tenant=?', (request.tenant_id,)):
                    if any(request.matches(self._origin(o)) for o in json.loads(row['origins'])):
                        db.execute('INSERT INTO privacy_targets(request_id,tenant,artifact,revision) VALUES(?,?,?,?)',
                                   (request.request_id, row['tenant'], row['artifact'], row['revision']))
            db.commit()
            return self._progress(db, request.request_id)

    @staticmethod
    def _progress(db, request_id):
        row = db.execute('SELECT status,reason FROM privacy_requests WHERE id=?', (request_id,)).fetchone()
        if row is None:
            raise LookupError(request_id)
        counts = db.execute('SELECT coalesce(sum(complete),0),count(*) FROM privacy_targets WHERE request_id=?', (request_id,)).fetchone()
        return PrivacyProgress(request_id, row['status'], counts[0], counts[1]-counts[0], row['reason'])

    async def progress(self, request_id):
        def read():
            with self._db() as db:
                return self._progress(db, request_id)
        return await run_sync(read)

    def _claim(self, request_id, covered):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM privacy_requests WHERE id=?', (request_id,)).fetchone()
            if row is None:
                raise LookupError(request_id)
            if row['status'] == 'complete' or row['lease_until'] > self.clock():
                return None, None, ()
            missing = self.required_surfaces - covered
            if missing:
                db.execute("UPDATE privacy_requests SET status='blocked',reason='coverage_incomplete',lease=NULL,lease_until=0 WHERE id=?", (request_id,))
                db.commit()
                return None, None, ()
            token = uuid.uuid4().hex
            db.execute("UPDATE privacy_requests SET status='processing',reason=NULL,lease=?,lease_until=? WHERE id=?",
                       (token, self.clock()+self.lease_ms, request_id))
            targets = db.execute('''SELECT r.* FROM privacy_targets t JOIN privacy_revisions r
                ON (t.tenant=r.tenant AND t.artifact=r.artifact AND t.revision=r.revision)
                WHERE t.request_id=? AND t.complete=0 ORDER BY t.artifact ASC,r.rowid DESC''', (request_id,)).fetchall()
            request=self._request(json.loads(row['request']))
            if request.subjects and any(not origin.subjects and request.matches(origin)
                    for target in targets for origin in (self._origin(value) for value in json.loads(target['origins']))):
                db.execute("UPDATE privacy_requests SET status='blocked',reason='subject_scope_unknown',lease=NULL,lease_until=0 WHERE id=?",(request_id,))
                db.commit()
                return None,None,()
            db.commit()
            return request, token, tuple(ContentRevision(
                r['tenant'], r['artifact'], r['revision'], r['sink'], r['digest']) for r in targets)

    def _finish_revision(self, request_id, token, revision):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT lease,lease_until FROM privacy_requests WHERE id=?', (request_id,)).fetchone()
            if row['lease'] != token or row['lease_until'] <= self.clock():
                return False
            db.execute('UPDATE privacy_targets SET complete=1 WHERE request_id=? AND tenant=? AND artifact=? AND revision=?',
                       (request_id, revision.tenant_id, revision.artifact_id, revision.revision_id))
            db.execute('UPDATE privacy_requests SET lease_until=? WHERE id=?', (self.clock()+self.lease_ms, request_id))
            db.commit()
            return True

    def _finish_job(self, request_id, token, reason):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT lease,lease_until FROM privacy_requests WHERE id=?', (request_id,)).fetchone()
            if row['lease'] == token and row['lease_until'] > self.clock():
                progress = self._progress(db, request_id)
                complete = reason is None and progress.remaining_revisions == 0
                db.execute('UPDATE privacy_requests SET status=?,reason=?,lease=NULL,lease_until=0 WHERE id=?',
                           ('complete' if complete else 'blocked', reason or (None if complete else 'retry_required'), request_id))
                db.commit()
            return self._progress(db, request_id)

    async def process(self, request_id, sinks, *, covered_surfaces):
        if not isinstance(covered_surfaces, frozenset):
            raise ValueError("Coverage must be an immutable host attestation")
        bindings = {sink.sink_id: sink for sink in sinks}
        if len(bindings) != len(sinks):
            raise ValueError("Canonical sink identities must be unique")
        request, token, targets = await run_sync(self._claim, request_id, covered_surfaces)
        if token is None:
            return await self.progress(request_id)
        reason = None
        try:
            for target in targets:
                sink = bindings.get(target.sink_id)
                if sink is None:
                    reason = 'sink_unavailable'
                    break
                outcome = await sink.apply(request, target)
                if outcome != 'complete':
                    reason = 'revision_conflict'
                    break
                if not await run_sync(self._finish_revision, request_id, token, target):
                    return await self.progress(request_id)
        except asyncio.CancelledError:
            # A started sink may have completed. Retry is idempotent; the lease
            # expires without making an unverifiable completion claim.
            raise
        except Exception:
            # No exception text/content enters durable receipts or logs.
            reason = 'sink_failed'
        return await run_sync(self._finish_job, request_id, token, reason)
