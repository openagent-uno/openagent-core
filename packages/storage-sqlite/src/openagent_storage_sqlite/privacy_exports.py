"""Encrypted, host-authorized local export staging and durable delivery receipts.

The host supplies encryption and current authorization; Core never chooses an
external recipient. Staging is not delivery completion. Copies inherit the exact
source revision through the public privacy ledger and are themselves redactable.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3

from openagent_core.content_privacy import ContentRevision, PrivacyConflict, PrivacyRequest
from openagent_core.persistence import run_sync


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


@dataclass(frozen=True)
class PrivacyExportBundle:
    payload: bytes
    delivery_token: str


class EncryptedPrivacyExports:
    """Opt-in spool using a host cipher ``encrypt/decrypt(aad, bytes)`` contract.

    ``authorize(request, actor)`` must check the host's current authenticated
    principal and destination policy. ``authorize_revision(request, revision,
    actor)`` must additionally check current access to each exact source copy.
    A shared provider grant never grants access to another user's history.
    This service must never be a model tool.
    The host ACKs delivery only after its authorized transport delivered the
    bundle. An encrypted durable snapshot alone does not complete that action.
    """
    sink_id = 'privacy-export-spool'

    def __init__(self, path, ledger, *, cipher, authorize, authorize_revision, max_part_bytes=8 << 20):
        self.path, self.ledger = Path(path).absolute(), ledger
        self.cipher, self.authorize = cipher, authorize
        self.authorize_revision = authorize_revision
        if self.path == ledger.path or self.path.is_symlink():
            raise ValueError('Export staging needs its own host-owned database')
        if not callable(authorize) or not callable(authorize_revision) or max_part_bytes < 1:
            raise ValueError('Current host authorization and a bounded export are required')
        self.max_part_bytes = max_part_bytes
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS export_requests(
                    id TEXT PRIMARY KEY,request BLOB NOT NULL,state TEXT NOT NULL,
                    delivery_hash TEXT,delivered INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS export_parts(
                    artifact TEXT PRIMARY KEY,request_id TEXT NOT NULL,revision TEXT NOT NULL,
                    parent_artifact TEXT NOT NULL,parent_revision TEXT NOT NULL,aad BLOB NOT NULL,
                    ciphertext BLOB,payload_digest TEXT NOT NULL,erased INTEGER NOT NULL DEFAULT 0);
            ''')

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        db.execute('PRAGMA secure_delete=ON')
        try:
            with db:
                yield db
        finally:
            db.close()

    async def export(self, request, revision, payload):
        if request.operation != 'export' or revision.tenant_id != request.tenant_id:
            raise PrivacyConflict('Export source is outside the authenticated request')
        if not isinstance(payload, bytes) or len(payload) > self.max_part_bytes:
            raise ValueError('Export payload exceeds its host bound')
        if not payload:
            # Canonical sinks use empty bytes only for an absent/erased exact
            # revision. There is no new content copy or inherited source to
            # stage; seal records the fully covered empty delivery explicitly.
            return
        request_bytes = _json(asdict(request))
        artifact = hashlib.sha256(_json([request.request_id, revision.artifact_id, revision.revision_id])).hexdigest()
        digest = hashlib.sha256(payload).hexdigest()
        if payload and digest != revision.content_sha256:
            raise PrivacyConflict('Export payload does not retain its exact canonical revision')
        staged = ContentRevision(request.tenant_id, artifact, digest, self.sink_id, digest)
        parents = ((revision.artifact_id, revision.revision_id),)
        aad = _json([asdict(request), asdict(revision), asdict(staged)])
        # The graph record precedes the external database write. A crash leaves
        # either an absent copy or a targetable encrypted copy, never lost lineage.
        await self.ledger.record(staged, parents=parents)
        ciphertext = self.cipher.encrypt(aad, payload)
        def publish():
            with self._db() as db:
                db.execute('BEGIN IMMEDIATE')
                old = db.execute('SELECT request,state FROM export_requests WHERE id=?', (request.request_id,)).fetchone()
                if old and (bytes(old['request']) != request_bytes or old['state'] == 'revoked'):
                    raise PrivacyConflict('Immutable export request conflicts')
                db.execute("INSERT OR IGNORE INTO export_requests(id,request,state) VALUES(?,?,'staging')",
                           (request.request_id, request_bytes))
                old_part = db.execute('SELECT aad,payload_digest,erased FROM export_parts WHERE artifact=?', (artifact,)).fetchone()
                if old_part:
                    if bytes(old_part['aad']) != aad or old_part['payload_digest'] != digest or old_part['erased']:
                        raise PrivacyConflict('Immutable export revision conflicts')
                    return
                db.execute('INSERT INTO export_parts VALUES(?,?,?,?,?,?,?,?,0)',
                           (artifact, request.request_id, digest, revision.artifact_id, revision.revision_id,
                            aad, ciphertext, digest))
        await self.ledger.publish(staged, parents=parents, publish=publish)

    async def seal(self, request):
        """Mark ready only after every canonical export sink has completed."""
        if request.operation != 'export':
            raise ValueError('Only export requests can have a delivery receipt')
        if (await self.ledger.progress(request.request_id)).status != 'complete':
            return False
        def ready():
            with self._db() as db:
                row = db.execute('SELECT request,state FROM export_requests WHERE id=?', (request.request_id,)).fetchone()
                if row is None:
                    # An empty, fully covered request still has an explicit receipt.
                    db.execute("INSERT INTO export_requests(id,request,state) VALUES(?,?,'ready')",
                               (request.request_id, _json(asdict(request))))
                elif bytes(row['request']) != _json(asdict(request)) or row['state'] == 'revoked':
                    raise PrivacyConflict('Export request is unavailable')
                else:
                    db.execute("UPDATE export_requests SET state='ready' WHERE id=?", (request.request_id,))
            return True
        return await run_sync(ready)

    async def _authorized(self, request_id, actor):
        def load():
            with self._db() as db:
                row = db.execute('SELECT request,state FROM export_requests WHERE id=?', (request_id,)).fetchone()
                if row is None or row['state'] != 'ready':
                    raise PermissionError('Export is unavailable')
                value = json.loads(row['request'])
                return PrivacyRequest(**{**value, 'subjects': tuple(value['subjects'])})
        request = await run_sync(load)
        if not await self.authorize(request, actor):
            raise PermissionError('Export is unavailable')
        return request

    async def download(self, request_id, actor):
        request = await self._authorized(request_id, actor)
        rows = await self._authorized_parts(request, actor)
        output = []
        for row in rows:
            revision = ContentRevision(request.tenant_id, row['artifact'], row['revision'], self.sink_id, row['payload_digest'])
            received = []
            def read():
                with self._db() as db:
                    current = db.execute('SELECT * FROM export_parts WHERE artifact=?', (row['artifact'],)).fetchone()
                    if current is None or current['erased'] or current['ciphertext'] is None:
                        raise PermissionError('Export is unavailable')
                    payload = self.cipher.decrypt(bytes(current['aad']), bytes(current['ciphertext']))
                    if hashlib.sha256(payload).hexdigest() != current['payload_digest']:
                        raise PrivacyConflict('Encrypted export integrity failed')
                    received.append(payload)
            # Recheck the origin fence before disclosure, using the same public
            # synchronous callback lane as canonical publication.
            await self.ledger.publish(revision, parents=((row['parent_artifact'], row['parent_revision']),), publish=read)
            output.append({'artifact': row['parent_artifact'], 'revision': row['parent_revision'],
                           'content_base64': __import__('base64').b64encode(received[0]).decode()})
        token = secrets.token_urlsafe(32)
        def issue():
            with self._db() as db:
                db.execute('UPDATE export_requests SET delivery_hash=? WHERE id=? AND state=?',
                           (hashlib.sha256(token.encode()).hexdigest(), request_id, 'ready'))
        await run_sync(issue)
        return PrivacyExportBundle(_json({'request_id': request_id, 'parts': output}), token)

    async def acknowledge(self, request_id, actor, delivery_token):
        request = await self._authorized(request_id, actor)
        await self._authorized_parts(request, actor)
        if not isinstance(delivery_token, str) or len(delivery_token) > 128:
            raise PermissionError('Export delivery is unavailable')
        def delivered():
            with self._db() as db:
                row = db.execute('SELECT delivery_hash FROM export_requests WHERE id=?', (request_id,)).fetchone()
                supplied = hashlib.sha256(delivery_token.encode()).hexdigest()
                if not row or not row['delivery_hash'] or not secrets.compare_digest(row['delivery_hash'], supplied):
                    raise PermissionError('Export delivery is unavailable')
                db.execute('UPDATE export_requests SET delivered=1 WHERE id=?', (request_id,))
        await run_sync(delivered)

    def _source(self, row, revision=None):
        try:
            request, source, staged = json.loads(row['aad'])
            source, staged = ContentRevision(**source), ContentRevision(**staged)
            if (source.artifact_id, source.revision_id) != (row['parent_artifact'], row['parent_revision']):
                raise ValueError('Invalid source locator')
            if (staged.artifact_id, staged.revision_id, staged.content_sha256, staged.sink_id) != (
                    row['artifact'], row['revision'], row['payload_digest'], self.sink_id):
                raise ValueError('Invalid staged locator')
            if source.tenant_id != staged.tenant_id or request['tenant_id'] != source.tenant_id:
                raise ValueError('Invalid source tenant')
            if request['request_id'] != row['request_id']:
                raise ValueError('Invalid export request')
            if revision is not None and staged != revision:
                raise ValueError('Different exact revision')
            return source
        except (ValueError, TypeError, KeyError):
            raise PermissionError('Export is unavailable') from None

    async def source_for_revision(self, revision):
        """Resolve one exact staged copy for the host's bounded ACL walk.

        Unknown, erased and mismatched revisions are unavailable. This returns
        only an immutable source locator, never decrypted export content.
        """
        if not isinstance(revision, ContentRevision) or revision.sink_id != self.sink_id:
            raise PermissionError('Export is unavailable')
        def read():
            with self._db() as db:
                row = db.execute('SELECT * FROM export_parts WHERE artifact=?', (revision.artifact_id,)).fetchone()
                if row is None or row['erased'] or row['ciphertext'] is None:
                    raise PermissionError('Export is unavailable')
                return self._source(row, revision)
        return await run_sync(read)

    async def _authorized_parts(self, request, actor):
        def parts():
            with self._db() as db:
                return db.execute('SELECT * FROM export_parts WHERE request_id=? ORDER BY artifact',
                                  (request.request_id,)).fetchall()
        rows = await run_sync(parts)
        # Check the entire bundle before decrypting any part. Partial delivery
        # must not turn a multi-owner request into a false completion receipt.
        for row in rows:
            source = self._source(row)
            if source.tenant_id != request.tenant_id or not await self.authorize_revision(request, source, actor):
                raise PermissionError('Export is unavailable')
        for row in rows:
            staged = ContentRevision(request.tenant_id, row['artifact'], row['revision'], self.sink_id, row['payload_digest'])
            await self.ledger.publish(staged, parents=((row['parent_artifact'], row['parent_revision']),), publish=lambda: None)
        return rows

    async def delivered(self, request_id):
        def read():
            with self._db() as db:
                row = db.execute('SELECT delivered,state FROM export_requests WHERE id=?', (request_id,)).fetchone()
                return bool(row and row['delivered'] and row['state'] == 'ready')
        return await run_sync(read)

    async def apply(self, request, revision):
        if revision.sink_id != self.sink_id or revision.tenant_id != request.tenant_id:
            return 'conflict'
        if request.operation == 'export':
            def read():
                with self._db() as db:
                    row = db.execute('SELECT * FROM export_parts WHERE artifact=?', (revision.artifact_id,)).fetchone()
                    if row is None or row['erased'] or row['ciphertext'] is None:
                        return b''
                    if row['payload_digest'] != revision.content_sha256:
                        raise PrivacyConflict('Export snapshot changed independently')
                    value = self.cipher.decrypt(bytes(row['aad']), bytes(row['ciphertext']))
                    if hashlib.sha256(value).hexdigest() != revision.content_sha256:
                        raise PrivacyConflict('Encrypted export integrity failed')
                    return value
            # A subsequent request includes previously staged local copies.
            # They retain their own lineage, instead of being skipped or
            # blocking every later data request as an unsupported sink.
            await self.export(request, revision, await run_sync(read))
            return 'complete'
        if request.operation != 'redact':
            return 'conflict'
        def erase():
            with self._db() as db:
                row = db.execute('SELECT request_id,payload_digest FROM export_parts WHERE artifact=?', (revision.artifact_id,)).fetchone()
                if row is None:
                    return 'complete'
                if row['payload_digest'] != revision.content_sha256:
                    return 'conflict'
                db.execute('UPDATE export_parts SET ciphertext=NULL,erased=1 WHERE artifact=?', (revision.artifact_id,))
                db.execute("UPDATE export_requests SET state='revoked',delivery_hash=NULL WHERE id=?", (row['request_id'],))
            with self._db() as db:
                if db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:
                    return 'conflict'
            return 'complete'
        return await run_sync(erase)
