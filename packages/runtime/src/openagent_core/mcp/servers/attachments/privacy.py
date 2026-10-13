"""Host-owned attachment snapshots with durable exact-revision recovery pins.

This is an explicit host API, not a model tool or an arbitrary-path collector.
The host supplies verified lineage and wholly attributed bytes. One exclusive
writer owns the root, and the same lock serializes publication and privacy jobs.
Copies made by channels or external recipients require separate qualified sinks.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Awaitable, Callable
import asyncio
import hashlib
import json
import os
import stat

from openagent_core.content_privacy import ContentPrivacyStore, ContentRevision, PrivacyConflict, PrivacyRequest


class ManagedAttachmentSnapshots:
    def __init__(self, sink_id: str, root: Path, ledger: ContentPrivacyStore, *,
                 export: Callable[[PrivacyRequest, ContentRevision, bytes], Awaitable[None]],
                 max_bytes: int = 8 << 20):
        if not sink_id or not callable(export) or type(max_bytes) is not int or max_bytes < 1:
            raise ValueError('Explicit sink, authorized export callback and positive bound required')
        if not root.is_dir() or root.is_symlink():
            raise ValueError('An existing exclusively host-owned snapshot root is required')
        self.sink_id, self.root, self.ledger = sink_id, root.resolve(), ledger
        self.export, self.max_bytes = export, max_bytes
        self.writer_lock = asyncio.Lock()

    def _paths(self, revision):
        if revision.sink_id != self.sink_id:
            raise PrivacyConflict('Attachment revision belongs to another sink')
        key = hashlib.sha256(json.dumps([revision.tenant_id, revision.artifact_id,
            revision.revision_id], separators=(',', ':')).encode()).hexdigest()
        return tuple(self.root / (key + suffix) for suffix in ('.pin.json', '.pending', '.bin'))

    @staticmethod
    def _fsync_directory(root):
        # Windows has no portable directory descriptor; file handles are always
        # writable and flushed before rename on every platform.
        if os.name != 'nt':
            descriptor = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _read(self, path, bound):
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        except FileNotFoundError:
            return None
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size > bound or path.is_symlink():
                raise PrivacyConflict('Managed attachment is not a bounded regular file')
            with os.fdopen(descriptor, 'rb', closefd=False) as stream:
                data = stream.read(bound + 1)
            after = path.stat(follow_symlinks=False)
            if len(data) > bound or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise PrivacyConflict('Managed attachment changed outside its canonical writer')
            return data
        finally:
            os.close(descriptor)

    def _pin(self, revision):
        pin, _, _ = self._paths(revision)
        expected = json.dumps(asdict(revision), sort_keys=True, separators=(',', ':')).encode()
        existing = self._read(pin, 16 << 10)
        if existing is not None:
            if existing != expected:
                raise PrivacyConflict('Immutable attachment recovery pin was reused')
            return
        descriptor = os.open(pin, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(expected)
            stream.flush()
            os.fsync(stream.fileno())
        self._fsync_directory(self.root)

    async def write(self, revision: ContentRevision, data: bytes, *, origins=(), parents=(),
                    wholly_attributed: bool = False) -> Path:
        """Publish only a host-verified complete fragment or generated summary.

        Mixed raw customer files must be partitioned before calling this API.
        Neither a model path nor current run union establishes file provenance.
        A durable pin and ledger intent precede every payload byte; a failed or
        interrupted write remains recoverable or an explicit digest conflict.
        """
        if (wholly_attributed is not True or type(data) is not bytes or len(data) > self.max_bytes
                or hashlib.sha256(data).hexdigest() != revision.content_sha256):
            raise PrivacyConflict('A bounded wholly attributed exact attachment revision is required')
        async with self.writer_lock:
            pin, pending, final = self._paths(revision)
            await asyncio.to_thread(self._pin, revision)
            await self.ledger.record(revision, origins=origins, parents=parents)

            def publish():
                for path in (pending, final):
                    current = self._read(path, self.max_bytes)
                    if current is not None and hashlib.sha256(current).hexdigest() != revision.content_sha256:
                        raise PrivacyConflict('Managed attachment requires explicit interrupted-write recovery')
                if final.exists():
                    return
                if not pending.exists():
                    descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                         | getattr(os, 'O_NOFOLLOW', 0), 0o600)
                    with os.fdopen(descriptor, 'wb') as stream:
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())
                os.replace(pending, final)
                self._fsync_directory(self.root)

            await self.ledger.publish(revision, publish=publish, origins=origins, parents=parents)
            return final

    async def apply(self, request: PrivacyRequest, revision: ContentRevision):
        if request.tenant_id != revision.tenant_id or revision.sink_id != self.sink_id:
            return 'conflict'
        async with self.writer_lock:
            try:
                pin, pending, final = self._paths(revision)
                expected = json.dumps(asdict(revision), sort_keys=True, separators=(',', ':')).encode()
                if await asyncio.to_thread(self._read, pin, 16 << 10) != expected:
                    return 'conflict'
                copies = []
                for path in (pending, final):
                    data = await asyncio.to_thread(self._read, path, self.max_bytes)
                    if data is not None:
                        if hashlib.sha256(data).hexdigest() != revision.content_sha256:
                            return 'conflict'
                        copies.append((path, data))
                if request.operation == 'export':
                    await self.export(request, revision, copies[-1][1] if copies else b'')
                else:
                    # Recheck every path before any deletion, including copies
                    # left between fsync and rename by an interrupted writer.
                    for path, data in copies:
                        if await asyncio.to_thread(self._read, path, self.max_bytes) != data:
                            return 'conflict'
                    for path, _ in copies:
                        path.unlink()
                    self._fsync_directory(self.root)
                return 'complete'
            except (OSError, PrivacyConflict):
                return 'conflict'
