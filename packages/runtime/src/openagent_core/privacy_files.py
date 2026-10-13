"""Exact-revision privacy sink for files exclusively managed by a host writer.

It deliberately refuses arbitrary shell paths and mixed/untracked human edits.
Hosts bind pins out of band and share the supplied writer lock with every normal
write to these files. Files and all parent directories must be regular, owned
paths below the explicit managed root; symlinks never expand the scope.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable
import hashlib
import os
import stat

from .content_privacy import ContentRevision, PrivacyRequest


@dataclass(frozen=True, slots=True)
class ManagedFilePin:
    artifact_id: str
    revision_id: str
    relative_path: str
    wholly_attributed: bool = False


class ManagedFilePrivacySink:
    def __init__(self, sink_id: str, root: Path, pins: tuple[ManagedFilePin, ...], *,
                 writer_lock, export: Callable[[PrivacyRequest, ContentRevision, bytes], Awaitable[None]]):
        self.sink_id = sink_id
        self.root = root.absolute()
        self.writer_lock = writer_lock
        self.export = export
        self.pins = {(pin.artifact_id, pin.revision_id): pin for pin in pins}
        if len(self.pins) != len(pins):
            raise ValueError("Managed revision pins must be unique")
        if not hasattr(writer_lock, '__aenter__'):
            raise ValueError("Every canonical writer must share the supplied lock")
        if not self.root.is_dir() or self.root.is_symlink():
            raise ValueError("Managed file root must be an existing host-owned directory")
        for pin in pins:
            path = Path(pin.relative_path)
            if path.is_absolute() or '..' in path.parts or not path.parts or path == Path('.'):
                raise ValueError("A pin must identify a file within the managed root")

    async def apply(self, request, revision):
        pin = self.pins.get((revision.artifact_id, revision.revision_id))
        if pin is None or not pin.wholly_attributed or revision.sink_id != self.sink_id or revision.tenant_id != request.tenant_id:
            return 'conflict'
        async with self.writer_lock:
            path = self.root
            for part in Path(pin.relative_path).parts:
                path = path / part
                if path.is_symlink():
                    return 'conflict'
            try:
                descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            except FileNotFoundError:
                # An export of an already deleted revision cannot reconstruct
                # content; callers receive an empty export through the same
                # authorized delivery callback, never silently fabricated data.
                if request.operation == 'export':
                    await self.export(request, revision, b'')
                return 'complete'
            try:
                original = os.fstat(descriptor)
                if not stat.S_ISREG(original.st_mode):
                    return 'conflict'
                with os.fdopen(descriptor, 'rb', closefd=False) as stream:
                    data = stream.read()
                if hashlib.sha256(data).hexdigest() != revision.content_sha256:
                    return 'conflict'
                current = path.stat(follow_symlinks=False)
                if (original.st_dev, original.st_ino, original.st_mtime_ns, original.st_size) != (
                        current.st_dev, current.st_ino, current.st_mtime_ns, current.st_size):
                    return 'conflict'
                if request.operation == 'export':
                    await self.export(request, revision, data)
                else:
                    path.unlink()
                return 'complete'
            finally:
                os.close(descriptor)
