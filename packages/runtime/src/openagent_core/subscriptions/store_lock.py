"""Serialize rotating OAuth credentials across Core workers and processes."""
from __future__ import annotations

import threading


class CredentialLock:
    def __init__(self, manager):
        self.manager = manager
        self.thread_lock = threading.RLock()
        self.file_lock = None

    def __enter__(self):
        self.thread_lock.acquire()
        try:
            from filelock import FileLock
            path = self.manager.creds_file
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if self.file_lock is None:
                self.file_lock = FileLock(str(path) + ".lock", timeout=30, mode=0o600)
            self.file_lock.acquire()
            # Another worker may have rotated a refresh token since our last read.
            self.manager._store = None
            return self
        except BaseException:
            self.thread_lock.release()
            raise

    def __exit__(self, *args):
        try:
            self.file_lock.release()
        finally:
            self.thread_lock.release()
