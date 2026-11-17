"""Host-private admission shared by workers using one subscription store."""
from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from filelock import FileLock, Timeout
from .credential_storage import write_credentials


class StoreSemaphore:
    """Bound concurrent work across threads/processes; OS locks release on exit."""
    def __init__(self, store: Path, label: str, capacity: int):
        self.store, self.label, self.capacity = store, label, max(1, capacity)
        self.local = threading.local()

    def acquire(self, blocking=True, timeout=None):
        self.store.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        deadline = None if timeout is None else time.monotonic() + max(0, timeout)
        while True:
            for index in range(self.capacity):
                lock = FileLock(str(self.store) + f'.{self.label}-{index}.lock', timeout=0, mode=0o600)
                try:
                    lock.acquire()
                except Timeout:
                    continue
                held = getattr(self.local, 'held', None)
                if held is None:
                    self.local.held = held = []
                held.append(lock)
                return True
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            delay = min(0.02, max(0, deadline - time.monotonic())) if deadline is not None else 0.02
            time.sleep(delay)

    def release(self):
        held = getattr(self.local, 'held', [])
        if not held:
            raise ValueError('Subscription admission released without a lease')
        held.pop().release()


class StorePacer:
    """Reserve upstream starts and cooldowns without sharing credential data."""
    def __init__(self, store: Path):
        self.path = Path(str(store) + '.pacing.json')

    def _update(self, interval=0.0, cooldown=0.0):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with FileLock(str(self.path) + '.lock', timeout=30, mode=0o600):
            now = time.time()
            try:
                next_at = float(json.loads(self.path.read_text()).get('next_at', now))
            except (FileNotFoundError, ValueError, TypeError):
                next_at = now
            # Clock corrections must not turn an old reservation into an
            # unbounded stall; account quota/cooldowns remain in the auth store.
            next_at = min(max(now, next_at), now + 10800)
            delay = next_at - now
            next_at = max(next_at + interval, now + cooldown)
            write_credentials(self.path, {'next_at': next_at})
            return delay

    def reserve(self, interval):
        return self._update(interval=max(0, interval))

    def cool_down(self, delay):
        self._update(cooldown=max(0, delay))
