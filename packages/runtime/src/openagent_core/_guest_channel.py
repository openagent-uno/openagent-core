"""Multiplex the guest capability protocol over one authenticated device connection."""

import asyncio
import fcntl
import json
from uuid import uuid4

from .capabilities import CapabilityUnavailable
from .contracts import canonical_json


class GuestChannel:
    """A single controller owns the capability-host socket for a running task VM."""

    def __init__(self, proc, lock, identity, ttl, remove, limit):
        self.proc = proc
        self.lock = lock
        self.identity = identity
        self.ttl = ttl
        self.remove = remove
        self.limit = limit
        self.pending = {}
        self.idle = None
        self.reader = asyncio.create_task(self._read())

    @classmethod
    async def open(cls, argv, env, directory, identity, ttl, remove, limit):
        lock = (directory / "transport.lock").open("a+b")
        try:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise CapabilityUnavailable("Another controller owns this device connection") from exc
            proc = await asyncio.create_subprocess_exec(
                *argv,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=limit,
            )
            return cls(proc, lock, identity, ttl, remove, limit)
        except BaseException:
            lock.close()
            raise

    def _stop_timer(self):
        if self.idle is not None:
            self.idle.cancel()
            self.idle = None

    async def _read(self):
        try:
            while raw := await self.proc.stdout.readline():
                frame = json.loads(raw)
                if frame.get("type") == "response":
                    future = self.pending.get(frame.get("id"))
                    if future is not None and not future.done():
                        future.set_result(frame)
        except (OSError, ValueError):
            pass
        finally:
            self._stop_timer()
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(CapabilityUnavailable("Device connection was lost"))
            if self.proc.returncode is None:
                try:
                    self.proc.kill()
                except ProcessLookupError:
                    pass
            await self.proc.wait()
            self.lock.close()
            self.remove(self)

    async def request(self, body):
        rid = uuid4().hex
        raw = (canonical_json({"id": rid, **body}) + "\n").encode()
        if len(raw) > self.limit:
            raise ValueError("Computer request exceeds its size limit")
        self._stop_timer()
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        try:
            async with asyncio.timeout(120):
                self.proc.stdin.write(raw)
                await self.proc.stdin.drain()
                frame = await future
            if not frame.get("ok"):
                raise CapabilityUnavailable("The assigned guest rejected the operation")
            return frame["result"]
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise CapabilityUnavailable("The device connection is unavailable") from exc
        finally:
            self.pending.pop(rid)
            future.cancel()
            if not self.pending and not self.reader.done() and not self.proc.stdin.is_closing():
                self.idle = asyncio.get_running_loop().call_later(self.ttl, self.proc.stdin.close)

    async def close(self):
        self._stop_timer()
        self.proc.stdin.close()
        try:
            async with asyncio.timeout(5):
                await asyncio.shield(self.reader)
        except TimeoutError:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
