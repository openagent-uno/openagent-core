"""Notifications for jobs owned by capability executors, without executing them.

The host supplies a context key when it registers and completes a job. The
engine drains only that key, so another user/device cannot inherit its output.
"""
from __future__ import annotations
import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class JobEvent:
    job_id: str
    kind: str
    source_id: str
    tool_name: str
    arguments: Mapping[str, Any]
    summary: str
    at: float = field(default_factory=time.time)


class BackgroundJobs:
    def __init__(self):
        self._running: dict[tuple[str,str],set[str]] = {}
        self._queues: dict[tuple[str,str],deque] = {}
        self._signals: dict[tuple[str,str],asyncio.Event] = {}

    def register(self, job_id: str, session_id: str, context_key: str) -> None:
        self._running.setdefault((session_id,context_key),set()).add(job_id)

    def complete(self, session_id: str, context_key: str, event: JobEvent) -> None:
        key=(session_id,context_key)
        # A stale transport completion cannot introduce a new job into a
        # different context. Only the host that registered it can finish it.
        running=self._running.get(key,set())
        if event.job_id not in running:
            return
        running.remove(event.job_id)
        self._queues.setdefault(key,deque(maxlen=200)).append(event)
        self._signals.setdefault(key,asyncio.Event()).set()

    def has_running(self, session_id, *, context_key: str) -> bool:
        return bool(self._running.get((session_id,context_key)))

    def drain(self, session_id, *, context_key: str) -> list[JobEvent]:
        key=(session_id,context_key)
        queue=self._queues.get(key)
        events=list(queue or ())
        if queue is not None: queue.clear()
        signal=self._signals.get(key)
        if signal is not None: signal.clear()
        return events

    async def wait(self, session_id, timeout: float, *, context_key: str) -> list[JobEvent]:
        key=(session_id,context_key)
        if self._queues.get(key) or timeout<=0:
            return self.drain(session_id,context_key=context_key)
        try:
            await asyncio.wait_for(self._signals.setdefault(key,asyncio.Event()).wait(),timeout)
        except TimeoutError:
            pass
        return self.drain(session_id,context_key=context_key)

    async def purge_session(self,session_id: str) -> None:
        # The executor owns process cancellation and resource cleanup.
        for mapping in (self._running,self._queues,self._signals):
            for key in tuple(mapping):
                if key[0]==session_id:
                    value=mapping.pop(key)
                    if isinstance(value,asyncio.Event): value.set()

    async def close(self):
        for signal in self._signals.values(): signal.set()
        self._signals.clear();self._running.clear();self._queues.clear()
