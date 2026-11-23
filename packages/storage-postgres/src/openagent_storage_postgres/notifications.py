"""One LISTEN connection per worker, shared by the observers of active runs."""
import asyncio
from collections import defaultdict
from contextlib import contextmanager
import hashlib

from psycopg import AsyncConnection, OperationalError, sql


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class RunNotifications:
    def __init__(self, dsn, schema, agent):
        self.dsn = dsn
        self.channel = "oa_" + digest(schema + "/" + agent)[:48]
        self.waiters = defaultdict(set)
        self.task = None

    def start(self):
        self.task = asyncio.create_task(self._listen(), name="openagent-pg-listen")

    async def close(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        self._wake_all()

    def _wake_all(self):
        for observers in self.waiters.values():
            for event in observers:
                event.set()

    async def _listen(self):
        while True:
            try:
                async with await AsyncConnection.connect(self.dsn, autocommit=True) as connection:
                    await connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)))
                    # Any notifications missed before LISTEN are recovered by a durable read.
                    self._wake_all()
                    async for notice in connection.notifies():
                        for event in self.waiters.get(notice.payload, ()):
                            event.set()
            except (OSError, OperationalError):
                self._wake_all()
                await asyncio.sleep(1)

    @contextmanager
    def observe(self, kind, identifier):
        key = digest(kind + "/" + identifier)
        event = asyncio.Event()
        observers = self.waiters[key]
        observers.add(event)
        try:
            yield event
        finally:
            observers.remove(event)
            if not observers:
                del self.waiters[key]

    async def publish(self, connection, kind, identifier):
        # PostgreSQL dispatches this opaque hint only after transaction commit.
        await connection.execute("SELECT pg_notify(%s,%s)", (self.channel, digest(kind + "/" + identifier)))
