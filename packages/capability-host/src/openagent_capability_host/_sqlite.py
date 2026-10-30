"""Explicitly bounded SQLite transactions for local capability state."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import sqlite3


@contextmanager
def transaction(path: Path) -> Iterator[sqlite3.Connection]:
    """Commit/rollback one transaction and always release its connection.

    sqlite3.Connection.__exit__ manages the transaction, but does not close
    the connection. Retained cursors/reference cycles must not defer the
    release of database/WAL handles until Python's garbage collector runs.
    """
    connection = sqlite3.connect(path, timeout=5.0)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        with connection:
            yield connection
    finally:
        connection.close()
