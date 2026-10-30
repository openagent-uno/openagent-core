"""SQLite handles must close without relying on GC or POSIX unlink semantics."""
from __future__ import annotations
import gc
import sqlite3

import pytest

from openagent_capability_host import _sqlite
from openagent_capability_host.audit import AuditLedger
from openagent_capability_host.idempotency import IdempotencyLedger
from openagent_capability_host.lease import MutatingLease
from openagent_tool_protocol.types import HostError, ToolResult


@pytest.fixture
def retained_connections(monkeypatch):
    original = sqlite3.connect
    retained = []
    def connect(*args, **kwargs):
        # Ledger I/O runs on worker threads. Only the retained-handle probe
        # relaxes thread affinity so the test can distinguish closed handles
        # from a misleading wrong-thread error. Production settings stay intact.
        kwargs["check_same_thread"] = False
        connection = original(*args, **kwargs)
        retained.append(connection)
        return connection
    was_enabled = gc.isenabled()
    gc.disable()
    monkeypatch.setattr(_sqlite.sqlite3, "connect", connect)
    try:
        yield retained
    finally:
        for connection in retained:
            connection.close()
        if was_enabled:
            gc.enable()


def assert_closed(connections):
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


@pytest.mark.parametrize("kind", [AuditLedger, IdempotencyLedger, MutatingLease])
def test_initialization_releases_handles_even_when_connections_are_retained(tmp_path, retained_connections, kind):
    database = tmp_path / "owned.sqlite3"
    kind(database)
    assert_closed(retained_connections)
    # This is a real Windows deletion oracle; retained Python references and
    # disabled cyclic GC cannot accidentally make a leaked handle pass.
    database.unlink()


@pytest.mark.asyncio
async def test_public_audit_operations_close_each_transaction(tmp_path, retained_connections):
    ledger = AuditLedger(tmp_path / "audit.sqlite3")
    await ledger.append(call_id="owned", principal="owner", server="fixture", tool="read",
        classification="read_only", outcome="success", argument_keys=[])
    assert (await ledger.recent())[0]["call_id"] == "owned"
    assert_closed(retained_connections)


@pytest.mark.asyncio
async def test_public_idempotency_replay_and_abandon_release_handles(tmp_path, retained_connections):
    ledger = IdempotencyLedger(tmp_path / "state.sqlite3")
    arguments = {"server": "fixture", "tool": "mutate", "args": {"owned": True}}
    assert (await ledger.claim("owner", "once", **arguments)).state == "new"
    assert await ledger.renew("owner", "once")
    await ledger.complete("owner", "once", ToolResult.text("recorded"))
    assert (await ledger.claim("owner", "once", **arguments)).state == "replay"
    assert (await ledger.claim("owner", "never-dispatched", **arguments)).state == "new"
    await ledger.abandon("owner", "never-dispatched")
    assert (await ledger.claim("owner", "never-dispatched", **arguments)).state == "new"
    await ledger.mark_indeterminate("owner", "never-dispatched")
    with pytest.raises(HostError, match="previous mutation"):
        await ledger.claim("owner", "never-dispatched", **arguments)
    assert_closed(retained_connections)


@pytest.mark.asyncio
async def test_public_lease_conflicts_and_release_preserve_transactions(tmp_path, retained_connections):
    lease = MutatingLease(tmp_path / "state.sqlite3")
    await lease.enter("owner", "call", "fixture")
    assert await lease.renew("owner", "call")
    with pytest.raises(HostError, match="another"):
        await lease.enter("other", "different", "fixture")
    assert (await lease.status())["holder"] == "owner"
    await lease.leave("owner", "call")
    assert (await lease.status())["state"] == "free"
    await lease.enter("owner", "next", "fixture")
    await lease.release_principal("owner")
    assert (await lease.status())["state"] == "free"
    assert_closed(retained_connections)


@pytest.mark.parametrize("abort", [False, True])
def test_scope_commits_or_rolls_back_before_closing(tmp_path, retained_connections, abort):
    path = tmp_path / "transaction.sqlite3"
    with _sqlite.transaction(path) as connection:
        connection.execute("CREATE TABLE owned (value TEXT)")
    class OwnedFailure(Exception):
        pass
    try:
        with _sqlite.transaction(path) as connection:
            connection.execute("INSERT INTO owned VALUES ('one')")
            if abort:
                raise OwnedFailure
    except OwnedFailure:
        assert abort
    with _sqlite.transaction(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM owned").fetchone()[0] == (0 if abort else 1)
    assert_closed(retained_connections)


def test_pragma_setup_failure_also_closes_connection(tmp_path, monkeypatch):
    retained = []
    original = sqlite3.connect
    class BrokenSetup(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if sql == "PRAGMA busy_timeout=5000":
                raise sqlite3.OperationalError("owned setup failure")
            return super().execute(sql, *args, **kwargs)
    def connect(*args, **kwargs):
        kwargs["factory"] = BrokenSetup
        connection = original(*args, **kwargs)
        retained.append(connection)
        return connection
    monkeypatch.setattr(_sqlite.sqlite3, "connect", connect)
    with pytest.raises(sqlite3.OperationalError, match="owned setup failure"):
        with _sqlite.transaction(tmp_path / "broken.sqlite3"):
            pytest.fail("setup failure must not enter the transaction body")
    assert_closed(retained)
