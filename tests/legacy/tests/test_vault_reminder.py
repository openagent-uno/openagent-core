"""Vault-save reminder — unit tests.

Verifies the turn-counter mechanic and reminder text without any
external services (SQLite in-memory, explicit instance settings).
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from ._framework import TestContext, test


@asynccontextmanager
async def _make_db():
    """Return a fresh in-memory MemoryDB (connected)."""
    import aiosqlite

    class _Shim:
        def __init__(self, conn):
            self._conn = conn

    conn = await aiosqlite.connect(":memory:")
    # Bootstrap the schema so vault_save_reminders exists.
    from openagent_core.memory.db import SCHEMA_SQL
    await conn.executescript(SCHEMA_SQL)
    try:
        yield _Shim(conn)
    finally:
        await conn.close()


@test("vault_reminder", "explicitly disabled — maybe_render_reminder returns None")
async def t_explicit_off(ctx: TestContext) -> None:
    from openagent_core.learning.vault_reminder import VaultReminderSettings, maybe_render_reminder
    async with _make_db() as db:
        result = await maybe_render_reminder(db, "sess-1", settings=VaultReminderSettings(enabled=False))
        assert result is None, f"expected None when disabled, got: {result!r}"


@test("vault_reminder", "on by default — fires on the first turn")
async def t_default_on_first_turn(ctx: TestContext) -> None:
    from openagent_core.learning.vault_reminder import VaultReminderSettings, maybe_render_reminder
    async with _make_db() as db:
        first = await maybe_render_reminder(db, "sess-default", settings=VaultReminderSettings())
        assert first is not None, "expected a reminder on the first turn by default"


@test("vault_reminder", "every=3: fires at turn 1, 3, 6 (first prompt + every 3)")
async def t_fires_first_and_every_n(ctx: TestContext) -> None:
    from openagent_core.learning.vault_reminder import VaultReminderSettings, maybe_render_reminder
    async with _make_db() as db:
        settings = VaultReminderSettings(enabled=True, every_n_turns=3)
        fired = []
        for _ in range(7):
            reminder = await maybe_render_reminder(db, "sess-fire", settings=settings)
            fired.append(reminder is not None)
        assert fired == [True, False, True, False, False, True, False], fired


@test("vault_reminder", "reminder text says ALWAYS save + has vault/wikilinks")
async def t_reminder_text_content(ctx: TestContext) -> None:
    from openagent_core.learning.vault_reminder import VaultReminderSettings, maybe_render_reminder
    async with _make_db() as db:
        reminder = await maybe_render_reminder(db, "sess-text", settings=VaultReminderSettings())
        assert reminder is not None, "expected a reminder on turn 1"
        low = reminder.lower()
        assert "always" in low, f"'ALWAYS' not in reminder: {reminder!r}"
        assert "vault" in low, f"'vault' not found in reminder text: {reminder!r}"
        assert "wikilinks" in low, f"'wikilinks' not found in reminder text: {reminder!r}"
