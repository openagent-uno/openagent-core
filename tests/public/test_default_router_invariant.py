"""At most one default router, and only one the runner can actually use."""
import sqlite3
import time

import pytest
import pytest_asyncio

from openagent_core.administration import ManagementContext, ProviderModelAdmin
from openagent_core.contracts import PrincipalRef
from openagent_core.memory.db import MemoryDB


@pytest_asyncio.fixture
async def db(tmp_path):
    database = MemoryDB(str(tmp_path / "agent.db"))
    await database.connect()
    yield database
    await database.close()


def flagged(path):
    with sqlite3.connect(path) as reader:
        return [row[0] for row in reader.execute("SELECT model FROM models WHERE is_classifier = 1 ORDER BY model")]


async def catalog(db):
    first = await db.upsert_provider(name="provider-a", framework="api-based")
    second = await db.upsert_provider(name="provider-b", framework="api-based")
    return first, second


@pytest.mark.asyncio
async def test_setting_a_default_router_clears_the_previous_one(db):
    first, second = await catalog(db)
    old = await db.upsert_model(provider_id=first, model="old-model", is_classifier=True)
    new = await db.upsert_model(provider_id=second, model="new-model")
    await db.set_model_is_classifier(new, True)
    assert flagged(db.db_path) == ["new-model"]
    # Inserting a flagged model is exclusive too.
    await db.upsert_model(provider_id=first, model="third-model", is_classifier=True)
    assert flagged(db.db_path) == ["third-model"]
    # And so is a host's own SQL: the database enforces it.
    with sqlite3.connect(db.db_path) as writer:
        writer.execute("UPDATE models SET is_classifier = 1 WHERE id = ?", (old,))
    assert flagged(db.db_path) == ["old-model"]


@pytest.mark.asyncio
async def test_disabled_model_or_provider_cannot_be_default(db):
    first, second = await catalog(db)
    model = await db.upsert_model(provider_id=first, model="leader", is_classifier=True)
    await db.set_model_enabled(model, False)
    assert flagged(db.db_path) == []
    with pytest.raises(ValueError, match="disabled"):
        await db.set_model_is_classifier(model, True)

    other = await db.upsert_model(provider_id=second, model="other", is_classifier=True)
    await db.set_provider_enabled(second, False)
    assert flagged(db.db_path) == []
    with pytest.raises(ValueError, match="disabled"):
        await db.set_model_is_classifier(other, True)

    # A flagged insert under a disabled provider never takes the default
    # away from the eligible one.
    await db.set_provider_enabled(second, True)
    await db.set_model_is_classifier(other, True)
    await db.set_provider_enabled(first, True)
    await db.set_provider_enabled(second, False)
    eligible = await db.upsert_model(provider_id=first, model="eligible", is_classifier=True)
    await db.upsert_model(provider_id=second, model="ineligible", is_classifier=True)
    assert flagged(db.db_path) == ["eligible"]
    assert (await db.get_default_router())["id"] == eligible


@pytest.mark.asyncio
async def test_update_fields_disabling_the_default_drops_the_flag(db):
    first, _ = await catalog(db)
    model = await db.upsert_model(provider_id=first, model="leader", is_classifier=True)
    row = await db.update_model_fields(model, {"enabled": False})
    assert row["enabled"] is False and row["is_classifier"] is False
    with pytest.raises(ValueError, match="disabled"):
        await db.update_model_fields(model, {"is_classifier": True})
    row = await db.update_model_fields(model, {"enabled": True, "is_classifier": True})
    assert row["is_classifier"] is True


@pytest.mark.asyncio
async def test_existing_duplicate_state_is_repaired_on_open(tmp_path):
    path = tmp_path / "agent.db"
    database = MemoryDB(str(path))
    await database.connect()
    off = await database.upsert_provider(name="provider-a", framework="api-based")
    on = await database.upsert_provider(name="provider-b", framework="api-based")
    await database.upsert_model(provider_id=off, model="stale-default")
    await database.upsert_model(provider_id=on, model="current-default")
    await database.upsert_model(provider_id=on, model="second-flag")
    await database.close()

    # The state an older release could leave behind: duplicate flags, one on
    # a disabled provider, written before the invariant existed.
    with sqlite3.connect(path) as writer:
        for name in ("oa_models_default_router_insert", "oa_models_default_router_update",
                     "oa_providers_default_router_disable"):
            writer.execute(f"DROP TRIGGER {name}")
        writer.execute("UPDATE providers SET enabled = 0 WHERE id = ?", (off,))
        writer.execute("UPDATE models SET is_classifier = 1, updated_at = ?", (time.time(),))
    assert flagged(path) == ["current-default", "second-flag", "stale-default"]

    database = MemoryDB(str(path))
    await database.connect()
    try:
        # The kept row is the one the dispatcher already routed to: the first
        # eligible flagged row in catalog order.
        assert flagged(path) == ["current-default"]
        default = await database.get_default_router()
        assert default["model"] == "current-default" and default["reason"] == "router_flag"
        # The invariant is enforced again after the repair.
        stale = (await database.list_models(provider_id=off))[0]["id"]
        with pytest.raises(ValueError):
            await database.set_model_is_classifier(stale, True)
    finally:
        await database.close()
    # Reopening a consistent database changes nothing.
    before = sqlite3.connect(path).execute("SELECT MAX(updated_at) FROM models").fetchone()[0]
    database = MemoryDB(str(path))
    await database.connect()
    await database.close()
    assert sqlite3.connect(path).execute("SELECT MAX(updated_at) FROM models").fetchone()[0] == before


@pytest.mark.asyncio
async def test_public_administration_reports_the_effective_default(db):
    class Allow:
        async def authorize(self, *args, **kwargs):
            return True

    async def reload():
        return None

    admin = ProviderModelAdmin(db, Allow(), reload_catalog=reload)
    principal = PrincipalRef("host", "tenant", "admin")
    context = ManagementContext(principal, "agent")
    assert await admin.default_model(context) is None

    first, second = await catalog(db)
    a = await admin.create_model(context, {"provider_id": first, "model": "model-a"})
    assert (await admin.default_model(context))["id"] == a["id"]
    assert (await admin.default_model(context))["reason"] == "first_enabled"

    b = await admin.create_model(context, {"provider_id": second, "model": "model-b", "is_classifier": True})
    default = await admin.default_model(context)
    assert default["id"] == b["id"] and default["reason"] == "router_flag"
    await admin.create_model(context, {"provider_id": first, "model": "model-c", "is_classifier": True})
    models = await admin.models(context)
    assert [m["model"] for m in models if m["is_classifier"]] == ["model-c"]

    await admin.update_provider(context, first, {"enabled": False})
    models = await admin.models(context)
    assert not any(m["is_classifier"] for m in models)
    default = await admin.default_model(context)
    assert default["model"] == "model-b" and default["reason"] == "first_enabled"
