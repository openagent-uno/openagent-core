"""Real SQLite CRUD identity/pin preservation and session-free authorization."""
from __future__ import annotations
import tempfile
from pathlib import Path
import unittest
from openagent_core.administration import ManagementContext, ProviderModelAdmin
from openagent_core.contracts import PrincipalRef, AuthorizationDenied
from openagent_core.engine import MemoryDB


class Policy:
    allowed = True
    async def authorize(self, context, action, resource, *, audience=()):
        return self.allowed and context.authority.subject_id == "alice" and resource.resource_id == "agent-a"


class AdministrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = MemoryDB(str(Path(self.tmp.name) / "state.sqlite"))
        await self.db.connect()
        self.policy = Policy()
        self.reloads = 0
        async def reload(): self.reloads += 1
        self.admin = ProviderModelAdmin(self.db, self.policy, reload_catalog=reload)
        self.context = ManagementContext(PrincipalRef("test", "tenant-a", "alice", "user"), "agent-a")

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def test_rename_preserves_ids_secrets_metadata_and_exact_pins(self):
        p = await self.admin.create_provider(self.context, dict(name="local", framework="api-based", api_key="private-key", base_url="http://127.0.0.1:9001/v1", metadata={"host":"a"}))
        m = await self.admin.create_model(self.context, dict(provider_id=p["id"], model="model-one", metadata={"input_modalities":["text","image"]}))
        self.assertNotIn("api_key", p)
        self.assertNotIn("api_key", m)
        await self.db.pin_session_model("session-one", m["runtime_id"])
        p2 = await self.admin.update_provider(self.context, p["id"], {"name":"renamed"})
        self.assertEqual(p["id"], p2["id"])
        self.assertEqual((await self.db.get_provider(p["id"]))["api_key"], "private-key")
        self.assertEqual(p2["metadata"], {"host":"a"})
        m2 = await self.admin.model(self.context, m["id"])
        self.assertEqual(await self.db.get_session_pin("session-one"), m2["runtime_id"])
        m3 = await self.admin.update_model(self.context, m["id"], {"model":"model-two", "display_name":"Second"})
        self.assertEqual(m["id"], m3["id"])
        self.assertEqual(m3["provider_id"], p["id"])
        self.assertEqual(m3["metadata"]["input_modalities"], ["text","image"])
        self.assertEqual(await self.db.get_session_pin("session-one"), m3["runtime_id"])
        self.assertEqual(len(await self.db.list_providers()), 1)
        self.assertEqual(len(await self.db.list_models()), 1)
        self.assertEqual(self.reloads, 4)

    async def test_invalid_rename_rolls_back_pin_and_identity(self):
        p = await self.admin.create_provider(self.context, dict(name="local", framework="api-based"))
        m = await self.admin.create_model(self.context, dict(provider_id=p["id"], model="one"))
        await self.admin.create_model(self.context, dict(provider_id=p["id"], model="two"))
        await self.db.pin_session_model("session-one", m["runtime_id"])
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            await self.admin.update_model(self.context, m["id"], {"model":"two"})
        self.assertEqual((await self.db.get_model(m["id"]))["model"], "one")
        self.assertEqual(await self.db.get_session_pin("session-one"), m["runtime_id"])
        with self.assertRaises(ValueError):
            await self.admin.update_provider(self.context, p["id"], {"framework":"claude-code"})

    async def test_fresh_authority_without_synthetic_session(self):
        self.assertFalse(hasattr(self.context, "session_id"))
        await self.admin.create_provider(self.context, dict(name="local", framework="api-based"))
        self.policy.allowed = False
        with self.assertRaises(AuthorizationDenied): await self.admin.providers(self.context)
        with self.assertRaises(AuthorizationDenied): await self.admin.create_provider(self.context, dict(name="other", framework="api-based"))
        self.assertEqual(len(await self.db.list_providers()), 1)

    def test_discovery_cache_separates_configured_endpoints(self):
        from openagent_core.models.discovery import _cache_key
        self.assertNotEqual(_cache_key("local", "same-key", "http://one/v1"), _cache_key("local", "same-key", "http://two/v1"))
        self.assertNotIn("same-key", _cache_key("local", "same-key", "http://one/v1"))


if __name__ == "__main__": unittest.main()
