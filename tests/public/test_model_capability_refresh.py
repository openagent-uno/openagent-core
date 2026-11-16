"""A model's media declaration follows its provider unless a person set it.

Registering a model reads the provider's declared ``input_modalities``; a
refresh (after a login or a provider change) applies the provider's current
declaration to every model that a person did not set by hand.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from openagent_core.administration import ManagementContext, ProviderModelAdmin
from openagent_core.contracts import PrincipalRef
from openagent_core.engine import MemoryDB


class Policy:
    async def authorize(self, context, action, resource, *, audience=()):
        return True


class ModelCapabilityRefresh(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = MemoryDB(str(Path(self.tmp.name) / "state.sqlite"))
        await self.db.connect()
        self.reloads = 0

        async def reload():
            self.reloads += 1

        self.admin = ProviderModelAdmin(self.db, Policy(), reload_catalog=reload)
        self.context = ManagementContext(PrincipalRef("test", "tenant", "alice", "user"), "agent")
        self.catalog = []
        patcher = mock.patch("openagent_core.models.discovery.list_provider_models", self._discover)
        patcher.start()
        self.addCleanup(patcher.stop)
        provider = await self.admin.create_provider(self.context, {
            "name": "proxy", "framework": "api-based", "base_url": "http://proxy.invalid/v1", "api_key": "k"})
        self.provider_id = provider["id"]

    async def asyncTearDown(self):
        await self.db.close()
        self.tmp.cleanup()

    async def _discover(self, provider, api_key=None, base_url=None):
        return list(self.catalog)

    async def _create(self, model, **fields):
        return await self.admin.create_model(self.context, {"provider_id": self.provider_id, "model": model, **fields})

    async def test_registration_uses_the_provider_declaration(self):
        self.catalog = [{"id": "vision-llm", "input_modalities": ["text", "image", "file"],
                         "capabilities": ["chat", "vision"]}]
        row = await self._create("vision-llm")
        self.assertEqual(["text", "image", "file"], row["metadata"]["input_modalities"])
        self.assertEqual("provider", row["metadata"]["input_modalities_source"])

    async def test_refresh_corrects_a_stale_declaration(self):
        row = await self._create("claude-opus-5")
        self.assertEqual("default", row["metadata"]["input_modalities_source"])
        await self.admin.update_model(self.context, row["id"], {"metadata": {
            "input_modalities": ["text"], "input_modalities_source": "default"}})
        self.catalog = [{"id": "claude-opus-5", "input_modalities": ["text", "image", "file"]}]
        result = await self.admin.refresh_capabilities(self.context, self.provider_id)
        self.assertEqual(["proxy:claude-opus-5"], result["updated"])
        refreshed = await self.admin.model(self.context, row["id"])
        self.assertEqual(["text", "image", "file"], refreshed["metadata"]["input_modalities"])

    async def test_a_manual_declaration_is_never_refreshed(self):
        row = await self._create("custom-llm", metadata={"input_modalities": ["text", "audio"]})
        self.assertEqual("manual", row["metadata"]["input_modalities_source"])
        self.catalog = [{"id": "custom-llm", "input_modalities": ["text"]}]
        result = await self.admin.refresh_capabilities(self.context, self.provider_id)
        self.assertEqual(([], 1), (result["updated"], result["manual"]))
        self.assertEqual(["text", "audio"], (await self.admin.model(self.context, row["id"]))["metadata"]["input_modalities"])

    async def test_auto_hands_a_manual_declaration_back_to_the_provider(self):
        row = await self._create("custom-llm", metadata={"input_modalities": ["text"]})
        await self.admin.update_model(self.context, row["id"], {"metadata": {"input_modalities_source": "auto"}})
        self.catalog = [{"id": "custom-llm", "input_modalities": ["text", "image"]}]
        await self.admin.refresh_capabilities(self.context, self.provider_id)
        metadata = (await self.admin.model(self.context, row["id"]))["metadata"]
        self.assertEqual((["text", "image"], "provider"), (metadata["input_modalities"], metadata["input_modalities_source"]))

    async def test_an_unreachable_provider_keeps_what_is_stored(self):
        self.catalog = [{"id": "vision-llm", "input_modalities": ["text", "image"]}]
        row = await self._create("vision-llm")
        self.catalog = []
        result = await self.admin.refresh_capabilities(self.context, self.provider_id)
        self.assertFalse(result["reachable"])
        self.assertEqual(["text", "image"], (await self.admin.model(self.context, row["id"]))["metadata"]["input_modalities"])


if __name__ == "__main__":
    unittest.main()
