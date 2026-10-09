import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from openagent_core.models.dispatcher import ModelDispatcher
from openagent_core.models.providers.fallback import FallbackConfig


def _catalog(moonshot_key="moonshot-key-one"):
    return [
        {
            "id": 1,
            "name": "moonshot",
            "framework": "api-based",
            "api_key": moonshot_key,
            "base_url": None,
            "enabled": True,
            "models": [
                {
                    "id": 11,
                    "model": "kimi-k3",
                    "enabled": True,
                    "metadata": {},
                }
            ],
        },
        {
            "id": 2,
            "name": "operator-cloud",
            "framework": "api-based",
            "api_key": "operator-key",
            "base_url": "https://models.example.test/v1",
            "enabled": True,
            "models": [
                {
                    "id": 21,
                    "model": "backup-model",
                    "enabled": True,
                    "metadata": {},
                }
            ],
        },
    ]


class CatalogFallbackResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dispatcher = ModelDispatcher(providers_config=[])
        self.dispatcher.set_db(
            SimpleNamespace(db_path=Path(self.temp.name) / "openagent.db"),
        )

    def test_boot_catalog_resolves_hosted_and_operator_models_with_credentials(self):
        fallback = FallbackConfig(
            on_rate_limit=[
                "moonshot:kimi-k3",
                "operator-cloud:backup-model",
            ],
        )
        self.dispatcher.set_fallback_config(fallback)
        self.assertEqual(
            ["moonshot:kimi-k3", "operator-cloud:backup-model"],
            fallback.on_rate_limit,
        )

        self.dispatcher.rebuild_routing(_catalog())
        kimi, operator = fallback.on_rate_limit
        self.assertEqual("kimi-k3", kimi.id)
        self.assertEqual("moonshot-key-one", kimi.api_key)
        self.assertEqual("https://api.moonshot.ai/v1", kimi.base_url)
        self.assertEqual("backup-model", operator.id)
        self.assertEqual("operator-key", operator.api_key)
        self.assertEqual("https://models.example.test/v1", operator.base_url)

        # Runtime initialization deep-copies already-built Model objects; it
        # no longer asks the small generic vendor map to parse these refs.
        fallback.resolve_models()
        self.assertEqual(["kimi-k3", "backup-model"], [
            model.id for model in fallback.on_rate_limit
        ])

    def test_catalog_reload_rebuilds_fallback_with_rotated_credentials(self):
        fallback = FallbackConfig(on_rate_limit=["moonshot:kimi-k3"])
        self.dispatcher.set_fallback_config(fallback)
        self.dispatcher.rebuild_routing(_catalog("moonshot-key-one"))
        first = fallback.on_rate_limit[0]

        self.dispatcher.rebuild_routing(_catalog("moonshot-key-two"))
        second = fallback.on_rate_limit[0]
        self.assertIsNot(first, second)
        self.assertEqual("moonshot-key-two", second.api_key)


if __name__ == "__main__":
    unittest.main()
