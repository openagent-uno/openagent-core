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
        effective = self.dispatcher.fallback_config
        kimi, operator = effective.on_rate_limit
        self.assertEqual("kimi-k3", kimi.id)
        self.assertEqual("moonshot-key-one", kimi.api_key)
        self.assertEqual("https://api.moonshot.ai/v1", kimi.base_url)
        self.assertEqual("backup-model", operator.id)
        self.assertEqual("operator-key", operator.api_key)
        self.assertEqual("https://models.example.test/v1", operator.base_url)

        # Runtime initialization deep-copies already-built Model objects; it
        # no longer asks the small generic vendor map to parse these refs.
        effective.resolve_models()
        self.assertEqual(["kimi-k3", "backup-model"], [
            model.id for model in effective.on_rate_limit
        ])

    def test_catalog_reload_rebuilds_fallback_with_rotated_credentials(self):
        fallback = FallbackConfig(on_rate_limit=["moonshot:kimi-k3"])
        self.dispatcher.set_fallback_config(fallback)
        self.dispatcher.rebuild_routing(_catalog("moonshot-key-one"))
        previous = self.dispatcher.fallback_config
        first = previous.on_rate_limit[0]

        self.dispatcher.rebuild_routing(_catalog("moonshot-key-two"))
        second = self.dispatcher.fallback_config.on_rate_limit[0]
        self.assertIsNot(first, second)
        self.assertEqual("moonshot-key-two", second.api_key)
        self.assertIs(first, previous.on_rate_limit[0])
        self.assertEqual("moonshot-key-one", first.api_key)

    def test_automatic_turn_has_backups_without_changing_manual_configuration(self):
        import asyncio
        from unittest.mock import patch, AsyncMock
        from openagent_core.models.dispatcher import TeamRouterProvider
        from openagent_core.models.base import ModelResponse

        self.dispatcher.rebuild_routing(_catalog())
        self.assertIsNone(self.dispatcher.fallback_config)
        with patch.object(TeamRouterProvider, "generate", AsyncMock(return_value=ModelResponse(content="ok"))):
            asyncio.run(self.dispatcher.generate([{"role": "user", "content": "hello"}]))
        automatic = self.dispatcher._team_providers["moonshot:kimi-k3"]
        self.assertIsNotNone(automatic._fallback_config)
        self.assertIn("backup-model", [model.id for model in automatic._fallback_config.on_rate_limit])
        self.assertIsNone(self.dispatcher.fallback_config, "Automatic recovery must not replace configured policy")
        manual = self.dispatcher.build_override_model("moonshot:kimi-k3")
        self.assertIsNot(manual, automatic)
        self.assertIsNone(manual._fallback_config)


if __name__ == "__main__":
    unittest.main()


class SubscriptionBackupTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolved_subscription_backup_keeps_in_process_account_transport(self):
        import httpx
        from unittest.mock import patch
        from openagent_core.models.native_provider import NativeProvider
        from openagent_core.models.providers.message import Message

        calls = []
        def completion(request):
            calls.append(request.url.host)
            return httpx.Response(200, json={"id": "fixture", "object": "chat.completion",
                "created": 1, "model": "gpt-example", "choices": [{"index": 0,
                "message": {"role": "assistant", "content": "RECOVERED"}, "finish_reason": "stop"}]})
        client = httpx.AsyncClient(transport=httpx.MockTransport(completion))
        service = SimpleNamespace(settings=SimpleNamespace(api_key="fixture-only"),
            inference_base_url=lambda *a, **k: "http://subscription.internal/v1",
            inference_client=lambda *a, **k: client)
        config = [{"name": "subscription-card", "framework": "api-based", "enabled": True,
            "api_key": "fixture-only", "base_url": "http://subscription.internal/v1",
            "metadata": {"access": "subscription", "vendor": "openai", "account_id": "account-one"},
            "models": [{"model": "gpt-example", "enabled": True, "metadata": {}}]}]
        try:
            with patch("openagent_core.subscriptions.service.subscription_for", return_value=service):
                model = NativeProvider("subscription-card:gpt-example", providers_config=config).build_runtime_model()
            fallback = FallbackConfig(on_rate_limit=[model])
            fallback.resolve_models()
            backup = fallback.on_rate_limit[0]
            async def forbid_network(*args, **kwargs):
                raise AssertionError("A subscription backup must never use DNS or an external socket")
            with patch.object(httpx.AsyncHTTPTransport, "handle_async_request", forbid_network):
                result = await backup.ainvoke([Message(role="user", content="hello")], Message(role="assistant"))
            self.assertEqual(result.content, "RECOVERED")
            self.assertEqual(calls, ["subscription.internal"])
        finally:
            await client.aclose()


class AutomaticEligibilityTests(unittest.TestCase):
    def test_explicit_disabled_models_and_providers_cannot_reenter_auto(self):
        from openagent_core.models.automatic_fallback import automatic_fallback_config
        from openagent_core.models.providers.openai.like import OpenAILike
        providers=_catalog()
        providers[0]['enabled']=False
        providers[1]['models'][0]['enabled']=False
        first=OpenAILike(id='kimi-k3');first._openagent_runtime_id='moonshot:kimi-k3'
        second=OpenAILike(id='backup-model');second._openagent_runtime_id='operator-cloud:backup-model'
        config=automatic_fallback_config(providers,FallbackConfig(on_error=[first,second]))
        self.assertFalse(config.eligible(first,{}))
        self.assertFalse(config.eligible(second,{}))

    def test_explicit_backup_precedence_is_preserved_before_connection_preference(self):
        from openagent_core.models.automatic_fallback import automatic_fallback_config
        from openagent_core.models.providers.fallback import _ordered_candidates,reset_breaker
        from openagent_core.models.providers.openai.like import OpenAILike
        providers=_catalog()
        providers[0]['models'].append({'model':'explicit-backup','enabled':True,'metadata':{}})
        explicit=OpenAILike(id='explicit-backup');explicit._openagent_runtime_id='moonshot:explicit-backup'
        config=automatic_fallback_config(providers,FallbackConfig(on_error=[explicit]))
        reset_breaker()
        candidates=_ordered_candidates(config.on_error,fallback_config=config,primary_model_id='moonshot:kimi-k3')
        self.assertEqual(candidates[0]._openagent_runtime_id,'moonshot:explicit-backup')

    def test_each_explicit_error_policy_keeps_its_own_relative_order(self):
        from openagent_core.models.automatic_fallback import automatic_fallback_config
        from openagent_core.models.providers.fallback import _ordered_candidates,reset_breaker
        from openagent_core.models.providers.openai.like import OpenAILike
        providers=_catalog();providers[0]['models'].append({'model':'b','enabled':True})
        b=OpenAILike(id='b');b._openagent_runtime_id='moonshot:b'
        c=OpenAILike(id='backup-model');c._openagent_runtime_id='operator-cloud:backup-model'
        config=automatic_fallback_config(providers,FallbackConfig(on_error=[b,c],on_rate_limit=[c,b]))
        reset_breaker()
        for field,expected in [('on_error',[b,c]),('on_rate_limit',[c,b])]:
            actual=_ordered_candidates(getattr(config,field),fallback_config=config,primary_model_id='moonshot:kimi-k3')
            self.assertEqual(actual[:2],expected)

    def test_backup_opt_out_does_not_disable_a_healthy_primary_or_explicit_policy(self):
        from openagent_core.models.automatic_fallback import automatic_fallback_config
        from openagent_core.models.native_provider import NativeProvider
        providers=_catalog();providers[0]['models'][0]['metadata']={'auto_fallback':False}
        primary=NativeProvider('moonshot:kimi-k3',providers_config=providers).build_runtime_model()
        config=automatic_fallback_config(providers,None)
        self.assertTrue(config.eligible(primary,{}))
        self.assertNotIn('moonshot:kimi-k3',[model._openagent_runtime_id for model in config.on_error])
        explicit=automatic_fallback_config(providers,FallbackConfig(on_error=[primary]))
        self.assertTrue(explicit.eligible(primary,{}))
        self.assertIs(explicit.on_error[0],primary)

    def test_health_failure_in_one_instance_does_not_disable_another_catalog(self):
        from openagent_core.models.automatic_fallback import automatic_fallback_config
        from openagent_core.models.native_provider import NativeProvider
        from openagent_core.models.providers.fallback import _record_automatic_failure, _check_automatic_primary, reset_breaker
        from openagent_core.core.runtime_errors import ModelRateLimitError, ModelProviderError
        providers=_catalog();reset_breaker()
        try:
            configs=[automatic_fallback_config(providers,None,db_path=f'/tmp/fixture-instance-{name}.sqlite') for name in ('a','b')]
            models=[NativeProvider('moonshot:kimi-k3',providers_config=providers,db_path=f'/tmp/fixture-instance-{name}.sqlite').build_runtime_model() for name in ('a','b')]
            _record_automatic_failure(models[0],configs[0],ModelRateLimitError('fixture quota'))
            with self.assertRaises(ModelProviderError):_check_automatic_primary(models[0],configs[0],{})
            _check_automatic_primary(models[1],configs[1],{})
        finally:reset_breaker()
