"""Credential pools honour the provider's Retry-After, rotate on rejected
credentials, keep pooled keys out of public views, and catalog models can
declare a live fallback chain.

LLM-free: fake models stand in for runtime Models and a fake SDK error carries
the HTTP response the provider adapters chain with ``raise ... from``.
"""
import asyncio
import tempfile
import time
import unittest
from email.utils import format_datetime
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from openagent_core.administration import public_provider
from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.credential_pool import CredentialPool, PooledAccount, retry_after_seconds, rotates_account
from openagent_core.models.dispatcher import ModelDispatcher
from openagent_core.models.providers.fallback import FallbackConfig, acall_model_with_fallback, acall_model_stream_with_fallback
from openagent_core.models.providers.response import ModelResponse


class _SDKError(Exception):
    def __init__(self, status, headers):
        super().__init__(f"HTTP {status}")
        self.response = SimpleNamespace(status_code=status, headers=headers)


def _failure(status, headers=None):
    try:
        raise _SDKError(status, headers or {})
    except _SDKError as cause:
        try:
            raise ModelProviderError("provider refused", status_code=status) from cause
        except ModelProviderError as error:
            return error


class _Model:
    def __init__(self, failures):
        self.id, self.api_key, self.base_url = "relay-model", "first", "http://relay.test/v1"
        self.client = self.async_client = object()
        self.failures = failures  # api_key -> ModelProviderError
        self.calls = []

    async def aresponse(self, **kwargs):
        self.calls.append(self.api_key)
        if self.api_key in self.failures:
            raise self.failures[self.api_key]
        return SimpleNamespace(key=self.api_key)

    async def aresponse_stream(self, **kwargs):
        self.calls.append(self.api_key)
        if self.api_key in self.failures:
            raise self.failures[self.api_key]
        yield ModelResponse(event="content", content="ok-" + self.api_key)


def _pooled(model, keys):
    pool = CredentialPool([PooledAccount(api_key=key, base_url=model.base_url) for key in keys])
    model._openagent_cred_pool = pool
    return pool


class RetryAfterTests(unittest.TestCase):
    def test_reads_seconds_milliseconds_and_http_dates_through_the_chain(self):
        self.assertEqual(retry_after_seconds(_failure(429, {"retry-after": "7200"})), 7200.0)
        self.assertEqual(retry_after_seconds(_failure(429, {"retry-after-ms": "1500"})), 1.5)
        when = datetime.now(timezone.utc) + timedelta(days=2)
        hint = retry_after_seconds(_failure(429, {"retry-after": format_datetime(when, usegmt=True)}))
        self.assertAlmostEqual(hint, 2 * 86400, delta=5)
        self.assertIsNone(retry_after_seconds(_failure(429)))
        self.assertIsNone(retry_after_seconds(_failure(429, {"retry-after": "soon"})))
        self.assertIsNone(retry_after_seconds(ModelProviderError("no cause", status_code=429)))

    def test_rotation_reasons(self):
        self.assertTrue(rotates_account(_failure(429)))
        self.assertTrue(rotates_account(_failure(529)))
        self.assertTrue(rotates_account(_failure(401)))
        self.assertTrue(rotates_account(_failure(403)))
        self.assertFalse(rotates_account(_failure(500)))
        self.assertFalse(rotates_account(_failure(400)))
        self.assertFalse(rotates_account(ValueError("not a provider error")))


class PoolRotationTests(unittest.TestCase):
    def test_usage_limit_cools_the_account_for_the_provider_reset(self):
        model = _Model({"first": _failure(429, {"retry-after": str(3 * 86400)})})
        pool = _pooled(model, ["first", "second"])
        before = time.monotonic()
        result = asyncio.run(acall_model_with_fallback(model, None, messages=[]))
        self.assertEqual(result.key, "second")
        first = pool._accounts[0]
        self.assertEqual(first.last_status, "EXHAUSTED")
        self.assertAlmostEqual(first.last_error_reset_at - before, 3 * 86400, delta=5)

    def test_short_burst_and_missing_hint(self):
        model = _Model({"first": _failure(429, {"retry-after": "20"})})
        pool = _pooled(model, ["first", "second"])
        asyncio.run(acall_model_with_fallback(model, None, messages=[]))
        self.assertAlmostEqual(pool._accounts[0].last_error_reset_at - time.monotonic(), 20, delta=2)
        model = _Model({"first": _failure(429)})
        pool = _pooled(model, ["first", "second"])
        asyncio.run(acall_model_with_fallback(model, None, messages=[]))
        self.assertAlmostEqual(pool._accounts[0].last_error_reset_at - time.monotonic(), 3600, delta=2)

    def test_rejected_credential_rotates_and_retires_the_account(self):
        model = _Model({"first": _failure(401)})
        pool = _pooled(model, ["first", "second"])
        result = asyncio.run(acall_model_with_fallback(model, None, messages=[]))
        self.assertEqual((result.key, model.calls), ("second", ["first", "second"]))
        self.assertEqual(pool._accounts[0].last_status, "DEAD")

    def test_stream_rotates_on_rejected_credential_before_output(self):
        model = _Model({"first": _failure(403)})
        pool = _pooled(model, ["first", "second"])

        async def collect():
            return [event async for event in acall_model_stream_with_fallback(model, None, messages=[])]

        events = asyncio.run(collect())
        self.assertEqual([event.content for event in events], ["ok-second"])
        self.assertEqual(pool._accounts[0].last_status, "DEAD")

    def test_other_failures_do_not_rotate(self):
        model = _Model({"first": _failure(500)})
        _pooled(model, ["first", "second"])
        with self.assertRaises(ModelProviderError):
            asyncio.run(acall_model_with_fallback(model, None, messages=[]))
        self.assertEqual(model.calls, ["first"])

    def test_every_account_limited_falls_back(self):
        backup = SimpleNamespace(id="backup", aresponse=None)

        async def answer(**kwargs):
            return SimpleNamespace(key="backup")

        backup.aresponse = answer
        model = _Model({"first": _failure(429), "second": _failure(429)})
        _pooled(model, ["first", "second"])
        result = asyncio.run(acall_model_with_fallback(model, FallbackConfig(on_rate_limit=[backup]), messages=[]))
        self.assertEqual(result.key, "backup")


class PublicProviderTests(unittest.TestCase):
    def test_pooled_keys_are_write_only(self):
        row = {"id": 1, "name": "relay", "framework": "api-based", "api_key": "primary-secret-key", "enabled": True,
               "metadata": {"pool_strategy": "fill_first", "accounts": [
                   {"api_key": "pooled-secret-one", "label": "work", "priority": 2},
                   {"api_key": "pooled-secret-two", "base_url": "http://other.test/v1"}]}}
        out = public_provider(row)
        rendered = repr(out)
        self.assertNotIn("pooled-secret", rendered)
        self.assertNotIn("primary-secret", rendered)
        self.assertEqual(out["metadata"]["accounts"][0], {"label": "work", "priority": 2, "api_key_display": "****-one"})
        self.assertEqual(out["metadata"]["accounts"][1]["base_url"], "http://other.test/v1")
        self.assertEqual(out["metadata"]["pool_strategy"], "fill_first")
        self.assertEqual(row["metadata"]["accounts"][0]["api_key"], "pooled-secret-one", "stored row untouched")


def _catalog(priorities):
    def model(model_id, name, provider_id):
        metadata = {} if name not in priorities else {"fallback_priority": priorities[name]}
        return {"id": model_id, "model": name, "enabled": True, "metadata": metadata}
    return [
        {"id": 1, "name": "operator-a", "framework": "api-based", "api_key": "key-a", "base_url": "https://a.example.test/v1", "enabled": True,
         "models": [model(11, "primary-model", 1), model(12, "second-model", 1)]},
        {"id": 2, "name": "operator-b", "framework": "api-based", "api_key": "key-b", "base_url": "https://b.example.test/v1", "enabled": True,
         "models": [model(21, "backup-model", 2)]},
    ]


class CatalogFallbackChainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dispatcher = ModelDispatcher(providers_config=[])
        self.dispatcher.set_db(SimpleNamespace(db_path=Path(self.temp.name) / "openagent.db"))

    def chain(self):
        config = self.dispatcher.fallback_config
        return None if config is None else ([m.id for m in config.on_rate_limit], [m.id for m in config.on_error])

    def test_catalog_declared_chain_follows_the_live_catalog(self):
        self.dispatcher.set_fallback_config(None)
        self.dispatcher.rebuild_routing(_catalog({}))
        self.assertIsNone(self.dispatcher.fallback_config)
        self.dispatcher.rebuild_routing(_catalog({"backup-model": 0, "second-model": 5}))
        self.assertEqual(self.chain(), (["backup-model", "second-model"], ["backup-model", "second-model"]))
        backup = self.dispatcher.fallback_config.on_rate_limit[0]
        self.assertEqual((backup.api_key, backup.base_url), ("key-b", "https://b.example.test/v1"))
        self.dispatcher.rebuild_routing(_catalog({"second-model": 1}))
        self.assertEqual(self.chain(), (["second-model"], ["second-model"]))
        self.dispatcher.rebuild_routing(_catalog({}))
        self.assertIsNone(self.dispatcher.fallback_config, "clearing the key removes the chain")

    def test_configured_refs_run_first_and_are_not_repeated(self):
        self.dispatcher.set_fallback_config(FallbackConfig(on_rate_limit=["operator-a:second-model"]))
        self.dispatcher.rebuild_routing(_catalog({"backup-model": 0, "second-model": 1}))
        self.assertEqual(self.chain(), (["second-model", "backup-model"], ["backup-model", "second-model"]))

    def test_invalid_priorities_are_ignored(self):
        self.dispatcher.set_fallback_config(None)
        for value in (-1, "1", True, 1.5, None):
            self.dispatcher.rebuild_routing(_catalog({"backup-model": value}))
            self.assertIsNone(self.dispatcher.fallback_config, value)


if __name__ == "__main__":
    unittest.main()
