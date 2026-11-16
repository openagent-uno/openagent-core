"""A backup model that never answers must not hold the turn.

A backup reached through a network path that accepts the connection but never
answers (an offline machine behind a userspace VPN, a stopped local server)
used to keep the turn waiting for the provider SDK's read timeout and retries,
which the user sees as a reply that loads forever. Each backup now has a
bounded wait for its first provider output; past it the next backup is tried,
and when none answers the original failure is reported.
"""
from __future__ import annotations

import asyncio
import os
import time
import unittest
from dataclasses import dataclass
from unittest import mock

from openagent_core.core.runtime_errors import ModelRateLimitError
from openagent_core.models.providers.fallback import (
    FallbackConfig,
    acall_model_stream_with_fallback,
    breaker_snapshot,
    reset_breaker,
)
from openagent_core.models.providers.openai.chat import OpenAIChat
from openagent_core.models.providers.response import ModelResponse, ModelResponseEvent

TIMEOUT_ENV = "OPENAGENT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S"


@dataclass
class ScriptedModel(OpenAIChat):
    id: str = "fixture-model"
    failure: Exception | None = None
    hang: bool = False
    delay_s: float = 0.0
    text: str = "recovered"
    closed: bool = False

    async def aresponse_stream(self, **kwargs):
        if self.failure is not None:
            raise self.failure
        try:
            # Local markers come before any network round trip.
            yield ModelResponse(event=ModelResponseEvent.model_request_started.value)
            if self.hang:
                await asyncio.Event().wait()
            await asyncio.sleep(self.delay_s)
            yield ModelResponse(content=self.text)
            await asyncio.sleep(self.delay_s)
            yield ModelResponse(content=self.text + " again")
        finally:
            self.closed = True


def _limit():
    return ModelRateLimitError(message="fixture pool exhausted", status_code=429)


async def _collect(primary, config):
    return [event.content async for event in acall_model_stream_with_fallback(primary, config)
            if getattr(event, "content", None) is not None]


class UnresponsiveBackup(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_breaker()
        patcher = mock.patch.dict(os.environ, {TIMEOUT_ENV: "0.2"})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(reset_breaker)

    async def test_a_silent_backup_gives_way_to_the_next_one(self):
        silent = ScriptedModel(id="silent-backup", hang=True)
        working = ScriptedModel(id="working-backup")
        config = FallbackConfig(on_rate_limit=[silent, working])
        started = time.monotonic()
        output = await asyncio.wait_for(_collect(ScriptedModel(failure=_limit()), config), 5)
        self.assertEqual(["recovered", "recovered again"], output)
        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(silent.closed, "the abandoned stream must be closed")
        self.assertIn("silent-backup", breaker_snapshot())

    async def test_when_no_backup_answers_the_original_failure_is_reported(self):
        failure = _limit()
        config = FallbackConfig(on_rate_limit=[ScriptedModel(id="silent-a", hang=True),
                                               ScriptedModel(id="silent-b", hang=True)])
        with self.assertRaises(ModelRateLimitError) as raised:
            await asyncio.wait_for(_collect(ScriptedModel(failure=failure), config), 5)
        self.assertIs(raised.exception, failure)

    async def test_the_wait_covers_only_the_first_output(self):
        # Gaps after the first output are normal streaming, never cut.
        late = ScriptedModel(id="late-chunks")

        async def stream(**kwargs):
            yield ModelResponse(event=ModelResponseEvent.model_request_started.value)
            yield ModelResponse(content="first")
            await asyncio.sleep(0.5)
            yield ModelResponse(content="second")

        late.aresponse_stream = stream
        output = await asyncio.wait_for(
            _collect(ScriptedModel(failure=_limit()), FallbackConfig(on_rate_limit=[late])), 5)
        self.assertEqual(["first", "second"], output)
        self.assertNotIn("late-chunks", breaker_snapshot())

    async def test_zero_disables_the_bound(self):
        slow = ScriptedModel(id="slow-backup", delay_s=0.4)
        with mock.patch.dict(os.environ, {TIMEOUT_ENV: "0"}):
            output = await asyncio.wait_for(
                _collect(ScriptedModel(failure=_limit()), FallbackConfig(on_rate_limit=[slow])), 5)
        self.assertEqual(["recovered", "recovered again"], output)


if __name__ == "__main__":
    unittest.main()
