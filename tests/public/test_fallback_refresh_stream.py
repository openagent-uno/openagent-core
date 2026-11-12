"""A refreshed fallback list must not replace a provider error with a type error."""
from dataclasses import dataclass
import unittest

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.providers.openai.chat import OpenAIChat
from openagent_core.models.providers.fallback import FallbackConfig, acall_model_stream_with_fallback
from openagent_core.models.providers.response import ModelResponse


@dataclass
class ScriptedModel(OpenAIChat):
    id: str = "fixture-model"
    failure: Exception | None = None

    async def aresponse_stream(self, **kwargs):
        if self.failure is not None:
            raise self.failure
        yield ModelResponse(content="recovered")


class RefreshedFallbackStream(unittest.IsolatedAsyncioTestCase):
    async def test_unresolved_ref_cannot_mask_original_failure(self):
        failure = ModelProviderError(message="fixture upstream timeout", status_code=503)
        config = FallbackConfig(on_error=["not-a-provider:missing"])
        with self.assertRaises(ModelProviderError) as raised:
            async for _ in acall_model_stream_with_fallback(ScriptedModel(failure=failure), config):
                self.fail("An unavailable provider produced output")
        self.assertIs(raised.exception, failure)
        self.assertEqual(config.on_error, ["not-a-provider:missing"])

    async def test_bad_ref_does_not_hide_available_fallback(self):
        failure = ModelProviderError(message="fixture upstream timeout", status_code=503)
        replacement = ScriptedModel(id="working-backup")
        config = FallbackConfig(on_error=["not-a-provider:missing", replacement])
        events = [event async for event in acall_model_stream_with_fallback(
            ScriptedModel(failure=failure), config)]
        output = [event.content for event in events if event.content is not None]
        self.assertEqual(output, ["recovered"])
        self.assertEqual(config.on_error[0], "not-a-provider:missing")
        self.assertIs(config.on_error[1], replacement)
