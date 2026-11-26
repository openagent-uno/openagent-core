"""A refreshed fallback list must not replace a provider error with a type error."""
import asyncio
from dataclasses import dataclass
import unittest
from unittest.mock import patch

from openagent_core.core.runtime_errors import ModelProviderError
from openagent_core.models.providers.openai.chat import OpenAIChat
from openagent_core.models.providers.fallback import FallbackConfig, acall_model_stream_with_fallback
from openagent_core.models.providers.response import ModelResponse
from openagent_core.models.providers.fallback import acall_model_with_fallback


@dataclass
class ScriptedModel(OpenAIChat):
    id: str = "fixture-model"
    failure: Exception | None = None

    async def aresponse_stream(self, **kwargs):
        if self.failure is not None:
            raise self.failure
        yield ModelResponse(content="recovered")

    async def aresponse(self, **kwargs):
        if self.failure is not None:
            raise self.failure
        return ModelResponse(content="recovered")


class RefreshedFallbackStream(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from openagent_core.models.providers.fallback import reset_breaker
        reset_breaker()
        self.addCleanup(reset_breaker)

    async def test_silent_provider_round_times_out_without_replaying_output(self):
        model = OpenAIChat(id="fixture-idle", retries=2)
        calls = 0
        closed = False

        async def provider(**kwargs):
            nonlocal calls, closed
            calls += 1
            try:
                yield ModelResponse(content="Initial output")
                await asyncio.Event().wait()
            finally:
                closed = True

        with patch.object(model, "ainvoke_stream", provider), patch(
            "openagent_core.models.providers.base._provider_stream_idle_timeout", return_value=0.03,
        ):
            async with asyncio.timeout(1):
                stream = model._ainvoke_stream_with_retry(messages=[])
                first = await anext(stream)
                await asyncio.sleep(0.06)
                self.assertFalse(closed)
                with self.assertRaises(ModelProviderError) as raised:
                    await anext(stream)
        self.assertEqual(first.content, "Initial output")
        self.assertEqual(raised.exception.status_code, 504)
        self.assertTrue(closed)
        self.assertEqual(calls, 1)

    async def test_automatic_chain_skips_ineligible_and_recovers_unsupported_model(self):
        primary = ScriptedModel(failure=ModelProviderError(
            message="This model is not supported when using Codex with this account", status_code=400))
        blocked = ScriptedModel(id="over-budget", failure=AssertionError("An excluded model was invoked"))
        working = ScriptedModel(id="working")
        config = FallbackConfig(on_error=[blocked, working], recover_unavailable=True,
                                eligible=lambda model, kwargs: model.id != "over-budget")
        events = [event async for event in acall_model_stream_with_fallback(primary, config)]
        self.assertEqual([event.content for event in events if event.content], ["recovered"])

    async def test_unavailable_connections_recover_but_request_errors_and_refusals_do_not(self):
        for streaming in (False, True):
            for status, message, allowed in (
                (401, "expired credential", True),
                (400, "model_not_found", True),
                (404, "model not found", True),
                (403, "content policy refusal", False),
                (400, "invalid temperature parameter", False),
            ):
                with self.subTest(streaming=streaming, status=status, message=message):
                    from openagent_core.models.providers.fallback import reset_breaker
                    reset_breaker()
                    failure = ModelProviderError(message=message, status_code=status)
                    config = FallbackConfig(on_error=[ScriptedModel(id="backup")], recover_unavailable=True)
                    async def execute():
                        primary = ScriptedModel(failure=failure)
                        if streaming:
                            events = [event async for event in acall_model_stream_with_fallback(primary, config)]
                            return "".join(event.content or "" for event in events)
                        return (await acall_model_with_fallback(primary, config)).content
                    if allowed:
                        self.assertEqual(await execute(), "recovered")
                    else:
                        with self.assertRaises(ModelProviderError) as raised:
                            await execute()
                        self.assertIs(raised.exception, failure)

    async def test_automatic_chain_does_not_replay_after_partial_output(self):
        failure = ModelProviderError(message="connection lost", status_code=503)
        primary = ScriptedModel()
        async def partial(**kwargs):
            yield ModelResponse(content="already visible")
            raise failure
        primary.aresponse_stream = partial
        config = FallbackConfig(on_error=[ScriptedModel(id="backup")], recover_unavailable=True)
        visible = []
        with self.assertRaises(ModelProviderError) as raised:
            async for event in acall_model_stream_with_fallback(primary, config):
                if event.content:
                    visible.append(event.content)
        self.assertIs(raised.exception, failure)
        self.assertEqual(visible, ["already visible"])

    async def test_no_eligible_backup_preserves_the_actual_failure(self):
        failure = ModelProviderError(message="provider unavailable", status_code=503)
        config = FallbackConfig(on_error=[ScriptedModel(id="blocked")], recover_unavailable=True,
                                eligible=lambda model, kwargs: model.id != "blocked")
        with self.assertRaises(ModelProviderError) as raised:
            async for _ in acall_model_stream_with_fallback(ScriptedModel(failure=failure), config):
                pass
        self.assertIs(raised.exception, failure)

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

    async def test_nonstream_tool_execution_is_never_replayed_on_backup(self):
        from openagent_core.models.providers.message import Message
        for fail_primary in (True,False):
            with self.subTest(fail_primary=fail_primary):
                failure=ModelProviderError('quota after tool',status_code=429)
                first=ScriptedModel(id='first');second=ScriptedModel(id='second');last=ScriptedModel(id='last')
                called=[]
                async def tool_then_fail(**kwargs):
                    kwargs['messages'].append(Message(role='assistant',tool_calls=[{'id':'one','function':{'name':'effect'}}]))
                    kwargs['messages'].append(Message(role='tool',content='effect already performed'))
                    called.append('effect')
                    raise failure
                if fail_primary:first.aresponse=tool_then_fail
                else:
                    first.failure=ModelProviderError('quota',status_code=429)
                    second.aresponse=tool_then_fail
                async def forbidden(**kwargs):
                    self.fail('a committed tool loop was replayed')
                if fail_primary:second.aresponse=forbidden
                last.aresponse=forbidden
                config=FallbackConfig(on_rate_limit=[second,last])
                with self.assertRaises(ModelProviderError) as raised:
                    await acall_model_with_fallback(first,config,messages=[Message(role='user',content='fixture')])
                self.assertIs(raised.exception,failure)
                self.assertEqual(called,['effect'])
