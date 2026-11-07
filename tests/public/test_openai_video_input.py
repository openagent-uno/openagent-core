"""Explicit opt-in to inline video in an OpenAI-compatible request."""
import asyncio
from base64 import b64encode
import json

import httpx

import pytest

from openagent_core.models.native_provider import NativeProvider
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.like import OpenAILike
from openagent_core.stream.media import Image, Video


def test_provider_opt_in_preserves_text_and_image_with_inline_video():
    provider = NativeProvider(
        model="fixture:vision",
        providers_config=[{
            "name": "fixture",
            "base_url": "http://example.invalid/v1",
            "video_input_format": "input_video",
        }],
    )
    model = provider.build_runtime_model()
    message = Message(
        role="user",
        content="Describe the motion and compare the still.",
        images=[Image(content=b"still", mime_type="image/png")],
        videos=[Video(content=b"clip", mime_type="video/mp4")],
    )

    content = model._format_message(message)["content"]

    assert [part["type"] for part in content] == ["text", "image_url", "input_video"]
    assert content[0]["text"] == message.content
    assert content[1]["image_url"]["url"].endswith(b64encode(b"still").decode())
    assert content[2]["input_video"] == {"data": b64encode(b"clip").decode()}


@pytest.mark.parametrize("video", [Video(url="https://example.invalid/video"), Video(filepath="/tmp/video.mp4")])
def test_inline_video_format_refuses_external_references(video):
    message = Message(role="user", videos=[video], content="Describe this")
    with pytest.raises(ValueError, match="requires inline video bytes"):
        OpenAILike(video_input_format="input_video")._format_message(message)


def test_default_openai_compatible_request_does_not_send_video_parts():
    message = Message(role="user", videos=[Video(content=b"clip")], content="Describe this")
    assert OpenAILike()._format_message(message)["content"] == "Describe this"

def test_video_only_reaches_the_chat_completions_wire():
    async def run():
        def complete(request):
            assert request.url.path == "/v1/chat/completions"
            assert json.loads(request.content)["messages"] == [{
                "role": "user",
                "content": [{"type": "input_video", "input_video": {"data": "Y2xpcA=="}}],
            }]
            return httpx.Response(200, json={
                "id": "completion", "model": "vision", "created": 1, "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "Seen."},
                             "finish_reason": "stop"}],
            })

        async with httpx.AsyncClient(transport=httpx.MockTransport(complete)) as client:
            model = OpenAILike(id="vision", base_url="http://example.invalid/v1", http_client=client,
                               video_input_format="input_video")
            result = await model.ainvoke(
                [Message(role="user", videos=[Video(content=b"clip")])], Message(role="assistant")
            )
            assert result.content == "Seen."

    asyncio.run(run())
