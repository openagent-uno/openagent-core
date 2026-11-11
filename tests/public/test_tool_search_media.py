"""Visual MCP results remain visible to models behind tool-search."""

import base64
import json
from pathlib import Path
import shutil
import subprocess
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core import DiscoveryPresentation
from openagent_core.capabilities import CapabilityCatalog, FunctionSource, ToolDefinition
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.engine import FfmpegVideoFrameExtractor
from openagent_core.mcp._runtime.function import Function, FunctionCall, ToolResult
from openagent_core.mcp.pool import MCPPool
from openagent_core.mcp.servers.tool_search.adapters import _model_visible_result, build_runtime_toolkit
from openagent_core.models.providers.message import Message
from openagent_core.models.providers.openai.chat import OpenAIChat
from openagent_core.runtime import execution_scope


class ToolSearchMediaTests(unittest.IsolatedAsyncioTestCase):
    async def test_screenshot_is_model_image_and_wire_envelope_is_preserved(self):
        png = b"\x89PNG\r\n\x1a\nfixture"
        encoded = base64.b64encode(png).decode("ascii")
        envelope = {
            "content": [
                {"type": "text", "text": '{"image_width":1280,"image_height":920}'},
                {"type": "image", "data": encoded, "mimeType": "image/png"},
            ],
            "structuredContent": {"display": ":101"},
            "_meta": {"source": "computer-control"},
            "isError": False,
        }

        result = await _model_visible_result(envelope)

        self.assertIsInstance(result, ToolResult)
        self.assertEqual(result.mcp_result, envelope)
        self.assertEqual(result.images[0].content, png)
        self.assertNotIn(encoded, result.content)
        self.assertIn("inspect the pixels", result.content)

        message = Message(role="tool", tool_call_id="call_1", content=result.content,
                          images=result.images)
        formatted = OpenAIChat(id="test", api_key="unused")._format_message(message)
        self.assertEqual(formatted["role"], "tool")
        self.assertEqual(formatted["content"][1]["type"], "image_url")
        self.assertTrue(formatted["content"][1]["image_url"]["url"].startswith(
            "data:image/png;base64,"))

    async def test_admitted_image_delivery_is_session_linked_and_requires_live_authority(self):
        from openagent_core.engine import MemoryDB, AgentExecutor
        from openagent_core.artifacts import ArtifactRepository, SESSION_ARTIFACT_PREFIX
        from openagent_core.contracts import RunRequest
        from openagent_storage_sqlite import SqliteRuntimeStore
        allowed = True
        async def authorize(*args, **kwargs): return allowed
        policy = SimpleNamespace(authorize=authorize)
        user = PrincipalRef("host", "tenant", "alice")
        context = ExecutionContext(user, user, user, "session", "agent", (user,))
        content = b"\x89PNG\r\n\x1a\nexact fixture pixels"
        envelope = {"content": [{"type": "image", "mimeType": "image/png",
            "data": base64.b64encode(content).decode()}]}
        with TemporaryDirectory() as root:
            db = MemoryDB(str(Path(root) / "state.sqlite")); await db.connect()
            self.addAsyncCleanup(db.close)
            store = SqliteRuntimeStore(Path(root) / "state.sqlite"); await store.start()
            self.addAsyncCleanup(store.close)
            await store.accept(RunRequest("image-run", "session", "key", "capture"), context)
            repository = ArtifactRepository(db, policy)
            published = []
            async def publish(actor, data, **kwargs):
                self.assertIs(actor, context); self.assertEqual(content, data)
                ref = await repository.upload(actor, data, session_id=actor.session_id, **kwargs)
                published.append(ref)
                return ref
            publisher = SimpleNamespace(publish_content=publish)
            events = []
            async def append(*event): events.append(event)
            async def check(actor, action, resource, **kwargs):
                from openagent_core.contracts import require_authorized
                await require_authorized(policy, actor, action, resource, **kwargs)
            class Agent:
                memory_db = db
                async def run_stream(self, *args, **kwargs):
                    yield {"kind": "done", "text": "Screenshot " + marker,
                        "final_answer": {"text": "Screenshot " + marker}}
            runtime = SimpleNamespace(authorize=check,
                service=lambda key: publisher if key == "attachments.publisher" else None,
                services=SimpleNamespace(authorizer=policy, executor=SimpleNamespace(agent=Agent()),
                    store=SimpleNamespace(append_event=append)))
            with execution_scope(runtime, context, "image-run"):
                result = await _model_visible_result(envelope)
                self.assertEqual(envelope, result.mcp_result)
                self.assertNotIn(root, result.content)
                marker = f"[FILE:{SESSION_ARTIFACT_PREFIX}{published[0]['artifact_id']}]"
                self.assertIn(marker, result.content)
                await AgentExecutor(Agent()).execute(RunRequest("image-run", "session", "key", "capture"), context, runtime)
                final = events[-1][-1]["final_answer"]
                self.assertEqual([published[0]["artifact_id"]], [ref["artifact_id"] for ref in final["attachments"]])
                allowed = False
                with self.assertRaises(PermissionError):
                    await _model_visible_result(envelope)
            self.assertEqual(1, len(published))

    async def test_optional_image_publication_failure_preserves_pixels_but_never_bypasses_revocation(self):
        allowed = True
        async def authorize(*_args, **_kwargs): return allowed
        async def publish(*_args, **_kwargs): raise RuntimeError("PRIVATE diagnostic")
        publisher = SimpleNamespace(publish_content=publish)
        runtime = SimpleNamespace(service=lambda key: publisher if key == "attachments.publisher" else None,
            services=SimpleNamespace(authorizer=SimpleNamespace(authorize=authorize)))
        user = PrincipalRef("host", "tenant", "alice")
        context = ExecutionContext(user, user, user, "session", "agent", (user,))
        envelope = {"content": [{"type": "image", "data": base64.b64encode(b"pixels").decode()}]}
        with execution_scope(runtime, context, "run"):
            with self.assertLogs("openagent_core.mcp.servers.tool_search.adapters", level="WARNING") as logs:
                result = await _model_visible_result(envelope)
            self.assertNotIn("PRIVATE", str(logs.output))
            self.assertEqual(b"pixels", result.images[0].content)
            self.assertNotIn("[FILE:", result.content)
            allowed = False
            with self.assertLogs("openagent_core.mcp.servers.tool_search.adapters", level="WARNING"):
                with self.assertRaises(PermissionError):
                    await _model_visible_result(envelope)

    async def test_text_only_result_keeps_existing_contract(self):
        envelope = {"content": [{"type": "text", "text": "ok"}], "isError": False}
        self.assertIs(await _model_visible_result(envelope), envelope)

    async def test_invalid_image_does_not_claim_pixels_are_visible(self):
        result = await _model_visible_result({"content": [{"type": "image", "data": "not-base64"}]})
        self.assertIsInstance(result, ToolResult)
        self.assertIsNone(result.images)
        self.assertIn("Rejected MCP image", result.content)

    async def test_embedded_video_is_opt_in_and_links_are_never_opened(self):
        blob = base64.b64encode(b"opaque-video").decode()
        resource = {"uri": "https://example.invalid/private.mp4", "mimeType": "video/mp4"}
        link = {"type": "resource_link", "name": "private", **resource}
        embedded = {"type": "resource", "resource": {**resource, "blob": blob}}
        extract = AsyncMock()
        extractor = SimpleNamespace(extract=extract)
        links = {"content": [link]}
        self.assertIs(await _model_visible_result(links, video_frame_extractor=extractor), links)
        for envelope in ({"content": [embedded]}, {"content": [embedded], "isError": True}):
            result = await _model_visible_result(
                envelope, video_frame_extractor=extractor if envelope.get("isError") else None,
            )
            self.assertEqual(result.mcp_result, envelope)
            self.assertIsNone(result.images)
            self.assertNotIn(blob, result.content)
        bad = {"content": [{"type": "resource", "resource": {**resource, "blob": "bad!"}}]}
        result = await _model_visible_result(bad, video_frame_extractor=extractor)
        self.assertIn("Rejected MCP embedded video", result.content)
        self.assertIsNone(result.images)
        with patch("openagent_core.models.video_frames.MAX_VIDEO_PREVIEW_BYTES", 4):
            oversized = await _model_visible_result({"content": [embedded]}, video_frame_extractor=extractor)
        self.assertIn("exceeds the 4-byte", oversized.content)
        extract.assert_not_awaited()

        extract.return_value = ()
        multiple = await _model_visible_result({"content": [embedded] * 3}, video_frame_extractor=extractor)
        self.assertIsNone(multiple.images)
        self.assertEqual(extract.await_count, 2)
        self.assertIn("preview limit reached", multiple.content)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg not installed")
    async def test_host_can_sample_inside_a_short_event_between_default_positions(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "event.mp4"
            subprocess.run(
                ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=black:s=160x90:r=20:d=2",
                 "-vf", "drawbox=color=white:t=fill:enable='between(t,0.4,0.6)'",
                 "-c:v", "mpeg4", str(path)], check=True, timeout=15,
            )
            clip = path.read_bytes()

        normal = await FfmpegVideoFrameExtractor().extract(clip, "video/mp4")
        detailed = await FfmpegVideoFrameExtractor(sample_positions=(0, .25, .75)).extract(clip, "video/mp4")
        self.assertEqual([frame.timestamp_seconds for frame in detailed], [0, .5, 1.5])

        def pixel(frame):
            return subprocess.run(
                ["ffmpeg", "-v", "error", "-i", "pipe:0", "-vf", "scale=1:1",
                 "-pix_fmt", "gray", "-f", "rawvideo", "pipe:1"], input=frame.content,
                capture_output=True, check=True, timeout=5,
            ).stdout[0]

        self.assertEqual(len(normal), 3)
        self.assertTrue(all(pixel(frame) < 10 for frame in normal))
        self.assertGreater(pixel(detailed[1]), 240)
        for invalid in ((), (0, 0), (.5, .2), (0, 1), (True,), (float("nan"),)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                FfmpegVideoFrameExtractor(sample_positions=invalid)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg not installed")
    async def test_frame_edge_changes_the_real_output_size(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "landscape.mp4"
            subprocess.run(
                ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=blue:s=960x540:r=4:d=1",
                 "-c:v", "mpeg4", str(path)], check=True, timeout=15,
            )
            clip = path.read_bytes()

        for extractor, size in ((FfmpegVideoFrameExtractor(sample_positions=(0,)), (384, 216)),
                                (FfmpegVideoFrameExtractor(sample_positions=(0,), frame_edge=768), (768, 432))):
            frame, = await extractor.extract(clip, "video/mp4")
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "stream=width,height", "-of", "json", "pipe:0"],
                input=frame.content, capture_output=True, check=True, timeout=5,
            )
            stream = json.loads(probe.stdout)["streams"][0]
            self.assertEqual((stream["width"], stream["height"]), size)

    def test_frame_edge_is_bounded(self):
        for invalid in (True, 0, 1, 384.0, "768", 769):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                FfmpegVideoFrameExtractor(frame_edge=invalid)

    @unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"), "FFmpeg not installed")
    async def test_provider_followup_receives_ordered_frames_from_authorized_tool_result(self):
        import httpx
        from openai import AsyncOpenAI

        with TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic.mp4"
            subprocess.run(
                ["ffmpeg", "-v", "error",
                 "-f", "lavfi", "-i", "color=c=red:s=160x90:r=10:d=1",
                 "-f", "lavfi", "-i", "color=c=green:s=160x90:r=10:d=1",
                 "-f", "lavfi", "-i", "color=c=blue:s=160x90:r=10:d=1",
                 "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0", "-c:v", "mpeg4", str(path)],
                check=True, timeout=15,
            )
            blob = base64.b64encode(path.read_bytes()).decode()

        envelope = {
            "content": [{"type": "text", "text": "Three colored frames."},
                        {"type": "resource", "resource": {
                            "uri": "https://example.invalid/private.mp4", "mimeType": "video/mp4", "blob": blob,
                        }}],
            "structuredContent": {"id": "clip-one"},
            "_meta": {"fixture": True},
            "isError": False,
        }

        class Policy:
            async def authorize(self, _context, _action, _resource, *, audience=()):
                return True

        async def read(_args, _context):
            return envelope

        catalog = CapabilityCatalog(Policy())
        source = FunctionSource((ToolDefinition("read", "Read a clip.", {"type": "object"}),), {"read": read})
        catalog.register("media-fixture", source, source, target_label="Fixture")
        principal = PrincipalRef("test", "tenant", "operator")
        context = ExecutionContext(principal, principal, principal, "session", "agent", (principal,))
        descriptor, = await catalog.discover(context)
        extractor = FfmpegVideoFrameExtractor()
        runtime = SimpleNamespace(
            capabilities=catalog, settings=SimpleNamespace(environment=()),
            services=SimpleNamespace(executor=None),
            service={"media.tool_video_frame_extractor": extractor}.get,
        )
        pool = MCPPool([]).configure_discovery(DiscoveryPresentation(
            named_tools={('media-fixture', 'read')},
        ))
        pool.bind_capability_catalog(catalog)
        toolkit = build_runtime_toolkit(pool=pool)
        requests = []

        def provider(request):
            payload = json.loads(request.content)
            requests.append(payload)
            if len(requests) == 1:
                message = {"role": "assistant", "content": None, "tool_calls": [{
                    "id": "call_clip", "type": "function", "function": {
                        "name": call.name, "arguments": json.dumps(arguments),
                    },
                }]}
                reason = "tool_calls"
            else:
                message = {"role": "assistant", "content": "The sequence was red, green, blue."}
                reason = "stop"
            if payload.get("stream"):
                if "tool_calls" in message:
                    message["tool_calls"][0]["index"] = 0
                chunk = {
                    "id": "completion", "object": "chat.completion.chunk", "created": 0, "model": "vision",
                    "choices": [{"index": 0, "delta": message, "finish_reason": reason}],
                }
                return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                      text=f"data: {json.dumps(chunk)}\n\ndata: [DONE]\n\n")
            return httpx.Response(200, json={
                "id": "completion", "object": "chat.completion", "created": 0, "model": "vision",
                "choices": [{"index": 0, "message": message, "finish_reason": reason}],
            })

        reference = {"tool_ref": descriptor.tool_ref}
        named = {"source_ref": "media-fixture", "tool_name": "read"}
        for streamed, tool, arguments in (
            (False, "tool_search_call_tool", reference),
            (True, "tool_search_call_tool", reference),
            (True, "tool_search_call_named_tool", named),
        ):
            with self.subTest(streamed=streamed, tool=tool):
                call = toolkit.async_functions[tool]
                requests.clear()
                async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
                    client = AsyncOpenAI(base_url="https://example.invalid/v1", api_key="unused", http_client=http)
                    model = OpenAIChat(id="vision", async_client=client)
                    messages = [Message(role="user", content="Review the clip.")]
                    with execution_scope(runtime, context, None):
                        if streamed:
                            async for _ in model.aresponse_stream(messages, tools=[call]):
                                pass
                        else:
                            await model.aresponse(messages, tools=[call])
                self.assertEqual(messages[-1].content, "The sequence was red, green, blue.")
                self.assertEqual(len(requests), 2)
                followup = requests[1]["messages"]
                receipt = next(message["content"] for message in followup if message["role"] == "tool")
                self.assertIn("image 1 at 0.000s, image 2 at 1.350s, image 3 at 2.550s", receipt)
                self.assertNotIn(blob, json.dumps(followup))
                frames = [part["image_url"]["url"] for message in followup if isinstance(message["content"], list)
                          for part in message["content"] if part["type"] == "image_url"]
                self.assertEqual(len(frames), 3)
                for channel, frame in enumerate(frames):
                    self.assertTrue(frame.startswith("data:image/jpeg;base64,"))
                    rgb = subprocess.run(
                        ["ffmpeg", "-v", "error", "-f", "image2pipe", "-i", "pipe:0", "-vf", "scale=1:1",
                         "-pix_fmt", "rgb24", "-f", "rawvideo", "pipe:1"],
                        input=base64.b64decode(frame.split(",", 1)[1]), capture_output=True, check=True, timeout=5,
                    ).stdout
                    self.assertEqual(len(rgb), 3)
                    self.assertEqual(rgb[channel], max(rgb))
                    self.assertGreater(rgb[channel], 100)
        with execution_scope(runtime, context, None):
            projected = await toolkit.async_functions["tool_search_call_tool"].entrypoint(**reference)
            named_projected = await toolkit.async_functions["tool_search_call_named_tool"].entrypoint(**named)
        self.assertEqual(projected.mcp_result, envelope)
        self.assertEqual(named_projected.mcp_result, envelope)
        self.assertEqual(named_projected.content, projected.content)
        self.assertEqual([frame.content for frame in named_projected.images],
                         [frame.content for frame in projected.images])

    async def test_error_with_an_image_preserves_diagnostic_media_and_sets_tool_status(self):
        png = b"\x89PNG\r\n\x1a\nfixture"
        encoded = base64.b64encode(png).decode("ascii")
        envelope = {"content": [
            {"type": "text", "text": "failed with diagnostic screenshot"},
            {"type": "image", "data": encoded, "mimeType": "image/png"},
        ], "isError": True}
        result = await _model_visible_result(envelope)
        self.assertEqual(result.mcp_result, envelope)
        self.assertNotIn(encoded, result.content)

        model = OpenAIChat(id="test", api_key="unused")
        call = FunctionCall(function=Function(name="inspect", entrypoint=lambda: result), call_id="failure")
        messages = []
        frames = list(model.run_function_call(call, messages))
        self.assertTrue(frames[-1].tool_executions[0].tool_call_error)
        self.assertTrue(messages[0].tool_call_error)
        self.assertIn("Error from MCP tool.", messages[0].content)
        self.assertEqual(messages[0].images[0].content, png)
