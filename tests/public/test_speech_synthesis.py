"""An opt-in tool publishes host-synthesized speech as an authorized artifact."""
from dataclasses import replace
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import wave

from openagent_core import (
    CapabilityCatalog, CapabilityUnavailable, ExecutionContext, PrincipalRef, RunRequest,
    Runtime, RuntimeServices, RuntimeSettings, SpeechSynthesisCapability,
)
from openagent_core.artifacts import ArtifactRepository
from openagent_core.audio import SynthesizedAudio, VoiceService
from openagent_storage_sqlite import SqliteRuntimeStore


def wav_fixture():
    data = io.BytesIO()
    with wave.open(data, "wb") as output:
        output.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
        output.writeframes(b"\x10\x00\xf0\xff" * 800)
    return data.getvalue()


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        if (action, resource.kind) in self.denied:
            return False
        if action.startswith("artifact.") and resource.kind == "session":
            return (context.tenant_id, resource.resource_id) == ("tenant-a", "session-a")
        return True


class NoRemoteVoice:
    async def latest_audio_model(self, kind):
        return None


class LocalVoiceFixture:
    def __init__(self):
        self.calls = []
        self.result = SynthesizedAudio(wav_fixture(), "audio/wav")
        self.on_call = lambda: None

    async def synthesize(self, text, *, language=None):
        self.calls.append((text, language))
        self.on_call()
        return self.result


class Executor:
    async def execute(self, request, context, runtime):
        (tool,) = await runtime.capabilities.discover(context)
        try:
            self.result = await runtime.capabilities.call_tool(tool.tool_ref, self.arguments, context)
        except Exception as exc:
            self.error = exc
            raise
        return "Speech artifact ready."


class SpeechSynthesisTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.store = SqliteRuntimeStore(root / "state.db")
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        self.voice = LocalVoiceFixture()
        self.artifacts = ArtifactRepository(SimpleNamespace(db_path=str(self.store.path)), self.policy)
        self.capability = SpeechSynthesisCapability(
            VoiceService(NoRemoteVoice(), fallback=self.voice), self.artifacts,
        )
        self.catalog.register("speech", self.capability, self.capability, target_label="Speech")
        self.executor = Executor()
        services = RuntimeServices(self.store, self.executor, self.policy, self.catalog)
        self.runtime = Runtime(RuntimeSettings("agent-a", root), services)
        await self.runtime.start()
        self.addAsyncCleanup(self.runtime.close)
        principal = PrincipalRef("host-a", "tenant-a", "user-a")
        self.context = ExecutionContext(principal, principal, principal, "session-a", "agent-a", (principal,))
        self.sequence = 0

    async def run_tool(self, arguments):
        self.executor.arguments = arguments
        self.sequence += 1
        run = f"run-{self.sequence}"
        await self.runtime.submit(RunRequest(run, self.context.session_id, run, "Read this aloud."), self.context)
        return await self.runtime.wait(run, self.context)

    async def test_explicit_registration_delivers_path_free_audio_artifact(self):
        self.assertEqual(await CapabilityCatalog(self.policy).discover(self.context), ())
        (tool,) = await self.catalog.discover(self.context)
        self.assertEqual(tool.name, "synthesize_speech")
        self.assertEqual(set(tool.input_schema["properties"]), {"text", "language"})
        self.assertFalse(tool.input_schema["additionalProperties"])
        record = await self.run_tool({"text": " Ciao, città. ", "language": "it-IT"})
        self.assertEqual(record.status, "success")
        self.assertEqual(self.voice.calls, [("Ciao, città.", "it-IT")])
        result = self.executor.result
        self.assertFalse(result["isError"])
        self.assertEqual([part["type"] for part in result["content"]], ["text"])
        ref = result["structuredContent"]["artifact"]
        self.assertEqual((ref["mime_type"], ref["filename"]), ("audio/wav", "speech.wav"))
        self.assertTrue(ref["artifact_link_id"])
        self.assertNotIn("path", ref)
        self.assertNotIn("url", ref)
        audio = await self.artifacts.read(self.context, artifact_id=ref["artifact_id"])
        self.assertEqual(audio.content, self.voice.result.data)
        with wave.open(io.BytesIO(audio.content), "rb") as reader:
            self.assertEqual((reader.getframerate(), reader.getnframes()), (16000, 1600))
        other = PrincipalRef("host-a", "tenant-b", "user-a")
        foreign = replace(self.context, initiator=other, authority=other, author=other, audience=(other,))
        with self.assertRaises(LookupError):
            await self.artifacts.read(foreign, artifact_id=ref["artifact_id"])

    async def test_unauthorized_or_malformed_requests_never_invoke_the_voice(self):
        (tool,) = await self.catalog.discover(self.context)
        with self.assertRaises(PermissionError):
            await self.catalog.call_tool(tool.tool_ref, {"text": "Hello"}, self.context)
        for arguments in (
            {"text": " "}, {"text": "x" * 2001}, {"text": "x\x00y"},
            {"text": "Hello", "language": "https://example.invalid"},
            {"text": "Hello", "language": None},
            {"text": "Hello", "voice": "arbitrary"},
        ):
            with self.subTest(arguments=arguments):
                self.assertEqual((await self.run_tool(arguments)).status, "failed")
                self.assertIsInstance(self.executor.error, ValueError)
        for denial in (("tool.call", "tool"), ("artifact.write", "agent"), ("artifact.write", "session")):
            with self.subTest(denial=denial):
                self.policy.denied = {denial}
                self.assertEqual((await self.run_tool({"text": "Hello"})).status, "failed")
                self.assertIsInstance(self.executor.error, PermissionError)
        self.assertEqual(self.voice.calls, [])

    async def test_revocation_after_synthesis_and_unusable_audio_prevent_publication(self):
        with patch.object(self.artifacts, "upload", wraps=self.artifacts.upload) as upload:
            self.voice.on_call = lambda: self.policy.denied.add(("artifact.write", "session"))
            self.assertEqual((await self.run_tool({"text": "First"})).status, "failed")
            self.assertIsInstance(self.executor.error, PermissionError)
            upload.assert_awaited_once()
        self.policy.denied.clear()
        self.voice.on_call = lambda: None
        with patch.object(self.artifacts, "upload", wraps=self.artifacts.upload) as upload:
            with patch("openagent_core.speech_synthesis._AUDIO_LIMIT", 4):
                self.assertEqual((await self.run_tool({"text": "Second"})).status, "failed")
                self.assertIn("output limit", str(self.executor.error))
            self.voice.result = SynthesizedAudio(b"html", "text/html")
            self.assertEqual((await self.run_tool({"text": "Third"})).status, "failed")
            self.assertIn("unsupported audio format", str(self.executor.error))
            self.voice.result = None
            self.assertEqual((await self.run_tool({"text": "Fourth"})).status, "failed")
            self.assertIsInstance(self.executor.error, CapabilityUnavailable)
            upload.assert_not_awaited()

    async def test_backend_errors_have_a_bounded_public_message(self):
        def fail():
            raise RuntimeError("private backend detail")
        self.voice.on_call = fail
        self.assertEqual((await self.run_tool({"text": "Hello"})).status, "failed")
        self.assertIsInstance(self.executor.error, CapabilityUnavailable)
        self.assertEqual(str(self.executor.error), "Speech synthesis is unavailable")
