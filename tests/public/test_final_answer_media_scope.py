"""Quiet answers deliver authored final media, never aggregate preview media."""
import unittest
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from openagent_core.stream.final_answer import project_final_answer


class FinalMediaScope(unittest.TestCase):
    def test_text_only_final_does_not_promote_user_images_or_tool_previews(self):
        refs = [{"artifact_id": "input-image", "type": "image"},
                {"artifact_id": "tool-preview", "type": "image"}]
        final = project_final_answer({"text": "The work is complete."},
            parts=[{"kind": "attachment", "attachment": ref} for ref in refs],
            attachments=refs)
        self.assertEqual([], final["attachments"])
        self.assertEqual([{"kind": "text", "text": "The work is complete."}], final["parts"])

    def test_only_exact_authored_final_markers_select_authorized_media(self):
        source = {"type": "image", "path": "/private/one/output.png"}
        ref = {"artifact_id": "generated", "type": "image", "path": "/private/CAS"}
        other = {"artifact_id": "preview", "type": "image"}
        final = project_final_answer({"text": "Result [IMAGE:/private/one/output.png]"},
            attachments=[other, ref], attachment_bindings=[(source, ref)])
        self.assertEqual([{"artifact_id": "generated", "type": "image"}], final["attachments"])
        self.assertNotIn("/private", str(final))
        for marker in ("[IMAGE:/different/output.png]", "[FILE:/private/one/output.png]"):
            with self.subTest(marker=marker):
                unbound = project_final_answer({"text": marker}, attachment_bindings=[(source, ref)])
                self.assertEqual([], unbound["attachments"])
                self.assertIn("attachment is unavailable", unbound["text"])

    def test_media_only_final_preserves_selected_order_without_duplicates(self):
        sources = [{"type": "image", "path": "/first.png"},
                   {"type": "file", "path": "/second.csv"}]
        refs = [{"artifact_id": "first"}, {"artifact_id": "second"}]
        final = project_final_answer({"text": "[FILE:/second.csv][IMAGE:/first.png][FILE:/second.csv]"},
            attachment_bindings=list(zip(sources, refs)))
        self.assertEqual("", final["text"])
        self.assertEqual([refs[1], refs[0]], final["attachments"])


class NormalDeliveryPaths(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from openagent_core import PrincipalRef, ExecutionContext, RunRequest
        from openagent_core.engine import MemoryDB
        from openagent_core.artifacts import ArtifactRepository
        from openagent_storage_sqlite import SqliteRuntimeStore
        self.temp = TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.db = MemoryDB(str(self.path / "state.sqlite"))
        await self.db.connect()
        self.store = SqliteRuntimeStore(self.path / "state.sqlite")
        await self.store.start()
        async def allow(*args, **kwargs): return True
        self.policy = SimpleNamespace(authorize=allow)
        user = PrincipalRef("host", "tenant", "alice")
        self.ctx = ExecutionContext(user, user, user, "session", "agent", (user,))
        self.request = RunRequest("run", "session", "key", "question")
        await self.store.accept(self.request, self.ctx)
        repository = ArtifactRepository(self.db, self.policy)
        self.input = await repository.upload(self.ctx, b"input-image", filename="input.png", session_id="session")
        self.generated = await repository.upload(self.ctx, b"generated-file", filename="answer.csv", session_id="session")
        conn = await self.db._ensure_connected()
        await conn.execute("CREATE TABLE IF NOT EXISTS ui_view_revisions(view_id TEXT,revision INTEGER,PRIMARY KEY(view_id,revision))")
        await conn.commit()

    async def asyncTearDown(self):
        await self.store.close()
        await self.db.close()
        self.temp.cleanup()

    def frames(self, final):
        # The earlier assistant/tool preview explicitly refers to admitted
        # input, matching the real incident. It remains in the transcript.
        text = f"Looking at [IMAGE:{self.input['path']}] and [FILE:{self.generated['path']}]\n" + final
        return {"kind": "done", "text": text, "final_answer": {"text": final}}

    async def test_engine_keeps_full_transcript_but_delivers_only_final_selection(self):
        from openagent_core.engine import AgentExecutor
        from openagent_core.workspace_administration import SessionInspection
        cases = [("All done.", []),
                 (f"Result [FILE:{self.generated['path']}]", [self.generated["artifact_id"]]),
                 (f"Forward [IMAGE:{self.input['path']}]", [self.input["artifact_id"]]),
                 (f"[IMAGE:{self.input['path']}]", [self.input["artifact_id"]])]
        for final, expected in cases:
            with self.subTest(final=final[:12]):
                events = []
                async def append(*args): events.append(args)
                frame = self.frames(final)
                class Agent:
                    memory_db = self.db
                    async def run_stream(_self, *args, **kwargs): yield frame
                runtime = SimpleNamespace(services=SimpleNamespace(authorizer=self.policy,
                    store=SimpleNamespace(append_event=append)))
                await AgentExecutor(Agent()).execute(self.request, self.ctx, runtime)
                done = events[-1][-1]
                self.assertGreaterEqual(len(done["attachments"]), 2)
                self.assertIn("Looking at", done["text"])
                self.assertEqual(expected, [ref["artifact_id"] for ref in done["final_answer"]["attachments"]])
                self.assertNotIn(self.temp.name, str(done))
                refreshed = await SessionInspection(self.db, self.policy)._public_output_frame(self.ctx, "session", done)
                self.assertEqual(expected, [ref["artifact_id"] for ref in refreshed["final_answer"]["attachments"]])
                self.assertEqual(len(done["attachments"]), len(refreshed["attachments"]))

    async def test_stream_session_keeps_full_transcript_but_delivers_only_final_selection(self):
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.collector import StreamCollector, fold_outbound_event
        from openagent_core.stream.events import TextFinal
        cases = [("All done.", []),
                 (f"Result [FILE:{self.generated['path']}]", [self.generated["artifact_id"]]),
                 (f"Forward [IMAGE:{self.input['path']}]", [self.input["artifact_id"]]),
                 (f"[IMAGE:{self.input['path']}]", [self.input["artifact_id"]])]
        for case, (final, expected) in enumerate(cases):
            with self.subTest(final=final[:12]):
                frame = self.frames(final)
                class Agent:
                    name = "fixture"
                    memory_db = self.db
                    async def run_stream(_self, *args, **kwargs):
                        # Scripted provider IO supplies its normalized turn,
                        # so real message-part persistence need not wait for
                        # a provider that this deterministic test never calls.
                        conn = await self.db._ensure_connected()
                        for ordinal, role in enumerate(("user", "assistant")):
                            sequence = (case + 1) * 10 + ordinal
                            await conn.execute("INSERT INTO session_messages "
                                "(id,tenant_id,session_id,run_id,sequence,ordinal,role,status,author_kind,author_principal_id,text,visibility,source_version,completeness,raw_envelope_json,raw_envelope_schema,legacy_inferred,created_at_ms,updated_at_ms) "
                                "VALUES (?,?,?,?,?,?,?,'complete',?,'user:alice',?,'user_visible',1,'complete','{}',1,0,1,1)",
                                (f"msg-{case}-{role}", "tenant", "session", "run", sequence, sequence,
                                 role, "user" if role == "user" else "agent", "question" if role == "user" else frame["text"]))
                        await conn.commit()
                        yield frame
                async def no_provider(_db): return None
                session = StreamSession(Agent(), client_id="fixture", session_id="session", coalesce_window_ms=0)
                await session.start(stt_factory=no_provider, tts_factory=no_provider)
                try:
                    await session.push_in(TextFinal(session_id="session", text="question"))
                    reply = StreamCollector()
                    while not fold_outbound_event(reply, await asyncio.wait_for(session.outbound.get(), 5)): pass
                    self.assertGreaterEqual(len(reply.attachments), 2)
                    self.assertIn("Looking at", reply.text)
                    self.assertEqual(expected, [ref["artifact_id"] for ref in reply.final_answer["attachments"]])
                    self.assertNotIn(self.temp.name, str(reply.final_answer))
                finally:
                    await session.close()

    async def test_deleted_and_unbound_final_artifact_never_delivers_or_exposes_path(self):
        from openagent_core.engine import AgentExecutor
        conn = await self.db._ensure_connected()
        await conn.execute("UPDATE artifacts SET deleted_at_ms=created_at_ms WHERE id=?", (self.input["artifact_id"],))
        await conn.commit()
        frame = self.frames(f"Forward [IMAGE:{self.input['path']}]")
        class Agent:
            memory_db = self.db
            async def run_stream(_self, *args, **kwargs): yield frame
        events = []
        async def append(*args): events.append(args)
        runtime = SimpleNamespace(services=SimpleNamespace(authorizer=self.policy,
            store=SimpleNamespace(append_event=append)))
        await AgentExecutor(Agent()).execute(self.request, self.ctx, runtime)
        final = events[-1][-1]["final_answer"]
        self.assertEqual([], final["attachments"])
        self.assertIn("attachment is unavailable", final["text"])
        self.assertNotIn(self.temp.name, str(final))
