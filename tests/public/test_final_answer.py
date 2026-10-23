"""Final output is an explicit completed-run message, never transcript slicing."""
import asyncio
from types import SimpleNamespace
import unittest

from openagent_core.core._run_state.agent import RunOutput
from openagent_core.core._run_state.team import TeamRunOutput
from openagent_core.core._runner.utils.events import (
    create_run_completed_event, create_team_run_completed_event,
)
from openagent_core.models.providers.message import Message


class CompletedAnswer(unittest.TestCase):
    def test_agent_and_team_preserve_transcript_but_attest_final_message(self):
        for output, factory in [(RunOutput, create_run_completed_event),
                                (TeamRunOutput, create_team_run_completed_event)]:
            with self.subTest(output=output.__name__):
                event = factory(output(content="Checking first.\nFinal answer.", messages=[
                    Message(role="user", content="question"),
                    Message(role="assistant", content="Checking first.", tool_calls=[{"id":"tool-1"}]),
                    Message(role="tool", content="untrusted result"),
                    Message(role="assistant", content="Final answer."),
                ]))
                self.assertEqual("Checking first.\nFinal answer.", event.content)
                self.assertEqual({"text":"Final answer."}, event.final_answer)

    def test_no_final_is_inferred_from_old_messages_or_tool_termination(self):
        for messages in [None, [],
                         [Message(role="assistant", content="old", from_history=True)],
                         [Message(role="assistant", content="calling", tool_calls=[{"id":"x"}])],
                         [Message(role="assistant", content="old"), Message(role="tool", content="not final")],
                         [Message(role="user", content="not final")]]:
            with self.subTest(messages=messages):
                event = create_run_completed_event(RunOutput(content="combined transcript", messages=messages))
                self.assertIsNone(event.final_answer)

    def test_attested_empty_answer_is_distinct_from_unknown(self):
        event = create_run_completed_event(RunOutput(messages=[Message(role="assistant", content="")]))
        self.assertEqual({"text":""}, event.final_answer)

    def test_structured_text_blocks_and_event_roundtrip(self):
        from openagent_core.core._run_state.agent import run_output_event_from_dict
        event = create_run_completed_event(RunOutput(messages=[Message(role="assistant", content=[
            {"type":"text","text":"first "}, {"type":"output_text","text":"second"},
        ])]))
        self.assertEqual({"text":"first second"}, run_output_event_from_dict(event.to_dict()).final_answer)

    def test_media_projection_has_no_narration_or_private_path(self):
        from openagent_core.stream.final_answer import project_final_answer
        ref = {"artifact_id":"file-one", "filename":"result.csv", "path":"/private/CAS"}
        answer = project_final_answer({"text":"Final answer. [FILE:/authorized/result.csv]"}, parts=[
            {"kind":"text","text":"Narration not part of final."},
            {"kind":"attachment","attachment":ref},
        ], attachments=[ref], attachment_bindings=[({"type":"file", "path":"/authorized/result.csv"}, ref)])
        self.assertEqual("Final answer.", answer["text"])
        self.assertEqual(["text","attachment"], [p["kind"] for p in answer["parts"]])
        self.assertEqual([{"artifact_id":"file-one","filename":"result.csv"}], answer["attachments"])
        self.assertNotIn("private", str(answer))
        self.assertIsNone(project_final_answer(None, parts=[]))


class _Runner:
    id = "fixture-root"
    def __init__(self, events): self.events = events
    def arun(self, *_args, **_kwargs):
        async def iterate():
            for event in self.events:
                await asyncio.sleep(0)
                yield event
        return iterate()


class AnswerPropagation(unittest.IsolatedAsyncioTestCase):
    async def test_team_stream_ignores_child_and_unrelated_run_finals(self):
        from openagent_core.core._run_state.team import RunContentEvent, RunCompletedEvent
        from openagent_core.models.dispatcher import _arun_runtime_stream
        answers = []
        async def capture(x): answers.append(x)
        events = [RunContentEvent(session_id="parent",run_id="parent-run",team_id="fixture-root",content="Narration."),
                  RunCompletedEvent(session_id="child",run_id="child-run",final_answer={"text":"CHILD"}),
                  RunCompletedEvent(session_id="parent",run_id="other-run",final_answer={"text":"WRONG RUN"}),
                  RunCompletedEvent(session_id="parent",run_id="parent-run",team_id="unrelated-team",final_answer={"text":"WRONG TEAM"}),
                  RunCompletedEvent(session_id="parent",run_id="parent-run",team_id="fixture-root",content="Narration.Final.",final_answer={"text":"Final."})]
        chunks = [c async for c in _arun_runtime_stream(_Runner(events),prompt="question",session_id="parent",
            user_id="user",on_status=None,error_event="fixture",on_final_answer=capture)]
        self.assertEqual([{"text":"Final."}], answers)
        self.assertIn("Narration.", "".join(chunks))

    async def test_error_or_cancel_never_attests_a_completed_draft(self):
        from openagent_core.core._run_state.agent import RunCompletedEvent, RunCancelledEvent, RunErrorEvent
        from openagent_core.models.dispatcher import _arun_runtime_stream
        for terminal, error in [(RunCancelledEvent(session_id="s",reason="stopped"),asyncio.CancelledError),
                                (RunErrorEvent(session_id="s",content="failed"),RuntimeError)]:
            answers = []
            async def capture(x): answers.append(x)
            with self.assertRaises(error):
                async for _ in _arun_runtime_stream(_Runner([
                    RunCompletedEvent(session_id="s",final_answer={"text":"draft"}),terminal]),
                    prompt="q",session_id="s",user_id="u",on_status=None,error_event="fixture",on_final_answer=capture): pass
            self.assertEqual([],answers)

    async def test_collector_preserves_envelope_and_clears_it_on_failure(self):
        from openagent_core.stream.collector import StreamCollector, fold_outbound_event
        from openagent_core.stream.events import OutTextFinal, OutError, TurnComplete
        answer={"text":"Final", "parts":[{"kind":"text","text":"Final"}],"attachments":[]}
        for terminal in [TurnComplete(session_id="s",reason="cancelled"),OutError(session_id="s",text="failed")]:
            reply=StreamCollector()
            fold_outbound_event(reply,OutTextFinal(session_id="s",text="Narration.Final",final_answer=answer))
            self.assertEqual(answer,reply.to_legacy_reply()["final_answer"])
            self.assertEqual("Narration.Final",reply.text)
            fold_outbound_event(reply,terminal)
            self.assertIsNone(reply.to_legacy_reply()["final_answer"])

    async def test_stream_session_preserves_additive_answer_without_changing_transcript(self):
        from openagent_core.stream.session import StreamSession
        from openagent_core.stream.collector import StreamCollector,fold_outbound_event
        from openagent_core.stream.events import TextFinal
        class Agent:
            name="fixture";db=None
            async def run_stream(self,**kwargs):
                yield {"kind":"delta","text":"Narration.Final."}
                yield {"kind":"done","text":"Narration.Final.","final_answer":{"text":"Final."}}
        async def no_provider(_db):return None
        session=StreamSession(Agent(),client_id="fixture",session_id="s",coalesce_window_ms=0)
        await session.start(stt_factory=no_provider,tts_factory=no_provider)
        try:
            await session.push_in(TextFinal(session_id="s",text="question"))
            reply=StreamCollector()
            while not fold_outbound_event(reply,await asyncio.wait_for(session.outbound.get(),2)): pass
            self.assertEqual("Narration.Final.",reply.text)
            self.assertEqual("Final.",reply.final_answer["text"])
        finally: await session.close()

    async def test_engine_persists_full_text_and_separate_final_envelope(self):
        from openagent_core.engine import AgentExecutor
        from openagent_core.workspace_administration import SessionInspection
        stored=[]
        async def append(*args): stored.append(args)
        async def authorize(*args,**kwargs):return True
        class Agent:
            async def run_stream(self,*args,**kwargs):
                yield {"kind":"done","text":"Narration.Final.","final_answer":{"text":"Final."}}
        runtime=SimpleNamespace(services=SimpleNamespace(authorizer=SimpleNamespace(authorize=authorize),store=SimpleNamespace(append_event=append)))
        context=SimpleNamespace(tenant_id="t",session_id="s",agent_id="a",audience=(),author=SimpleNamespace(kind="user",key="alice"))
        request=SimpleNamespace(run_id="r",session_id="s",model_ref=None,input="q",attachments=())
        self.assertEqual("Narration.Final.",await AgentExecutor(Agent()).execute(request,context,runtime))
        frame=stored[-1][-1]
        self.assertEqual("Final.",frame["final_answer"]["text"])
        public=await SessionInspection(None,None)._public_output_frame(context,"s",frame)
        self.assertEqual(frame["final_answer"],public["final_answer"])

    async def test_durable_snapshot_preserves_final_media_and_rechecks_deleted_artifact(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from openagent_core import PrincipalRef,ExecutionContext,RunRequest,Runtime,RuntimeServices,RuntimeSettings
        from openagent_core.administration import ManagementContext
        from openagent_core.artifacts import ArtifactRepository
        from openagent_core.engine import AgentExecutor,MemoryDB
        from openagent_core.workspace_administration import SessionInspection
        from openagent_storage_sqlite import SqliteRuntimeStore
        async def authorize(*args,**kwargs):return True
        policy=SimpleNamespace(authorize=authorize)
        with TemporaryDirectory() as temp:
            path=Path(temp);db=MemoryDB(str(path/'state.sqlite'));await db.connect()
            store=SqliteRuntimeStore(path/'state.sqlite');await store.start()
            try:
                user=PrincipalRef('host','tenant','alice')
                ctx=ExecutionContext(user,user,user,'session','agent',(user,))
                request=RunRequest('run','session','key','question')
                await store.accept(request,ctx)
                ref=await ArtifactRepository(db,policy).upload(ctx,b'answer,41\n',filename='answer.csv',session_id='session')
                class Agent:
                    memory_db=db
                    async def run_stream(self,*args,**kwargs):
                        yield {'kind':'done','text':'Narration.Final [FILE:'+ref['path']+']',
                               'final_answer':{'text':'Final [FILE:'+ref['path']+']'}}
                executor=AgentExecutor(Agent());runtime=Runtime(RuntimeSettings('agent',path),RuntimeServices(store,executor,policy))
                await executor.execute(request,ctx,runtime)
                inspector=SessionInspection(db,policy);management=ManagementContext(user,'agent')
                snapshot=await inspector.live_runs(management,'session')
                frame=snapshot['runs'][0]['events'][-1]['payload']
                self.assertIn('Narration.',frame['text'])
                self.assertEqual('Final',frame['final_answer']['text'])
                self.assertEqual(ref['artifact_id'],frame['final_answer']['attachments'][0]['artifact_id'])
                self.assertNotIn(temp,str(snapshot));self.assertNotIn('[FILE:',str(snapshot))
                conn=await db._ensure_connected()
                await conn.execute('UPDATE artifacts SET deleted_at_ms=created_at_ms WHERE id=?',(ref['artifact_id'],));await conn.commit()
                revoked=await inspector.live_runs(management,'session')
                final=revoked['runs'][0]['events'][-1]['payload']['final_answer']
                self.assertEqual([],final['attachments'])
                self.assertNotIn(ref['artifact_id'],str(final))
                await store.transition('run','cancelled')
                cancelled=await inspector.live_runs(management,'session')
                self.assertIsNone(cancelled['runs'][0]['events'][-1]['payload']['final_answer'])
            finally:await store.close();await db.close()


class ActualProviderToolLoop(unittest.IsolatedAsyncioTestCase):
    async def test_native_terminal_failure_never_attests_prior_completion(self):
        from unittest.mock import patch
        from openagent_core.models.native_provider import NativeProvider
        from openagent_core.core._run_state.agent import RunContentEvent,RunCompletedEvent,RunErrorEvent
        native=NativeProvider('openai:fixture',api_key='fixture-only')
        runner=_Runner([RunContentEvent(session_id='s',run_id='r',agent_id='fixture-root',content='partial'),
                        RunCompletedEvent(session_id='s',run_id='r',agent_id='fixture-root',final_answer={'text':'draft'}),
                        RunErrorEvent(session_id='s',run_id='r',agent_id='fixture-root',content='failed')])
        answers=[]
        async def capture(x):answers.append(x)
        with patch.object(native,'_ensure_team',return_value=None),patch.object(native,'_ensure_agent',return_value=runner):
            chunks=[c async for c in native.stream([{'role':'user','content':'q'}],session_id='s',on_final_answer=capture)]
        self.assertEqual(['partial'],chunks)
        self.assertEqual([None],answers)

    async def test_real_native_runner_tool_loop_excludes_prelude_from_final(self):
        from unittest.mock import patch
        from openagent_core.models.native_provider import NativeProvider
        from openagent_core.models.providers.base import Model
        from openagent_core.models.providers.response import ModelResponse
        from openagent_core.core._runner.agent.agent import Agent as RuntimeAgent
        calls=[]
        def read_value() -> str:
            """Read the deterministic fixture value."""
            calls.append("tool")
            return "41"
        class ProviderIO(Model):
            def __init__(self):super().__init__(id="fixture",provider="fixture")
            def invoke(self,*a,**k):raise AssertionError("no network")
            def invoke_stream(self,*a,**k):raise AssertionError("no network")
            async def ainvoke(self,*a,**k):raise AssertionError("stream only")
            async def ainvoke_stream(self,*a,**k):
                if not calls:
                    yield ModelResponse(role="assistant",content="I will check.",tool_calls=[{"id":"call-1","type":"function","function":{"name":"read_value","arguments":"{}"}}])
                else:yield ModelResponse(role="assistant",content="The answer is 41.")
            def _parse_provider_response(self,response,**k):return response
            def _parse_provider_response_delta(self,response):return response
        native=NativeProvider('openai:fixture',api_key='fixture-only')
        runner=RuntimeAgent(name="fixture",model=ProviderIO(),tools=[read_value],markdown=False)
        answers=[]
        async def capture(x):answers.append(x)
        with patch.object(native,'_ensure_team',return_value=None),patch.object(native,'_ensure_agent',return_value=runner):
            chunks=[c async for c in native.stream([{"role":"user","content":"question"}],session_id="actual-runner",on_final_answer=capture)]
        self.assertEqual(["tool"],calls)
        self.assertEqual("I will check.The answer is 41.","".join(chunks))
        self.assertEqual([{"text":"The answer is 41."}],answers)


class AgentBoundary(unittest.IsolatedAsyncioTestCase):
    class Model:
        history_mode="caller";model="fixture"
        def __init__(self, mode="stream"):self.mode=mode;self.generate_calls=0
        def effective_model_id(self, _sid=None):return self.model
        async def close_session(self, _sid):pass
        async def stream(self, messages, system=None, session_id=None, on_status=None, on_final_answer=None):
            if self.mode=="empty":return
            yield "Checking."
            await asyncio.sleep(0)
            if self.mode=="cancel":raise asyncio.CancelledError()
            if on_final_answer is not None:await on_final_answer({"text":session_id})
            await asyncio.sleep(0)
            yield session_id
        async def generate(self,*args,**kwargs):
            from openagent_core.models.base import ModelResponse
            self.generate_calls+=1
            return ModelResponse(content="fallback",final_answer={"text":"fallback"})

    async def make_agent(self, mode="stream"):
        from openagent_core.core.agent import Agent
        agent=Agent(name="fixture",model=self.Model(mode),memory=None,system_prompt="fixture")
        self.addAsyncCleanup(agent.shutdown)
        return agent

    async def test_concurrent_runs_do_not_share_answers(self):
        agent=await self.make_agent()
        async def collect(sid):return [e async for e in agent.run_stream("question",session_id=sid)]
        a,b=await asyncio.gather(collect("first"),collect("second"))
        self.assertEqual({"text":"first"},a[-1]["final_answer"])
        self.assertEqual({"text":"second"},b[-1]["final_answer"])
        self.assertEqual("Checking.first",a[-1]["text"])
        self.assertEqual(0,agent.model.generate_calls)

    async def test_empty_stream_fallback_carries_its_own_attestation(self):
        agent=await self.make_agent("empty")
        events=[e async for e in agent.run_stream("question",session_id="s")]
        self.assertEqual({"text":"fallback"},events[-1]["final_answer"])
        self.assertEqual(1,agent.model.generate_calls)

    async def test_cancel_has_no_done_and_never_falls_back(self):
        agent=await self.make_agent("cancel");events=[]
        with self.assertRaises(asyncio.CancelledError):
            async for e in agent.run_stream("question",session_id="s"):events.append(e)
        self.assertFalse(any(e["kind"]=="done" for e in events))
        self.assertEqual(0,agent.model.generate_calls)

    async def test_pre_send_guard_rewrite_invalidates_original_final(self):
        from unittest.mock import AsyncMock,patch
        agent=await self.make_agent()
        with patch('openagent_core.core.reply_guard.guard_reply',new=AsyncMock(return_value="Guarded replacement")):
            events=[e async for e in agent.run_stream("question",session_id="s")]
        self.assertEqual("Guarded replacement",events[-1]["text"])
        self.assertIsNone(events[-1]["final_answer"])


if __name__ == '__main__':
    unittest.main()
