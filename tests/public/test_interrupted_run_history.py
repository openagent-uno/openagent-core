"""A failed or cancelled run keeps its executed tool calls in history."""

from openagent_core.core._run_state.agent import RunOutput
from openagent_core.core._run_state.base import RunContext, RunStatus
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.memory.sessions.interrupted import (
    INTERRUPTED_NOTE,
    consistent_prefix,
    recover_interrupted_messages,
)
from openagent_core.models.providers.message import Message


def _call(call_id, name="shell_exec"):
    return {"id": call_id, "type": "function", "function": {"name": name, "arguments": "{}"}}


def _turn(prompt, *, results=True):
    calls = Message(role="assistant", content=None, tool_calls=[_call("a"), _call("b")])
    tail = [
        Message(role="tool", tool_call_id="a", content="cloned"),
        Message(role="tool", tool_call_id="b", content="tests passed"),
    ] if results else [Message(role="tool", tool_call_id="a", content="cloned")]
    return [Message(role="user", content=prompt), calls, *tail]


def test_consistent_prefix_drops_an_unanswered_tool_call_group():
    complete = _turn("work")
    partial = complete + [Message(role="assistant", tool_calls=[_call("c")])]
    assert consistent_prefix(partial) == complete
    assert [m.content for m in consistent_prefix(_turn("work", results=False))] == ["work"]


def test_failed_run_history_keeps_effects_and_closes_with_a_note():
    failed = RunOutput(run_id="r1", session_id="s", status=RunStatus.error, messages=_turn("clone it"))
    done = RunOutput(
        run_id="r2", session_id="s", status=RunStatus.completed,
        messages=[Message(role="user", content="next"), Message(role="assistant", content="ok")],
    )
    session = AgentSession(session_id="s", runs=[failed, done])

    history = session.get_messages()

    contents = [m.content for m in history]
    assert "cloned" in contents and "tests passed" in contents
    assert history[4].role == "assistant" and history[4].content == INTERRUPTED_NOTE
    assert contents[-2:] == ["next", "ok"]
    # The stored run is not mutated by reading history.
    assert failed.messages[-1].content == "tests passed"


def test_paused_runs_and_explicit_skips_are_still_excluded():
    paused = RunOutput(run_id="p", session_id="s", status=RunStatus.paused, messages=_turn("wait"))
    failed = RunOutput(run_id="f", session_id="s", status=RunStatus.error, messages=_turn("x"))
    session = AgentSession(session_id="s", runs=[paused, failed])
    assert all(m.content != "wait" for m in session.get_messages())
    assert session.get_messages(skip_statuses=[RunStatus.paused, RunStatus.error]) == []


def test_interrupted_run_without_user_message_adds_nothing():
    empty = RunOutput(run_id="e", session_id="s", status=RunStatus.cancelled, messages=[])
    assert AgentSession(session_id="s", runs=[empty]).get_messages() == []


def test_recover_keeps_carried_history_and_the_runs_consistent_part():
    carried = Message(role="user", content="old", from_history=True)
    live = [carried, *_turn("now"), Message(role="assistant", tool_calls=[_call("z")])]
    recovered = recover_interrupted_messages(live)
    assert recovered[0] is carried
    assert [m.content for m in recovered[1:]] == ["now", None, "cloned", "tests passed"]


def test_cleanup_persists_the_live_messages_of_a_failed_run():
    from openagent_core.core._runner.agent._run import _keep_interrupted_messages

    run = RunOutput(run_id="r", session_id="s", status=RunStatus.error)
    context = RunContext(run_id="r", session_id="s")
    context.messages = _turn("deploy") + [Message(role="assistant", tool_calls=[_call("q")])]
    _keep_interrupted_messages(run, context)
    assert [m.role for m in run.messages] == ["user", "assistant", "tool", "tool"]

    completed = RunOutput(run_id="c", session_id="s", status=RunStatus.completed)
    _keep_interrupted_messages(completed, context)
    assert completed.messages is None
