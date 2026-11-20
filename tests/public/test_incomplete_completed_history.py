"""Older fallback runs keep their recorded answer without modifying storage."""
import copy

import pytest

from openagent_core.core._run_state.agent import RunOutput
from openagent_core.core._run_state.base import RunStatus
from openagent_core.core._run_state.team import TeamRunOutput
from openagent_core.memory.sessions.agent import AgentSession
from openagent_core.memory.sessions.team import TeamSession
from openagent_core.models.final_answer import completed_final_answer
from openagent_core.models.providers.message import Message


@pytest.mark.parametrize("team", [False, True])
@pytest.mark.parametrize("limit", [None, 2])
def test_completed_user_only_run_recovers_exact_historical_answer(team, limit):
    cls, session_cls = (TeamRunOutput, TeamSession) if team else (RunOutput, AgentSession)
    run = cls(run_id="earlier", status=RunStatus.completed, content="  Existing visible answer.\n",
              messages=[Message(role="user", content="Earlier question"),
                        Message(role="assistant", content="Copied old answer", from_history=True)])
    before = copy.deepcopy(run.to_dict())
    session = session_cls(session_id="s", runs=[run])
    history = session.get_messages(limit=limit)
    assert [(m.role, m.content) for m in history] == [("user", "Earlier question"), ("assistant", run.content)]
    assert run.to_dict() == before
    assert completed_final_answer(run) is None  # No new completion attestation.
    assert [m.role for m in session.get_messages(skip_roles=["assistant"])] == ["user"]
    assert session.get_messages(last_n_runs=0) == []


@pytest.mark.parametrize("team", [False, True])
@pytest.mark.parametrize("variant", ["assistant", "tool-call", "empty-assistant", "no-user", "copied-user", "temporary-user",
                                      "empty", "structured", "erased", "compaction", "error", "cancelled", "paused", "running"])
def test_never_guesses_an_unrecorded_answer(team, variant):
    cls, session_cls = (TeamRunOutput, TeamSession) if team else (RunOutput, AgentSession)
    messages = [Message(role="user", content="Question")]
    content, status, metadata = "Existing visible answer", RunStatus.completed, None
    if variant in ("assistant", "tool-call", "empty-assistant"):
        messages.append(Message(role="assistant", content="Already present" if variant == "assistant" else None,
                                tool_calls=[{"id": "call"}] if variant == "tool-call" else None))
    elif variant == "no-user": messages = []
    elif variant == "copied-user": messages[0].from_history = True
    elif variant == "temporary-user": messages[0].temporary = True
    elif variant == "empty": content = "  "
    elif variant == "structured": content = {"value": "not attested text"}
    elif variant == "erased": content = None
    elif variant == "compaction": metadata = {"compaction": True}
    else: status = getattr(RunStatus, variant)
    run = cls(run_id="earlier", status=status, content=content, messages=messages, metadata=metadata)
    session = session_cls(session_id="s", runs=[run])
    assert all(m.content != "Existing visible answer" for m in session.get_messages())


def test_team_member_and_parent_filters_still_bound_recovery():
    own = TeamRunOutput(run_id="own", team_id="root", status=RunStatus.completed,
                        content="Own answer", messages=[Message(role="user", content="Own question")])
    member = RunOutput(run_id="member", agent_id="worker", parent_run_id="own", status=RunStatus.completed,
                       content="Member answer", messages=[Message(role="user", content="Member question")])
    own.member_responses = [member]
    session = TeamSession(session_id="s", runs=[own, member])
    assert [m.content for m in session.get_messages()] == ["Own question", "Own answer"]
    assert [m.content for m in session.get_messages(member_ids=["worker"])] == ["Member question", "Member answer"]
    assert session.get_messages(team_id="unrelated") == []
