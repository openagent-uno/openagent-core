"""Shared chat commands: one catalogue and one semantics for every host."""
import asyncio
import sys

import pytest
from openagent_core import ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core import chat_commands as cc
from openagent_core.chat_commands import (ChatCommandHost, ChatCommandRequest, ChatCommandService,
                                          DeferredChatCommandHost, SessionRef, SessionStatus, StopOutcome)
from openagent_storage_sqlite import SqliteRuntimeStore


class Agent:
    def __init__(self, *, fail=False):
        self.calls, self.fail, self.memory_db = [], fail, None

    async def forget_session(self, session_id):
        self.calls.append(("forget", session_id))

    async def release_session(self, session_id):
        if self.fail:
            raise RuntimeError("PRIVATE provider diagnostic")
        self.calls.append(("release", session_id))


class Host(ChatCommandHost):
    def __init__(self):
        self.calls = []

    async def stop_session(self, session):
        self.calls.append("stop")
        return StopOutcome(stopped=True, cleared=2)

    async def rotate_session(self, session):
        self.calls.append("rotate")
        return "session-12345678"

    async def session_status(self, session):
        return SessionStatus(busy=True, queued=3, sessions=2)


def run(host, name, *, agent=None, session=SessionRef("s1"), scopes=None, argument=None):
    return asyncio.run(ChatCommandService(agent or Agent(), host, scopes=scopes).execute(
        ChatCommandRequest(name, session, argument)))


def test_catalogue_import_starts_no_engine():
    code = ("import sys;import openagent_core.chat_commands;"
            "print([m for m in sys.modules if m.startswith(('agno','openagent_core.core.agent','openagent_core.models'))])")
    import subprocess
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True, env={"PYTHONPATH": ":".join(sys.path)})
    assert out.stdout.strip() == "[]"


def test_scopes_separate_session_from_instance_commands():
    names = {spec.name for spec in cc.commands_for_scopes(["session"])}
    assert {"new", "clear", "reset", "stop", "status", "queue", "help", "compact", "model", "context"} == names
    assert {spec.name for spec in cc.COMMAND_SPECS} - names == {"usage", "update", "restart"}
    with pytest.raises(ValueError):
        cc.commands_for_scopes(["unknown"])
    assert "/update" not in cc.command_help_text(["session"]) and "/update" in cc.command_help_text()


@pytest.mark.parametrize("name", ["new", "clear", "reset"])
def test_reset_stops_clears_forgets_then_rotates(name):
    host, agent = Host(), Agent()
    result = run(host, name, agent=agent)
    assert host.calls == ["stop", "rotate"] and agent.calls == [("forget", "s1")]
    assert result.text == "Stopped current operation. cleared 2 queued messages. forgot 1 prior conversation. fresh session: 12345678."
    assert result.status == cc.COMPLETED and result.host_actions == ()


def test_retaining_host_never_forgets_stored_history():
    class Retaining(Host):
        retains_history = True
    agent = Agent()
    result = run(Retaining(), "new", agent=agent)
    assert agent.calls == [("release", "s1")]
    assert "kept" in result.text and "vault are unchanged" in result.text


def test_shared_conversation_cannot_be_reset_and_nothing_is_stopped():
    host, agent = Host(), Agent()
    result = run(host, "new", agent=agent, session=SessionRef("s1", shared=True))
    assert result.status == cc.UNSUPPORTED and host.calls == [] and agent.calls == []
    assert "not reset" in result.text


def test_a_host_without_rotation_reports_unsupported_not_success():
    class NoRotation(Host):
        async def rotate_session(self, session):
            raise cc.ChatCommandUnavailable
    result = run(NoRotation(), "new")
    assert result.status == cc.UNSUPPORTED and "not reset" in result.text


def test_failure_is_an_outcome_and_private_detail_can_be_withheld():
    class Retaining(Host):
        retains_history = True
        expose_errors = False
    host = Retaining()
    result = run(host, "new", agent=Agent(fail=True))
    assert result.status == cc.FAILED and result.host_actions == ()
    assert "not reset" in result.text and "PRIVATE" not in result.text


def test_deferred_host_receives_ordered_actions_with_truthful_text():
    host, agent = DeferredChatCommandHost(active_runs=1, queued_runs=2), Agent()
    result = run(host, "new", agent=agent)
    assert result.host_actions == (cc.STOP_RUNS, cc.RELEASE_SESSION, cc.ROTATE_SESSION)
    assert agent.calls == []  # A deferred host must stop real turns before release.
    assert result.text.startswith("Stopped current operation. cleared 2 queued messages. started a fresh conversation.")
    stopped = run(DeferredChatCommandHost(active_runs=1), "stop")
    assert stopped.text == "Stopped current operation." and stopped.host_actions == (cc.STOP_RUNS,)
    assert run(DeferredChatCommandHost(), "stop").text == "Nothing running."
    assert run(DeferredChatCommandHost(active_runs=1, queued_runs=4), "status").text == "Busy | Queue: 4 | Sessions: 1"
    assert run(DeferredChatCommandHost(queued_runs=4), "queue").text == "Queue depth: 4"


def test_nobody_to_apply_actions_means_no_reset_and_no_side_effect():
    agent = Agent()
    host = DeferredChatCommandHost(can_apply=False)
    result = run(host, "new", agent=agent)
    assert result.status == cc.UNSUPPORTED and result.host_actions == () and agent.calls == []


def test_instance_command_outside_scope_and_unknown_command_are_unsupported():
    assert run(DeferredChatCommandHost(), "restart", scopes=["session"]).status == cc.UNSUPPORTED
    assert run(Host(), "usage").status == cc.UNSUPPORTED
    unknown = run(Host(), "nope")
    assert unknown.status == cc.UNSUPPORTED and unknown.text == "Unknown command: nope"


def test_help_lists_the_hosts_scopes():
    text = run(DeferredChatCommandHost(), "help", scopes=["session"]).text
    assert "/new" in text and "/model" in text and "/restart" not in text


def test_model_change_requires_host_authorization():
    class DB:
        async def get_session_pin(self, sid): return None
    class Denied(Host):
        async def authorize_model_change(self, session):
            raise PermissionError
    agent = Agent()
    agent.memory_db = DB()
    result = run(Denied(), "model", agent=agent, argument="auto")
    assert result.status == cc.FAILED and "not allowed" in result.text


def _context():
    p = PrincipalRef("host", "tenant", "alice")
    return ExecutionContext(p, p, p, "session", "agent", (p,))


class _Executor:
    def __init__(self):
        self.agent = Agent()

    async def execute(self, request, context, runtime):
        return ""


class _Policy:
    allowed = True

    async def authorize(self, *args, **kwargs):
        return self.allowed


@pytest.mark.asyncio
async def test_runtime_entrypoint_verifies_session_and_policy(tmp_path):
    policy, executor = _Policy(), _Executor()
    runtime = Runtime(RuntimeSettings("agent", tmp_path), RuntimeServices(SqliteRuntimeStore(tmp_path / "state.sqlite3"), executor, policy))
    await runtime.start()
    try:
        request = ChatCommandRequest("new", SessionRef("session"))
        result = await runtime.chat_command(request, _context(), DeferredChatCommandHost(), scopes=["session"])
        assert result.host_actions == (cc.STOP_RUNS, cc.RELEASE_SESSION, cc.ROTATE_SESSION)
        assert executor.agent.calls == []
        with pytest.raises(ValueError):
            await runtime.chat_command(ChatCommandRequest("new", SessionRef("other")), _context(), DeferredChatCommandHost())
        policy.allowed = False
        with pytest.raises(PermissionError):
            await runtime.chat_command(request, _context(), DeferredChatCommandHost())
        with pytest.raises(PermissionError):
            await runtime.chat_command_action(cc.RELEASE_SESSION, _context(), operation_id="reset-session")
        assert executor.agent.calls == []
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_deferred_reset_drains_real_runtime_turn_before_provider_release(tmp_path):
    """A stop acknowledgement may precede a provider's cancellation cleanup."""
    from dataclasses import replace
    from openagent_core import RunRequest

    class LiveAgent(Agent):
        active = False
        async def release_session(self, session_id):
            assert not self.active, "provider released while its stream was alive"
            await super().release_session(session_id)

    class LiveExecutor:
        def __init__(self):
            self.agent = LiveAgent()
            self.started, self.stopping, self.drained = asyncio.Event(), asyncio.Event(), asyncio.Event()
            self.calls = []
        async def execute(self, request, context, runtime):
            self.calls.append(request.run_id)
            if request.input != "block":
                return request.input
            self.agent.active = True
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.stopping.set()
                await self.drained.wait()
                raise
            finally:
                self.agent.active = False

    executor = LiveExecutor()
    runtime = Runtime(RuntimeSettings("agent", tmp_path), RuntimeServices(SqliteRuntimeStore(tmp_path / "state.sqlite3"), executor, _Policy()))
    await runtime.start()
    context = _context()
    stop = action = None
    try:
        await runtime.submit(RunRequest("active", "session", "active", "block"), context)
        await asyncio.wait_for(executor.started.wait(), 1)
        await runtime.submit(RunRequest("queued", "session", "queued", "queued"), context)
        plan = await runtime.chat_command(ChatCommandRequest("new", SessionRef("session")), context, DeferredChatCommandHost(active_runs=1, queued_runs=1))
        assert plan.host_actions == (cc.STOP_RUNS, cc.RELEASE_SESSION, cc.ROTATE_SESSION)
        assert executor.agent.calls == [] and executor.agent.active
        await runtime.cancel("queued", context)
        stop = asyncio.create_task(runtime.cancel("active", context))
        await asyncio.wait_for(executor.stopping.wait(), 1)
        # Simulate a premature transport acknowledgement: cleanup still waits
        # for the actual session lock, even though the caller advances its plan.
        action = asyncio.create_task(runtime.chat_command_action(cc.RELEASE_SESSION, context, operation_id="reset-old-session"))
        await asyncio.sleep(0)
        assert not action.done() and executor.agent.calls == []
        executor.drained.set()
        await asyncio.wait_for(asyncio.gather(stop, action), 1)
        assert (await runtime.get_run("active", context)).status == "cancelled"
        assert (await runtime.get_run("queued", context)).status == "cancelled"
        assert executor.calls == ["active"]
        assert executor.agent.calls == [("release", "session")]
        # The host changes only its selector; the old rows/transcript remain.
        fresh = replace(context, session_id="fresh-session")
        await runtime.submit(RunRequest("fresh", "fresh-session", "fresh", "new context"), fresh)
        assert (await runtime.wait("fresh", fresh)).status == "success"
        assert (await runtime.get_run("active", context)).session_id == "session"
        # Reapplying an idempotent resource release never targets the new id.
        await runtime.chat_command_action(cc.RELEASE_SESSION, context, operation_id="reset-old-session")
        assert executor.agent.calls[-1] == ("release", "session")
        # A provider that never drains cannot hang cleanup or reach rotation.
        from unittest.mock import patch
        real_timeout = asyncio.timeout
        lock = runtime._sessions[(context.tenant_id, context.session_id)]
        before = list(executor.agent.calls)
        await lock.acquire()
        try:
            with patch("openagent_core.runtime.asyncio.timeout", lambda _seconds: real_timeout(0.01)):
                with pytest.raises(TimeoutError):
                    await runtime.chat_command_action(cc.RELEASE_SESSION, context, operation_id="blocked-cleanup")
        finally:
            lock.release()
        assert executor.agent.calls == before
        with pytest.raises(ValueError):
            await runtime.chat_command_action("erase_history", context, operation_id="reset-old-session")
    finally:
        executor.drained.set()
        if stop is not None:
            await asyncio.gather(stop, return_exceptions=True)
        if action is not None:
            await asyncio.gather(action, return_exceptions=True)
        await runtime.close()
