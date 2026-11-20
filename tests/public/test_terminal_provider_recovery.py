"""Terminal failures cross the actual SDK/runner boundary without another run."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import AsyncMock, patch

import pytest

from openagent_core.core.public_errors import PUBLIC_MESSAGES, classify_run_error
from openagent_core.engine import MemoryDB, ModelDispatcher
from openagent_core.models.providers.fallback import reset_breaker


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status,diagnostic", [
    ("auth", 401, "Invalid API key PRIVATE-PROVIDER-TOKEN"),
    ("auth_expired", 401, "OAuth token expired PRIVATE-PROVIDER-TOKEN"),
    ("credit_exhausted", 400, "Your credit balance is too low PRIVATE-PROVIDER-TOKEN"),
    ("quota_exhausted", 400, "You exceeded your current quota PRIVATE-PROVIDER-TOKEN"),
    ("rate_limit", 429, "rate_limit_error PRIVATE-PROVIDER-TOKEN"),
])
@pytest.mark.parametrize("backup", [None, "disabled", "limited", "unavailable"])
async def test_http_stream_retains_original_category_and_safe_cooldown(tmp_path, code, status, diagnostic, backup):
    seen = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            seen.append(self.headers.get("Authorization"))
            unavailable = backup == "unavailable" and seen[-1] == "Bearer backup"
            data = json.dumps({"error": {"message": "connection refused PRIVATE-BACKUP" if unavailable else diagnostic, "type": "fixture_error"}}).encode()
            self.send_response(503 if unavailable else status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    reset_breaker()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    db = MemoryDB(str(tmp_path / "agent.sqlite"))
    await db.connect()
    dispatcher = None
    try:
        for name in (["primary", "backup"] if backup else ["primary"]):
            pid = await db.upsert_provider(name=name, framework="api-based", api_key=name,
                base_url=f"http://127.0.0.1:{server.server_port}/v1", enabled=not (name == "backup" and backup == "disabled"))
            await db.upsert_model(provider_id=pid, model="fixture-text", is_classifier=name == "primary")
        dispatcher = ModelDispatcher(providers_config=await db.materialise_providers_config(enabled_only=False))
        dispatcher.set_db(db)
        for attempt in range(2):
            with pytest.raises(Exception) as raised:
                async for _ in dispatcher.stream([{"role": "user", "content": "fixture"}], session_id="failure-session"):
                    pytest.fail("A failed provider produced answer text")
            failure = classify_run_error(raised.value)
            assert failure.code == code
            assert failure.public_message == PUBLIC_MESSAGES[code]
            assert "PRIVATE" not in failure.public_message
            if attempt == 0:
                first_calls = len(seen)
            elif status in (401, 429):
                assert len(seen) == first_calls, "A cooling route must not call the provider again"
                assert "PRIVATE" not in str(raised.value), "Cooldown retains only the safe public failure"
        expected = (["Bearer primary", "Bearer backup"] if backup in ("limited", "unavailable") and status in (401, 429)
                    else ["Bearer primary"] if status in (401, 429) else ["Bearer primary"] * 2)
        assert seen == expected
    finally:
        if dispatcher is not None:
            await dispatcher.shutdown()
        await db.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        reset_breaker()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["cancel", "partial", "tool"])
async def test_native_committed_or_cancelled_stream_never_calls_generate(kind):
    from openagent_core.models.native_provider import NativeProvider
    from openagent_core.core._run_state.agent import RunCancelledEvent, RunCompletedEvent, RunContentEvent, ToolCallStartedEvent
    closed = []
    class Runner:
        id = "fixture-root"
        def arun(self, *args, **kwargs):
            async def events():
                try:
                    common = dict(session_id="s", run_id="r", agent_id=self.id)
                    if kind == "cancel":
                        yield RunCancelledEvent(**common, reason="stopped")
                        yield RunContentEvent(**common, content="late content must be suppressed")
                        yield RunCompletedEvent(**common, final_answer={"text": "late draft"})
                    elif kind == "partial":
                        yield RunContentEvent(**common, content="partial")
                        raise RuntimeError("transport lost")
                    else:
                        yield ToolCallStartedEvent(**common)
                        raise RuntimeError("transport lost after tool")
                finally:
                    closed.append(True)
            return events()
    native = NativeProvider("openai:fixture", api_key="fixture-only")
    generate = AsyncMock(side_effect=AssertionError("A committed stream was replayed"))
    output, answers = [], []
    async def capture(value): answers.append(value)
    with patch.object(native, "_ensure_team", return_value=None), patch.object(native, "_ensure_agent", return_value=Runner()), patch.object(native, "generate", generate):
        with pytest.raises(asyncio.CancelledError if kind == "cancel" else RuntimeError):
            async for value in native.stream([{"role": "user", "content": "fixture"}], session_id="s", on_final_answer=capture):
                output.append(value)
    generate.assert_not_called()
    assert closed == [True]
    assert output == (["partial"] if kind == "partial" else [])
    assert answers == []


@pytest.mark.asyncio
async def test_empty_stream_generate_failure_is_not_replayed_again():
    from openagent_core.models.native_provider import NativeProvider
    class Runner:
        id = "fixture-root"
        def arun(self, *args, **kwargs):
            async def events():
                if False:
                    yield
            return events()
    native = NativeProvider("openai:fixture", api_key="fixture-only")
    generate = AsyncMock(side_effect=RuntimeError("terminal generate failure"))
    with patch.object(native, "_ensure_team", return_value=None), patch.object(native, "_ensure_agent", return_value=Runner()), patch.object(native, "generate", generate):
        with pytest.raises(RuntimeError, match="terminal generate failure"):
            async for _ in native.stream([{"role": "user", "content": "fixture"}], session_id="s"):
                pass
    generate.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("team", [False, True])
@pytest.mark.parametrize("partial", [False, True])
async def test_native_cancelled_actual_runner_persists_session_before_propagation(tmp_path, team, partial):
    from openagent_core.core._runner.agent.agent import Agent as RuntimeAgent
    from openagent_core.core.runtime_errors import RunCancelledException
    from openagent_core.memory.store.sqlite import SqliteDb
    from openagent_core.models.native_provider import NativeProvider
    from openagent_core.models.providers.openai.like import OpenAILike
    class CancelledModel(OpenAILike):
        async def aresponse_stream(self, **kwargs):
            if partial:
                from openagent_core.models.providers.response import ModelResponse
                yield ModelResponse(role="assistant", content="partial work")
            raise RunCancelledException("stopped before provider IO")
    store = SqliteDb(db_file=str(tmp_path / "cancelled.sqlite"))
    runner = RuntimeAgent(id="fixture-root", model=CancelledModel(id="fixture", api_key="fixture-only"), db=store)
    if team:
        from openagent_core.core._runner.team.team import Team
        runner = Team(id="fixture-team", members=[RuntimeAgent(id="member", model=CancelledModel(id="fixture", api_key="fixture-only"))],
                      model=CancelledModel(id="fixture", api_key="fixture-only"), db=store)
    native = NativeProvider("openai:fixture", api_key="fixture-only")
    generate = AsyncMock(side_effect=AssertionError("Cancelled run was replayed"))
    try:
        with patch.object(native, "_ensure_team", return_value=runner if team else None), patch.object(native, "_ensure_agent", return_value=runner), patch.object(native, "generate", generate):
            with pytest.raises(asyncio.CancelledError, match="stopped before provider IO"):
                async for _ in native.stream([{"role": "user", "content": "fixture input"}], session_id="cancelled-session"):
                    pass
        session = store.get_session("cancelled-session")
        assert session is not None
        assert len(session.runs) == 1
        # The pre-existing promote_interrupted_run policy stores interrupted
        # provider history as COMPLETED to retain context. The cancellation
        # outcome above remains CancelledError; this verifies cleanup persisted
        # the actual input/messages, not a new storage-status policy.
        raw = store.get_session("cancelled-session", deserialize=False)
        assert raw["runs"][0]["status"] == "COMPLETED"
        assert session.runs[0].input.input_content == "fixture input"
        assert any(message.role == "user" and message.content == "fixture input"
                   for message in session.get_messages())
        if partial:
            assert any(message.role == "assistant" and message.content == "partial work"
                       for message in session.get_messages())
        generate.assert_not_called()
    finally:
        store.close()
