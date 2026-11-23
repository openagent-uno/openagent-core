"""Exercise the installed native model runner against a local HTTP provider and PostgreSQL."""

import asyncio
import base64
import hashlib
from contextlib import asynccontextmanager
import json
import os
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from openagent_core import ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_gateway import create_app


@pytest.mark.asyncio
async def test_external_provider_history_across_gateways(tmp_path, monkeypatch):
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from openagent_core.engine import Agent, AgentExecutor, NativeProvider
    from openagent_storage_postgres import PostgresRuntimeStore

    def forbidden(*args, **kwargs):
        raise AssertionError("external model attempted local storage")

    monkeypatch.setattr(NativeProvider, "_ensured_runtime_db_path", forbidden)
    monkeypatch.setattr(NativeProvider, "_runtime_db_path", forbidden)
    monkeypatch.chdir(tmp_path)

    arrived, release = asyncio.Event(), asyncio.Event()
    wires = []
    media_bytes = b"\x10\x05\x03\x80"
    uploaded, resolved = {}, []

    async def respond(request):
        payload = await request.json()
        messages = payload["messages"]
        latest = messages[-1]["content"]
        text = next(
            value
            for value in (
                "turn-one",
                "turn-two",
                "turn-three",
                "private-user",
                "binary-stream",
                "binary-completion",
                "binary-revoked",
            )
            if value in str(latest)
        )
        wires.append((text, payload))
        if text == "turn-one":
            arrived.set()
            await release.wait()
        content = f"answer to {text}"
        audio = (
            {
                "audio": {
                    "id": "provider-audio",
                    "data": base64.b64encode(media_bytes).decode(),
                    "transcript": content,
                    "expires_at": 9999999999,
                }
            }
            if text.startswith("binary-")
            else {}
        )
        base = {"id": "test-id", "model": "test-model", "created": 1}
        if not payload.get("stream"):
            return web.json_response(
                {
                    **base,
                    "object": "chat.completion",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": content, **audio},
                            "finish_reason": "stop",
                        }
                    ],
                }
            )
        frames = [
            {
                **base,
                "object": "chat.completion.chunk",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "content": content, **audio},
                        "finish_reason": None,
                    }
                ],
            },
            {
                **base,
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            },
        ]
        sse = "".join(f"data: {json.dumps(frame)}\n\n" for frame in frames) + "data: [DONE]\n\n"
        return web.Response(text=sse, content_type="text/event-stream")

    from openagent_core import ProviderHistory, ProviderHistoryMessage

    revoked_sessions = set()
    revoked_artifacts = set()

    class Policy:
        async def authorize(self, context, action, resource, *, audience=()):
            return context.tenant_id == resource.tenant_id and not (
                (action == "session.read" and context.session_id in revoked_sessions)
                or (action == "artifact.read" and context.session_id in revoked_artifacts)
            )

    @asynccontextmanager
    async def resolve(request, action, run_id, session_id):
        principal = PrincipalRef("host", "tenant", request.headers.get("X-Test-User", "alice"))
        yield ExecutionContext(
            principal, principal, principal, session_id or "s", "agent", (principal,)
        )

    class Publisher:
        async def publish_content(self, context, content, *, filename, mime_type):
            from openagent_core.runtime import current_run_id

            key = current_run_id()
            assert key not in uploaded
            ref = {
                "artifact_id": "art_" + uuid4().hex,
                "artifact_link_id": "link-test",
                "kind": "voice",
                "filename": filename,
                "mime_type": mime_type,
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            uploaded[key] = (context.session_id, content, ref)
            return ref

        async def resolve_session_output(self, context, run_id, artifact_id):
            session_id, _, ref = uploaded[run_id]
            assert session_id == context.session_id and artifact_id == ref["artifact_id"]
            resolved.append(run_id)
            if run_id == "binary-revoked":
                revoked_artifacts.add(context.session_id)
            return {**ref, "path": "private-host-path"}

    class History:
        def __init__(self, store):
            self.store = store

        async def provider_history(self, context, run_id, **budget):
            messages = await self.store.provider_history(context, run_id, **budget)
            if run_id == "revoked":
                revoked_sessions.add(context.session_id)
            if run_id == "oversized":
                return (ProviderHistoryMessage("assistant", "x" * (budget["max_bytes"] + 1)),)
            return messages

    class GenerateExecutor(AgentExecutor):
        async def start(self, runtime):
            from openagent_core.mcp._runtime.toolkit import Toolkit

            await super().start(runtime)
            self.agent.model.set_mcp_toolkits([Toolkit(name="catalog"), Toolkit(name="calendar")])

        async def _execute(self, request, context, runtime):
            result = await self.agent.model.generate(
                [{"role": "user", "content": request.input}],
                system="Assist.",
                session_id=request.session_id,
            )
            return result.content

    app = web.Application()
    app.router.add_post("/v1/chat/completions", respond)
    schema = "oa_model_" + uuid4().hex
    async with TestServer(app) as provider:

        def gateway(executor_type):
            model = NativeProvider(
                "openai:test-model", api_key="offline-test", base_url=str(provider.make_url("/v1"))
            )
            agent = Agent(name="agent", model=model, config={"skills": {"enabled": False}})
            executor = executor_type(agent)
            runtime = Runtime(
                RuntimeSettings("agent"),
                RuntimeServices(
                    PostgresRuntimeStore(dsn, agent_id="agent", schema=schema), executor, Policy()
                ),
                modules=(executor,),
            )
            runtime.services.registry.bind("attachments.publisher", Publisher())
            if executor_type is GenerateExecutor:
                runtime.services.registry.bind(ProviderHistory, History(runtime.services.store))
            return TestClient(TestServer(create_app(runtime, resolve)))

        async with gateway(AgentExecutor) as left, gateway(GenerateExecutor) as right:

            async def submit(client, text, session="s", user="alice"):
                response = await client.post(
                    "/api/v1/runs",
                    json={
                        "run_id": text,
                        "idempotency_key": text,
                        "session_id": session,
                        "input": text,
                    },
                    headers={"X-Test-User": user},
                )
                assert response.status == 202, await response.text()

            async def result(client, text, user="alice", status="success"):
                async with asyncio.timeout(20):
                    while True:
                        response = await client.get(
                            "/api/v1/runs/" + text, headers={"X-Test-User": user}
                        )
                        assert response.status == 200, await response.text()
                        value = await response.json()
                        if value["status"] not in {"queued", "running"}:
                            assert value["status"] == status, value
                            return value["output"]
                        await asyncio.sleep(0.02)

            await submit(left, "turn-one")
            pending = asyncio.create_task(result(left, "turn-one"))
            arrival = asyncio.create_task(arrived.wait())
            done, _ = await asyncio.wait(
                {pending, arrival}, timeout=20, return_when=asyncio.FIRST_COMPLETED
            )
            if pending in done:
                await pending
            assert arrival in done
            await submit(right, "turn-two")
            await submit(left, "turn-three")
            release.set()
            assert await pending == "answer to turn-one"
            assert await result(right, "turn-two") == "answer to turn-two"
            assert await result(left, "turn-three") == "answer to turn-three"
            await submit(right, "private-user", "private", "bob")
            assert await result(right, "private-user", "bob") == "answer to private-user"
            for case in ("revoked", "oversized"):
                await submit(right, case, case)
                await result(right, case, status="failed")
            await submit(left, "binary-stream", "media")
            assert await result(left, "binary-stream") == "answer to binary-stream"
            events_response = await left.get("/api/v1/runs/binary-stream/events")
            event_body = await events_response.json()
            assert "private-host-path" not in str(event_body)
            assert uploaded["binary-stream"][2]["artifact_id"] in str(event_body)
            final_response = await left.get("/api/v1/runs/binary-stream/final-answer")
            assert final_response.status == 200, await final_response.text()
            final = (await final_response.json())["final_answer"]
            assert [ref["artifact_id"] for ref in final["attachments"]] == [
                uploaded["binary-stream"][2]["artifact_id"]
            ]
            assert "private-host-path" not in str(final)
            assert "native_audio_sha256" not in str(final)
            await submit(right, "binary-completion", "other-media")
            answer = await result(right, "binary-completion")
            assert uploaded["binary-completion"][2]["artifact_id"] in answer
            await submit(left, "binary-revoked", "revoked-media")
            assert "unavailable" in await result(left, "binary-revoked")
            final_response = await left.get("/api/v1/runs/binary-revoked/final-answer")
            assert not (await final_response.json())["final_answer"]["attachments"]
    turns = {key: body for key, body in wires}
    assert len(wires) == 7
    assert resolved == ["binary-stream", "binary-revoked"]
    assert {key: item[1] for key, item in uploaded.items()} == {
        "binary-stream": media_bytes,
        "binary-completion": media_bytes,
        "binary-revoked": media_bytes,
    }
    assert not turns["turn-two"].get("stream") and turns["turn-three"]["stream"]
    assert any("member" in tool["function"]["name"] for tool in turns["turn-two"]["tools"])

    def conversation(key):
        return [
            (message["role"], message["content"])
            for message in turns[key]["messages"]
            if message["role"] in {"assistant", "user"}
        ]

    assert conversation("turn-two")[:-1] == [
        ("user", "turn-one"),
        ("assistant", "answer to turn-one"),
    ]
    assert conversation("turn-three")[:-1] == [
        ("user", "turn-one"),
        ("assistant", "answer to turn-one"),
        ("user", "turn-two"),
        ("assistant", "answer to turn-two"),
    ]
    assert len(conversation("private-user")) == 1
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_narrow_turns_in_shared_session_are_filtered_from_all_public_reads():
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from dataclasses import replace
    from openagent_core import RunRequest, SessionRef
    from openagent_storage_postgres import PostgresRuntimeStore

    store = PostgresRuntimeStore(dsn, agent_id="agent", schema="oa_acl_" + uuid4().hex)
    alice, bob = (PrincipalRef("host", "tenant", name) for name in ("alice", "bob"))
    shared = ExecutionContext(alice, alice, alice, "s", "agent", (alice, bob))
    private = replace(shared, audience=(alice,))
    viewer = ExecutionContext(bob, bob, bob, "s", "agent", (bob,))
    session = SessionRef("host", "tenant", "agent", "s")
    await store.start()
    try:
        for key, ctx in (("shared", shared), ("narrow", private), ("later", shared)):
            req = RunRequest(key, "s", key, key)
            await store.accept(req, ctx)
            await store.transition(key, "running")
            if key == "later":
                assert [
                    m.text
                    for m in await store.provider_history(ctx, key, limit=128, max_bytes=65536)
                ] == ["shared", "response shared"]
                assert [
                    m.text for m in await store.provider_history(ctx, key, limit=128, max_bytes=15)
                ] == ["response shared"]
                assert not await store.provider_history(ctx, key, limit=1, max_bytes=1)
            await store.transition(key, "success", output="response " + key)
        await store.check_run_access(viewer, "shared")
        with pytest.raises(PermissionError):
            await store.check_run_access(viewer, "narrow")
        assert len((await store.read_session(session, viewer, limit=20))["messages"]) == 4
        assert len((await store.read_session(session, private, limit=20))["messages"]) == 6
        assert not (await store.search_sessions(viewer, query="narrow", limit=10))["sessions"]
    finally:
        await store.close()


def test_external_media_does_not_follow_file_or_url(tmp_path):
    from types import SimpleNamespace
    from openagent_core.models.native_provider import _output_media_bytes

    private = tmp_path / "private.dat"
    private.write_bytes(b"local bytes")
    assert _output_media_bytes(SimpleNamespace(filepath=private), allow_file=False) is None
    assert (
        _output_media_bytes(SimpleNamespace(url="https://example.invalid/test"), allow_file=False)
        is None
    )
