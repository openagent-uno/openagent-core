"""Exercise the installed native model runner against a local HTTP provider and PostgreSQL."""

import asyncio
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

    async def respond(request):
        payload = await request.json()
        messages = payload["messages"]
        latest = messages[-1]["content"]
        text = next(
            value
            for value in ("turn-one", "turn-two", "turn-three", "private-user")
            if value in str(latest)
        )
        wires.append((text, payload))
        if text == "turn-one":
            arrived.set()
            await release.wait()
        content = f"answer to {text}"
        base = {"id": "test-id", "model": "test-model", "created": 1}
        if not payload.get("stream"):
            return web.json_response(
                {
                    **base,
                    "object": "chat.completion",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": content},
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
                        "delta": {"role": "assistant", "content": content},
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

    class Policy:
        async def authorize(self, context, action, resource, *, audience=()):
            return context.tenant_id == resource.tenant_id and not (
                action == "session.read" and context.session_id in revoked_sessions
            )

    @asynccontextmanager
    async def resolve(request, action, run_id, session_id):
        principal = PrincipalRef("host", "tenant", request.headers.get("X-Test-User", "alice"))
        yield ExecutionContext(
            principal, principal, principal, session_id or "s", "agent", (principal,)
        )

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
    turns = {key: body for key, body in wires}
    assert len(wires) == 4
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
