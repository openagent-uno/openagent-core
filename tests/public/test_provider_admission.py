"""Physical account admission across independent PostgreSQL-backed native provider runtimes."""

import asyncio
import json
import os
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from openagent_core import (
    CredentialResolver,
    ExecutionContext,
    PrincipalRef,
    ProviderAccount,
    ProviderAdmission,
    ProviderRoute,
    RunRequest,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)


@pytest.mark.asyncio
async def test_credentials_concurrency_cancellation_and_shared_cooldown():
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from openagent_core.engine import NativeProvider
    from openagent_storage_postgres import PostgresRuntimeStore

    first_seen, disconnected = asyncio.Event(), asyncio.Event()
    inspected = []
    resolved = []
    accounts = (
        ProviderAccount("primary", "vault-primary", 1),
        ProviderAccount("secondary", "vault-secondary", 1),
    )
    schema = "oa_admit_" + uuid4().hex

    class Policy:
        async def authorize(self, context, action, resource, *, audience=()):
            return context.tenant_id == resource.tenant_id

    class Secrets:
        async def resolve(self, credential_ref, context):
            resolved.append((context.initiator.subject_id, credential_ref))
            return f"test-{context.initiator.subject_id}-{credential_ref}"

    class Select:
        async def select(self, context, model_ref):
            assert model_ref == "openai:test-model"
            return ProviderRoute("physical-endpoint", 1, endpoint, accounts, wait_seconds=5)

    class Execute:
        def __init__(self):
            self.model = NativeProvider(
                "openai:test-model", api_key="unused-static", base_url=inactive_endpoint
            )

        async def execute(self, request, context, runtime):
            messages = [{"role": "user", "content": request.input}]
            if request.input == "hold":
                return "".join(
                    [
                        part
                        async for part in self.model.stream(
                            messages, system="Assist.", session_id=request.session_id
                        )
                    ]
                )
            answer = await self.model.generate(
                messages, system="Assist.", session_id=request.session_id
            )
            return answer.content

    async def chat(request):
        payload = await request.json()
        text = payload["messages"][-1]["content"]
        inspected.append((text, request.headers["Authorization"], payload.get("stream", False)))
        if text == "hold":
            first_seen.set()
            try:
                async with asyncio.timeout(7):
                    while request.transport and not request.transport.is_closing():
                        await asyncio.sleep(0.025)
            except asyncio.CancelledError:
                disconnected.set()
                raise
            disconnected.set()
            return web.Response(status=204)
        if text == "busy":
            return web.json_response(
                {"error": {"message": "capacity", "type": "rate_limit", "code": "rate_limit"}},
                status=429,
                headers={"Retry-After": "30"},
            )
        return web.json_response(
            {
                "id": "completion",
                "object": "chat.completion",
                "model": "test-model",
                "created": 1,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "finished"},
                        "finish_reason": "stop",
                    }
                ],
            }
        )

    app = web.Application()
    app.router.add_post("/v1/chat/completions", chat)

    def context(user):
        p = PrincipalRef("auth", "tenant", user)
        return ExecutionContext(p, p, p, user, "agent", (p,))

    def request(key, user, text):
        return RunRequest(key, user, key, text)

    async with TestServer(app) as server:
        endpoint = str(server.make_url("/v1"))
        inactive_endpoint = str(server.make_url("/must-never-receive-credentials"))

        def make():
            store = PostgresRuntimeStore(dsn, agent_id="agent", schema=schema, lease_seconds=6)
            services = RuntimeServices(store, Execute(), Policy())
            services.registry.bind(ProviderAdmission, Select())
            services.registry.bind(CredentialResolver, Secrets())
            return Runtime(RuntimeSettings("agent", require_provider_admission=True), services)

        a, b = make(), make()
        await a.start()
        await b.start()
        try:
            await a.submit(request("one", "alice", "hold"), context("alice"))
            await asyncio.wait_for(first_seen.wait(), 5)
            await b.submit(request("two", "bob", "next"), context("bob"))
            await asyncio.sleep(0.25)
            assert resolved == [("alice", "vault-primary")]
            assert inspected == [("hold", "Bearer test-alice-vault-primary", True)]
            await b.cancel("one", context("alice"))
            async with asyncio.timeout(8):
                assert (await a.wait("one", context("alice"))).status == "cancelled"
                assert (await b.wait("two", context("bob"))).output == "finished"
                await disconnected.wait()
            assert inspected[-1] == ("next", "Bearer test-bob-vault-primary", False)
            await a.submit(request("three", "alice", "busy"), context("alice"))
            async with asyncio.timeout(7):
                assert (await a.wait("three", context("alice"))).status == "failed"
            await b.submit(request("four", "bob", "after"), context("bob"))
            async with asyncio.timeout(7):
                assert (await b.wait("four", context("bob"))).output == "finished"
            assert inspected[-1] == ("after", "Bearer test-bob-vault-secondary", False)
            assert resolved[-1] == ("bob", "vault-secondary")
            b.services.registry.unbind(ProviderAdmission)
            before = list(inspected)
            await b.submit(request("revoked", "bob", "denied"), context("bob"))
            async with asyncio.timeout(7):
                assert (await b.wait("revoked", context("bob"))).status == "failed"
            assert inspected == before
            async with b.services.store._db() as db:
                counts = await (
                    await db.execute("SELECT count(*) AS n FROM provider_attempts")
                ).fetchone()
                assert counts["n"] == 0
                health = await (
                    await db.execute(
                        "SELECT cooldown_until>clock_timestamp() AS cooling FROM provider_accounts WHERE account='primary'"
                    )
                ).fetchone()
                assert health["cooling"]
                payloads = await (
                    await db.execute("SELECT request,context,output FROM runs")
                ).fetchall()
                assert "Bearer test-" not in json.dumps(payloads)
                assert "vault-primary" not in json.dumps(payloads)
        finally:
            await a.close()
            await b.close()
            await a.services.executor.model.shutdown()
            await b.services.executor.model.shutdown()


@pytest.mark.asyncio
async def test_account_capacity_and_process_lease_recovery():
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from contextlib import AsyncExitStack
    from dataclasses import replace
    from openagent_storage_postgres import PostgresRuntimeStore

    schema = "oa_accounts_" + uuid4().hex
    first, second = (
        PostgresRuntimeStore(dsn, agent_id="agent", schema=schema, lease_seconds=9)
        for _ in range(2)
    )
    await first.start()
    await second.start()
    p = PrincipalRef("host", "t", "actor")
    ctx = ExecutionContext(p, p, p, "s", "agent", (p,))
    a, b, c = (ProviderAccount(name, "unused", 1) for name in ("a", "b", "c"))
    route = ProviderRoute(
        "physical", 2, "https://provider.example.invalid", (a,), wait_seconds=0.15
    )
    try:
        for store, key in ((first, "r1"), (second, "r2")):
            own = replace(ctx, session_id=key)
            await store.accept(RunRequest(key, key, key, "text"), own)
            await store.transition(key, "running")
        one, two = (replace(ctx, session_id=id) for id in ("r1", "r2"))
        other = PrincipalRef("host", "another-tenant", "actor")
        three = ExecutionContext(other, other, other, "r3", "agent", (other,))
        await second.accept(RunRequest("r3", "r3", "r3", "test"), three)
        await second.transition("r3", "running")
        limited = replace(route, provider_id="per-tenant", capacity=3, tenant_capacity=1)
        async with first.provider_slot(one, "r1", limited):
            with pytest.raises(TimeoutError):
                async with second.provider_slot(two, "r2", replace(limited, accounts=(b,))):
                    pytest.fail("tenant quota bypass")
            async with second.provider_slot(
                three, "r3", replace(limited, accounts=(b,))
            ) as eligible:
                assert eligible == b
        async with AsyncExitStack() as held:
            await held.enter_async_context(first.provider_slot(one, "r1", route))
            # Account a is saturated while provider capacity is still available.
            with pytest.raises(TimeoutError):
                async with second.provider_slot(two, "r2", route):
                    pytest.fail("account quota bypass")
            await held.enter_async_context(
                second.provider_slot(two, "r2", replace(route, accounts=(b,)))
            )
            # Account c is free but the physical endpoint has two active calls.
            with pytest.raises(TimeoutError):
                async with second.provider_slot(two, "r2", replace(route, accounts=(c,))):
                    pytest.fail("provider quota bypass")
            async with second._db() as db:
                await db.execute(
                    "UPDATE workers SET expires_at=clock_timestamp() WHERE id=%s", (first._worker,)
                )
            await second.recover()
            assert (await second.get("r1")).status == "interrupted"
            async with second.provider_slot(two, "r2", route) as granted:
                assert granted == a
            with pytest.raises(RuntimeError):
                await first.check_provider_slot(one, "r1", route.provider_id, a.account_id)
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["anthropic", "gemini", "groq", "responses"])
async def test_managed_sdk_credentials_on_the_wire(driver, monkeypatch):
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from openagent_storage_postgres import PostgresRuntimeStore
    from openagent_core.models.providers.message import Message
    from openagent_core.models.providers.anthropic.claude import Claude
    from openagent_core.models.providers.google.gemini import Gemini
    from openagent_core.models.providers.groq.groq import Groq
    from openagent_core.models.providers.openai.responses import OpenAIResponses

    # Poison ambient credentials; none must reach the fixture service.
    for key in (
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_API_KEY",
        "GOOGLE_API_KEY",
        "OPENAI_API_KEY",
        "GROQ_API_KEY",
    ):
        monkeypatch.setenv(key, "unused-static")
    inspected = []

    async def handler(request):
        headers = dict((key.lower(), value) for key, value in request.headers.items())
        inspected.append((request.path, headers))
        if driver == "anthropic":
            return web.json_response(
                {
                    "id": "msg_fixture",
                    "type": "message",
                    "role": "assistant",
                    "model": "test-model",
                    "content": [{"type": "text", "text": "ok"}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                }
            )
        if driver == "gemini":
            return web.json_response(
                {
                    "candidates": [
                        {
                            "content": {"role": "model", "parts": [{"text": "ok"}]},
                            "finishReason": "STOP",
                            "index": 0,
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 1,
                        "candidatesTokenCount": 1,
                        "totalTokenCount": 2,
                    },
                }
            )
        if driver == "responses":
            return web.json_response(
                {
                    "id": "resp_fixture",
                    "object": "response",
                    "created_at": 1,
                    "model": "test-model",
                    "status": "completed",
                    "output": [
                        {
                            "type": "message",
                            "id": "msg_x",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{"type": "output_text", "text": "ok", "annotations": []}],
                        }
                    ],
                    "parallel_tool_calls": False,
                    "tools": [],
                    "tool_choice": "auto",
                    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                }
            )
        return web.json_response(
            {
                "id": "complete",
                "created": 1,
                "model": "test-model",
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        )

    app = web.Application()
    app.router.add_post("/{path:.*}", handler)
    model = {"anthropic": Claude, "gemini": Gemini, "groq": Groq, "responses": OpenAIResponses}[
        driver
    ](id="test-model")
    model._openagent_runtime_id = driver + ":test-model"
    model.api_key = "unused-static"

    class Execute:
        async def execute(self, request, ctx, runtime):
            reply = await model.aresponse(messages=[Message(role="user", content="hi")])
            return reply.content

    class Services:
        async def select(self, ctx, ref):
            assert ref == model._openagent_runtime_id
            return ProviderRoute(driver, 1, endpoint, (ProviderAccount(driver, "secret-ref", 1),))

        async def resolve(self, ref, ctx):
            return "wire-" + driver

        async def authorize(self, ctx, action, resource, **options):
            return True

    async with TestServer(app) as server:
        endpoint = str(server.make_url("/"))
        service = Services()
        ledger = PostgresRuntimeStore(dsn, agent_id="agent", schema="oa_sdk_" + uuid4().hex)
        services = RuntimeServices(ledger, Execute(), service)
        services.registry.bind(ProviderAdmission, service)
        services.registry.bind(CredentialResolver, service)
        runtime = Runtime(RuntimeSettings("agent", require_provider_admission=True), services)
        principal = PrincipalRef("host", "t", "actor")
        ctx = ExecutionContext(principal, principal, principal, "s", "agent", (principal,))
        await runtime.start()
        try:
            await runtime.submit(RunRequest("r", "s", "r", "hi"), ctx)
            async with asyncio.timeout(12):
                result = await runtime.wait("r", ctx)
            assert result.status == "success", result
            assert result.output == "ok"
            assert len(inspected) == 1
            path, headers = inspected[0]
            assert "unused-static" not in str(headers)
            if driver == "anthropic":
                assert (
                    headers.get("x-api-key") == "wire-anthropic"
                    or headers.get("authorization") == "Bearer wire-anthropic"
                )
            elif driver == "gemini":
                assert headers["x-goog-api-key"] == "wire-gemini"
            else:
                assert headers["authorization"] == "Bearer wire-" + driver
            assert model.api_key == "unused-static" and model.client is None
        finally:
            await runtime.close()


@pytest.mark.asyncio
async def test_probe_receipt_is_redacted():
    dsn = os.environ.get("OPENAGENT_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    from pathlib import Path
    import sys

    received = []

    async def response(request):
        payload = await request.json()
        received.append((request.headers["Authorization"], payload["max_completion_tokens"]))
        text = payload["messages"][-1]["content"].split()[-1]
        return web.json_response(
            {
                "id": "ok",
                "model": "test",
                "created": 1,
                "object": "chat.completion",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": text},
                    }
                ],
            }
        )

    app = web.Application()
    app.router.add_post("/v1/chat/completions", response)
    async with TestServer(app) as server:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).resolve().parents[2] / "examples/provider-admission-probe.py"),
            "--model",
            "test",
            "--endpoint",
            str(server.make_url("/v1")),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                "OPENAGENT_PROBE_API_KEY": "probe-fixture-only",
                "PYTHONPATH": os.pathsep.join(sys.path),
            },
        )
        output, err = await asyncio.wait_for(process.communicate(), 20)
    assert process.returncode == 0, output
    receipt = json.loads(output)
    assert set(receipt) == {"passed", "status", "duration_ms"} and receipt["passed"]
    assert b"probe-fixture-only" not in output + err
    assert received == [("Bearer probe-fixture-only", 64)]
