"""Registered proxy models delegate through the real engine and host catalog.

Only the low-level provider model construction/response boundary is scripted.
Core.Agent, AgentExecutor, AgentModelCatalog, ModelDispatcher, Team's tool loop,
Runtime admission and both SQLite projections execute normally. No transport,
credential, paid model service, external process or consumer product is used.
This is a deterministic integration fixture, not live provider acceptance.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from openagent_core import (
    ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices,
    RuntimeSettings,
)
from openagent_core.administration import ManagementContext
from openagent_core.engine import Agent, AgentExecutor, AgentModelCatalog, MemoryDB
from openagent_core.mcp.pool import MCPPool
from openagent_core.models.dispatcher import ModelDispatcher
from openagent_core.models.native_provider import NativeProvider
from openagent_core.models.providers.base import Model
from openagent_core.models.providers.response import ModelResponse
from openagent_core.runtime import current_execution_context, current_run_id, current_runtime
from openagent_core.workspace_administration import SessionInspection
from openagent_storage_sqlite import SqliteRuntimeStore


PROVIDER = "subscription_codex"
LEADER_REF = PROVIDER + ":fixture-router"
MEMBER_REF = PROVIDER + ":fixture-model:high"
MISSION = "Return the deterministic specialist fixture answer."
ANSWER = "Specialist fixture answer."


class HostPolicy:
    def __init__(self):
        self.calls = []
        self.denied_model_refs = set()

    async def authorize(self, context, action, resource, *, audience=()):
        self.calls.append((context, action, resource, audience))
        return (resource.tenant_id == context.tenant_id
                and not (action == "model.use" and resource.resource_id in self.denied_model_refs))


class ScriptedProviderIO:
    """One scripted tool-call decision per *actual admitted* parent run."""
    def __init__(self, case):
        self.case = case
        self.constructed = []
        self.calls = []
        self.delegated = set()
        self.member_ids = []

    def construct(self, provider):
        # This is the sole patched boundary. Preserve the actual NativeProvider
        # ref and parsing instead of inventing a separate override object.
        ref = provider.model
        self.case.assertIn(ref, {LEADER_REF, MEMBER_REF})
        self.case.assertEqual(provider._runtime_parts(), tuple(ref.split(":", 1)))
        self.constructed.append(ref)
        return ProviderModel(ref, self)

    def response(self, ref, tools):
        context, runtime, run_id = current_execution_context(), current_runtime(), current_run_id()
        self.case.assertIsNotNone(context)
        self.case.assertIsNotNone(runtime)
        self.case.assertIsNotNone(run_id)
        self.calls.append((ref, run_id, context, runtime))
        if context.parent_run_id is not None:
            self.case.assertEqual(ref, MEMBER_REF, "Child used a fallback instead of its exact registered pin")
            return ModelResponse(role="assistant", content=ANSWER)

        self.case.assertEqual(ref, LEADER_REF)
        if run_id in self.delegated:
            return ModelResponse(role="assistant", content="The delegated fixture task is complete.")

        # Inspect the real, already constructed Team to choose the same member
        # ID a provider sees. Underscores/colons in canonical refs must not be
        # reverse-engineered from the tool-facing normalized identifier.
        router = runtime.services.executor.agent.model._team_providers[LEADER_REF]
        team = router._session_runtime[context.session_id]
        from openagent_core.core._runner.utils.team import get_member_id
        selected = [member for member in team.members
                    if team._runtime_member_model_refs.get(id(member)) == MEMBER_REF]
        self.case.assertEqual(len(selected), 1)
        member = selected[0]
        member_id = get_member_id(member)
        self.case.assertIsInstance(member_id, str)
        self.case.assertNotEqual(member_id, MEMBER_REF)
        self.case.assertNotIn(":", member_id)
        self.case.assertNotIn("_", member_id)
        self.case.assertEqual(team._runtime_member_model_refs[id(member)], MEMBER_REF)
        names = {tool.get("function", tool).get("name") for tool in tools or []}
        self.case.assertIn("delegate_task_to_member", names)
        self.member_ids.append(member_id)
        self.delegated.add(run_id)
        return ModelResponse(role="assistant", tool_calls=[{
            "id": "delegate-" + run_id,
            "type": "function",
            "function": {"name": "delegate_task_to_member", "arguments": json.dumps({
                "member_id": member_id, "task": MISSION,
            })},
        }])


class ProviderModel(Model):
    """Provider-response stub; the base Model still executes the real tool loop."""
    def __init__(self, ref, script):
        super().__init__(id=ref.split(":", 1)[1], provider=PROVIDER)
        self.runtime_ref = ref
        self.script = script

    def __deepcopy__(self, memo):
        copied = type(self)(self.runtime_ref, self.script)
        memo[id(self)] = copied
        return copied

    def invoke(self, *args, **kwargs):
        raise AssertionError("Unexpected synchronous provider IO")

    def invoke_stream(self, *args, **kwargs):
        raise AssertionError("Unexpected synchronous provider stream")

    async def ainvoke(self, *args, **kwargs):
        return self.script.response(self.runtime_ref, kwargs.get("tools"))

    async def ainvoke_stream(self, *args, **kwargs):
        yield self.script.response(self.runtime_ref, kwargs.get("tools"))

    def _parse_provider_response(self, response, **kwargs):
        assert isinstance(response, ModelResponse)
        return response

    def _parse_provider_response_delta(self, response):
        assert isinstance(response, ModelResponse)
        return response


class TeamModelCatalogDelegation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / "state.sqlite"
        self.policy = HostPolicy()
        self.io = ScriptedProviderIO(self)
        self.enterContext(patch.object(
            NativeProvider, "build_runtime_model", autospec=True, side_effect=self.io.construct,
        ))
        actor = PrincipalRef("test-host", "host-tenant", "fixture-owner")
        self.context = ExecutionContext(actor, actor, actor, "host-parent", "host-agent", (actor,),
                                        scopes=("fixture:read", "fixture:delegate"))
        self.management = ManagementContext(actor, "host-agent")
        self.active = await self.open_runtime(register=True)

    async def open_runtime(self, *, register=False):
        db = MemoryDB(str(self.path))
        await db.connect()
        self.addAsyncCleanup(db.close)
        if register:
            provider_id = await db.upsert_provider(
                name=PROVIDER, framework="api-based", enabled=True,
                base_url="https://proxy.example.invalid/v1", api_key=None,
            )
            await db.upsert_model(provider_id=provider_id, model="fixture-router", enabled=True,
                                  is_classifier=True, tier_hint="Coordinate the fixture task.")
            # A colon-bearing registered model is an explicit runtime ref in
            # the existing catalog contract, not a bare model suffix. Preserve
            # its provider prefix instead of registering "fixture-model:high"
            # as though "fixture-model" were the provider.
            await db.upsert_model(provider_id=provider_id, model=MEMBER_REF, enabled=True,
                                  tier_hint="Return the specialist fixture answer.")
        registered = await db.get_model_by_runtime_id(MEMBER_REF)
        self.assertIsNotNone(registered)
        self.assertEqual(registered["provider_name"], PROVIDER)
        self.assertEqual(registered["model"], MEMBER_REF)
        self.assertEqual(registered["runtime_id"], MEMBER_REF)
        config = await db.materialise_providers_config(enabled_only=True)
        self.assertEqual([provider["name"] for provider in config], [PROVIDER])
        self.assertEqual({model["model"] for model in config[0]["models"]},
                         {"fixture-router", MEMBER_REF})
        dispatcher = ModelDispatcher(config)
        self.assertEqual({model.runtime_id for model in dispatcher._configured_enabled_catalog()},
                         {LEADER_REF, MEMBER_REF})
        agent = Agent(name="Host fixture agent", model=dispatcher, memory=db,
                      mcp_pool=MCPPool.from_config([], include_defaults=False),
                      system_prompt="Complete only the deterministic fixture task.",
                      config={"_enabled_prompt_modules": []})
        executor = AgentExecutor(agent)
        store = SqliteRuntimeStore(self.path)
        models = AgentModelCatalog(agent, self.policy)
        runtime = Runtime(
            RuntimeSettings("host-agent", self.root),
            RuntimeServices(store, executor, self.policy, models=models), modules=(executor,),
        )
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        return SimpleNamespace(db=db, store=store, agent=agent, dispatcher=dispatcher,
                               models=models, runtime=runtime,
                               inspection=SessionInspection(db, self.policy))

    def request(self, run_id="parent-run", model_ref=LEADER_REF):
        return RunRequest(run_id, self.context.session_id, run_id, "Delegate the fixture task.", model_ref=model_ref)

    async def run_turn(self, active=None, run_id="parent-run"):
        active = active or self.active
        await active.runtime.submit(self.request(run_id), self.context)
        return await asyncio.wait_for(active.runtime.wait(run_id, self.context), 10)

    async def assert_host_only_namespace(self, active, *, session_count):
        conn = await active.db._ensure_connected()
        self.assertEqual((await (await conn.execute("SELECT count(*) FROM network")).fetchone())[0], 0)
        rows = await (await conn.execute("SELECT id,tenant_id,agent_id,parent_session_id FROM sessions_v2")).fetchall()
        self.assertEqual(len(rows), session_count)
        self.assertEqual({row["tenant_id"] for row in rows}, {self.context.tenant_id})
        self.assertEqual({row["agent_id"] for row in rows}, {self.context.agent_id})
        self.assertEqual({row["id"] for row in rows if row["parent_session_id"] is None}, {self.context.session_id})

    async def assert_child(self, active, parent_id):
        children = await active.runtime.children(parent_id, self.context)
        self.assertEqual(len(children), 1)
        child = children[0]
        self.assertEqual(child.status, "success")
        self.assertEqual(child.output, ANSWER)
        self.assertNotEqual(child.session_id, self.context.session_id)
        self.assertEqual(child.parent_run_id, parent_id)
        accepted = await active.runtime.accepted_request(child.run_id, self.context)
        self.assertEqual(accepted.request.model_ref, MEMBER_REF)
        self.assertEqual(accepted.request.input, MISSION)
        self.assertEqual(accepted.author, PrincipalRef("test-host", "host-tenant", "host-agent", "agent"))
        self.assertEqual(accepted.authority, self.context.authority)
        self.assertEqual(accepted.audience, self.context.audience)
        self.assertEqual(accepted.scopes, self.context.scopes)
        child_io = [call for call in self.io.calls if call[1] == child.run_id]
        self.assertTrue(child_io)
        self.assertEqual({call[0] for call in child_io}, {MEMBER_REF})
        self.assertTrue(all(call[2].tenant_id == "host-tenant" and call[3] is active.runtime for call in child_io))
        uses = [(context, resource) for context, action, resource, _ in self.policy.calls
                if action == "model.use" and resource.resource_id == MEMBER_REF]
        self.assertTrue(uses)
        self.assertTrue(all(context.author.kind == "agent" and resource.tenant_id == "host-tenant"
                            for context, resource in uses))
        events = [event for event in await active.runtime.events(parent_id, 0, self.context)
                  if event.kind == "run.child"]
        self.assertEqual([event.payload for event in events],
                         [{"child_run_id": child.run_id, "child_session_id": child.session_id}])
        transcript = await active.inspection.transcript(self.management, child.session_id)
        missions = [message for message in transcript["messages"] if message.get("role") == "user"]
        self.assertEqual([message["text"] for message in missions], [MISSION])
        self.assertEqual(missions[0]["author"]["kind"], "agent")
        parent = await active.inspection.transcript(self.management, self.context.session_id)
        cards = [message["toolInfo"] for message in parent["messages"]
                 if isinstance(message.get("toolInfo"), dict)
                 and message["toolInfo"].get("child_run_id") == child.run_id]
        self.assertTrue(cards, "Real provider tool result must link the canonical child")
        self.assertTrue(all(card["child_session_id"] == child.session_id for card in cards))
        return child

    async def test_registered_proxy_delegates_in_fresh_database_and_survives_restart_then_delegates_again(self):
        self.assertEqual((await self.run_turn()).status, "success")
        first = await self.assert_child(self.active, "parent-run")
        await self.assert_host_only_namespace(self.active, session_count=2)
        self.assertTrue(self.io.member_ids)
        self.assertEqual(len(set(self.io.member_ids)), 1)
        self.assertNotEqual(self.io.member_ids[0], MEMBER_REF)
        before_calls = list(self.io.calls)
        before_events = await self.active.runtime.events("parent-run", 0, self.context)
        await self.active.runtime.close()

        # Reconstruct the real Agent, dispatcher and catalog with the same
        # authenticated host identity and the already registered SQLite rows.
        reopened = await self.open_runtime()
        self.assertIsNot(reopened.agent, self.active.agent)
        self.assertEqual((await reopened.runtime.submit(self.request(), self.context)).status, "success")
        self.assertEqual(self.io.calls, before_calls, "Durable replay must not invoke any provider again")
        self.assertEqual(await reopened.runtime.events("parent-run", 0, self.context), before_events)
        self.assertEqual(await reopened.runtime.children("parent-run", self.context), (first,))
        replay = await reopened.inspection.transcript(self.management, first.session_id)
        self.assertIn(MISSION, [message["text"] for message in replay["messages"]])
        self.assertEqual(self.io.calls, before_calls)

        self.assertEqual((await self.run_turn(reopened, "next-parent-run")).status, "success")
        second = await self.assert_child(reopened, "next-parent-run")
        self.assertNotEqual((second.run_id, second.session_id), (first.run_id, first.session_id))
        await self.assert_host_only_namespace(reopened, session_count=3)

    async def test_host_model_use_denial_prevents_child_provider_io_without_selecting_another_model(self):
        self.policy.denied_model_refs.add(MEMBER_REF)
        await self.run_turn()
        children = await self.active.runtime.children("parent-run", self.context)
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0].status, "failed")
        self.assertEqual(children[0].output, {"error_type": "AuthorizationDenied"})
        self.assertFalse([call for call in self.io.calls if call[2].parent_run_id is not None])
        accepted = await self.active.runtime.accepted_request(children[0].run_id, self.context)
        self.assertEqual(accepted.request.model_ref, MEMBER_REF)
        self.assertNotIn(ANSWER, str(children[0].output))
        await self.assert_host_only_namespace(self.active, session_count=2)

    async def test_unregistered_exact_pin_fails_before_provider_construction_or_inference(self):
        missing = MEMBER_REF + ":unregistered"
        request = self.request("missing-pin-run", model_ref=missing)
        await self.active.runtime.submit(request, self.context)
        record = await asyncio.wait_for(self.active.runtime.wait(request.run_id, self.context), 10)
        self.assertEqual(record.status, "failed")
        self.assertEqual(self.io.constructed, [])
        self.assertEqual(self.io.calls, [])
        self.assertEqual(await self.active.runtime.children(request.run_id, self.context), ())
        self.assertTrue(any(action == "model.use" and resource.resource_id == missing
                            for _, action, resource, _ in self.policy.calls))
        await self.assert_host_only_namespace(self.active, session_count=1)


if __name__ == "__main__":
    unittest.main()
