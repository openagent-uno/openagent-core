"""The actual Team tool delegates through the host's durable Runtime boundary.

Executors stand in for provider IO only. Admission, authorization, child context,
SQLite lineage and the Team tool factory are real; legacy runners are tripwires.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core import (
    CapabilityLease, ExecutionContext, PrincipalRef, RunRequest,
    Runtime, RuntimeServices, RuntimeSettings,
)
from openagent_core.core._run_state import RunContext
from openagent_core.core._run_state.team import TeamRunOutput
from openagent_core.core._runner.agent import Agent
from openagent_core.core._runner.team.team import Team
from openagent_core.memory.sessions import TeamSession
from openagent_core.models.providers.response import ToolExecution
from openagent_core.runtime import (
    current_execution_context, current_run_id, current_runtime,
    execution_scope, provider_tool_call_scope,
)
from openagent_storage_sqlite import SqliteRuntimeStore


class HostPolicy:
    def __init__(self):
        self.denied = set()
        self.calls = []
        self.delegation_seen = asyncio.Queue()

    async def authorize(self, context, action, resource, *, audience=()):
        self.calls.append((context, action, resource, audience))
        if action == "run.delegate":
            self.delegation_seen.put_nowait(context.session_id)
        return action not in self.denied and resource.tenant_id == context.tenant_id


class LegacyDatabase:
    """Conflicting installation/network metadata must not choose the tenant."""
    network_id = "legacy-network-not-the-host-tenant"
    installation_id = "legacy-installation-not-the-host-tenant"

    def __getattr__(self, name):
        raise AssertionError(f"Hosted delegation touched legacy database: {name}")


class TeamExecutor:
    def __init__(self, case):
        self.case = case
        self.children = []
        self.errors = []
        self.outputs = []
        self.responses = []
        self.sessions = []
        self.parent_hook = None
        self.child_mode = "success"
        self.child_started = asyncio.Event()
        self.release_children = asyncio.Event()
        self.expected_parallel_children = 0
        self.stream = False
        self.broadcast = False
        self.bind = True
        self.async_mode = True
        self.session_override = None
        self.context_override = None
        self.run_id_override = False
        self.response_run_id_override = None
        self.delegation_calls = None
        self.nested_children = 0

    async def execute(self, request, context, runtime):
        if context.parent_run_id is not None:
            self.children.append((request, context, current_runtime(), current_execution_context(), current_run_id()))
            self.child_started.set()
            if len(self.children) <= self.nested_children:
                return await self.case.invoke_team_tool(self, request, context, runtime)
            if self.expected_parallel_children:
                if len(self.children) == self.expected_parallel_children:
                    self.release_children.set()
                await self.release_children.wait()
            if self.child_mode == "hold":
                await self.release_children.wait()
            if self.child_mode == "failed":
                raise ValueError("provider sentinel must not become successful delegated content")
            return "child result: " + request.input

        if self.parent_hook is not None:
            await self.parent_hook(request, context, runtime)
        try:
            output = await self.case.invoke_team_tool(self, request, context, runtime)
            self.outputs.append(output)
            return output
        except BaseException as exc:
            self.errors.append(exc)
            raise


class TeamRuntimeDelegation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in ("_announce_member_child_session", "_persist_member_child_session", "_upsert_member_child_row"):
            self.enterContext(patch(
                "openagent_core.core._runner.team._default_tools." + name,
                new=AsyncMock(side_effect=AssertionError("Legacy child persistence was called")),
            ))
        self.enterContext(patch(
            "openagent_core.core._runner.team._init._initialize_member",
            side_effect=AssertionError("Legacy member initialization was called"),
        ))
        self.member = Agent(id="specialist", name="Display name is not a model ref")
        self.other_member = Agent(id="second", name="Second display name")
        for member in (self.member, self.other_member):
            member.arun = AsyncMock(side_effect=AssertionError("member.arun bypassed Runtime"))
            member.run = lambda *args, **kwargs: self.fail("member.run bypassed Runtime")
            member.db = LegacyDatabase()
        self.model_ref = "registered/provider 日本語:exact model ref with spaces"
        self.second_ref = "registered/second:exact-model"
        self.media = [SimpleNamespace(url="untrusted-provider-media", content=b"not an accepted attachment")]
        self.runtime, self.context, self.executor, self.policy = await self.make_runtime("one", "host-tenant")

    async def make_runtime(self, name, tenant):
        author = PrincipalRef("verified-host", tenant, "human-author")
        authority = PrincipalRef("verified-host", tenant, "credential-owner")
        initiator = PrincipalRef("verified-host", tenant, "initiating-device", "device")
        observer = PrincipalRef("verified-host", tenant, "result-reader")
        context = ExecutionContext(
            author, initiator, authority, "parent-session", "host-agent", (author, observer),
            scopes=("read:bounded", "delegate:registered"),
            capabilities=(CapabilityLease("host-source", "instance-" + name, "generation-7"),),
            ingress_id="verified-ingress-" + name,
        )
        policy = HostPolicy()
        executor = TeamExecutor(self)
        store = SqliteRuntimeStore(self.root / name / "state.sqlite")
        runtime = Runtime(RuntimeSettings("host-agent", self.root / name), RuntimeServices(store, executor, policy))
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        return runtime, context, executor, policy

    def request(self, context=None, run_id="parent"):
        context = context or self.context
        return RunRequest(
            run_id, context.session_id, run_id, "human parent input",
            attachments=({"artifact_id": "host-accepted", "media_type": "image/png", "revision": 3},),
        )

    async def invoke_team_tool(self, executor, request, context, runtime):
        members = [self.member, self.other_member] if executor.broadcast or executor.delegation_calls else [self.member]
        team = Team(members=members, id="leader", delegate_to_all_members=executor.broadcast)
        team.db = LegacyDatabase()
        # Bind object identity, never the display label, provider model object or caller args.
        team._runtime_member_model_refs = {
            id(self.member): self.model_ref, id(self.other_member): self.second_ref,
        } if executor.bind else {self.member.id: self.model_ref}
        sid = executor.session_override or context.session_id
        response = TeamRunOutput(
            run_id=executor.response_run_id_override or request.run_id, session_id=sid, team_id="leader",
            tools=[
                ToolExecution(tool_call_id="active-delegation", tool_name="delegate_task_to_member",
                              tool_args={"member_id": "specialist", "task": "bounded mission"}),
                ToolExecution(tool_call_id="other-delegation", tool_name="delegate_task_to_member",
                              tool_args={"member_id": "specialist", "task": "another invocation"}),
                ToolExecution(tool_call_id="second-delegation", tool_name="delegate_task_to_member",
                              tool_args={"member_id": "second", "task": "another member"}),
            ],
        )
        executor.responses.append(response)
        session = TeamSession(session_id=sid, team_id="leader", metadata={"client_id": "legacy-owner"})
        executor.sessions.append(session)
        function = team._get_delegate_task_function(
            run_response=response,
            run_context=RunContext(request.run_id, sid, session_state={}),
            session=session,
            team_run_context={}, async_mode=executor.async_mode, stream=executor.stream,
            images=self.media,
        )

        async def consume():
            if executor.delegation_calls is not None:
                parts = []
                for member_id, task, call_id in executor.delegation_calls:
                    with provider_tool_call_scope(call_id):
                        parts.extend([part async for part in function.entrypoint(member_id=member_id, task=task)])
                return parts
            with provider_tool_call_scope("active-delegation"):
                if not executor.async_mode:
                    return list(function.entrypoint(member_id="specialist", task="bounded mission"))
                kwargs = {"task": "bounded mission"}
                if not executor.broadcast:
                    kwargs["member_id"] = "specialist"
                return [part async for part in function.entrypoint(**kwargs)]

        if executor.run_id_override or executor.context_override is not None:
            with execution_scope(runtime, executor.context_override or context,
                                 None if executor.run_id_override else request.run_id):
                return await consume()
        return await consume()

    async def run_parent(self, *, runtime=None, context=None, run_id="parent"):
        runtime, context = runtime or self.runtime, context or self.context
        await runtime.submit(self.request(context, run_id), context)
        return await asyncio.wait_for(runtime.wait(run_id, context), 4)

    async def test_real_factory_preserves_host_context_canonical_model_attachments_and_exact_card(self):
        for stream in (False, True):
            with self.subTest(stream=stream):
                self.executor.stream = stream
                record = await self.run_parent(run_id="parent-" + str(stream))
                self.assertEqual(record.status, "success", self.executor.errors)
                request, child, active_runtime, active_context, active_run = self.executor.children[-1]
                self.assertIs(active_runtime, self.runtime)
                self.assertEqual(active_context, child)
                self.assertEqual(active_run, request.run_id)
                self.assertEqual(request.model_ref, self.model_ref)
                self.assertEqual(request.input, "bounded mission")
                self.assertEqual(request.attachments, self.request().attachments)
                self.assertNotIn("untrusted-provider-media", str(request.attachments))
                self.assertEqual(child.author, PrincipalRef("verified-host", "host-tenant", "host-agent", "agent"))
                self.assertEqual(child.authority, self.context.authority)
                self.assertEqual(child.initiator, self.context.initiator)
                self.assertEqual(child.scopes, self.context.scopes)
                self.assertEqual(child.audience, self.context.audience)
                self.assertEqual(child.capabilities, self.context.capabilities)
                self.assertEqual(child.ingress_id, self.context.ingress_id)
                self.assertEqual(child.parent_run_id, record.run_id)
                self.assertEqual(child.tenant_id, "host-tenant")
                self.assertNotEqual(child.session_id, self.context.session_id)
                card, unrelated, other_member_card = self.executor.responses[-1].tools
                self.assertEqual((card.child_session_id, card.child_run_id), (request.session_id, request.run_id))
                self.assertIsNone(unrelated.child_session_id)
                self.assertIsNone(unrelated.child_run_id)
                self.assertIsNone(other_member_card.child_session_id)
                self.assertIsNone(other_member_card.child_run_id)
                child_events = [event for event in await self.runtime.events(record.run_id, 0, self.context)
                                if event.kind == "run.child"]
                self.assertEqual([event.payload for event in child_events],
                                 [{"child_session_id": request.session_id, "child_run_id": request.run_id}])
                accepted = await self.runtime.accepted_request(request.run_id, child)
                self.assertEqual(accepted.author, child.author)
                self.assertEqual(accepted.request, request)
                row = self.runtime.services.store.connection.execute(
                    "SELECT parent_session_id, root_session_id FROM sessions_v2 WHERE id=?", (request.session_id,),
                ).fetchone()
                self.assertEqual(tuple(row), (self.context.session_id, self.context.session_id))
                self.assertTrue(any(action == "run.delegate" and audience == self.context.audience
                                    for _, action, _, audience in self.policy.calls))
        self.member.arun.assert_not_called()
        self.assertIsNone(current_runtime())
        self.assertIsNone(current_execution_context())

    async def test_sibling_cards_for_repeated_and_different_members_retain_exact_run_and_session(self):
        self.executor.delegation_calls = [
            ("specialist", "first mission", "active-delegation"),
            ("specialist", "second mission", "other-delegation"),
            ("second", "third mission", "second-delegation"),
        ]
        self.assertEqual((await self.run_parent()).status, "success", self.executor.errors)
        cards = self.executor.responses[0].tools
        requests = [entry[0] for entry in self.executor.children]
        self.assertEqual(len(requests), 3)
        self.assertEqual(len({request.session_id for request in requests}), 3)
        self.assertEqual(len({request.run_id for request in requests}), 3)
        for card, request in zip(cards, requests):
            self.assertEqual((card.child_session_id, card.child_run_id), (request.session_id, request.run_id))
        self.assertEqual([request.model_ref for request in requests], [self.model_ref, self.model_ref, self.second_ref])

    async def test_hosted_children_ignore_legacy_disable_member_sessions_flag(self):
        self.runtime.settings = replace(self.runtime.settings, environment=(("OPENAGENT_TEAM_MEMBER_SESSIONS", "0"),))
        self.assertEqual((await self.run_parent()).status, "success", self.executor.errors)
        child = (await self.runtime.children("parent", self.context))[0]
        self.assertNotEqual(child.session_id, self.context.session_id)
        self.assertEqual(len(self.executor.responses[0].member_responses), 1)
        self.assertFalse(self.executor.sessions[0].runs)
        self.member.arun.assert_not_called()

    async def test_long_parent_and_model_labels_produce_unique_bounded_opaque_child_ids(self):
        self.model_ref = "registered:模型 / " + "long label " * 500
        context = replace(self.context, session_id="根 session/" + "é" * 2000)
        for run_id in ("first", "second"):
            self.assertEqual((await self.run_parent(context=context, run_id=run_id)).status, "success")
        requests = [entry[0] for entry in self.executor.children]
        self.assertNotEqual(requests[0].session_id, requests[1].session_id)
        for request in requests:
            self.assertLessEqual(len(request.session_id.encode()), 128)
            self.assertRegex(request.session_id, re.compile(r"^[A-Za-z0-9_.:@-]+$"))
            self.assertEqual(request.model_ref, self.model_ref)

    async def test_denied_delegation_reaches_no_child_executor_or_legacy_runner(self):
        self.policy.denied.add("run.delegate")
        result = await self.run_parent()
        self.assertEqual(result.status, "failed")
        self.assertIsInstance(self.executor.errors[-1], PermissionError)
        self.assertEqual(self.executor.children, [])
        self.assertEqual(await self.runtime.children("parent", self.context), ())
        self.member.arun.assert_not_called()

    async def test_missing_object_identity_model_binding_never_falls_back_to_display_or_provider(self):
        self.executor.bind = False
        result = await self.run_parent()
        self.assertEqual(result.status, "failed")
        self.assertIsInstance(self.executor.errors[-1], PermissionError)
        self.assertEqual(self.executor.children, [])
        self.assertEqual(await self.runtime.children("parent", self.context), ())

    async def test_session_mismatch_missing_live_run_and_foreign_agent_are_fail_closed(self):
        variants = (
            ("session", {"session_override": "other-session"}),
            ("missing-run", {"run_id_override": True}),
            ("foreign-agent", {"context_override": replace(self.context, agent_id="foreign-agent")}),
        )
        for run_id, changes in variants:
            with self.subTest(run_id=run_id):
                self.executor.session_override = None
                self.executor.run_id_override = False
                self.executor.context_override = None
                for key, value in changes.items():
                    setattr(self.executor, key, value)
                self.assertEqual((await self.run_parent(run_id=run_id)).status, "failed")
                self.assertIsInstance(self.executor.errors[-1], PermissionError)
                self.assertEqual(await self.runtime.children(run_id, self.context), ())
        self.assertEqual(self.executor.children, [])

    async def test_synchronous_hosted_tool_cannot_use_legacy_runner(self):
        self.executor.async_mode = False
        self.assertEqual((await self.run_parent()).status, "failed")
        self.assertIsInstance(self.executor.errors[-1], PermissionError)
        self.assertEqual(self.executor.children, [])

    async def test_foreign_team_output_run_id_cannot_delegate_from_the_unchanged_live_parent(self):
        # Only the provider-facing output is forged. Runtime, accepted parent,
        # execution context and Team session retain their exact admitted IDs.
        self.executor.response_run_id_override = "another-provider-output-run"
        self.assertEqual((await self.run_parent()).status, "failed")
        self.assertIsInstance(self.executor.errors[-1], PermissionError)
        self.assertEqual(self.executor.responses[0].run_id, "another-provider-output-run")
        self.assertEqual(self.executor.sessions[0].session_id, self.context.session_id)
        self.assertEqual(self.executor.children, [])
        self.assertEqual(await self.runtime.children("parent", self.context), ())
        self.member.arun.assert_not_called()
        self.assertFalse(self.executor.responses[0].member_responses)
        self.assertTrue(all(card.child_run_id is None and card.child_session_id is None
                            for card in self.executor.responses[0].tools))

    async def test_failed_child_is_durable_failure_not_successful_delegation_text(self):
        self.executor.child_mode = "failed"
        self.assertEqual((await self.run_parent()).status, "failed")
        children = await self.runtime.children("parent", self.context)
        self.assertEqual([child.status for child in children], ["failed"])
        self.assertIsInstance(self.executor.errors[-1], RuntimeError)
        self.assertIn("failed", str(self.executor.errors[-1]))
        self.assertEqual(self.executor.outputs, [])

    async def test_cancelling_parent_cancels_real_child_and_retains_lineage(self):
        self.executor.child_mode = "hold"
        await self.runtime.submit(self.request(), self.context)
        await asyncio.wait_for(self.executor.child_started.wait(), 3)
        self.assertEqual((await self.runtime.cancel("parent", self.context)).status, "cancelled")
        children = await self.runtime.children("parent", self.context)
        self.assertEqual([child.status for child in children], ["cancelled"])
        self.assertEqual(children[0].parent_run_id, "parent")
        self.assertEqual(self.executor.outputs, [])

    async def test_broadcast_admits_two_real_children_without_shared_session_or_authority_widening(self):
        self.executor.broadcast = True
        self.executor.stream = True
        self.executor.expected_parallel_children = 2
        self.assertEqual((await self.run_parent()).status, "success", self.executor.errors)
        children = await self.runtime.children("parent", self.context)
        self.assertEqual(len(children), 2)
        self.assertEqual(len({child.session_id for child in children}), 2)
        self.assertEqual({entry[0].model_ref for entry in self.executor.children}, {self.model_ref, self.second_ref})
        for request, context, runtime, active_context, _ in self.executor.children:
            self.assertIs(runtime, self.runtime)
            self.assertEqual(active_context, context)
            self.assertEqual(context.capabilities, self.context.capabilities)
            self.assertEqual(context.authority, self.context.authority)
            self.assertEqual(request.attachments, self.request().attachments)

    async def test_two_simultaneous_runtimes_never_share_host_tenant_context_or_store(self):
        other, other_context, other_executor, _ = await self.make_runtime("two", "other-host-tenant")
        results = await asyncio.gather(self.run_parent(), self.run_parent(runtime=other, context=other_context))
        self.assertEqual([record.status for record in results], ["success", "success"])
        for runtime, context, executor in ((self.runtime, self.context, self.executor),
                                           (other, other_context, other_executor)):
            request, child, active_runtime, active_context, _ = executor.children[0]
            self.assertIs(active_runtime, runtime)
            self.assertEqual(active_context, child)
            self.assertEqual(child.tenant_id, context.tenant_id)
            self.assertEqual(child.capabilities, context.capabilities)
            self.assertEqual(child.authority, context.authority)
            self.assertIsNone(await (other if runtime is self.runtime else self.runtime).services.store.get(request.run_id))
        self.assertIsNone(current_runtime())

    async def test_restart_replays_canonical_child_lineage_without_reexecuting_member(self):
        parent = await self.run_parent()
        self.assertEqual(parent.status, "success")
        before = await self.runtime.children("parent", self.context)
        events = await self.runtime.events("parent", 0, self.context)
        settings, path = self.runtime.settings, self.runtime.services.store.path
        await self.runtime.close()
        executor = TeamExecutor(self)
        reopened = Runtime(settings, RuntimeServices(SqliteRuntimeStore(path), executor, self.policy))
        await reopened.start()
        self.addAsyncCleanup(reopened.close)
        self.assertEqual(await reopened.children("parent", self.context), before)
        self.assertEqual(await reopened.events("parent", 0, self.context), events)
        self.assertEqual((await reopened.submit(self.request(), self.context)).status, "success")
        self.assertEqual(executor.children, [])
        accepted = await reopened.accepted_request(before[0].run_id, self.context)
        self.assertEqual(accepted.request.model_ref, self.model_ref)
        self.assertEqual(accepted.request.attachments, self.request().attachments)
        self.assertEqual(accepted.author.kind, "agent")
        self.assertEqual(accepted.authority, self.context.authority)

    async def test_concurrency_one_allows_nested_children_without_parent_slot_deadlock(self):
        self.runtime.settings = replace(self.runtime.settings, child_concurrency=1, child_max_depth=3,
                                        child_chain_concurrency=1)
        self.executor.nested_children = 2
        self.assertEqual((await self.run_parent()).status, "success", self.executor.errors)
        self.assertEqual(len(self.executor.children), 3)
        parent_id = "parent"
        for request, context, _, _, _ in self.executor.children:
            self.assertEqual(context.parent_run_id, parent_id)
            self.assertEqual((await self.runtime.get_run(request.run_id, context)).status, "success")
            parent_id = request.run_id

    async def test_depth_limit_uses_durable_lineage_and_never_calls_an_extra_child_executor(self):
        self.runtime.settings = replace(self.runtime.settings, child_concurrency=1, child_max_depth=2)
        self.executor.nested_children = 2
        self.assertEqual((await self.run_parent()).status, "failed")
        self.assertEqual(len(self.executor.children), 2)
        for request, context, _, _, _ in self.executor.children:
            self.assertEqual((await self.runtime.get_run(request.run_id, context)).status, "failed")
        self.assertEqual(await self.runtime.children(self.executor.children[-1][0].run_id, self.context), ())

    async def queued_second_parent(self):
        self.runtime.settings = replace(self.runtime.settings, child_concurrency=1)
        self.executor.child_mode = "hold"
        await self.runtime.submit(self.request(), self.context)
        await asyncio.wait_for(self.executor.child_started.wait(), 3)
        second = replace(self.context, session_id="second-parent-session")
        await self.runtime.submit(self.request(second, "second-parent"), second)

        async def second_requested_capacity():
            while await self.policy.delegation_seen.get() != second.session_id:
                pass

        await asyncio.wait_for(second_requested_capacity(), 3)
        self.assertEqual(await self.runtime.children("second-parent", second), ())
        return second

    async def test_revocation_while_waiting_for_child_capacity_rechecks_before_admission_or_provider_io(self):
        second = await self.queued_second_parent()
        self.policy.denied.add("run.delegate")
        self.executor.release_children.set()
        self.assertEqual((await asyncio.wait_for(self.runtime.wait("parent", self.context), 3)).status, "success")
        self.assertEqual((await asyncio.wait_for(self.runtime.wait("second-parent", second), 3)).status, "failed")
        self.assertEqual(len(self.executor.children), 1)
        self.assertEqual(await self.runtime.children("second-parent", second), ())

    async def test_cancellation_while_waiting_for_child_capacity_admits_no_child(self):
        second = await self.queued_second_parent()
        self.assertEqual((await self.runtime.cancel("second-parent", second)).status, "cancelled")
        self.assertEqual(await self.runtime.children("second-parent", second), ())
        self.executor.release_children.set()
        self.assertEqual((await asyncio.wait_for(self.runtime.wait("parent", self.context), 3)).status, "success")
        self.assertEqual(len(self.executor.children), 1)

    async def test_one_runtime_full_child_budget_does_not_block_another_runtime(self):
        self.runtime.settings = replace(self.runtime.settings, child_concurrency=1)
        self.executor.child_mode = "hold"
        await self.runtime.submit(self.request(), self.context)
        await asyncio.wait_for(self.executor.child_started.wait(), 3)
        other, other_context, other_executor, _ = await self.make_runtime("other-budget", "other-tenant")
        other.settings = replace(other.settings, child_concurrency=1)
        self.assertEqual((await self.run_parent(runtime=other, context=other_context)).status, "success")
        self.assertEqual(len(other_executor.children), 1)
        self.assertEqual((await self.runtime.get_run("parent", self.context)).status, "running")
        self.executor.release_children.set()
        self.assertEqual((await asyncio.wait_for(self.runtime.wait("parent", self.context), 3)).status, "success")

    def test_dispatcher_construction_binds_actual_member_objects_to_exact_registered_refs(self):
        from openagent_core.models.catalog import CatalogModel
        from openagent_core.models.dispatcher import TeamRouterProvider

        refs = ("vendor:leader-model", "vendor:member:model-with-colons", "other:member-model")
        entries = [CatalogModel("vendor", ref, ref, "platform") for ref in refs]
        router = TeamRouterProvider(refs[0], [])
        built = []

        def build(entry, *, name, **kwargs):
            member = Agent(id=name, name=name)
            built.append((member, entry.runtime_id))
            return member

        with patch.object(router, "_enabled_llm_models", return_value=entries), \
                patch.object(router, "_build_api_agent_for", side_effect=build), \
                patch.object(router, "_build_agent_for", side_effect=build), \
                patch("openagent_core.core.execution_profile.lean_local_event_active", return_value=False):
            team = router._ensure_runtime("dispatcher-session", system="host framework prompt")
        self.assertIsInstance(team, Team)
        self.assertEqual(team._runtime_member_model_refs, {id(member): ref for member, ref in built})
        self.assertEqual(set(team._runtime_member_model_refs), {id(member) for member in team.members})
        self.assertEqual(router._session_member_map["dispatcher-session"]["vendor-member-model-with-colons"], refs[1])
        self.assertNotIn("vendor-member-model-with-colons", team._runtime_member_model_refs)

    def test_dispatcher_rejects_normalization_collisions_and_reserved_leader_before_provider_construction(self):
        from openagent_core.models.catalog import CatalogModel
        from openagent_core.models.dispatcher import TeamRouterProvider

        for refs in (("vendor:root", "vendor:a:b", "vendor:a-b"), ("vendor:root", "leader")):
            with self.subTest(refs=refs):
                entries = [CatalogModel("vendor", ref, ref, "platform") for ref in refs]
                router = TeamRouterProvider(refs[0], [])
                with patch.object(router, "_enabled_llm_models", return_value=entries), \
                        patch.object(router, "_build_api_agent_for") as leader, \
                        patch.object(router, "_build_agent_for") as member:
                    with self.assertRaises(ValueError):
                        router._ensure_runtime("dispatcher-session", system="host framework prompt")
                    leader.assert_not_called()
                    member.assert_not_called()


if __name__ == "__main__":
    unittest.main()
