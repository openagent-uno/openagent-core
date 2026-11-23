"""Durable task allocations exercised through ordinary ToolRefs."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core.capabilities import (
    CapabilityCatalog,
    CapabilityUnavailable,
    FunctionSource,
    ToolDefinition,
)
from openagent_core.computer_tasks import ComputerConnection, ComputerTask, TaskComputerBroker
from openagent_core.contracts import ExecutionContext, IdempotencyConflict, PrincipalRef


class Policy:
    enabled = True

    async def authorize(self, context, action, resource, *, audience=()):
        return self.enabled and all(person == context.initiator for person in audience)

    async def validate(self, context):
        return self.enabled and context.delegation_id == "task-grant"


class Machines:
    """A provider with idempotent creation and an independently enrolled device registry."""

    def __init__(self, clock):
        self.clock = clock
        self.registrations = {}
        self.calls = []
        self.before_return = None

    async def ensure(self, allocation):
        await asyncio.sleep(0)
        machine = "instance-" + allocation.key
        self.registrations.setdefault(machine, (allocation, 1))
        return machine

    async def connection(self, allocation, machine):
        owner, generation = self.registrations[machine]
        if owner != allocation:
            raise PermissionError("Missing enrollment")
        return ComputerConnection(machine, machine, generation, self.clock() + 30)

    async def catalog(self, context, selectors):
        # Deliberately noisy discovery checks that another healthy machine can
        # never substitute for the disconnected assigned target.
        return {
            "servers": [
                {
                    "host_id": machine,
                    "generation": generation,
                    "server": "files",
                    "version": "1",
                    "catalog_digest": "a" * 64,
                    "tools": [{"name": "read", "input_schema": {}}],
                }
                for machine, (_, generation) in self.registrations.items()
            ]
        }

    async def call(self, context, scope, name, arguments):
        owner, generation = self.registrations[scope["host_id"]]
        if generation != scope["generation"] or context.initiator != owner.owner:
            raise PermissionError("Stale device generation")
        self.calls.append((scope["host_id"], name))
        if self.before_return:
            await self.before_return()
        return owner.task.task


class ComputerTasks(unittest.IsolatedAsyncioTestCase):
    database = None

    async def asyncSetUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.engine = create_async_engine(
            self.database or "sqlite+aiosqlite:///" + str(self.root / "tasks.db")
        )
        self.addAsyncCleanup(self.engine.dispose)
        self.now = 100.0
        self.policy = Policy()
        self.fleet = Machines(lambda: self.now)
        self.broker = TaskComputerBroker(
            self.engine,
            self.policy,
            self.fleet,
            self.fleet,
            delegations=self.policy,
            clock=lambda: self.now,
        )
        await self.broker.initialize()
        tenant = uuid4().hex

        def context(user):
            p = PrincipalRef("verified", tenant, user)
            return ExecutionContext(p, p, p, "session-" + user, "agent", (p,))

        self.alice, self.bob = context("alice"), context("bob")
        self.first = ComputerTask("project", "first", "snapshot-7")
        self.second = replace(self.first, task="second")
        self.catalog = CapabilityCatalog(self.policy)

        async def worker_tool(args, context):
            self.fail("A cloud computer request ran on the core worker")

        local = FunctionSource((ToolDefinition("read", "local worker", {}),), {"read": worker_tool})
        self.catalog.register("worker-local", local, local, target_label="Worker")

    async def setup_task(self, context, task):
        await self.broker.provision(context, task)
        return await self.broker.attach(context, task)

    async def test_concurrent_tasks_and_users_never_share_tools(self):
        inputs = ((self.alice, self.first), (self.alice, self.second), (self.bob, self.first))
        leases = await asyncio.gather(*(self.setup_task(c, t) for c, t in inputs))
        self.assertEqual(len({lease.machine_id for lease in leases}), 3)
        async with AsyncExitStack() as stack:
            bound = [
                await stack.enter_async_context(self.broker.tools(self.catalog, c, lease))
                for (c, _), lease in zip(inputs, leases)
            ]
            tools = [(await self.catalog.discover(c)) for c in bound]
            self.assertEqual([len(items) for items in tools], [1, 1, 1])
            answers = await asyncio.gather(
                *(self.catalog.call_tool(items[0].tool_ref, {}, ctx) for items, ctx in zip(tools, bound))
            )
            self.assertEqual(answers, ["first", "second", "first"])
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools[0][0].tool_ref, {}, bound[1])
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools[0][0].tool_ref, {}, bound[2])
            with self.assertRaises(CapabilityUnavailable):
                await stack.enter_async_context(self.broker.tools(self.catalog, self.bob, leases[0]))
            async with self.broker.tools(self.catalog, self.alice, leases[0]) as duplicate:
                self.assertEqual((await self.catalog.discover(duplicate)), tools[0])
            self.assertEqual(len(await self.catalog.discover(bound[0])), 1)
        self.assertEqual([t.source_id for t in await self.catalog.discover(self.alice)], ["worker-local"])
        self.assertFalse(self.broker._mounts)

    async def test_idempotent_provision_including_recovery(self):
        async def get():
            return await self.broker.provision(self.alice, self.first)

        a, b = await asyncio.gather(get(), get())
        self.assertEqual(a, b)
        self.assertEqual(len(self.fleet.registrations), 1)
        with self.assertRaises(IdempotencyConflict):
            await self.broker.provision(self.alice, replace(self.first, base_version="snapshot-8"))
        broker = TaskComputerBroker(
            self.engine, self.policy, self.fleet, self.fleet, clock=lambda: self.now
        )
        self.assertEqual(await broker.provision(self.alice, self.first), a)
        leases = await asyncio.gather(
            broker.attach(self.alice, self.first), self.broker.attach(self.alice, self.first)
        )
        self.assertEqual(leases[0], leases[1])

    async def test_provider_response_lost_after_allocation_and_device_edge_fencing(self):
        ensure = self.fleet.ensure

        async def interrupted(allocation):
            await ensure(allocation)
            raise ConnectionError("Provisioning receipt lost")

        self.fleet.ensure = interrupted
        with self.assertRaises(ConnectionError):
            await self.broker.provision(self.alice, self.first)
        self.fleet.ensure = ensure
        lease = await self.setup_task(self.alice, self.first)
        self.assertEqual(len(self.fleet.registrations), 1)
        async with self.broker.tools(self.catalog, self.alice, lease) as bound:
            with self.assertRaises(CapabilityUnavailable):
                await self.broker.admit_target(bound, "another-enrollment", lease.generation)
            await self.broker.revoke(self.alice, self.first)
            with self.assertRaises(CapabilityUnavailable):
                await self.broker.admit_target(bound, lease.host_id, lease.generation)

    async def test_disconnect_reconnect_expiry_and_revocation(self):
        first = await self.setup_task(self.alice, self.first)
        async with self.broker.tools(self.catalog, self.alice, first) as bound:
            ref = (await self.catalog.discover(bound))[0].tool_ref
            self.assertTrue(await self.broker.disconnected(first))
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(ref, {}, bound)
            self.assertFalse(await self.catalog.discover(bound))
            with self.assertRaises(CapabilityUnavailable):
                await self.broker.attach(self.alice, self.first)
            allocation, generation = self.fleet.registrations[first.machine_id]
            self.fleet.registrations[first.machine_id] = (allocation, generation + 1)
            second = await self.broker.attach(self.alice, self.first)
            self.assertFalse(await self.broker.disconnected(first))
            async with self.broker.tools(self.catalog, self.alice, second) as resumed:
                new = (await self.catalog.discover(resumed))[0].tool_ref
                self.assertNotEqual(new, ref)
                self.assertEqual(await self.catalog.call_tool(new, {}, resumed), "first")
                self.now += 31
                with self.assertRaises(CapabilityUnavailable):
                    await self.catalog.call_tool(new, {}, resumed)
                with self.assertRaises(CapabilityUnavailable):
                    await self.broker.attach(self.alice, self.first)
        await self.broker.revoke(self.alice, self.first)
        with self.assertRaises(PermissionError):
            await self.broker.provision(self.alice, self.first)

    async def test_deferred_grant_and_result_revocation(self):
        lease = await self.setup_task(self.alice, self.first)
        deferred = replace(self.alice, deferred=True, delegation_id="task-grant")
        with self.assertRaises(PermissionError):
            async with self.broker.tools(self.catalog, replace(deferred, delegation_id="wrong"), lease):
                self.fail("Unverified background task was admitted")
        async with self.broker.tools(self.catalog, deferred, lease) as bound:
            ref = (await self.catalog.discover(bound))[0].tool_ref
            self.fleet.before_return = lambda: self.broker.revoke(self.alice, self.first)
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(ref, {}, bound)
            self.assertEqual(len(self.fleet.calls), 1)

    async def test_project_version_access_is_independent_of_task_permission(self):
        authorize = self.policy.authorize

        async def denied_version(context, action, resource, *, audience=()):
            if action == "computer.base.read":
                self.assertEqual(resource.kind, "computer-project-version")
                return False
            return await authorize(context, action, resource, audience=audience)

        self.policy.authorize = denied_version
        with self.assertRaises(PermissionError):
            await self.broker.provision(self.alice, self.first)
        self.assertFalse(self.fleet.registrations)
        self.policy.authorize = authorize
        lease = await self.setup_task(self.alice, self.first)
        async with self.broker.tools(self.catalog, self.alice, lease) as bound:
            self.policy.authorize = denied_version
            self.assertFalse(await self.catalog.discover(bound))
            with self.assertRaises(PermissionError):
                await self.broker.admit_target(bound, lease.host_id, lease.generation)


@unittest.skipUnless(os.getenv("OPENAGENT_TEST_POSTGRES_URL"), "Requires a dedicated PostgreSQL")
class PostgresComputerTasks(ComputerTasks):
    database = os.getenv("OPENAGENT_TEST_POSTGRES_URL")
