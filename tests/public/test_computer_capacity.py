"""Durable capacity, hypervisor reconciliation and scoped suspend with concurrent workers."""

import asyncio
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.computer_capacity import (
    ComputerPoolLimits,
    ComputerResources,
    SqlComputerCapacity,
)
from openagent_core.computer_tasks import (
    ComputerAllocation,
    ComputerTask,
    TaskComputerBroker,
)
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.lima_computers import LimaComputerProvider
from sqlalchemy.ext.asyncio import create_async_engine


class InventoryLima(LimaComputerProvider):
    """Interrupt commands after a durable effect, as when a controller loses its RPC response."""

    lost = None

    async def _lima(self, command, *args):
        name = args[1] if command == "clone" else args[-1]
        directory = self.home / name
        status = directory / "lima.yaml"
        if command == "list":
            return json.dumps(
                {"name": name, "status": status.read_text(), "cpus": 1, "memory": 100, "disk": 200}
            )
        if command == "clone":
            directory.mkdir()
            status.write_text("Stopped")
        elif command == "start":
            (directory / "boot").write_text(uuid4().hex)
            status.write_text("Running")
        elif command == "stop":
            status.write_text("Stopped")
        else:
            raise AssertionError(command)
        if self.lost == command:
            self.lost = None
            raise ConnectionError("lost hypervisor response")

    async def _pin(self, directory, name):
        (directory / "configured").write_text("1")
        (directory / "verified-base").write_text("1")

    async def _boot(self, key):
        return (self.home / self._machine(key) / "boot").read_text()


class Computers(unittest.IsolatedAsyncioTestCase):
    database = None

    async def asyncSetUp(self):
        self.home = Path(self.enterContext(TemporaryDirectory()))
        (self.home / "base").mkdir()
        (self.home / "base/lima.yaml").write_text("Stopped")
        self.url = self.database or "sqlite+aiosqlite:///" + str(self.home / "quota.db")
        self.pool = uuid4().hex
        self.tenant = uuid4().hex
        self.quota = ComputerPoolLimits(2, 1, 1, 1, 1, 100, 400)
        self.task = ComputerTask("project", "notes", "v1")

    def context(self, name):
        p = PrincipalRef("fixture", self.tenant, name)
        return ExecutionContext(p, p, p, "session-" + name, "agent", (p,))

    async def authorize(self, context, action, resource, *, audience):
        return audience == (context.initiator,) and context.tenant_id == resource.tenant_id

    async def replica(self):
        engine = create_async_engine(self.url)
        self.addAsyncCleanup(engine.dispose)
        capacity = SqlComputerCapacity(engine, self.pool, self.quota)
        provider = InventoryLima(self.home, {("project", "v1"): ("base", "a" * 40)}, capacity=capacity)
        broker = TaskComputerBroker(engine, self, provider, provider, required_capacity=capacity)
        provider.bind(broker)
        await broker.initialize()
        return capacity, provider, broker

    async def test_capacity_after_concurrent_provision_and_lost_clone_or_stop(self):
        cap, provider, broker = await self.replica()
        other_cap, other, second = await self.replica()
        alice, bob = self.context("alice"), self.context("bob")
        provider.lost = "clone"
        with self.assertRaises(ConnectionError):
            await broker.provision(alice, self.task)
        self.assertEqual((await other_cap.usage())["machines"], 1)
        one = await second.provision(alice, self.task)
        self.assertEqual(await broker.provision(alice, self.task), one)
        extra = ComputerTask("project", "extra", "v1")
        with self.assertRaises(CapabilityUnavailable):
            await second.provision(alice, extra)
        excess = ComputerAllocation.for_context(alice, extra)
        self.assertIsNone(await second._row(excess.key))
        self.assertFalse(other._dir(excess.key).exists())
        self.assertEqual(len(list(self.home.glob("oa-*"))), 1)
        two = await broker.provision(bob, self.task)
        self.assertNotEqual(one, two)
        with self.assertRaises(CapabilityUnavailable):
            await second.provision(self.context("third"), self.task)
        self.assertEqual(len(list(self.home.glob("oa-*"))), 2)

        results = await asyncio.gather(
            broker.attach(alice, self.task), second.attach(bob, self.task), return_exceptions=True
        )
        self.assertEqual(sum(isinstance(r, CapabilityUnavailable) for r in results), 1)
        index = 0 if not isinstance(results[0], Exception) else 1
        current, waiting = (alice, bob) if index == 0 else (bob, alice)
        lease = results[index]
        allocation = ComputerAllocation.for_context(current, self.task)
        self.assertEqual(
            await cap.usage(),
            {"machines": 2, "running": 1, "cpus": 1, "memory_bytes": 100, "disk_bytes": 400},
        )
        with self.assertRaises(CapabilityUnavailable):
            await second.suspend(waiting, self.task, lease)
        provider.lost = "stop"
        with self.assertRaises(ConnectionError):
            await broker.suspend(current, self.task, lease)
        self.assertEqual(await provider._status(lease.machine_id), "Stopped")
        self.assertEqual((await cap.usage())["running"], 1)
        with self.assertRaises(CapabilityUnavailable):
            await second.attach(current, self.task)
        with self.assertRaises(CapabilityUnavailable):
            await second.attach(waiting, self.task)

        cap3, revived, restarted = await self.replica()
        await restarted.suspend(current, self.task)
        await second.suspend(current, self.task, lease)
        self.assertEqual((await cap3.usage())["running"], 0)
        with self.assertRaises(CapabilityUnavailable):
            await other.connection_at(allocation, lease.machine_id, admission_epoch=lease.epoch)
        waiting_lease = await restarted.attach(waiting, self.task)
        await restarted.suspend(waiting, self.task, waiting_lease)
        fresh = await second.attach(current, self.task)
        self.assertGreater(fresh.generation, lease.generation)
        with self.assertRaises(CapabilityUnavailable):
            await broker.suspend(current, self.task, lease)
        with self.assertRaises(CapabilityUnavailable):
            await revived.suspend_at(allocation, lease.epoch + 1)
        self.assertEqual(await provider._status(fresh.machine_id), "Running")
        await second.revoke(current, self.task)
        self.assertEqual((await cap.usage())["running"], 0)
        self.assertEqual((await cap.usage())["machines"], 2)
        with self.assertRaises(CapabilityUnavailable):
            await broker.attach(current, self.task)

    async def test_restart_after_reservation_before_clone_or_boot(self):
        capacity, initial, broker = await self.replica()
        owner = self.context("a")
        task = self.task
        allocation = ComputerAllocation.for_context(owner, task)
        reserve = initial.reserve_allocation_capacity

        async def interrupted(assignment):
            await reserve(assignment)
            raise ConnectionError("controller stopped before clone")

        initial.reserve_allocation_capacity = interrupted
        with self.assertRaises(ConnectionError):
            await broker.provision(owner, task)
        self.assertIsNone(await broker._row(allocation.key))
        self.assertFalse(initial._dir(allocation.key).exists())
        self.assertEqual((await capacity.usage())["machines"], 1)

        _, second, replacement = await self.replica()
        machine = await replacement.provision(owner, task)
        original = second._lima

        async def interrupted_start(command, *args):
            if command == "start":
                raise ConnectionError("controller stopped before guest boot")
            return await original(command, *args)

        second._lima = interrupted_start
        with self.assertRaises(ConnectionError):
            await replacement.attach(owner, task)
        self.assertEqual(await second._status(machine), "Stopped")
        self.assertEqual((await capacity.usage())["running"], 1)
        _, _, recovered = await self.replica()
        fresh = await recovered.attach(owner, task)
        with self.assertRaises(CapabilityUnavailable):
            await recovered.suspend(owner, task)
        await recovered.suspend(owner, task, fresh)
        self.assertEqual((await capacity.usage())["running"], 0)

    async def test_resource_limits_and_policy_disagreement(self):
        cap, _provider, _broker = await self.replica()
        a = ComputerAllocation.for_context(self.context("a"), self.task)
        b = ComputerAllocation.for_context(self.context("b"), self.task)
        with self.assertRaises(CapabilityUnavailable):
            await cap.reserve(a, ComputerResources(1, 100, 401))
        await cap.reserve(a, ComputerResources(1, 101, 150))
        with self.assertRaises(CapabilityUnavailable):
            await cap.reserve(a, ComputerResources(1, 101, 150), running=True)
        await cap.reserve(b, ComputerResources(2, 80, 150))
        with self.assertRaises(CapabilityUnavailable):
            await cap.reserve(b, ComputerResources(2, 80, 150), running=True)
        with self.assertRaises(CapabilityUnavailable):
            await cap.reserve(b, ComputerResources(1, 80, 150), running=True)
        with self.assertRaises(CapabilityUnavailable):
            await SqlComputerCapacity(
                cap.engine, self.pool, ComputerPoolLimits(2, 2, 2, 2, 2, 200, 400)
            ).initialize()
        with self.assertRaises(CapabilityUnavailable):
            TaskComputerBroker(cap.engine, self, object(), object(), required_capacity=cap)

    async def test_concurrent_persistent_reservation(self):
        (cap, _, _), (other, _, _) = await asyncio.gather(self.replica(), self.replica())
        allocations = [
            ComputerAllocation.for_context(self.context("a"), ComputerTask("project", str(i), "v1"))
            for i in range(8)
        ]
        results = await asyncio.gather(
            *[
                (cap if i % 2 == 0 else other).reserve(a, ComputerResources(1, 100, 200))
                for i, a in enumerate(allocations)
            ],
            return_exceptions=True,
        )
        self.assertEqual(results.count(None), 1)
        self.assertEqual(sum(isinstance(r, CapabilityUnavailable) for r in results), 7)
        self.assertEqual((await other.usage())["machines"], 1)


@unittest.skipUnless(os.getenv("OPENAGENT_TEST_POSTGRES_URL"), "Requires a dedicated PostgreSQL")
class PostgresComputers(Computers):
    database = os.getenv("OPENAGENT_TEST_POSTGRES_URL")
