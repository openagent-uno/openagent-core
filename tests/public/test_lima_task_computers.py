"""Run against an operator-prepared, stopped test base on a private Lima hypervisor."""

import asyncio
import base64
from contextlib import AsyncExitStack
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine
from openagent_core import ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.computer_interaction import ComputerInteraction
from openagent_core.computer_tasks import ComputerAllocation, ComputerTask, TaskComputerBroker
from openagent_core.lima_computers import LimaComputerProvider
from openagent_storage_sqlite import SqliteRuntimeStore


@unittest.skipUnless(
    os.getenv("OPENAGENT_TEST_LIMA_BASE"), "Requires a sealed test VM and dedicated hypervisor"
)
class LimaComputers(unittest.IsolatedAsyncioTestCase):
    async def test_isolated_snapshot_review_viewer_input_and_revocation(self):
        home = os.environ["OPENAGENT_TEST_LIMA_HOME"]
        base = os.environ["OPENAGENT_TEST_LIMA_BASE"]
        commit = os.environ["OPENAGENT_TEST_LIMA_COMMIT"]
        task = ComputerTask("workspace", "change", "sealed")
        provider = LimaComputerProvider(
            home, {(task.project, task.base_version): (base, commit)}, lease_seconds=300
        )
        temp = Path(self.enterContext(TemporaryDirectory()))
        engine = create_async_engine("sqlite+aiosqlite:///" + str(temp / "controller.sqlite"))
        self.addAsyncCleanup(engine.dispose)

        class Policy:
            async def authorize(self, context, action, resource, *, audience=()):
                return all(p == context.initiator for p in audience)

        policy = Policy()
        broker = TaskComputerBroker(engine, policy, provider, provider)
        provider.bind(broker)
        await broker.initialize()
        runtime = Runtime(
            RuntimeSettings("agent"), RuntimeServices(SqliteRuntimeStore(temp / "runs.db"), None, policy)
        )
        viewer = ComputerInteraction(runtime)
        tenant = uuid4().hex

        def context(name):
            p = PrincipalRef("auth", tenant, name)
            return ExecutionContext(p, p, p, "session-" + name, "agent", (p,))

        a, b = context("alpha"), context("bravo")
        allocations = [ComputerAllocation.for_context(c, task) for c in (a, b)]
        for alloc in allocations:
            self.addAsyncCleanup(provider.suspend, alloc)
        machines = await asyncio.gather(*(broker.provision(c, task) for c in (a, b)))
        self.assertNotEqual(*machines)
        leases = await asyncio.gather(*(broker.attach(c, task) for c in (a, b)))

        async def call(ctx, name, **args):
            tool = next(t for t in await runtime.capabilities.discover(ctx) if t.name == name)
            result = await runtime.capabilities.call_tool(tool.tool_ref, args, ctx)
            self.assertFalse(
                result.get("isError"), result.get("content", [])[:1] if result.get("isError") else ""
            )
            return result

        async def prove(ctx, user, lease):
            self.assertEqual(
                (await call(ctx, "read_text_file", path="seed.txt"))["content"][0]["text"], "sealed\n"
            )
            await call(ctx, "write_file", path="seed.txt", content=user)
            process = await call(ctx, "shell_exec", command="id -u; cat seed.txt")
            uid, identity = process["structuredContent"]["stdout"].splitlines()
            self.assertNotEqual(uid, "0")
            self.assertEqual(identity, user)
            (target,) = await viewer.targets(ctx)

            async def interact(action, **parameters):
                result = await viewer.interact(
                    ctx, {"tool_ref": target["tool_ref"], "action": action, **parameters}
                )
                self.assertFalse(result.get("isError"))
                return result

            screen = await interact("get_screenshot")
            frame = next(block for block in screen["content"] if block["type"] == "image")
            self.assertGreater(len(base64.b64decode(frame["data"], validate=True)), 100)
            await interact("type", text=user)
            await interact("key", text="Return")
            typed = await call(ctx, "read_text_file", path="viewer-input.txt")
            self.assertEqual(typed["content"][0]["text"].strip(), user)
            return await broker.checkpoint(ctx, lease, "review")

        async with AsyncExitStack() as stack:
            ca, cb = [
                await stack.enter_async_context(broker.tools(runtime.capabilities, c, l))
                for c, l in zip((a, b), leases)
            ]
            receipts = await asyncio.gather(prove(ca, "alpha", leases[0]), prove(cb, "bravo", leases[1]))
            self.assertNotEqual(receipts[0]["sha256"], receipts[1]["sha256"])
            await provider.suspend(allocations[0])
            self.assertTrue(await broker.disconnected(leases[0]))
            new = await broker.attach(a, task)
            self.assertGreater(new.generation, leases[0].generation)
            active = await stack.enter_async_context(broker.tools(runtime.capabilities, a, new))
            self.assertEqual(
                (await call(active, "read_text_file", path="seed.txt"))["content"][0]["text"], "alpha"
            )
            self.assertEqual(await broker.checkpoint(active, new, "review"), receipts[0])
            shell = next(t for t in await runtime.capabilities.discover(active) if t.name == "shell_exec")
            running = asyncio.create_task(
                runtime.capabilities.call_tool(
                    shell.tool_ref,
                    {"command": "printf ready > inflight; sleep 60; printf completed > completed"},
                    active,
                )
            )
            try:
                async with asyncio.timeout(35):
                    while True:
                        files = await call(active, "list_directory", path=".")
                        if "inflight" in str(files["structuredContent"]):
                            break
                        await asyncio.sleep(0.2)
                await broker.revoke(a, task)
                with self.assertRaises(CapabilityUnavailable):
                    await asyncio.wait_for(running, 20)
            finally:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            self.assertEqual(
                (await call(cb, "read_text_file", path="seed.txt"))["content"][0]["text"], "bravo"
            )
        for c, who, receipt in zip((a, b), ("alpha", "bravo"), receipts):
            info, patch = await broker.read_checkpoint(c, task, "review")
            self.assertEqual(info, receipt)
            self.assertIn(("+" + who).encode(), patch)
            self.assertIn(b"-sealed", patch)
            self.assertIn(b"viewer-input.txt", patch)
