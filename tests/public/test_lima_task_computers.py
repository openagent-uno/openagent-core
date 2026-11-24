"""Run against an operator-prepared, stopped test base on a private Lima hypervisor."""

import asyncio
import base64
import json
import os
import shlex
import subprocess
import time
import unittest
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from openagent_core import (
    ExecutionContext,
    PrincipalRef,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.computer_capacity import (
    ComputerPoolLimits,
    ComputerResources,
    SqlComputerCapacity,
)
from openagent_core.computer_interaction import ComputerInteraction
from openagent_core.computer_tasks import (
    ComputerAllocation,
    ComputerTask,
    TaskComputerBroker,
)
from openagent_core.lima_computers import LimaComputerProvider
from openagent_storage_sqlite import SqliteRuntimeStore
from sqlalchemy.ext.asyncio import create_async_engine


@unittest.skipUnless(
    os.getenv("OPENAGENT_TEST_LIMA_BASE"), "Requires a sealed test VM and dedicated hypervisor"
)
class LimaComputers(unittest.IsolatedAsyncioTestCase):
    async def setup_computers(self, lease_seconds, *, limits=None):
        home = os.environ["OPENAGENT_TEST_LIMA_HOME"]
        base = os.environ["OPENAGENT_TEST_LIMA_BASE"]
        commit = os.environ["OPENAGENT_TEST_LIMA_COMMIT"]
        task = ComputerTask("workspace", "change", "sealed")
        temp = Path(self.enterContext(TemporaryDirectory()))
        engine = create_async_engine("sqlite+aiosqlite:///" + str(temp / "controller.sqlite"))
        self.addAsyncCleanup(engine.dispose)
        capacity = SqlComputerCapacity(engine, "qualification", limits) if limits is not None else None
        provider = LimaComputerProvider(
            home, {(task.project, task.base_version): (base, commit)}, lease_seconds=lease_seconds,
            capacity=capacity,
        )

        denied = set()

        class Policy:
            async def authorize(self, context, action, resource, *, audience=()):
                return action not in denied and all(p == context.initiator for p in audience)

        policy = Policy()
        broker = TaskComputerBroker(engine, policy, provider, provider, required_capacity=capacity)
        provider.bind(broker)
        self.addAsyncCleanup(provider.close)
        await broker.initialize()
        runtime = Runtime(
            RuntimeSettings("agent"), RuntimeServices(SqliteRuntimeStore(temp / "runs.db"), None, policy)
        )
        tenant = uuid4().hex

        def context(name):
            p = PrincipalRef("auth", tenant, name)
            return ExecutionContext(p, p, p, "session-" + name, "agent", (p,))

        self.temp, self.denied, self.runtime, self.context = temp, denied, runtime, context
        return provider, broker, task

    async def call(self, ctx, name, **args):
        tool = next(t for t in await self.runtime.capabilities.discover(ctx) if t.name == name)
        result = await self.runtime.capabilities.call_tool(tool.tool_ref, args, ctx)
        self.assertFalse(
            result.get("isError"), result.get("content", [])[:1] if result.get("isError") else ""
        )
        return result

    @unittest.skipUnless(os.getenv("OPENAGENT_TEST_LIMA_CAPACITY"), "Requires a sealed guest with Chromium")
    async def test_two_metered_linux_computers_suspend_resume_and_revoke(self):
        profile = ComputerResources(*map(int, os.environ["OPENAGENT_TEST_LIMA_CAPACITY"].split(",")))
        limits = ComputerPoolLimits(2, 1, 1, 1, profile.cpus, profile.memory_bytes, 2 * profile.disk_bytes)
        provider, broker, task = await self.setup_computers(300, limits=limits)
        a, b = self.context("alpha"), self.context("bravo")
        allocations = [ComputerAllocation.for_context(c, task) for c in (a, b)]
        for allocation in allocations:
            self.addAsyncCleanup(provider.suspend, allocation)
        machines = await asyncio.gather(*(broker.provision(context, task) for context in (a, b)))
        self.assertNotEqual(*machines)
        for machine in machines:
            self.assertEqual(await provider.resource_profile(machine), profile)
        excess = replace(task, task="excess")
        with self.assertRaises(CapabilityUnavailable):
            await broker.provision(a, excess)
        refused = ComputerAllocation.for_context(a, excess)
        self.assertEqual(await provider._status(provider._machine(refused.key)), "Absent")

        page = """<!doctype html><title>Workspace</title><main></main><script>
const owner = new URL(location.href).searchParams.get('owner');
if (['alpha','bravo'].includes(owner)) document.cookie =
    'workspace=' + owner + '; path=/; max-age=86400; SameSite=Lax';
document.querySelector('main').textContent = 'Cookie=' + document.cookie;
</script>"""
        server = f"""import pathlib, subprocess, sys, time, urllib.request
root = pathlib.Path('/tmp/oa-capacity-page')
root.mkdir(exist_ok=True)
(root / 'index.html').write_text({page!r})
subprocess.Popen([sys.executable, '-m','http.server','18286','--bind','127.0.0.1',
                 '--directory',str(root)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                 stderr=subprocess.DEVNULL, start_new_session=True)
for attempt in range(50):
    try:
        assert urllib.request.urlopen('http://127.0.0.1:18286/',timeout=1).read().decode() == {page!r}
        break
    except OSError:
        time.sleep(.1)
else:
    raise RuntimeError('Guest HTTP fixture unavailable')
"""

        async def visit(context, lease, name, *, initialize):
            async with broker.tools(self.runtime.capabilities, context, lease) as ctx:
                async def shell(command):
                    out = await self.call(ctx, "shell_exec", command=command)
                    return out["structuredContent"]["stdout"]
                self.assertEqual(int(await shell("nproc")), profile.cpus)
                memory = int((await shell("awk '/MemTotal/ {print $2}' /proc/meminfo")).strip()) * 1024
                self.assertLessEqual(memory, profile.memory_bytes)
                self.assertGreater(memory, profile.memory_bytes * .65)
                sectors = await shell("for file in /sys/block/*/size; do cat \"$file\"; done")
                self.assertIn(profile.disk_bytes, [512 * int(value) for value in sectors.split()])
                document = await self.call(ctx, "read_text_file", path="seed.txt")
                self.assertEqual(document["content"][0]["text"], "sealed\n" if initialize else name)
                if initialize:
                    await self.call(ctx, "write_file", path="seed.txt", content=name)
                self.assertEqual((await shell("id -un; cat seed.txt")).splitlines(), ["oawork", name])
                await shell("python3 -c " + shlex.quote(server))
                tabs = await self.call(ctx, "tabs_context_mcp", createIfEmpty=True)
                tab = json.loads(tabs["content"][0]["text"].splitlines()[0])["availableTabs"][0]["tabId"]
                url = "http://127.0.0.1:18286/"
                await self.call(ctx, "navigate", tabId=tab, url=url)
                initial = await self.call(ctx, "get_page_text", tabId=tab)
                expected = "" if initialize else "workspace=" + name
                self.assertEqual(initial["content"][0]["text"].splitlines()[-1], "Cookie=" + expected)
                if initialize:
                    await self.call(ctx, "navigate", tabId=tab, url=url + "?owner=" + name)
                    text = await self.call(ctx, "get_page_text", tabId=tab)
                    self.assertEqual(text["content"][0]["text"].splitlines()[-1], "Cookie=workspace=" + name)

        first = await broker.attach(a, task)
        await visit(a, first, "alpha", initialize=True)
        with self.assertRaises(CapabilityUnavailable):
            await broker.attach(b, task)
        with self.assertRaises(CapabilityUnavailable):
            await broker.suspend(b, task, first)
        await broker.suspend(a, task, first)
        self.assertEqual(await provider._status(first.machine_id), "Stopped")
        second = await broker.attach(b, task)
        await visit(b, second, "bravo", initialize=True)
        await broker.suspend(b, task, second)
        fresh = await broker.attach(a, task)
        self.assertEqual(fresh.machine_id, first.machine_id)
        self.assertGreater(fresh.generation, first.generation)
        await visit(a, fresh, "alpha", initialize=False)
        await broker.revoke(a, task)
        with self.assertRaises(CapabilityUnavailable):
            await broker.attach(a, task)
        self.assertEqual(await provider._status(fresh.machine_id), "Stopped")
        self.assertEqual(await provider.capacity.usage(), {"machines": 2, "disk_bytes": 2*profile.disk_bytes,
                                                             "running": 0, "cpus": 0, "memory_bytes": 0})

    async def test_isolated_snapshot_review_viewer_input_and_revocation(self):
        provider, broker, task = await self.setup_computers(300)
        runtime = self.runtime
        call = self.call
        a, b = self.context("alpha"), self.context("bravo")
        allocations = [ComputerAllocation.for_context(c, task) for c in (a, b)]
        for alloc in allocations:
            self.addAsyncCleanup(provider.suspend, alloc)
        machines = await asyncio.gather(*(broker.provision(c, task) for c in (a, b)))
        self.assertNotEqual(*machines)
        leases = await asyncio.gather(*(broker.attach(c, task) for c in (a, b)))

        # Separate human browser sessions keep HTTP mounts short-lived, so each
        # request must rediscover the same remote tool after the last mount closed.
        from aiohttp.test_utils import TestClient, TestServer
        from openagent_gateway import ComputerViewerGateway, create_app

        sessions = {
            p.initiator.subject_id: replace(p, session_id="viewer-" + p.initiator.subject_id)
            for p in (a, b)
        }

        @asynccontextmanager
        async def identity(request, action, run_id, session):
            user = request.headers.get("Authorization", "").removeprefix("Bearer ")
            if user not in sessions or sessions[user].session_id != session:
                raise PermissionError("Unverified viewer")
            yield sessions[user]

        async def resolve(request, context, alias):
            if alias != "workspace":
                raise LookupError(alias)
            return task

        client = TestClient(
            TestServer(
                create_app(
                    runtime,
                    identity,
                    manage_lifecycle=False,
                    computer_viewer=ComputerViewerGateway(broker, resolve),
                )
            )
        )
        await client.start_server()
        self.addAsyncCleanup(client.close)
        handles = {}

        def path(user):
            return "/api/v1/sessions/" + sessions[user].session_id + "/computers/workspace"

        def auth(user):
            return {"Authorization": "Bearer " + user}

        async def post(user, handle, action, **parameters):
            return await client.post(
                path(user) + "/interact",
                headers=auth(user),
                json={**handle, "action": action, **parameters},
            )

        async def browser(ctx, user, *, initialize):
            page = os.environ.get("OPENAGENT_TEST_LIMA_BROWSER_PAGE")
            if not page:
                return
            tabs = await call(ctx, "tabs_context_mcp", createIfEmpty=True)
            tab = json.loads(tabs["content"][0]["text"].splitlines()[0])["availableTabs"][0]["tabId"]
            if initialize:
                await call(ctx, "navigate", tabId=tab, url=page + "?owner=" + user)
            await call(ctx, "navigate", tabId=tab, url=page)
            text = await call(ctx, "get_page_text", tabId=tab)
            self.assertEqual(text["content"][0]["text"].splitlines()[-1], "Session workspace=" + user)

        async def prove(ctx, user, lease):
            self.assertEqual(
                (await call(ctx, "read_text_file", path="seed.txt"))["content"][0]["text"], "sealed\n"
            )
            await call(ctx, "write_file", path="seed.txt", content=user)
            process = await call(ctx, "shell_exec", command="id -u; cat seed.txt")
            uid, identity = process["structuredContent"]["stdout"].splitlines()
            self.assertNotEqual(uid, "0")
            self.assertEqual(identity, user)
            async with client.get(path(user) + "/targets", headers=auth(user)) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.headers["Cache-Control"], "no-store")
                available = await response.json()
            (desktop,) = available["targets"]
            handle = {key: available[key] for key in ("epoch", "generation")}
            handles[user] = handle = {**handle, "target_id": desktop["target_id"]}

            async def interact(action, **parameters):
                async with await post(user, handle, action, **parameters) as response:
                    self.assertEqual(response.status, 200)
                    result = await response.json()
                    self.assertFalse(result.get("isError"))
                    return result

            screen = await interact("get_screenshot")
            frame = next(block for block in screen["content"] if block["type"] == "image")
            self.assertGreater(len(base64.b64decode(frame["data"], validate=True)), 100)
            await interact("type", text=user)
            await interact("key", text="Return")
            typed = await call(ctx, "read_text_file", path="viewer-input.txt")
            self.assertEqual(typed["content"][0]["text"].strip(), user)
            await browser(ctx, user, initialize=True)
            return await broker.checkpoint(ctx, lease, "review")

        async with AsyncExitStack() as stack:
            ca, cb = [
                await stack.enter_async_context(broker.tools(runtime.capabilities, c, l))
                for c, l in zip((a, b), leases)
            ]
            receipts = await asyncio.gather(prove(ca, "alpha", leases[0]), prove(cb, "bravo", leases[1]))
            self.assertNotEqual(receipts[0]["sha256"], receipts[1]["sha256"])
            self.assertNotEqual(handles["alpha"]["target_id"], handles["bravo"]["target_id"])
            self.denied.add("computer.viewer.input")
            async with await post("alpha", handles["alpha"], "type", text="read-only") as refused:
                self.assertEqual(refused.status, 403)
            async with await post("alpha", handles["alpha"], "get_screenshot") as capture:
                self.assertEqual(capture.status, 200)
                self.assertTrue(any(item["type"] == "image" for item in (await capture.json())["content"]))
            self.denied.clear()
            async with await post("bravo", handles["alpha"], "type", text="wrong") as refused:
                self.assertEqual(refused.status, 404)
            async with client.get(path("alpha") + "/targets", headers=auth("bravo")) as refused:
                self.assertEqual(refused.status, 403)
            await provider.suspend(allocations[0])
            self.assertTrue(await broker.disconnected(leases[0]))
            new = await broker.attach(a, task)
            self.assertGreater(new.generation, leases[0].generation)
            async with await post("alpha", handles["alpha"], "type", text="stale") as refused:
                self.assertEqual(refused.status, 409)
            active = await stack.enter_async_context(broker.tools(runtime.capabilities, a, new))
            self.assertEqual(
                (await call(active, "read_text_file", path="seed.txt"))["content"][0]["text"], "alpha"
            )
            self.assertEqual(await broker.checkpoint(active, new, "review"), receipts[0])
            await browser(active, "alpha", initialize=False)
            await browser(cb, "bravo", initialize=False)
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
                async with await post("alpha", handles["alpha"], "type", text="revoked") as refused:
                    self.assertEqual(refused.status, 404)
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

    async def test_same_owner_parallel_edits_conflict_and_expired_generation_recovers(self):
        provider, broker, task = await self.setup_computers(20)
        owner, outsider = self.context("owner"), self.context("outsider")
        tasks = [task, ComputerTask(task.project, "second", task.base_version)]
        sentinel = self.temp / "controller-private"
        sentinel.write_text("outside every guest")
        self.denied.add("computer.base.read")
        with self.assertRaises(PermissionError):
            await broker.provision(owner, task)
        self.denied.clear()
        for t in tasks:
            self.addAsyncCleanup(provider.suspend, ComputerAllocation.for_context(owner, t))
        machines = await asyncio.gather(*(broker.provision(owner, t) for t in tasks))
        self.assertNotEqual(*machines)
        leases = await asyncio.gather(*(broker.attach(owner, t) for t in tasks))
        viewers = ComputerInteraction(self.runtime)
        async with AsyncExitStack() as stack:
            contexts = [
                await stack.enter_async_context(broker.tools(self.runtime.capabilities, owner, lease))
                for lease in leases
            ]
            with self.assertRaises(CapabilityUnavailable):
                await stack.enter_async_context(
                    broker.tools(self.runtime.capabilities, outsider, leases[0])
                )
            targets = await viewers.targets(contexts[0])
            with self.assertRaises((PermissionError, CapabilityUnavailable)):
                await viewers.interact(
                    contexts[1], {"tool_ref": targets[0]["tool_ref"], "action": "get_screenshot"}
                )

            async def change(context, lease, label):
                shell = await self.call(
                    context,
                    "shell_exec",
                    command=(
                        "test ! -e "
                        + shlex.quote(str(sentinel))
                        + " && test ! -r /etc/ssh/ssh_host_ed25519_key && test ! -x /mnt/lima-cidata && ! grep -Eq ' - (virtiofs|9p) ' /proc/self/mountinfo && git rev-parse HEAD^{tree}"
                    ),
                )
                tree = shell["structuredContent"]["stdout"].strip()
                self.assertRegex(tree, r"^[a-f0-9]{40,64}$")
                await self.call(context, "write_file", path="seed.txt", content=label + "\n")
                await self.call(context, "write_file", path=label + ".txt", content=label + "\n")
                await broker.checkpoint(context, lease, "review")
                return tree

            base_trees = await asyncio.gather(
                *(change(c, l, label) for c, l, label in zip(contexts, leases, ("left", "right")))
            )
            self.assertEqual(*base_trees)
            for c, foreign in zip(contexts, ("right.txt", "left.txt")):
                names = (await self.call(c, "list_directory", path="."))["structuredContent"]
                self.assertNotIn(foreign, str(names))
            self.denied.add("computer.base.read")
            with self.assertRaises(PermissionError):
                await broker.admit_target(contexts[0], leases[0].host_id, leases[0].generation)
            self.denied.clear()
            provider.lease_seconds = 240
            await broker.attach(owner, tasks[1])
            self.denied.add("computer.review")
            with self.assertRaises(PermissionError):
                await broker.read_checkpoint(owner, tasks[0], "review")
            self.denied.clear()
            with self.assertRaises(CapabilityUnavailable):
                await broker.read_checkpoint(outsider, tasks[0], "review")

            integration = self.temp / "integration"
            integration.mkdir()

            def git(*args, data=None, check=True):
                return subprocess.run(
                    ["git", "-c", "core.hooksPath=/dev/null", *args],
                    cwd=integration,
                    env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull},
                    input=data,
                    capture_output=True,
                    check=check,
                )

            git("init", "-q")
            (integration / "seed.txt").write_text("sealed\n")
            git("add", "seed.txt")
            self.assertEqual(git("write-tree").stdout.decode().strip(), base_trees[0])
            checkpoints = await asyncio.gather(*(broker.read_checkpoint(owner, t, "review") for t in tasks))
            (left, left_patch), (right, right_patch) = checkpoints
            self.assertEqual(left["base_commit"], right["base_commit"])
            for patch in (left_patch, right_patch):
                git("apply", "--index", "--check", "-", data=patch)
            git("apply", "--index", "-", data=left_patch)
            self.assertEqual(git("write-tree").stdout.decode().strip(), left["tree"])
            conflict = git("apply", "--index", "--check", "-", data=right_patch, check=False)
            self.assertNotEqual(conflict.returncode, 0)
            self.assertIn(b"seed.txt", conflict.stderr)
            self.assertEqual(git("write-tree").stdout.decode().strip(), left["tree"])
            self.assertFalse((integration / "right.txt").exists())

            row = await broker._row(leases[0].allocation)
            await asyncio.sleep(max(0, row["expires_at"] - time.time()) + 0.05)
            with self.assertRaises(CapabilityUnavailable):
                await broker.admit_target(contexts[0], leases[0].host_id, leases[0].generation)
            later = replace(owner, session_id="another-verified-conversation")
            restored, duplicate = await asyncio.gather(
                broker.attach(later, tasks[0]), broker.attach(owner, tasks[0])
            )
            self.assertEqual(restored, duplicate)
            self.assertEqual(restored.machine_id, leases[0].machine_id)
            self.assertGreater(restored.generation, leases[0].generation)
            with self.assertRaises(CapabilityUnavailable):
                await broker.admit_target(contexts[0], leases[0].host_id, leases[0].generation)
            recovered = await stack.enter_async_context(
                broker.tools(self.runtime.capabilities, later, restored)
            )
            for ctx, label in ((recovered, "left"), (contexts[1], "right")):
                self.assertEqual(
                    (await self.call(ctx, "read_text_file", path="seed.txt"))["content"][0]["text"],
                    label + "\n",
                )
