"""Optional end-to-end qualification with installed upstream tools and dedicated Chrome profiles."""

import asyncio
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import threading
import time
import unittest
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import (
    ExecutionContext,
    PrincipalRef,
    RunRequest,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_core.computer_tasks import ComputerConnection, ComputerTask, TaskComputerBroker
from openagent_core.contracts import canonical_json
from openagent_core.engine import AgentExecutor
from openagent_core.runtime import current_runtime, current_execution_context
from openagent_storage_sqlite import SqliteRuntimeStore


class CookiePage(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if self.path.startswith("/set/"):
            self.send_header(
                "Set-Cookie", "workspace=" + self.path.rsplit("/", 1)[-1] + "; Path=/; Max-Age=300"
            )
        self.end_headers()
        self.wfile.write(
            (
                "<html><body><main>Session " + self.headers.get("Cookie", "empty") + "</main></body></html>"
            ).encode()
        )

    def log_message(self, *args):
        pass


class RegisteredMachines:
    """Fixture infrastructure. Tool execution goes through real CapabilityHost providers."""

    def __init__(self, root):
        self.root = root
        self.hosts = {}
        self.generations = {}
        self.owners = {}

    async def ensure(self, allocation):
        from openagent_capability_host import CapabilityHost, HostPaths, MCPStdioServer, PluginSpec
        from openagent_filesystem.server import FilesystemServer
        from openagent_shell.server import ShellServer

        machine = allocation.key
        if machine in self.hosts:
            return machine
        root = self.root / machine
        root.mkdir(exist_ok=True)
        sidecar = MCPStdioServer(
            PluginSpec(
                "agent-in-chrome", (shutil.which("node"), os.environ["OPENAGENT_TEST_CHROME_SIDECAR"])
            ),
            environment={
                "PATH": os.environ["PATH"],
                "HOME": str(root),
                "OPENAGENT_CHROME_HEADLESS": "true",
                "OPENAGENT_CHROME_PROFILE_DIR": str(root / "profile"),
                "OPENAGENT_CHROME_TRANSPORT": "pipe",
                "OPENAGENT_CHROME_BINARY": os.environ["OPENAGENT_CHROME_BINARY"],
            },
        )
        try:
            await sidecar.start()
            host = CapabilityHost(
                paths=HostPaths(root / "host"),
                servers=(FilesystemServer(root, allowed_roots=[root]), ShellServer(root), sidecar),
            )
            await host.set_consent(True)
        except BaseException:
            await sidecar.close()
            raise
        self.hosts[machine], self.owners[machine] = host, allocation
        self.generations[machine] = self.generations.get(machine, 0) + 1
        return machine

    async def restart(self, machine):
        await self.hosts.pop(machine).close()
        await self.ensure(self.owners[machine])

    async def connection(self, allocation, machine_id):
        if self.owners[machine_id] != allocation:
            raise PermissionError("Wrong enrollment")
        return ComputerConnection(machine_id, machine_id, self.generations[machine_id], time.time() + 120)

    async def catalog(self, context, selectors):
        machine = selectors["host_id"]
        generation = self.generations[machine]
        rows = []
        for manifest in await self.hosts[machine].catalog():
            if selectors.get("server", manifest["name"]) == manifest["name"]:
                rows.append(
                    {
                        "host_id": machine,
                        "generation": generation,
                        "server": manifest["name"],
                        "version": manifest["version"],
                        "tools": manifest["tools"],
                        "catalog_digest": sha256(canonical_json(manifest).encode()).hexdigest(),
                    }
                )
        return {"servers": rows}

    async def call(self, context, scope, name, args):
        machine = scope["host_id"]
        generation = self.generations[machine]
        await self.broker.admit_target(context, machine, generation)
        if scope["generation"] != generation:
            raise PermissionError("Stale enrollment")
        result = await self.hosts[machine].call(
            scope["server"], name, args, principal=context.initiator.key
        )
        if result.is_error:
            raise RuntimeError(str(result.content))
        return result.to_wire()

    async def close(self):
        await asyncio.gather(*(host.close() for host in self.hosts.values()))


class ToolTurn:
    def __init__(self, url):
        self.url = url

    async def run_stream(self, prompt, **kwargs):
        context, runtime = current_execution_context(), current_runtime()
        refs = {item.name: item.tool_ref for item in await runtime.capabilities.discover(context)}

        async def act(name, **args):
            return await runtime.capabilities.call_tool(refs[name], args, context)

        subject = context.initiator.subject_id
        if prompt == "initialize":
            await act("write_file", path="workspace-id", content=subject)
        file = await act("read_text_file", path="workspace-id")
        shell = await act("shell_exec", command="cat workspace-id")
        tabs = await act("tabs_context_mcp", createIfEmpty=True)
        tab = json.loads(tabs["content"][0]["text"].splitlines()[0])["availableTabs"][0]["tabId"]
        if prompt == "initialize":
            await act("navigate", tabId=tab, url=self.url + "/set/" + subject)
        await act("navigate", tabId=tab, url=self.url + "/read")
        page = await act("get_page_text", tabId=tab)
        answer = canonical_json(
            {
                "file": file["content"][0]["text"],
                "shell": shell["structuredContent"]["stdout"].strip(),
                "page": page["content"][0]["text"].splitlines()[-1],
            }
        )
        yield {"kind": "done", "text": answer, "final_answer": {"text": answer}}


@unittest.skipUnless(
    os.getenv("OPENAGENT_TEST_CHROME_SIDECAR") and os.getenv("OPENAGENT_CHROME_BINARY"),
    "Requires installed filesystem/shell and a dedicated Chrome MCP sidecar",
)
class ComputerDeviceQualification(unittest.IsolatedAsyncioTestCase):
    async def test_separate_chrome_cookies_shell_and_files_survive_reattachment(self):
        root = Path(self.enterContext(TemporaryDirectory()))
        web = ThreadingHTTPServer(("127.0.0.1", 0), CookiePage)
        thread = threading.Thread(target=web.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(web.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(web.shutdown)
        fleet = RegisteredMachines(root)
        self.addAsyncCleanup(fleet.close)

        class Policy:
            async def authorize(self, context, action, resource, *, audience=()):
                return all(p == context.initiator for p in audience)

        policy = Policy()
        engine = create_async_engine(
            os.getenv("OPENAGENT_TEST_POSTGRES_URL") or "sqlite+aiosqlite:///" + str(root / "control.db")
        )
        self.addAsyncCleanup(engine.dispose)
        broker = fleet.broker = TaskComputerBroker(engine, policy, fleet, fleet)
        await broker.initialize()
        runtime = Runtime(
            RuntimeSettings("agent"),
            RuntimeServices(
                SqliteRuntimeStore(root / "runs.db"),
                AgentExecutor(ToolTurn(f"http://127.0.0.1:{web.server_port}")),
                policy,
            ),
        )
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        tenant = uuid4().hex

        def context(user):
            p = PrincipalRef("host", tenant, user)
            return ExecutionContext(p, p, p, "session-" + user, "agent", (p,))

        contexts = [context("alpha"), context("bravo")]
        task = ComputerTask("project", "work", "sealed-base")
        machines = await asyncio.gather(*(broker.provision(c, task) for c in contexts))
        self.assertEqual(len(set(machines)), 2)

        async def turn(c, lease, stage):
            async with broker.tools(runtime.capabilities, c, lease) as bound:
                record = await runtime.submit(
                    RunRequest(uuid4().hex, bound.session_id, uuid4().hex, stage), bound
                )
                result = await asyncio.wait_for(runtime.wait(record.run_id, bound), 90)
                self.assertEqual(result.status, "success", result)
                answer = json.loads(result.output)
                who = c.initiator.subject_id
                self.assertEqual(answer, {"file": who, "shell": who, "page": "Session workspace=" + who})
                receipts = [
                    event.payload["binding"]["execution_host"]
                    for event in await runtime.events(record.run_id, 0, bound)
                    if event.kind == "tool.invoking"
                ]
                self.assertGreaterEqual(len(receipts), 4)
                self.assertTrue(
                    all(
                        (host["instance_id"], host["generation"], host["catalog_partition"])
                        == (lease.host_id, str(lease.generation), lease.partition)
                        for host in receipts
                    )
                )

        leases = await asyncio.gather(*(broker.attach(c, task) for c in contexts))
        await asyncio.gather(*(turn(c, lease, "initialize") for c, lease in zip(contexts, leases)))
        for lease in leases:
            self.assertTrue(await broker.disconnected(lease))
        await asyncio.gather(*(fleet.restart(lease.machine_id) for lease in leases))
        resumed = await asyncio.gather(*(broker.attach(c, task) for c in contexts))
        await asyncio.gather(*(turn(c, lease, "resume") for c, lease in zip(contexts, resumed)))
