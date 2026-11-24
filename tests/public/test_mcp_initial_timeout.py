"""Exercise an initial stdio handshake timeout through an authenticated agent turn."""

import asyncio
from dataclasses import replace
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from openagent_core import Runtime, RuntimeServices, RuntimeSettings, RunRequest
from openagent_core.capabilities import CapabilityCatalog
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.engine import Agent, AgentExecutor
from openagent_core.mcp.pool import MCPPool, _ServerSpec
from openagent_core.models.base import BaseModel
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    def __init__(self):
        self.allow_status = True
        self.status_audiences = []

    async def authorize(self, context, action, resource, *, audience=()):
        if action == "tool.discover_source_status":
            self.status_audiences.append(audience)
            return (
                self.allow_status
                and resource.kind == "capability"
                and resource.resource_id == "sample"
                and audience == (context.initiator,)
                and context.initiator.subject_id == "owner"
            )
        return True


class InitialHandshakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.scope = patch("openagent_core.mcp.pool._MCP_CONNECT_TIMEOUT", 0.25)
        self.scope.start()
        self.addCleanup(self.scope.stop)
        fixture = Path(__file__).parent / "fixtures" / "mcp_startup_stdio.py"
        self.pool = MCPPool.from_config(
            [
                {
                    "name": "sample",
                    "command": [sys.executable],
                    "args": [str(fixture), str(self.directory)],
                },
                {"builtin": "tool-search"},
            ],
            include_defaults=False,
        )
        self.policy = Policy()
        self.catalog = CapabilityCatalog(self.policy)
        p = PrincipalRef("test", "tenant", "owner")
        self.ctx = ExecutionContext(p, p, p, "session", "agent", (p,))

    async def test_bounded_retry_collapses_demand_and_shutdown_cancels_its_owner(self):
        attempts = 0
        entered, stopped = asyncio.Event(), asyncio.Event()
        pool = MCPPool([_ServerSpec("sample", command=["unused"])])
        pool.bind_capability_catalog(self.catalog)

        async def build(spec):
            nonlocal attempts
            attempts += 1
            if attempts == 5:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    stopped.set()
            pool._record_connection_failure("sample", TimeoutError(), "connect")
            return None

        self.addAsyncCleanup(pool.close_all)
        with (
            patch.object(pool, "_build_and_enter_toolkit", build),
            patch.dict(
                "os.environ",
                {"OPENAGENT_MCP_STARTUP_RETRY_SECONDS": ".05"},
            ),
        ):
            await pool.connect_all()
            for _ in range(20):
                self.assertEqual(
                    len(await self.catalog.temporarily_unavailable_sources(self.ctx)), 1
                )
            await asyncio.wait_for(pool._startup_retries["sample"], 2)
            self.assertEqual(attempts, 4)
            await self.catalog.temporarily_unavailable_sources(self.ctx)
            self.assertFalse(pool._startup_retries)
            pool._startup_retry_after["sample"] = 0
            self.policy.allow_status = False
            self.assertFalse(
                await self.catalog.temporarily_unavailable_sources(self.ctx)
            )
            self.assertFalse(pool._startup_retries)
            self.policy.allow_status = True
            await asyncio.gather(
                *(
                    self.catalog.temporarily_unavailable_sources(self.ctx)
                    for _ in range(10)
                )
            )
            await asyncio.wait_for(entered.wait(), 1)
            await pool.close_all()
            self.assertTrue(stopped.is_set())
            self.assertEqual(attempts, 5)
            self.assertFalse(pool._startup_retries)
            self.assertFalse(self.catalog.has_source("sample", self.ctx))

    async def test_non_timeout_and_nonlocal_failures_never_install_status_or_retry(
        self,
    ):
        specs = [
            _ServerSpec("missing", command=["unused"]),
            _ServerSpec("remote", url="https://example.invalid"),
        ]
        pool = MCPPool(specs)
        pool.bind_capability_catalog(self.catalog)
        self.addAsyncCleanup(pool.close_all)

        async def fail(spec):
            error = TimeoutError() if spec.url else FileNotFoundError()
            pool._record_connection_failure(spec.name, error, "connect")

        with patch.object(pool, "_build_and_enter_toolkit", fail):
            await pool.connect_all()
        self.assertEqual(len(pool.connection_status()), 2)
        self.assertFalse(pool._startup_retries)
        self.assertFalse(await self.catalog.temporarily_unavailable_sources(self.ctx))
        self.assertFalse(self.catalog.has_source("missing", self.ctx))

    async def test_authorized_discovery_and_call_recover_in_subsequent_normal_turn(
        self,
    ):
        observed = []
        prompts = []
        pool = self.pool

        class BrokerModel(BaseModel):
            model = "fixture"

            async def stream(self, messages, system=None, **kwargs):
                prompts.append(str(system))
                functions = pool.toolkit_by_name("tool-search").async_functions
                listed = await functions["tool_search_list_servers"].entrypoint()
                try:
                    page = await functions["tool_search_list_tools"].entrypoint(
                        source_ref="sample"
                    )
                except LookupError:
                    page = None
                answer = None
                if page and page["tools"]:
                    tool_ref = page["tools"][0]["tool_ref"]
                    answer = await functions["tool_search_call_tool"].entrypoint(
                        tool_ref=tool_ref, args={}
                    )
                observed.append((listed, page, answer))
                yield "Turn complete"

            async def generate(self, *args, **kwargs):
                return "Turn complete"

        agent = Agent(
            model=BrokerModel(),
            mcp_pool=pool,
            system_prompt="Use the sample",
            config={"_enabled_prompt_modules": []},
        )
        executor = AgentExecutor(agent)
        settings = RuntimeSettings(
            "agent",
            self.directory,
            environment=(("OPENAGENT_MCP_STARTUP_RETRY_SECONDS", ".25"),),
        )
        runtime = Runtime(
            settings,
            RuntimeServices(
                SqliteRuntimeStore(self.directory / "state.db"),
                executor,
                self.policy,
                self.catalog,
            ),
            modules=(executor,),
        )
        pool.bind_capability_catalog(self.catalog)
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        self.addAsyncCleanup(pool.close_all)

        async def turn(label, context):
            await runtime.submit(
                RunRequest(label, context.session_id, label, "Read the sample"), context
            )
            result = await runtime.wait(label, context)
            self.assertEqual(result.status, "success", result)
            return observed[-1]

        listed, page, answer = await turn("first", self.ctx)
        self.assertEqual(
            {item["name"]: item.get("status") for item in listed},
            {"sample": "temporarily_unavailable"},
            (pool.connection_status(), self.policy.status_audiences),
        )
        self.assertEqual(
            (page["status"], page["tools"], answer),
            ("temporarily_unavailable", [], None),
        )
        self.assertTrue(self.catalog.has_source("sample", self.ctx))
        self.assertIn("temporarily unavailable", prompts[-1])
        other = PrincipalRef("test", "tenant", "other")
        shared = replace(
            self.ctx, session_id="shared", audience=(self.ctx.initiator, other)
        )
        self.assertEqual(await turn("shared-denied", shared), ([], None, None))
        self.assertNotIn("temporarily unavailable", prompts[-1])
        self.policy.allow_status = False
        self.assertEqual(await turn("only-other-tool", self.ctx), ([], None, None))
        self.policy.allow_status = True
        self.assertFalse((self.directory / "calls").exists())
        self.assertIn(shared.audience, self.policy.status_audiences)
        (self.directory / "ready").touch()
        async with asyncio.timeout(8):
            while pool.connection_status()[0]["state"] != "ready":
                await asyncio.sleep(0.02)
        self.assertFalse((self.directory / "calls").exists())
        listed, page, answer = await turn("after", self.ctx)
        self.assertEqual(page["total"], 1)
        self.assertIn("local-stdio-answer", str(answer))
        self.assertEqual((self.directory / "calls").read_text(), "call\n")
        self.assertLessEqual(
            len((self.directory / "starts").read_text().splitlines()), 4
        )
