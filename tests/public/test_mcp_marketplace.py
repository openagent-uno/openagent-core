from pathlib import Path
import unittest

import httpx

from openagent_core import CapabilityCatalog, ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.mcp.marketplace import Marketplace
from openagent_core.mcp.marketplace_capability import MarketplaceCapability
from openagent_core.mcp.public_https import _PublicHTTPSTransport, validate_public_https
from openagent_core.runtime import execution_scope


class MarketplaceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        principal = PrincipalRef("host-a", "tenant-a", "owner")
        self.context = ExecutionContext(principal, principal, principal, "session-a", "agent-a", (principal,))
        self.entry = {"name": "io.example/tool", "version": "1.0.0", "description": "A sample service",
                      "remotes": [{"type": "streamable-http", "url": "https://tools.example/{region}"}]}
        self.calls = []
        self.deny_install = False
        self.catalog = CapabilityCatalog(self)

        async def registry(request):
            self.assertEqual("registry.modelcontextprotocol.io", request.url.host)
            if request.url.path == "/v0.1/servers":
                self.assertEqual("latest", request.url.params["version"])
                self.assertEqual("tool", request.url.params["search"])
                return httpx.Response(200, json={"servers": [{"server": self.entry}],
                                                 "metadata": {"nextCursor": "next"}})
            self.assertIn("/io.example%2Ftool/versions/", request.url.raw_path.decode())
            return httpx.Response(200, json={"server": self.entry})

        self.client = httpx.AsyncClient(transport=httpx.MockTransport(registry))
        source = MarketplaceCapability(Marketplace(self.client), self)
        self.catalog.register("marketplace", source, source, target_label="Marketplace")
        self.runtime = Runtime(RuntimeSettings("agent-a", Path.cwd()),
                               RuntimeServices(self, None, self, self.catalog))

    async def asyncTearDown(self):
        await self.client.aclose()

    async def authorize(self, context, action, resource, *, audience=()):
        return audience == self.context.audience and (action != "catalog.install" or not self.deny_install)

    async def begin_tool(self, run_id, call_id, binding, arguments):
        pass

    async def finish_tool(self, run_id, call_id, *, result=None, error=None):
        pass

    def supports(self, kind, choice):
        return kind == "remote"

    async def install(self, context, entry, recipe):
        self.calls.append(recipe)
        return "sample-mcp"

    async def test_search_inspect_install_and_share_exact_registry_reference(self):
        with execution_scope(self.runtime, self.context, "run-a"):
            tools = {item.name: item.tool_ref for item in await self.catalog.discover(self.context)}
            results = await self.catalog.call_tool(tools["search_mcp_marketplace"], {"query": "tool"}, self.context)
            self.assertEqual("next", results["next_cursor"])
            selection = {"name": "io.example/tool", "version": "1.0.0"}
            detail = await self.catalog.call_tool(tools["inspect_mcp_marketplace_entry"], selection, self.context)
            install = {**selection, "revision": detail["revision"], "kind": "remote", "index": 0,
                       "settings": {"region": "eu"}}
            result = await self.catalog.call_tool(tools["install_mcp_marketplace_entry"], install, self.context)
            self.assertEqual("sample-mcp", result["installation"])
            self.assertEqual("https://tools.example/eu", self.calls[0]["url"])
            self.assertEqual(results["servers"][0]["registry_url"], result["registry_url"])
            with self.assertRaises(ValueError):
                await self.catalog.call_tool(tools["install_mcp_marketplace_entry"],
                    {**install, "headers": {"Authorization": "test-only"}}, self.context)
            self.entry["description"] = "Changed since inspection"
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools["install_mcp_marketplace_entry"], install, self.context)
            self.deny_install = True
            self.assertNotIn("install_mcp_marketplace_entry", {
                tool.name for tool in await self.catalog.discover(self.context)})
        self.assertEqual(1, len(self.calls))

    async def test_requires_host_credential_binding_and_keeps_secret_out_of_inspection(self):
        self.entry["remotes"][0]["headers"] = [
            {"name": "Authorization", "isSecret": True, "isRequired": True, "value": "Bearer {key}"}
        ]
        with execution_scope(self.runtime, self.context, "run-a"):
            tools = {item.name: item.tool_ref for item in await self.catalog.discover(self.context)}
            selection = {"name": "io.example/tool", "version": "1.0.0"}
            detail = await self.catalog.call_tool(tools["inspect_mcp_marketplace_entry"], selection, self.context)
            self.assertNotIn("Bearer", str(detail))
            with self.assertRaises(CapabilityUnavailable):
                await self.catalog.call_tool(tools["install_mcp_marketplace_entry"],
                    {**selection, "revision": detail["revision"], "kind": "remote", "index": 0}, self.context)
        self.assertEqual([], self.calls)


class PublicHTTPSTests(unittest.IsolatedAsyncioTestCase):
    async def test_pins_dns_with_original_tls_hostname_and_rejects_redirect(self):
        sent = []

        async def resolve(host):
            self.assertEqual("tools.example", host)
            return ("8.8.8.8",)

        async def upstream(request):
            sent.append(request)
            return httpx.Response(302, headers={"Location": "https://127.0.0.1/mcp"})

        guard = _PublicHTTPSTransport("https://tools.example/mcp", httpx.MockTransport(upstream), resolve)
        async with httpx.AsyncClient(transport=guard) as client:
            with self.assertRaises(httpx.ConnectError):
                await client.get("https://tools.example/mcp")
        request, = sent
        self.assertEqual("8.8.8.8", request.url.host)
        self.assertEqual("tools.example", request.headers["Host"])
        self.assertEqual("tools.example", request.extensions["sni_hostname"])

    async def test_denies_mixed_dns_and_invalid_endpoints(self):
        async def resolve(host):
            return ("8.8.8.8", "127.0.0.1")

        async def upstream(request):
            self.fail("A rejected endpoint reached the transport")

        guard = _PublicHTTPSTransport("https://tools.example/mcp", httpx.MockTransport(upstream), resolve)
        async with httpx.AsyncClient(transport=guard) as client:
            with self.assertRaises(httpx.ConnectError):
                await client.get("https://tools.example/mcp")
            with self.assertRaises(httpx.ConnectError):
                await client.get("https://different.example/mcp")
        for endpoint in ("http://tools.example", "https://user@tools.example/mcp",
                         "https://tools.example:8443/mcp", "https://tools.example/mcp?token=x",
                         "https://127.0.0.1", "https://host.local"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                validate_public_https(endpoint)
