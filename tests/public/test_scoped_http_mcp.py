import asyncio
import time
import unittest

from aiohttp import web
from aiohttp.test_utils import TestServer

from openagent_core.mcp.scoped_http import McpHttpGrant, ScopedHttpMcpSource


class ScopedHttpExchange(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_discovery_call_and_inflight_revocation_on_one_endpoint(self):
        permits = set()
        observed = []
        reached, release = asyncio.Event(), asyncio.Event()

        class Grants:
            async def issue(self, owner, tool):
                scope = tool or "catalog"
                return McpHttpGrant(owner, time.time() + 60, owner + "." + scope)

            async def valid(self, owner, tool, binding):
                return binding == owner and owner in permits

        async def serve(request):
            if request.method != "POST":
                return web.Response(status=405)
            token = request.headers["Authorization"].removeprefix("Bearer ")
            owner, scope = token.split(".")
            body = await request.json()
            method = body["method"]
            if "id" not in body:
                return web.Response(status=202)
            if method == "initialize":
                result = {"protocolVersion": body["params"]["protocolVersion"], "capabilities": {"tools": {}},
                          "serverInfo": {"name": "fixture", "version": "1"}}
            elif method == "tools/list":
                result = {"tools": [{"name": "read", "description": owner,
                                     "inputSchema": {"type": "object"}}]}
            elif method == "tools/call":
                self.assertEqual(scope, body["params"]["name"])
                observed.append((owner, scope))
                if body["params"]["arguments"].get("wait"):
                    reached.set()
                    await release.wait()
                result = {"content": [{"type": "text", "text": owner}]}
            else:
                return web.Response(status=400)
            return web.json_response({"jsonrpc": "2.0", "id": body["id"], "result": result})

        app = web.Application()
        app.router.add_route("*", "/mcp", serve)
        async with TestServer(app) as server:
            source = ScopedHttpMcpSource(str(server.make_url("/mcp")), Grants(), name_prefix="service_")
            permits.update(("first", "second"))
            for owner, catalog in zip(("first","second"),await asyncio.gather(
                    source.discover("first"), source.discover("second"))):
                self.assertEqual([(d.name,d.description) for d in catalog], [("service_read",owner)])
            records = await asyncio.gather(*(source.call_tool("service_read", {}, owner)
                                             for owner in ("first","second")))
            self.assertEqual([r["content"][0]["text"] for r in records], ["first","second"])
            pending = asyncio.create_task(source.call_tool("service_read", {"wait": True}, "first"))
            await asyncio.wait_for(reached.wait(), 5)
            permits.remove("first")
            release.set()
            with self.assertRaises(PermissionError):
                await pending
            with self.assertRaises(PermissionError):
                await source.call_tool("service_read", {}, "first")
            self.assertEqual(sorted(observed), [("first","read"),("first","read"),("second","read")])
