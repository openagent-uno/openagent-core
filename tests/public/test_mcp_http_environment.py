"""Both MCP HTTP transports use explicit credentials and destination settings."""
import unittest
from dataclasses import asdict
from unittest.mock import patch
from aiohttp import web
from aiohttp.test_utils import TestServer
from openagent_core.mcp._runtime.mcp.params import SSEClientParams, StreamableHTTPClientParams

class MCPHttpEnvironment(unittest.IsolatedAsyncioTestCase):
    async def test_proxy_and_certificate_environment_cannot_redirect_either_transport(self):
        async def endpoint(request):
            self.assertEqual('Bearer configured',request.headers.get('Authorization'))
            return web.json_response({'ok':True})
        app=web.Application();app.router.add_get('/',endpoint)
        server=TestServer(app);await server.start_server()
        try:
            with patch.dict('os.environ',{'HTTP_PROXY':'http://127.0.0.1:1','HTTPS_PROXY':'http://127.0.0.1:1','ALL_PROXY':'http://127.0.0.1:1','NO_PROXY':'','SSL_CERT_FILE':'/missing/ambient-certificate'}):
                for transport in (SSEClientParams,StreamableHTTPClientParams):
                    params=asdict(transport(str(server.make_url('/')),headers={'Authorization':'Bearer configured'}))
                    async with params['httpx_client_factory'](headers=params['headers']) as client:
                        result=await client.get(params['url'])
                        self.assertEqual({'ok':True},result.json())
        finally:await server.close()
