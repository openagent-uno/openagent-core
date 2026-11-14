"""MCP sources reference host secrets instead of storing credentials."""
import json
import logging
import unittest
from unittest.mock import patch

from openagent_core.mcp import secret_refs
from openagent_core.mcp.pool import MCPPool, _ServerSpec
from openagent_core.mcp.secret_refs import SecretReferenceError

VALUE = "fixture-secret-value-0123456789"
NAME = "OPENAGENT_MCP_SECRET_FIXTURE_TOKEN"


class FakeTools:
    def __init__(self, server_params=None, transport=None, **_kwargs):
        self.server_params = server_params
        self.transport = transport


class ResolutionTests(unittest.TestCase):
    def test_reference_with_reserved_prefix_is_replaced(self):
        self.assertEqual(
            "Bearer " + VALUE,
            secret_refs.resolve_value("Bearer ${secret:%s}" % NAME, {NAME: VALUE}),
        )

    def test_name_outside_reserved_prefix_is_refused_without_reading_it(self):
        env = {"MODEL_API_KEY": VALUE}
        with self.assertRaises(SecretReferenceError) as caught:
            secret_refs.resolve_value("Bearer ${secret:MODEL_API_KEY}", env)
        self.assertEqual("not_allowed", caught.exception.reason)
        self.assertNotIn(VALUE, str(caught.exception))
        for name in ("openagent_mcp_secret_x", "OPENAGENT_MCP_SECRET_", "OPENAGENT_MCP_SECRET_A-B", ""):
            with self.assertRaises(SecretReferenceError):
                secret_refs.resolve_value("${secret:%s}" % name, {name: VALUE})

    def test_missing_or_empty_variable_is_an_error(self):
        for env in ({}, {NAME: ""}):
            with self.assertRaises(SecretReferenceError) as caught:
                secret_refs.resolve_value("${secret:%s}" % NAME, env)
            self.assertEqual("missing", caught.exception.reason)
            self.assertIn(NAME, str(caught.exception))

    def test_values_without_references_are_unchanged(self):
        self.assertEqual({"User-Agent": "client/1"}, secret_refs.resolve_mapping({"User-Agent": "client/1"}, {}))
        self.assertIsNone(secret_refs.resolve_mapping(None, {}))

    def test_redaction_keeps_reference_only_values_and_masks_the_rest(self):
        redacted = secret_refs.redact_mapping({
            "Authorization": "Bearer ${secret:%s}" % NAME,
            "X-Key": "${secret:%s}" % NAME,
            "X-Raw": "Bearer raw-credential",
            "X-Mixed": "raw-credential ${secret:%s}" % NAME,
        })
        self.assertEqual("Bearer ${secret:%s}" % NAME, redacted["Authorization"])
        self.assertEqual("${secret:%s}" % NAME, redacted["X-Key"])
        self.assertEqual("***", redacted["X-Raw"])
        self.assertEqual("***", redacted["X-Mixed"])


class PoolResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def build(self, spec, environ):
        pool = MCPPool([spec])

        async def entered(toolkit, _spec):
            return toolkit

        with patch("openagent_core.mcp._runtime.mcp.mcp.MCPTools", FakeTools), \
             patch.dict("os.environ", environ, clear=False), \
             patch.object(pool, "_safe_enter", entered):
            return pool, await pool._build_and_enter_toolkit(spec)

    async def test_http_headers_are_resolved_only_in_the_transport(self):
        spec = _ServerSpec("remote-fixture", url="https://mcp.example.invalid/mcp",
                           headers={"Authorization": "Bearer ${secret:%s}" % NAME})
        pool, toolkit = await self.build(spec, {NAME: VALUE})
        self.assertEqual("Bearer " + VALUE, toolkit.server_params.headers["Authorization"])
        self.assertEqual("Bearer ${secret:%s}" % NAME, spec.headers["Authorization"])
        self.assertNotIn(VALUE, json.dumps(pool.connection_status()))

    async def test_stdio_environment_is_resolved(self):
        spec = _ServerSpec("stdio-fixture", command=["/bin/true"], env={"TOKEN": "${secret:%s}" % NAME})
        _pool, toolkit = await self.build(spec, {NAME: VALUE})
        self.assertEqual(VALUE, toolkit.server_params.env["TOKEN"])
        self.assertEqual("${secret:%s}" % NAME, spec.env["TOKEN"])

    async def test_unresolvable_reference_keeps_the_source_down_and_diagnosable(self):
        for header, environ in (
            ("Bearer ${secret:%s}" % NAME, {}),
            ("Bearer ${secret:MODEL_API_KEY}", {"MODEL_API_KEY": VALUE}),
        ):
            spec = _ServerSpec("remote-fixture", url="https://mcp.example.invalid/mcp",
                               headers={"Authorization": header})
            with self.assertLogs("openagent_core.mcp.pool", level=logging.WARNING) as logs, \
                 patch("openagent_core.mcp.pool.elog") as events:
                pool, toolkit = await self.build(spec, environ)
            self.assertIsNone(toolkit)
            status = pool.connection_status()
            self.assertEqual({"code": "secret_unavailable", "phase": "configure"}, status[0]["failure"])
            emitted = json.dumps([call.kwargs for call in events.call_args_list] + [str(call.args) for call in events.call_args_list])
            for text in ("\n".join(logs.output), json.dumps(status), emitted):
                self.assertNotIn(VALUE, text)
            self.assertIn("secret reference", "\n".join(logs.output))


class ManagerToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_headers_are_passed_to_the_host_and_masked_in_answers(self):
        from openagent_core.mcp.servers.mcp_manager import server as manager

        calls = []

        class Service:
            async def create(self, context, name, **values):
                calls.append(("create", name, values))
                return {"name": name, **values}

            async def update(self, context, name, **values):
                calls.append(("update", name, values))
                return {"name": name, "headers": {"X-Raw": "raw-credential"}, **values}

            async def get(self, context, name):
                return {"name": name, "headers": {"X-Raw": "raw-credential",
                                                   "Authorization": "Bearer ${secret:%s}" % NAME}}

            async def list(self, context):
                return [await self.get(context, "remote"), {"name": "plain", "enabled": True}]

        class Runtime:
            def service(self, name):
                return Service() if name == "catalog_management" else None

        headers = {"Authorization": "Bearer ${secret:%s}" % NAME, "User-Agent": "client/1"}
        with patch.object(manager, "current_runtime", Runtime), \
             patch.object(manager, "current_execution_context", lambda: object()):
            created = await manager.add_custom_mcp("remote", url="https://mcp.example.invalid/mcp", headers=headers)
            updated = await manager.update_mcp("remote", headers={"X-Raw": "raw-credential"})
            fetched = await manager.get_mcp("remote")
            listed = await manager.list_mcps()

        self.assertEqual(headers, calls[0][2]["headers"])
        self.assertEqual({"X-Raw": "raw-credential"}, calls[1][2]["headers"])
        self.assertEqual("Bearer ${secret:%s}" % NAME, created["headers"]["Authorization"])
        self.assertEqual("***", created["headers"]["User-Agent"])
        self.assertEqual("***", updated["headers"]["X-Raw"])
        self.assertEqual("***", fetched["headers"]["X-Raw"])
        self.assertNotIn("raw-credential", json.dumps([created, updated, fetched, listed]))
        self.assertEqual("plain", listed[1]["name"])


if __name__ == "__main__":
    unittest.main()
