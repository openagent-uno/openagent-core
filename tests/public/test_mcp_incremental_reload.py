"""Adding or editing one MCP source keeps the other sources' registrations live."""
from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from openagent_core.capabilities import CapabilityCatalog
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.mcp._runtime import Toolkit
from openagent_core.mcp.pool import MCPPool, _ServerSpec
from openagent_core.runtime import execution_scope


class AllowAll:
    async def authorize(self, context, action, resource, *, audience=()):
        return True


def toolkit(name):
    def read(value: str = "x"):
        """Read a fixture value."""
        return name
    return Toolkit(name=name, tools=[read])


class IncrementalReloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.catalog = CapabilityCatalog(AllowAll(), allow_dynamic=True)
        self.runtime = SimpleNamespace(capabilities=self.catalog, settings=SimpleNamespace(workspace=Path("."), environment=()))
        principal = PrincipalRef("test", "tenant", "owner")
        self.context = ExecutionContext(principal, principal, principal, "session", "agent", (principal,))
        self.specs = [_ServerSpec(name="shell", command=["shell-server"]),
                      _ServerSpec(name="vault", command=["vault-server"])]
        self.pool = MCPPool(list(self.specs))
        self.built = []

        async def build(spec):
            built = toolkit(spec.name)
            self.built.append(spec.name)
            return built

        self.pool._build_and_enter_toolkit = build
        await self.pool.connect_all()
        self.pool.bind_capability_catalog(self.catalog)
        self.pool._db = object()

    async def asyncTearDown(self):
        self.pool._db = None
        await self.pool.close_all()

    async def _visible(self, revision):
        with execution_scope(self.runtime, self.context, "run", capability_revision=revision):
            return {tool.source_id for tool in await self.catalog.discover(self.context)}

    async def _reload(self, specs):
        with patch.object(self.pool, "rebuild_specs", AsyncMock(return_value=specs)):
            await self.pool.reload()

    async def test_adding_a_source_does_not_revoke_unchanged_sources_for_a_running_turn(self):
        running_turn = self.catalog.snapshot_revision()
        self.assertEqual({"shell", "vault"}, await self._visible(running_turn))
        shell = self.pool.toolkit_by_name("shell")

        await self._reload([*self.specs, _ServerSpec(name="staff", url="https://staff.example/mcp")])

        self.assertIs(shell, self.pool.toolkit_by_name("shell"))
        self.assertEqual(["shell", "vault", "staff"], self.built)
        # The running turn keeps every tool it started with ...
        self.assertEqual({"shell", "vault"}, await self._visible(running_turn))
        # ... and the next turn also sees the new source.
        self.assertEqual({"shell", "vault", "staff"}, await self._visible(self.catalog.snapshot_revision()))

    async def test_editing_a_source_restarts_only_that_source(self):
        vault = self.pool.toolkit_by_name("vault")
        edited = [self.specs[0], _ServerSpec(name="vault", command=["vault-server", "--v2"])]
        await self._reload(edited)
        self.assertIsNot(vault, self.pool.toolkit_by_name("vault"))
        self.assertEqual(["shell", "vault", "vault"], self.built)

    async def test_removing_a_source_closes_only_that_source(self):
        shell = self.pool.toolkit_by_name("shell")
        await self._reload([self.specs[0]])
        self.assertIs(shell, self.pool.toolkit_by_name("shell"))
        self.assertIsNone(self.pool.toolkit_by_name("vault"))
        self.assertEqual({"shell"}, await self._visible(self.catalog.snapshot_revision()))


if __name__ == "__main__":
    unittest.main()
