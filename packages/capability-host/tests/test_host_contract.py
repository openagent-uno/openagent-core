import asyncio
import sys
import subprocess
from pathlib import Path

import pytest

from openagent_capability_host import CapabilityHost, HostPaths, PluginSpec
from openagent_tool_protocol import HostError, ServerManifest, ToolManifest, ToolResult


class Echo:
    manifest = ServerManifest("echo", "1", "Injected fixture", (
        ToolManifest("read", "Return fixture value", {"type": "object"}),
    ))

    def __init__(self, value):
        self.value = value
        self.calls = 0
        self.closed = False

    async def call(self, name, arguments):
        self.calls += 1
        return ToolResult(content=[{"type": "text", "text": self.value}],
                          structured_content={"value": self.value}, meta={"fixture": True})

    async def close(self):
        self.closed = True


def test_construction_has_no_resources_or_implicit_tools(tmp_path):
    home = tmp_path / "not-created"
    host = CapabilityHost(paths=HostPaths(home))
    assert not home.exists()
    assert host._servers == {}
    subprocess.run([sys.executable, "-c", "import sys; import openagent_capability_host; assert not any(name in sys.modules for name in ('openagent_filesystem', 'openagent_host_tools', 'openagent_core'))"], check=True)


@pytest.mark.asyncio
async def test_overlapping_principals_and_keys_are_isolated_and_close_is_owned(tmp_path):
    left, right = Echo("left"), Echo("right")
    a = CapabilityHost(paths=HostPaths(tmp_path / "a"), servers=(left,))
    b = CapabilityHost(paths=HostPaths(tmp_path / "b"), servers=(right,))
    principal = {"authority": "host", "tenant_id": "one", "subject_id": "same", "kind": "person"}
    await a.start()
    await b.start()
    await a.set_consent(True)
    await b.set_consent(True)
    try:
        for host, expected in ((a, "left"), (b, "right")):
            result = await host.call("echo", "read", {}, principal=principal, call_id="same")
            assert result.structured_content == {"value": expected}
            assert result.meta["fixture"] is True
            assert "openagent/location" not in result.meta
        await a.close()
        assert left.closed and not right.closed
        assert (await b.call("echo", "read", {}, principal=principal, call_id="same")).meta["openagent/replayed"]
        await b.set_consent(False)
        with pytest.raises(HostError, match="disabled"):
            await b.call("echo", "read", {}, principal=principal)
        assert right.calls == 1
    finally:
        await b.close()


@pytest.mark.asyncio
async def test_fixed_catalog_does_not_load_persisted_plugins(tmp_path):
    home = tmp_path / "fixed"
    host = CapabilityHost(paths=HostPaths(home), servers=(Echo("fixed"),))
    host.plugin_store.save([PluginSpec("unexpected", ("this-command-must-never-run",))])
    await host.start()
    await host.set_consent(True)
    try:
        assert [item["name"] for item in await host.catalog()] == ["echo"]
    finally:
        await host.close()


def test_duplicate_registration_is_rejected_before_side_effects(tmp_path):
    home = tmp_path / "duplicate"
    with pytest.raises(ValueError, match="unique"):
        CapabilityHost(paths=HostPaths(home), servers=(Echo("a"), Echo("b")))
    assert not home.exists()
