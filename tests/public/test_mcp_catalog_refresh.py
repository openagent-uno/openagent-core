"""Complete MCP catalog snapshots and generation-bound stdio refresh."""
from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from mcp.types import Tool

from openagent_capability_host.config import PluginSpec
from openagent_capability_host.mcp_stdio import MCPStdioServer
from openagent_core.mcp._runtime.mcp.mcp import MCPTools
from openagent_tool_protocol import HostError


def definition(name):
    return Tool(name=name, inputSchema={"type": "object"})


def page(*names, cursor=None):
    return SimpleNamespace(tools=[definition(name) for name in names], nextCursor=cursor)


@pytest.mark.asyncio
async def test_runtime_reads_all_pages_before_filters_and_removes_stale_tools():
    session = SimpleNamespace(list_tools=AsyncMock(side_effect=[page("old", cursor="second"), page("kept")]))
    toolkit = MCPTools(session=session, tool_name_prefix="source")
    await toolkit.build_tools()
    assert set(toolkit.functions) == {"source_old", "source_kept"}
    assert session.list_tools.call_args_list[1].kwargs == {"cursor": "second"}
    toolkit.include_tools = ["new"]
    session.list_tools.side_effect = [page("kept", cursor="next"), page("new")]
    await toolkit.build_tools()
    assert set(toolkit.functions) == {"source_new"}


@pytest.mark.asyncio
@pytest.mark.parametrize("pages", [
    [page("first", cursor="repeat"), page("second", cursor="repeat")],
    [page("same", cursor="second"), page("same")],
    [page("first", cursor="")],
])
async def test_runtime_invalid_catalog_never_publishes_a_partial_snapshot(pages):
    session = SimpleNamespace(list_tools=AsyncMock(return_value=page("existing")))
    toolkit = MCPTools(session=session)
    await toolkit.build_tools()
    original = toolkit.functions
    session.list_tools.side_effect = pages
    with pytest.raises(ValueError):
        await toolkit.build_tools()
    assert toolkit.functions is original
    assert set(toolkit.functions) == {"existing"}


@pytest.mark.asyncio
async def test_runtime_session_change_cannot_publish_an_old_catalog():
    session = SimpleNamespace()
    toolkit = MCPTools(session=session)
    async def list_tools(**kwargs):
        toolkit.session = SimpleNamespace()
        return page("old_session_tool")
    session.list_tools = list_tools
    with pytest.raises(RuntimeError, match="session changed"):
        await toolkit.build_tools()
    assert toolkit.functions == {}


@pytest.mark.asyncio
async def test_runtime_connect_and_initialize_preserve_cancellation():
    session = SimpleNamespace(initialize=AsyncMock(side_effect=asyncio.CancelledError))
    toolkit = MCPTools(session=session)
    with pytest.raises(asyncio.CancelledError):
        await toolkit.initialize()
    with pytest.raises(asyncio.CancelledError):
        await toolkit.connect()
    assert not toolkit.initialized


# A real independent process proves the reader continues handling responses
# while catalog refresh is pending; no in-process transport mock hides deadlocks.
PLUGIN = r'''
import json,sys
changed=False
invalid=False
cancelled=False
slow_id=None

def emit(value):
 print(json.dumps(value),flush=True)

def notify():
 emit({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})

def tool(name):
 return {"name":name,"inputSchema":{"type":"object"},"annotations":{"readOnlyHint":True}}

for line in sys.stdin:
 frame=json.loads(line)
 request_id=frame.get("id")
 method=frame.get("method")
 if method=="notifications/cancelled":
  cancelled=frame.get("params",{}).get("requestId")==slow_id
  continue
 if request_id is None: continue
 if method=="initialize":
  result={"protocolVersion":"2024-11-05","capabilities":{"tools":{"listChanged":True}},"serverInfo":{"name":"fixture","version":"1"}}
 elif method=="tools/list":
  cursor=frame.get("params",{}).get("cursor")
  if cursor is None:
   result={"tools":[tool("new" if changed else "old")],"nextCursor":"second"}
  else:
   result={"tools":[tool("new" if invalid else "control"),tool("slow"),tool("status") ]}
 elif method=="tools/call":
  name=frame["params"]["name"]
  args=frame["params"].get("arguments",{})
  if name=="slow":
   slow_id=request_id
   continue
  if args.get("change"):
   changed=True
   invalid=args.get("invalid",False)
   for _ in range(5): notify()
  if args.get("wrong_id"):
   emit({"jsonrpc":"2.0","id":str(request_id),"result":{"structuredContent":{"forged":True}}})
  if args.get("bad_result"):
   result=[]
  elif args.get("bad_error"):
   emit({"jsonrpc":"2.0","id":request_id,"error":"not an error object"})
   continue
  else:
   result={"content":[{"type":"text","text":"ok"}],"structuredContent":{"cancelled":cancelled},"_meta":{"fixture":True}}
 else:
  result={}
 emit({"jsonrpc":"2.0","id":request_id,"result":result})
'''


def stdio(on_state_change=None):
    return MCPStdioServer(PluginSpec("fixture", (sys.executable, "-u", "-c", PLUGIN)),
                          startup_timeout=2, restart_limit=0, on_state_change=on_state_change)


@pytest.mark.asyncio
async def test_stdio_all_pages_hot_refresh_removed_tools_and_lossless_results():
    changed = asyncio.Event()
    server = stdio(lambda _: changed.set())
    await server.start()
    try:
        assert {tool.name for tool in server.manifest.tools} == {"old", "control", "slow", "status"}
        result = await server.call("control", {"change": True, "wrong_id": True})
        assert result.structured_content == {"cancelled": False}
        assert result.meta == {"fixture": True}
        await asyncio.wait_for(changed.wait(), 2)
        assert {tool.name for tool in server.manifest.tools} == {"new", "control", "slow", "status"}
        with pytest.raises(HostError, match="no tool"):
            await server.call("old", {})
        assert server.manifest.available
    finally:
        await server.close()
    assert server._catalog_task is None


@pytest.mark.asyncio
async def test_stdio_failed_refresh_invalidates_stale_catalog_and_can_recover():
    changed = asyncio.Event()
    server = stdio(lambda _: changed.set())
    await server.start()
    try:
        await server.call("control", {"change": True, "invalid": True})
        await asyncio.wait_for(changed.wait(), 2)
        assert not server.manifest.available
        assert server.manifest.tools == ()
        with pytest.raises(HostError, match="not running"):
            await server.call("old", {})
        # Recovery is initiated by the same live process, not a replayed call.
        changed.clear()
        await server._request("tools/call", {"name": "control", "arguments": {"change": True}})
        await asyncio.wait_for(changed.wait(), 2)
        assert server.manifest.available
        assert server.manifest.tool("new") is not None
    finally:
        await server.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("argument", ["bad_result", "bad_error"])
async def test_stdio_malformed_response_does_not_strand_reader(argument):
    server = stdio()
    await server.start()
    try:
        with pytest.raises(HostError) as error:
            await server.call("control", {argument: True})
        assert error.value.code == "plugin_protocol_error"
        result = await asyncio.wait_for(server.call("status", {}), 2)
        assert result.structured_content == {"cancelled": False}
        assert not server._reader.done()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_stdio_cancel_notifies_exact_request_without_replaying_mutation():
    server = stdio()
    await server.start()
    try:
        task = asyncio.create_task(server.call("slow", {}))
        # Wait until its request was written, then ensure the child consumed it.
        while not server._pending:
            await asyncio.sleep(0)
        await server.call("status", {})
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await server.call("status", {})
        assert result.structured_content == {"cancelled": True}
        assert not server._pending
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_stdio_rejects_wrong_generation_catalog_request_before_write():
    server = stdio()
    await server.start()
    try:
        with pytest.raises(HostError) as error:
            await server._list_tools(SimpleNamespace(stdin=True))
        assert error.value.code == "plugin_disconnected"
        assert server._next_id == 3  # initialize plus both catalog pages
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_runtime_refresh_preserves_explicit_non_mcp_functions():
    session = SimpleNamespace(list_tools=AsyncMock(side_effect=[page("old"), page("new")]))
    toolkit = MCPTools(session=session)
    custom = object()
    toolkit.functions["custom_host_tool"] = custom
    await toolkit.build_tools()
    await toolkit.build_tools()
    assert set(toolkit.functions) == {"new", "custom_host_tool"}
    assert toolkit.functions["custom_host_tool"] is custom


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["_MAX_CATALOG_PAGES", "_MAX_CATALOG_TOOLS"])
async def test_runtime_catalog_resource_limits_are_finite(monkeypatch, limit):
    import openagent_core.mcp._runtime.mcp.mcp as runtime_mcp
    monkeypatch.setattr(runtime_mcp, limit, 1)
    session = SimpleNamespace(list_tools=AsyncMock(side_effect=[
        page("one", cursor="second"), page("two")
    ]))
    toolkit = MCPTools(session=session)
    with pytest.raises(ValueError, match="limit"):
        await toolkit.build_tools()
    assert toolkit.functions == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("pages", [
    [{"tools": [], "nextCursor": "same"}, {"tools": [], "nextCursor": "same"}],
    [{"tools": [], "nextCursor": ""}],
    [{"tools": "not an array"}],
    [{"tools": [{"name": "", "inputSchema": {}}]}],
    [{"tools": [{"name": "name", "inputSchema": []}]}],
])
async def test_stdio_catalog_rejects_malformed_or_looping_pages(pages):
    server = stdio()
    server.process = process = SimpleNamespace(stdin=True)
    server._request = AsyncMock(side_effect=pages)
    with pytest.raises(HostError) as error:
        await server._list_tools(process)
    assert error.value.code == "plugin_catalog_invalid"


@pytest.mark.asyncio
async def test_notification_from_retired_process_cannot_refresh_current_catalog():
    server = stdio()
    server.process = SimpleNamespace(stdin=True)
    old = SimpleNamespace(stdin=True)
    server._schedule_catalog_refresh(old)
    assert not server._catalog_dirty
    assert server._catalog_task is None


@pytest.mark.asyncio
async def test_public_capability_host_publishes_refreshed_plugin_inventory(tmp_path):
    from openagent_capability_host import CapabilityHost, HostPaths
    host = CapabilityHost(paths=HostPaths(tmp_path), allow_plugins=True,
                          external_restart_limit=0)
    host.plugin_store.save([PluginSpec("fixture", (sys.executable, "-u", "-c", PLUGIN))])
    await host.start()
    await host.set_consent(True)
    principal = {"authority": "fixture", "tenant_id": "fixture", "subject_id": "device", "kind": "interactive-client"}
    try:
        assert {item["name"] for item in (await host.catalog())[0]["tools"]} == {
            "old", "control", "slow", "status"
        }
        await host.call("fixture", "control", {"change": True}, principal=principal, call_id="change")
        async with asyncio.timeout(2):
            while True:
                catalog = await host.catalog()
                names = {item["name"] for item in catalog[0]["tools"]}
                if "new" in names:
                    break
                await asyncio.sleep(0)
        assert "old" not in names
        with pytest.raises(HostError):
            await host.call("fixture", "old", {}, principal=principal, call_id="old")
    finally:
        await host.close()


def test_plugin_config_round_trips_multiline_commands_env_and_unicode(tmp_path):
    from openagent_capability_host.config import PluginConfigStore
    from openagent_capability_host.paths import HostPaths
    store = PluginConfigStore(HostPaths(tmp_path))
    spec = PluginSpec("fixture", (sys.executable, "-c", "print('è 🌞')\nprint('two')"),
                      env={"value.with dots": "line1\r\nline2\t\x00\x01\x7f\\\"🌞"})
    store.save([spec])
    assert store.load() == [spec]
