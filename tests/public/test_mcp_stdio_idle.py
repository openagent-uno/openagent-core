"""On-demand MCP stdio processes: idle stop keeps the catalog, a call restarts.

Real independent child processes; each one appends its PID to a file so the
tests count launches without trusting the adapter's own bookkeeping.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from openagent_capability_host.config import PluginSpec
from openagent_capability_host.mcp_stdio import MCPStdioServer
from openagent_tool_protocol import HostError

PLUGIN = r'''
import json,os,sys,time
log=sys.argv[1]
mode=sys.argv[2] if len(sys.argv)>2 else ""
with open(log,"a") as handle: handle.write(str(os.getpid())+"\n")
starts=sum(1 for _ in open(log))
if mode=="fail-after-first" and starts>1:
    sys.exit(3)
def emit(value): print(json.dumps(value),flush=True)
for line in sys.stdin:
    frame=json.loads(line)
    request_id=frame.get("id"); method=frame.get("method")
    if request_id is None: continue
    if method=="initialize":
        result={"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}}
    elif method=="tools/list":
        result={"tools":[{"name":"echo","inputSchema":{"type":"object"}},{"name":"wait","inputSchema":{"type":"object"}}]}
    elif method=="tools/call":
        if frame["params"]["name"]=="wait": time.sleep(float(frame["params"]["arguments"].get("seconds",0)))
        result={"content":[{"type":"text","text":str(os.getpid())}],"structuredContent":{"pid":os.getpid()}}
    else:
        result={}
    emit({"jsonrpc":"2.0","id":request_id,"result":result})
'''


def server(tmp_path: Path, idle: float | None, mode: str = "", **kwargs) -> tuple[MCPStdioServer, Path]:
    log = tmp_path / "starts.log"
    return MCPStdioServer(
        PluginSpec("fixture", (sys.executable, "-u", "-c", PLUGIN, str(log), mode)),
        startup_timeout=5, restart_limit=kwargs.pop("restart_limit", 0), idle_timeout=idle, **kwargs,
    ), log


def starts(log: Path) -> list[int]:
    return [int(line) for line in log.read_text().split()] if log.exists() else []


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def eventually(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_idle_process_stops_keeps_catalog_and_restarts_on_the_next_call(tmp_path):
    changes = []
    adapter, log = server(tmp_path, 0.3, on_state_change=lambda value: changes.append(value.manifest.available))
    await adapter.start()
    try:
        first = starts(log)[0]
        assert {tool.name for tool in adapter.manifest.tools} == {"echo", "wait"}
        await eventually(lambda: adapter.idle_stopped)
        assert adapter.process is None
        await eventually(lambda: not alive(first))
        # The catalog stays published: discovery needs no live process.
        assert adapter.manifest.available and {tool.name for tool in adapter.manifest.tools} == {"echo", "wait"}
        result = await adapter.call("echo", {})
        second = starts(log)[1]
        assert second != first and result.structured_content == {"pid": second}
        assert not adapter.idle_stopped and adapter.manifest.available
        assert changes == [], "an idle stop and an on-demand start with the same catalog are not state changes"
        # It stops again after the next idle period.
        await eventually(lambda: adapter.idle_stopped)
        assert len(starts(log)) == 2
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_a_running_call_holds_the_process_and_activity_resets_the_timer(tmp_path):
    adapter, log = server(tmp_path, 0.3)
    await adapter.start()
    try:
        # A call longer than the idle period is never interrupted.
        result = await adapter.call("wait", {"seconds": 0.6})
        assert result.structured_content == {"pid": starts(log)[0]}
        for _ in range(4):  # calls closer together than the period keep it alive
            await asyncio.sleep(0.15)
            await adapter.call("echo", {})
        assert len(starts(log)) == 1 and not adapter.idle_stopped
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_concurrent_calls_after_idle_start_exactly_one_process(tmp_path):
    adapter, log = server(tmp_path, 0.2)
    await adapter.start()
    try:
        await eventually(lambda: adapter.idle_stopped)
        results = await asyncio.gather(*(adapter.call("echo", {}) for _ in range(5)))
        assert len(starts(log)) == 2
        assert {result.structured_content["pid"] for result in results} == {starts(log)[1]}
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_without_idle_timeout_the_process_stays_up(tmp_path):
    adapter, log = server(tmp_path, None)
    await adapter.start()
    try:
        await asyncio.sleep(0.4)
        assert not adapter.idle_stopped and adapter.process is not None and alive(starts(log)[0])
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_a_failed_on_demand_start_is_unavailable_and_supervised(tmp_path):
    changes = []
    adapter, log = server(tmp_path, 0.2, "fail-after-first", restart_limit=1,
                          restart_initial_delay=0, on_state_change=lambda value: changes.append(value.manifest.available))
    await adapter.start()
    try:
        await eventually(lambda: adapter.idle_stopped)
        with pytest.raises(HostError, match="could not start on demand"):
            await adapter.call("echo", {})
        assert not adapter.manifest.available and not adapter.idle_stopped
        assert False in changes
        # The bounded supervisor retried once; the fixture keeps failing.
        await eventually(lambda: len(starts(log)) >= 3)
        with pytest.raises(HostError, match="not running"):
            await adapter.call("echo", {})
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_close_while_idle_releases_the_timer_and_refuses_calls(tmp_path):
    adapter, log = server(tmp_path, 0.2)
    await adapter.start()
    await eventually(lambda: adapter.idle_stopped)
    await adapter.close()
    assert adapter._idle_task is None and not adapter.idle_stopped
    with pytest.raises(HostError, match="not running"):
        await adapter.call("echo", {})
    assert len(starts(log)) == 1
