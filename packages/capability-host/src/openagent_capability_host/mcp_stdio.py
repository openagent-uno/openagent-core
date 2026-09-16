"""Minimal MCP stdio client used for configured plugins and optional sidecars."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import socket
from dataclasses import replace
from pathlib import Path
from typing import Any, Awaitable, Callable

from ._version import __version__
from .config import PluginSpec
from openagent_tool_protocol.context import current_principal
from openagent_tool_protocol.types import (
    HostError,
    ServerManifest,
    ToolClassification,
    ToolManifest,
    ToolResult,
)

_MCP_PROTOCOL_VERSION = "2024-11-05"
# MCP stdio uses one JSON-RPC message per line. A single supported 64 MiB raw
# artifact expands to roughly 86 MiB when base64 encoded, so asyncio's 64 KiB
# StreamReader default is far too small for screenshots and other media. Keep a
# finite ceiling above the capability protocol's largest supported artifact to
# avoid turning an untrusted plugin response into an unbounded buffer.
_MAX_MCP_STDIO_LINE_BYTES = 128 * 1024 * 1024


class MCPStdioServer:
    def __init__(
        self,
        spec: PluginSpec,
        *,
        placeholder: ServerManifest | None = None,
        startup_timeout: float = 20.0,
        on_state_change: (
            Callable[["MCPStdioServer"], Awaitable[None] | None] | None
        ) = None,
        restart_limit: int = 5,
        restart_initial_delay: float = 0.25,
        restart_max_delay: float = 5.0,
        environment: dict[str, str] | None = None,
    ):
        self.spec = spec
        self.environment = dict(os.environ if environment is None else environment)
        self.placeholder = placeholder
        self.startup_timeout = startup_timeout
        self.on_state_change = on_state_change
        self.restart_limit = max(0, int(restart_limit))
        self.restart_initial_delay = max(0.0, float(restart_initial_delay))
        self.restart_max_delay = max(
            self.restart_initial_delay, float(restart_max_delay)
        )
        self.manifest = placeholder or ServerManifest(spec.name, "unknown", "", ())
        self.process: asyncio.subprocess.Process | None = None
        self._reader: asyncio.Task[None] | None = None
        self._stderr_reader: asyncio.Task[None] | None = None
        self._watcher: asyncio.Task[None] | None = None
        self._restart_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._pending_process: dict[int, asyncio.subprocess.Process] = {}
        self._write_lock = asyncio.Lock()
        self._spawn_lock = asyncio.Lock()
        self._next_id = 0
        self._stderr_tail = ""
        self._closing = False
        self._expected_exits: set[asyncio.subprocess.Process] = set()
        self._ready_processes: set[asyncio.subprocess.Process] = set()
        self._restart_attempts = 0
        self._restart_requested = False
        self.raw_initialize: dict[str, Any] | None = None

    async def start(self) -> None:
        async with self._spawn_lock:
            if self.process is not None and self.process.returncode is None:
                return
            self._closing = False
            self._restart_attempts = 0
            self._restart_requested = False
            await self._launch()

    def supervise_initial_failure(self, error: BaseException) -> None:
        """Keep a failed first launch under the normal bounded supervisor.

        ``_watch_process`` deliberately ignores a process which never became
        ready because the caller still owns initial registration.  Once that
        caller has published this adapter as unavailable, it invokes this
        method so transient spawn/initialize/catalog failures receive the same
        restart policy as a runtime crash.
        """

        if self._closing:
            return
        self.manifest = replace(
            self.manifest,
            available=False,
            unavailable_reason=str(error),
        )
        self._schedule_restart()

    async def _launch(self) -> None:
        env = self.environment.copy()
        env.update(self.spec.env)
        try:
            process = await asyncio.create_subprocess_exec(
                *self.spec.command,
                cwd=self.spec.cwd,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_MAX_MCP_STDIO_LINE_BYTES,
            )
        except OSError as exc:
            raise HostError(
                "plugin_unavailable", f"cannot start MCP {self.spec.name!r}: {exc}"
            ) from exc
        self.process = process
        self._stderr_tail = ""
        self._reader = asyncio.create_task(
            self._read_loop(process), name=f"mcp-read-{self.spec.name}"
        )
        self._stderr_reader = asyncio.create_task(
            self._read_stderr(process), name=f"mcp-stderr-{self.spec.name}"
        )
        self._watcher = asyncio.create_task(
            self._watch_process(process), name=f"mcp-watch-{self.spec.name}"
        )
        try:
            initialized = await asyncio.wait_for(
                self._request(
                    "initialize",
                    {
                        "protocolVersion": _MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "openagent-host-tools",
                            "version": __version__,
                        },
                    },
                ),
                timeout=self.startup_timeout,
            )
            self.raw_initialize = dict(initialized)
            server_info = initialized.get("serverInfo") or {}
            if self.placeholder is not None and server_info.get("name") != self.placeholder.name:
                raise HostError(
                    "plugin_identity_mismatch",
                    f"MCP {self.spec.name!r} reported serverInfo.name {server_info.get('name')!r}",
                )
            await self._notify("notifications/initialized", {})
            listed = await asyncio.wait_for(
                self._request("tools/list", {}), timeout=self.startup_timeout
            )
        except BaseException:
            await self._stop_process(process)
            raise
        server_info = initialized.get("serverInfo") or {}
        tools = tuple(self._tool_manifest(raw) for raw in listed.get("tools", []))
        instructions = str(initialized.get("instructions") or "")
        base = self.placeholder or self.manifest
        self.manifest = replace(
            base,
            name=self.spec.name,
            version=str(server_info.get("version") or "unknown"),
            instructions=instructions or base.instructions,
            tools=tools,
            available=True,
            unavailable_reason=None,
        )
        if self.process is not process or process.returncode is not None:
            await self._stop_process(process)
            raise HostError("plugin_disconnected", self._disconnect_message())
        self._ready_processes.add(process)

    def _tool_manifest(self, raw: dict[str, Any]) -> ToolManifest:
        annotations = raw.get("annotations") or {}
        read_only = annotations.get("readOnlyHint") is True
        idempotent = annotations.get("idempotentHint") is True
        classification = (
            ToolClassification.READ_ONLY
            if read_only
            else ToolClassification.IDEMPOTENT
            if idempotent
            else ToolClassification.MUTATING
        )
        name = str(raw.get("name") or "")
        fallback = self.placeholder.tool(name) if self.placeholder is not None else None
        return ToolManifest(
            name=name,
            description=str(raw.get("description") or ""),
            input_schema=dict(raw.get("inputSchema") or {"type": "object"}),
            classification=classification,
            classification_by_argument=(
                fallback.classification_by_argument if fallback is not None else {}
            ),
        )

    async def call(self, tool: str, args: dict[str, Any]) -> ToolResult:
        if self.process is None:
            raise HostError("plugin_unavailable", f"MCP {self.spec.name!r} is not running")
        result = await self._request("tools/call", {"name": tool, "arguments": args})
        return ToolResult.from_wire(result)

    async def close(self) -> None:
        self._closing = True
        restart = self._restart_task
        if restart is not None and restart is not asyncio.current_task():
            restart.cancel()
            await asyncio.gather(restart, return_exceptions=True)
        self._restart_task = None
        process = self.process
        if process is not None:
            await self._stop_process(process)
        else:
            self._fail_pending()
        for task in (self._reader, self._stderr_reader, self._watcher):
            if task is not None and task is not asyncio.current_task() and not task.done():
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (self._reader, self._stderr_reader, self._watcher)
                if task is not None and task is not asyncio.current_task()
            ),
            return_exceptions=True,
        )
        self._reader = None
        self._stderr_reader = None
        self._watcher = None

    async def _stop_process(self, process: asyncio.subprocess.Process) -> None:
        self._expected_exits.add(process)
        self._ready_processes.discard(process)
        if self.process is process:
            self.process = None
        if process.returncode is None:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
        watcher = self._watcher
        if watcher is not None and watcher is not asyncio.current_task():
            await asyncio.gather(watcher, return_exceptions=True)
        self._expected_exits.discard(process)
        self._fail_pending(process)

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        process = self.process
        if process is None or process.stdin is None:
            raise HostError("plugin_disconnected", self._disconnect_message())
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._pending_process[request_id] = process
        try:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                },
                process=process,
            )
            return await future
        except asyncio.CancelledError:
            try:
                await self._notify(
                    "notifications/cancelled",
                    {"requestId": request_id},
                    process=process,
                )
            except HostError:
                pass
            raise
        finally:
            self._pending.pop(request_id, None)
            self._pending_process.pop(request_id, None)
            if not future.done():
                future.cancel()

    async def _notify(
        self,
        method: str,
        params: dict[str, Any],
        *,
        process: asyncio.subprocess.Process | None = None,
    ) -> None:
        await self._send(
            {"jsonrpc": "2.0", "method": method, "params": params},
            process=process,
        )

    async def _send(
        self,
        value: dict[str, Any],
        *,
        process: asyncio.subprocess.Process | None = None,
    ) -> None:
        target = process or self.process
        if (
            target is None
            or target is not self.process
            or target.stdin is None
            or target.returncode is not None
        ):
            raise HostError("plugin_disconnected", self._disconnect_message())
        data = (json.dumps(value, separators=(",", ":")) + "\n").encode()
        async with self._write_lock:
            if target is not self.process or target.returncode is not None:
                raise HostError("plugin_disconnected", self._disconnect_message())
            target.stdin.write(data)
            await target.stdin.drain()

    async def _read_loop(self, process: asyncio.subprocess.Process) -> None:
        assert process.stdout is not None
        try:
            while True:
                try:
                    line = await process.stdout.readline()
                except ValueError as exc:
                    # StreamReader raises ValueError when a newline-delimited
                    # JSON-RPC frame crosses its finite limit. Leaving the
                    # child alive here would strand a healthy-looking adapter
                    # with no stdout reader, so fail closed and let the normal
                    # process watcher apply the bounded restart policy.
                    self._stderr_tail = (
                        "MCP stdout frame exceeded "
                        f"{_MAX_MCP_STDIO_LINE_BYTES} bytes: {exc}"
                    )[-8192:]
                    if process.returncode is None:
                        try:
                            process.terminate()
                        except ProcessLookupError:
                            pass
                    break
                if not line:
                    break
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(value, dict):
                    continue
                request_id = value.get("id")
                if request_id is None:
                    continue
                try:
                    normalized_request_id = int(request_id)
                except (TypeError, ValueError):
                    continue
                future = self._pending.get(normalized_request_id)
                if (
                    future is None
                    or future.done()
                    or self._pending_process.get(normalized_request_id) is not process
                ):
                    continue
                if "error" in value:
                    error = value.get("error") or {}
                    future.set_exception(
                        HostError(
                            "plugin_error",
                            str(error.get("message") or "MCP request failed"),
                            {"mcp_code": error.get("code"), "mcp_data": error.get("data")},
                        )
                    )
                else:
                    future.set_result(dict(value.get("result") or {}))
        except asyncio.CancelledError:
            raise
        finally:
            self._fail_pending(process)

    async def _read_stderr(self, process: asyncio.subprocess.Process) -> None:
        assert process.stderr is not None
        try:
            while True:
                chunk = await process.stderr.read(4096)
                if not chunk:
                    break
                self._stderr_tail = (self._stderr_tail + chunk.decode(errors="replace"))[-8192:]
        except asyncio.CancelledError:
            raise

    async def _watch_process(self, process: asyncio.subprocess.Process) -> None:
        returncode = await process.wait()
        expected = process in self._expected_exits
        was_ready = process in self._ready_processes
        self._expected_exits.discard(process)
        self._ready_processes.discard(process)
        if self.process is process:
            self.process = None
        self._fail_pending(process)
        # A process that never completed initialize/tools-list is a failed
        # launch, not a runtime death. Its caller owns retry policy.
        if expected or self._closing or not was_ready:
            return
        reason = self._disconnect_message()
        if returncode is not None:
            reason = f"{reason} (exit {returncode})"
        self.manifest = replace(
            self.manifest, available=False, unavailable_reason=reason
        )
        await self._notify_state_change()
        self._schedule_restart()

    def _schedule_restart(self) -> None:
        if self._closing or self._restart_attempts >= self.restart_limit:
            return
        if self._restart_task is not None and not self._restart_task.done():
            self._restart_requested = True
            return
        self._restart_task = asyncio.create_task(
            self._restart_loop(), name=f"mcp-restart-{self.spec.name}"
        )

    async def _restart_loop(self) -> None:
        try:
            while self._restart_attempts < self.restart_limit:
                attempt = self._restart_attempts + 1
                delay = min(
                    self.restart_max_delay,
                    self.restart_initial_delay * (2 ** (attempt - 1)),
                )
                if delay:
                    await asyncio.sleep(delay)
                if self._closing:
                    return
                self._restart_attempts = attempt
                self._restart_requested = False
                try:
                    async with self._spawn_lock:
                        if self._closing:
                            return
                        if self.process is not None and self.process.returncode is None:
                            return
                        await self._launch()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - health records exact failure
                    self.manifest = replace(
                        self.manifest,
                        available=False,
                        unavailable_reason=(
                            f"MCP {self.spec.name!r} restart {attempt}/"
                            f"{self.restart_limit} failed: {exc}"
                        ),
                    )
                    await self._notify_state_change()
                    continue
                await self._notify_state_change()
                # A sidecar can finish its handshake and die immediately. Let
                # its watcher run, then consume the same bounded restart budget
                # instead of losing the death while this task is still active.
                await asyncio.sleep(0)
                if self._restart_requested or self.process is None:
                    continue
                return
        finally:
            if self._restart_task is asyncio.current_task():
                self._restart_task = None

    def _fail_pending(
        self, process: asyncio.subprocess.Process | None = None
    ) -> None:
        for request_id, future in self._pending.items():
            if process is not None and self._pending_process.get(request_id) is not process:
                continue
            if not future.done():
                future.set_exception(
                    HostError("plugin_disconnected", self._disconnect_message())
                )

    async def _notify_state_change(self) -> None:
        if self.on_state_change is None:
            return
        try:
            result = self.on_state_change(self)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # A UI/catalog observer cannot take down supervision itself.
            return

    def _disconnect_message(self) -> str:
        suffix = f": {self._stderr_tail.strip()}" if self._stderr_tail.strip() else ""
        return f"MCP {self.spec.name!r} disconnected{suffix}"

