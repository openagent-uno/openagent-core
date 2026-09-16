"""Capability host: the single local discovery and dispatch chokepoint."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .audit import AuditLedger
from .config import PluginConfigStore, PluginSpec
from .consent import CONSENT_VERSION, ConsentState, ConsentStore
from openagent_tool_protocol.context import current_principal
from .idempotency import IdempotencyLedger
from .lease import MutatingLease
from openagent_tool_protocol.manifests import build_manifest_lock
from .mcp_stdio import MCPStdioServer
from .paths import HostPaths
from openagent_tool_protocol.types import (
    CapabilityServer,
    HostError,
    ServerManifest,
    ToolClassification,
    ToolManifest,
    ToolResult,
    tool_error_result,
)


@dataclass(slots=True)
class _AdmittedCall:
    provider: CapabilityServer
    tool_manifest: ToolManifest
    mutating: bool
    safe_retry: bool
    claimed: bool
    effective_idempotency_key: str | None
    lease_entered: bool
    task: asyncio.Task[ToolResult]
    done_event: asyncio.Event
    cancellation_requires_drain: bool


class CapabilityHost:
    """Own local built-ins/plugins and execute calls with no policy bypass."""

    def __init__(
        self,
        *,
        paths: HostPaths,
        servers: tuple[CapabilityServer, ...] = (),
        inventory: tuple[ServerManifest, ...] = (),
        allow_plugins: bool = False,
        process_environment: dict[str, str] | None = None,
        cwd: str | Path | None = None,
        lease_seconds: float = 15.0,
        external_restart_limit: int = 5,
        external_restart_initial_delay: float = 0.25,
        external_restart_max_delay: float = 5.0,
    ):
        self.paths = paths
        self.allow_plugins = allow_plugins
        self.process_environment = dict(process_environment or {})
        self.consent_store = ConsentStore(self.paths)
        self.plugin_store = PluginConfigStore(self.paths)
        self._idempotency = None
        self._lease = None
        self._audit = None
        self._lease_seconds = lease_seconds
        self.cwd = Path(cwd or Path.cwd()).expanduser().resolve()
        self.external_restart_limit = max(0, int(external_restart_limit))
        self.external_restart_initial_delay = max(
            0.0, float(external_restart_initial_delay)
        )
        self.external_restart_max_delay = max(
            self.external_restart_initial_delay,
            float(external_restart_max_delay),
        )
        self._event_sinks: set[Any] = set()
        self._catalog_sinks: set[Any] = set()
        self._catalog_tasks: set[asyncio.Task[None]] = set()
        self._terminal_events: dict[tuple[str, str], dict[str, Any]] = {}

        names = [server.manifest.name for server in servers]
        if len(names) != len(set(names)):
            raise ValueError("registered capability names must be unique")
        self._servers = {server.manifest.name: server for server in servers}
        self._registered_names = frozenset(self._servers)
        self._external_names: set[str] = set()
        self._inventory = {manifest.name: manifest for manifest in inventory}
        self._inventory.update({server.manifest.name: server.manifest for server in servers})
        self._health: dict[str, dict[str, Any]] = {
            name: {
                "available": manifest.available,
                "reason": manifest.unavailable_reason,
                "source": "builtin",
            }
            for name, manifest in self._inventory.items()
        }
        self._started = False
        self._external_started = False
        self._external_stopping = False
        self._closing = False
        self._active: dict[str, asyncio.Task[ToolResult]] = {}
        self._active_principals: dict[str, str] = {}
        self._active_mutating: set[str] = set()
        self._active_done: dict[str, asyncio.Event] = {}
        self._uncancellable_active: set[str] = set()
        self._retained_lease_principals: set[str] = set()
        self._principals: set[str] = set()
        self._resource_owners: dict[tuple[str, str], str] = {}
        # Admission and consent revocation share this barrier. A disable writes
        # durable consent first, then waits here until every pre-dispatch call
        # either becomes tracked in _active or aborts without an effect.
        self._admission_lock = asyncio.Lock()
        self._consent_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._started:
                return
            self.paths.ensure()
            self._started = True
            if self.consent_store.load().enabled:
                await self._start_external_locked()

    async def set_consent(self, enabled: bool, *, version: int = CONSENT_VERSION) -> ConsentState:
        async with self._consent_lock:
            state = self.consent_store.set_enabled(enabled, version=version)
            if enabled:
                async with self._lifecycle_lock:
                    await self._start_external_locked()
            else:
                # Consent is already durably false. Holding the admission
                # barrier guarantees that no call can cross from ledger/lease
                # setup into provider dispatch after the snapshot below.
                async with self._admission_lock, self._lifecycle_lock:
                    for call_id, task in list(self._active.items()):
                        if call_id not in self._uncancellable_active:
                            task.cancel()
                    if self._active:
                        await asyncio.gather(*self._active.values(), return_exceptions=True)
                    for principal in list(self._principals):
                        await self._release_principal_admitted(principal)
                    await self._stop_external_locked()
        await self._emit_catalog_changed()
        return state

    def consent(self) -> ConsentState:
        return self.consent_store.load()

    async def catalog(self) -> list[dict[str, Any]]:
        """Return only capabilities that can be called now.

        Disabled consent produces an empty catalog. The complete five-builtin
        inventory, including unavailable sidecars, remains visible in status().
        """
        if not self.consent().enabled:
            return []
        if not self._started:
            await self.start()
        return [
            server.manifest.to_wire()
            for name, server in sorted(self._servers.items())
            if self._health.get(name, {}).get("available", True)
        ]

    async def status(self) -> dict[str, Any]:
        consent = self.consent()
        inventory = []
        for name, manifest in sorted(self._inventory.items()):
            health = self._health.get(name, {})
            entry = manifest.to_wire()
            entry["available"] = bool(health.get("available", manifest.available))
            entry["source"] = health.get("source", "builtin")
            reason = health.get("reason")
            if reason:
                entry["unavailable_reason"] = reason
            inventory.append(entry)
        return {
            "protocol": "openagent-host-tools/1",
            "started": self._started,
            "consent": consent.to_wire(),
            "config_path": str(self.paths.plugins),
            "consent_path": str(self.paths.consent),
            "audit_path": str(self.paths.audit_db),
            "servers": inventory,
            "lease": await self.lease.status(),
            "active_calls": sorted(self._active),
            "manifest_lock": build_manifest_lock(self._inventory.values()),
        }

    async def call(
        self,
        server: str,
        tool: str,
        args: dict[str, Any],
        *,
        principal: str | dict[str, Any],
        call_id: str | None = None,
        idempotency_key: str | None = None,
        deadline_ms: int | float | None = None,
        arguments_sha256: str | None = None,
    ) -> ToolResult:
        call_id = call_id or uuid.uuid4().hex
        started = time.monotonic()
        principal = self._normalize_principal(principal)
        if not principal:
            raise HostError("invalid_principal", "principal is required")
        if not isinstance(args, dict):
            raise HostError("invalid_arguments", "args must be an object")
        actual_arguments_hash = self.idempotency.arguments_sha256(args)
        if arguments_sha256 is not None and arguments_sha256 != actual_arguments_hash:
            raise HostError(
                "arguments_hash_mismatch",
                "tool arguments do not match arguments_sha256",
                {"expected": arguments_sha256, "actual": actual_arguments_hash},
            )
        self._principals.add(principal)

        admitted = await self._admit_call(
            server=server,
            tool=tool,
            args=args,
            principal=principal,
            call_id=call_id,
            idempotency_key=idempotency_key,
            started=started,
            arguments_sha256=actual_arguments_hash,
        )
        if isinstance(admitted, ToolResult):
            return admitted
        provider = admitted.provider
        tool_manifest = admitted.tool_manifest
        mutating = admitted.mutating
        safe_retry = admitted.safe_retry
        claimed = admitted.claimed
        effective_idempotency_key = admitted.effective_idempotency_key
        lease_entered = admitted.lease_entered
        task = admitted.task
        done_event = admitted.done_event
        cancellation_requires_drain = admitted.cancellation_requires_drain
        renewer = (
            asyncio.create_task(
                self._renew_call(
                    principal,
                    call_id,
                    effective_idempotency_key if claimed else None,
                ),
                name=f"local-tool-renew-{call_id}",
            )
            if claimed
            else None
        )
        lease_indeterminate = False
        try:
            timeout = _deadline_seconds(deadline_ms)
            try:
                awaited = asyncio.shield(task) if cancellation_requires_drain else task
                result = (
                    await asyncio.wait_for(awaited, timeout=timeout) if timeout else await awaited
                )
            except HostError as exc:
                if not safe_retry and server in self._external_names:
                    lease_indeterminate = True
                    if claimed and effective_idempotency_key:
                        await self.idempotency.mark_indeterminate(
                            principal, effective_idempotency_key
                        )
                    await self.audit.append(
                        call_id=call_id,
                        principal=principal,
                        server=server,
                        tool=tool,
                        classification=tool_manifest.classification.value,
                        outcome="indeterminate",
                        argument_keys=list(args),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        error_code="CLIENT_RESULT_INDETERMINATE",
                        arguments_sha256=actual_arguments_hash,
                    )
                    raise HostError(
                        "idempotency_indeterminate",
                        "the local MCP failed after mutation dispatch",
                        {
                            "local_code": exc.code,
                            "manual_reconciliation_required": True,
                        },
                    ) from exc
                # Tool-level failures are replayable MCP results. Control-plane
                # failures above remain typed protocol errors.
                result = tool_error_result(exc)
            except TimeoutError as exc:
                if cancellation_requires_drain:
                    definitive = await _drain_mutation(task)
                    if claimed and effective_idempotency_key:
                        await self.idempotency.complete(
                            principal, effective_idempotency_key, definitive
                        )
                elif claimed and effective_idempotency_key:
                    if not safe_retry:
                        lease_indeterminate = True
                        await self.idempotency.mark_indeterminate(
                            principal, effective_idempotency_key
                        )
                    else:
                        await self.idempotency.abandon(principal, effective_idempotency_key)
                await self.audit.append(
                    call_id=call_id,
                    principal=principal,
                    server=server,
                    tool=tool,
                    classification=tool_manifest.classification.value,
                    outcome="timeout" if safe_retry else "indeterminate",
                    argument_keys=list(args),
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error_code=(
                        "CLIENT_RESULT_INDETERMINATE" if not safe_retry else "deadline_exceeded"
                    ),
                    arguments_sha256=actual_arguments_hash,
                )
                if not safe_retry:
                    raise HostError(
                        "idempotency_indeterminate",
                        "the local mutation crossed its deadline after dispatch",
                        {
                            "idempotency_key": effective_idempotency_key,
                            "manual_reconciliation_required": True,
                            "result_recorded": cancellation_requires_drain,
                        },
                    ) from exc
                raise HostError("deadline_exceeded", "local tool deadline exceeded") from exc
            except asyncio.CancelledError:
                if cancellation_requires_drain:
                    definitive = await _drain_mutation(task)
                    if claimed and effective_idempotency_key:
                        await self.idempotency.complete(
                            principal, effective_idempotency_key, definitive
                        )
                elif claimed and effective_idempotency_key:
                    if not safe_retry:
                        lease_indeterminate = True
                        await self.idempotency.mark_indeterminate(
                            principal, effective_idempotency_key
                        )
                    else:
                        await self.idempotency.abandon(principal, effective_idempotency_key)
                await self.audit.append(
                    call_id=call_id,
                    principal=principal,
                    server=server,
                    tool=tool,
                    classification=tool_manifest.classification.value,
                    outcome="cancelled" if safe_retry else "indeterminate",
                    argument_keys=list(args),
                    duration_ms=int((time.monotonic() - started) * 1000),
                    error_code=("CLIENT_RESULT_INDETERMINATE" if not safe_retry else "cancelled"),
                    arguments_sha256=actual_arguments_hash,
                )
                if not safe_retry:
                    raise HostError(
                        "idempotency_indeterminate",
                        "the local mutation was cancelled after dispatch",
                        {
                            "idempotency_key": effective_idempotency_key,
                            "manual_reconciliation_required": True,
                            "result_recorded": cancellation_requires_drain,
                        },
                    ) from None
                raise
            except Exception as exc:  # noqa: BLE001 - tool errors stay in result channel
                if not safe_retry and server in self._external_names:
                    lease_indeterminate = True
                    if claimed and effective_idempotency_key:
                        await self.idempotency.mark_indeterminate(
                            principal, effective_idempotency_key
                        )
                    await self.audit.append(
                        call_id=call_id,
                        principal=principal,
                        server=server,
                        tool=tool,
                        classification=tool_manifest.classification.value,
                        outcome="indeterminate",
                        argument_keys=list(args),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        error_code="CLIENT_RESULT_INDETERMINATE",
                        arguments_sha256=actual_arguments_hash,
                    )
                    raise HostError(
                        "idempotency_indeterminate",
                        "the local MCP disconnected after mutation dispatch",
                        {
                            "local_code": getattr(exc, "code", "tool_error"),
                            "manual_reconciliation_required": True,
                        },
                    ) from exc
                result = ToolResult.text(
                    f"local tool error: {exc}",
                    is_error=True,
                    meta={"openagent/error": {"code": "tool_error", "message": str(exc)}},
                )

            if claimed and effective_idempotency_key:
                await self.idempotency.complete(principal, effective_idempotency_key, result)
                result.meta = {
                    **result.meta,
                    "openagent/idempotent": True,
                    "openagent/replayed": False,
                }
            elif mutating:
                result.meta = {**result.meta, "openagent/replaySafe": False}
            self._update_resource_ownership(server, args, principal, result)
            self._mark_result(result)
            await self.audit.append(
                call_id=call_id,
                principal=principal,
                server=server,
                tool=tool,
                classification=tool_manifest.classification.value,
                outcome="tool_error" if result.is_error else "success",
                argument_keys=list(args),
                duration_ms=int((time.monotonic() - started) * 1000),
                arguments_sha256=actual_arguments_hash,
            )
            return result
        finally:
            if renewer is not None:
                renewer.cancel()
                try:
                    await renewer
                except asyncio.CancelledError:
                    pass
            if lease_entered:
                if not lease_indeterminate:
                    await self.lease.leave(principal, call_id)
                else:
                    self._retained_lease_principals.add(principal)
            self._active.pop(call_id, None)
            self._active_principals.pop(call_id, None)
            self._active_mutating.discard(call_id)
            self._uncancellable_active.discard(call_id)
            self._active_done.pop(call_id, None)
            done_event.set()

    async def _admit_call(
        self,
        *,
        server: str,
        tool: str,
        args: dict[str, Any],
        principal: str,
        call_id: str,
        idempotency_key: str | None,
        started: float,
        arguments_sha256: str,
    ) -> _AdmittedCall | ToolResult:
        """Cross the consent boundary and register dispatch atomically.

        ``set_consent(False)`` writes the durable bit before taking the same
        barrier. It therefore cannot return while a call is hidden between its
        first consent check and ``_active`` registration, and a call already in
        that interval observes the second check and abandons its ledger/lease
        without invoking the provider.
        """

        async with self._admission_lock:
            claimed = False
            lease_entered = False
            dispatched = False
            effective_idempotency_key = idempotency_key or call_id
            try:
                # Re-read durable consent on every call so a revoke from
                # Desktop or CLI takes effect without a broker restart.
                if not self.consent().enabled:
                    await self._audit_denial(
                        call_id, principal, server, tool, args, "consent_required"
                    )
                    raise HostError(
                        "consent_required",
                        "local tools are disabled on this device",
                        {"consent_path": str(self.paths.consent)},
                    )
                if not self._started:
                    await self.start()
                provider = self._servers.get(server)
                manifest = provider.manifest if provider is not None else None
                tool_manifest = manifest.tool(tool) if manifest is not None else None
                if (
                    provider is None
                    or tool_manifest is None
                    or not self._health.get(server, {}).get("available", True)
                ):
                    await self._audit_denial(
                        call_id, principal, server, tool, args, "tool_not_found"
                    )
                    raise HostError(
                        "tool_not_found",
                        f"no local tool {server}.{tool}",
                        {"server": server, "tool": tool},
                    )

                tool_manifest = replace(
                    tool_manifest,
                    classification=tool_manifest.classification_for(args),
                )
                await self._validate_provider(provider, server, tool, args, principal, call_id)

                mutating = tool_manifest.classification != ToolClassification.READ_ONLY
                safe_retry = tool_manifest.classification in {
                    ToolClassification.READ_ONLY,
                    ToolClassification.IDEMPOTENT,
                }
                if effective_idempotency_key:
                    try:
                        claim = await self.idempotency.claim(
                            principal,
                            effective_idempotency_key,
                            server=server,
                            tool=tool,
                            args=args,
                            retry_stale=safe_retry,
                        )
                    except HostError as exc:
                        await self.audit.append(
                            call_id=call_id,
                            principal=principal,
                            server=server,
                            tool=tool,
                            classification=tool_manifest.classification.value,
                            outcome="error",
                            argument_keys=list(args),
                            duration_ms=int((time.monotonic() - started) * 1000),
                            error_code=exc.code,
                            arguments_sha256=arguments_sha256,
                        )
                        raise
                    if claim.state == "replay" and claim.result is not None:
                        if not self.consent().enabled:
                            await self._audit_denial(
                                call_id,
                                principal,
                                server,
                                tool,
                                args,
                                "consent_required",
                            )
                            raise HostError(
                                "consent_required",
                                "local tools are disabled on this device",
                                {"consent_path": str(self.paths.consent)},
                            )
                        result = claim.result
                        result.meta = {
                            **result.meta,
                            "openagent/idempotent": True,
                            "openagent/replayed": True,
                        }
                        self._mark_result(result)
                        await self.audit.append(
                            call_id=call_id,
                            principal=principal,
                            server=server,
                            tool=tool,
                            classification=tool_manifest.classification.value,
                            outcome="replay",
                            argument_keys=list(args),
                            duration_ms=int((time.monotonic() - started) * 1000),
                            replayed=True,
                            arguments_sha256=arguments_sha256,
                        )
                        return result
                    claimed = True

                if mutating:
                    await self.lease.enter(principal, call_id, f"{server}.{tool}")
                    lease_entered = True
                    self._retained_lease_principals.discard(principal)

                # A disable can write the durable state while this coroutine is
                # waiting in SQLite. Reject before create_task: no provider code
                # has run, so the claim and lease remain safely abandonable.
                if not self.consent().enabled:
                    await self._audit_denial(
                        call_id, principal, server, tool, args, "consent_required"
                    )
                    raise HostError(
                        "consent_required",
                        "local tools were disabled before dispatch",
                        {"consent_path": str(self.paths.consent)},
                    )

                async def invoke_provider() -> ToolResult:
                    token = current_principal.set(principal)
                    try:
                        return await provider.call(tool, args)
                    finally:
                        current_principal.reset(token)

                task = asyncio.create_task(invoke_provider(), name=f"local-tool-{call_id}")
                done_event = asyncio.Event()
                self._active[call_id] = task
                self._active_principals[call_id] = principal
                self._active_done[call_id] = done_event
                if mutating:
                    self._active_mutating.add(call_id)
                cancellation_requires_drain = mutating and bool(
                    getattr(provider, "cancellation_requires_drain", False)
                )
                if cancellation_requires_drain:
                    self._uncancellable_active.add(call_id)
                dispatched = True
                return _AdmittedCall(
                    provider=provider,
                    tool_manifest=tool_manifest,
                    mutating=mutating,
                    safe_retry=safe_retry,
                    claimed=claimed,
                    effective_idempotency_key=effective_idempotency_key,
                    lease_entered=lease_entered,
                    task=task,
                    done_event=done_event,
                    cancellation_requires_drain=cancellation_requires_drain,
                )
            finally:
                if not dispatched:
                    if lease_entered:
                        await self.lease.leave(principal, call_id)
                    if claimed and effective_idempotency_key:
                        await self.idempotency.abandon(principal, effective_idempotency_key)

    async def cancel(self, call_id: str) -> bool:
        # If the call is still waiting in ledger/lease admission, wait until it
        # is either rejected or visible in _active before taking the snapshot.
        async with self._admission_lock:
            task = self._active.get(call_id)
            if task is None or task.done():
                return False
            if call_id not in self._uncancellable_active:
                task.cancel()
            return True

    async def release_principal(self, principal: str | dict[str, Any]) -> None:
        principal_id = self._normalize_principal(principal)
        async with self._admission_lock:
            active = self._begin_principal_release(principal_id)
        await self._finish_principal_release(principal_id, active)

    async def _release_principal_admitted(self, principal_id: str) -> None:
        active = self._begin_principal_release(principal_id)
        await self._finish_principal_release(principal_id, active)

    def _begin_principal_release(
        self, principal_id: str
    ) -> list[tuple[str, asyncio.Task[ToolResult] | None, asyncio.Event | None]]:
        # A channel disappearing is not permission to unlock the machine while
        # one of its effects is still running. Drain local thread-backed work;
        # cancel cooperative work and wait until host.call has made the lease
        # decision. Ambiguous external dispatch deliberately retains its lease
        # until expiry.
        active = [
            (
                call_id,
                self._active.get(call_id),
                self._active_done.get(call_id),
            )
            for call_id, owner in list(self._active_principals.items())
            if owner == principal_id
        ]
        for call_id, task, _done in active:
            if (
                task is not None
                and call_id in self._active_mutating
                and call_id not in self._uncancellable_active
            ):
                task.cancel()
        return active

    async def _finish_principal_release(
        self,
        principal_id: str,
        active: list[tuple[str, asyncio.Task[ToolResult] | None, asyncio.Event | None]],
    ) -> None:
        waits = [done.wait() for _call_id, _task, done in active if done is not None]
        if waits:
            await asyncio.gather(*waits)
        await self._release_owned_resources(principal_id)
        for provider in list(self._servers.values()):
            release = getattr(provider, "release_principal", None)
            if release is None:
                continue
            try:
                await release(principal_id)
            except Exception:
                continue
        if principal_id not in self._retained_lease_principals:
            await self.lease.release_principal(principal_id)
        self._principals.discard(principal_id)
        for key, event in list(self._terminal_events.items()):
            if event.get("principal") == principal_id:
                self._terminal_events.pop(key, None)
        for key, owner in list(self._resource_owners.items()):
            if owner == principal_id:
                self._resource_owners.pop(key, None)

    def subscribe_events(self, sink) -> None:
        self._event_sinks.add(sink)
        for event in self._terminal_events.values():
            try:
                result = sink(dict(event))
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                continue

    def unsubscribe_events(self, sink) -> None:
        self._event_sinks.discard(sink)

    def subscribe_catalog(self, sink) -> None:
        self._catalog_sinks.add(sink)

    def unsubscribe_catalog(self, sink) -> None:
        self._catalog_sinks.discard(sink)

    async def ack_event(self, principal: str | dict[str, Any], shell_id: str) -> bool:
        """Forget one terminal event only after the Gateway accepted it."""

        principal_id = self._normalize_principal(principal)
        return self._terminal_events.pop((principal_id, str(shell_id)), None) is not None

    async def _emit_event(self, event: dict[str, Any]) -> None:
        if event.get("type") == "shell_completed" and event.get("shell_id"):
            key = (str(event.get("principal") or ""), str(event["shell_id"]))
            self._terminal_events[key] = dict(event)
        for sink in list(self._event_sinks):
            try:
                result = sink(dict(event))
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                continue

    async def close(self) -> None:
        self._closing = True
        for task in list(self._catalog_tasks):
            task.cancel()
        if self._catalog_tasks:
            await asyncio.gather(*self._catalog_tasks, return_exceptions=True)
        async with self._admission_lock:
            for call_id, task in list(self._active.items()):
                if call_id not in self._uncancellable_active:
                    task.cancel()
            if self._active:
                await asyncio.gather(*self._active.values(), return_exceptions=True)
            async with self._lifecycle_lock:
                await self._stop_external_locked()
            for server in list(self._servers.values()):
                if server.manifest.name not in self._external_names:
                    await server.close()
            for principal in list(self._principals):
                await self.lease.release_principal(principal)

    async def _start_external_locked(self) -> None:
        if self._external_started:
            return
        self._external_started = True
        if not self.allow_plugins:
            return
        for spec in self.plugin_store.load():
            if spec.enabled and spec.name not in self._registered_names:
                await self._start_mcp(spec, placeholder=None, source="plugin")

    async def _start_mcp(
        self, spec: PluginSpec, *, placeholder: ServerManifest | None, source: str
    ) -> None:
        adapter = MCPStdioServer(
            spec,
            placeholder=placeholder,
            on_state_change=self._on_external_state_change,
            restart_limit=self.external_restart_limit,
            restart_initial_delay=self.external_restart_initial_delay,
            restart_max_delay=self.external_restart_max_delay,
            environment=self.process_environment,
        )
        # Register the unavailable generation before launch so a transient
        # first-start failure can remain supervised and publish its later
        # recovery through the normal state-change callback.
        self._servers[spec.name] = adapter
        self._external_names.add(spec.name)
        base = placeholder or adapter.manifest
        self._inventory[spec.name] = replace(
            base,
            available=False,
            unavailable_reason=f"MCP {spec.name!r} is starting",
        )
        self._health[spec.name] = {
            "available": False,
            "reason": f"MCP {spec.name!r} is starting",
            "source": source,
        }
        try:
            await adapter.start()
        except Exception as exc:  # failure isolation: one plugin never removes core tools
            manifest = placeholder or ServerManifest(
                spec.name,
                "unknown",
                "Explicitly configured local MCP.",
                (),
                available=False,
                unavailable_reason=str(exc),
            )
            adapter.manifest = replace(
                manifest, available=False, unavailable_reason=str(exc)
            )
            self._inventory[spec.name] = replace(
                manifest, available=False, unavailable_reason=str(exc)
            )
            self._health[spec.name] = {
                "available": False,
                "reason": str(exc),
                "source": source,
            }
            adapter.supervise_initial_failure(exc)
            return
        self._inventory[spec.name] = adapter.manifest
        self._health[spec.name] = {
            "available": adapter.manifest.available,
            "reason": adapter.manifest.unavailable_reason,
            "source": source,
        }

    async def _stop_external_locked(self) -> None:
        self._external_stopping = True
        try:
            for name in list(self._external_names):
                server = self._servers.pop(name, None)
                if server is not None:
                    await server.close()
                manifest = self._inventory.get(name)
                if manifest is not None:
                    self._inventory[name] = replace(
                        manifest,
                        available=False,
                        unavailable_reason="local tools are disabled",
                    )
                self._health[name] = {
                    **self._health.get(name, {}),
                    "available": False,
                    "reason": "local tools are disabled",
                }
            self._external_names.clear()
            self._external_started = False
        finally:
            self._external_stopping = False

    async def _on_external_state_change(self, provider: CapabilityServer) -> None:
        """Publish one supervised MCP generation's availability atomically."""

        name = provider.manifest.name
        if (
            self._closing
            or self._external_stopping
            or name not in self._external_names
            or self._servers.get(name) is not provider
        ):
            return
        manifest = provider.manifest
        self._inventory[name] = manifest
        self._health[name] = {
            **self._health.get(name, {}),
            "available": manifest.available,
            "reason": manifest.unavailable_reason,
        }
        self._schedule_catalog_changed()

    async def _emit_catalog_changed(self) -> None:
        if self._closing:
            return
        event = {
            "type": "catalog_changed",
            "servers": await self.catalog(),
        }
        for sink in list(self._catalog_sinks):
            try:
                result = sink(dict(event))
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                continue

    def _schedule_catalog_changed(self) -> None:
        if self._closing:
            return
        task = asyncio.create_task(
            self._emit_catalog_changed(), name="local-tool-catalog-update"
        )
        self._catalog_tasks.add(task)
        task.add_done_callback(self._catalog_tasks.discard)

    async def _renew_call(self, principal: str, call_id: str, idempotency_key: str | None) -> None:
        interval = max(1.0, self.lease.lease_seconds / 3)
        while True:
            await asyncio.sleep(interval)
            await self.lease.renew(principal, call_id)
            if idempotency_key:
                await self.idempotency.renew(principal, idempotency_key)

    async def _audit_denial(
        self,
        call_id: str,
        principal: str,
        server: str,
        tool: str,
        args: dict[str, Any],
        code: str,
    ) -> None:
        await self.audit.append(
            call_id=call_id,
            principal=principal,
            server=server,
            tool=tool,
            classification=None,
            outcome="denied",
            argument_keys=list(args),
            error_code=code,
            arguments_sha256=self.idempotency.arguments_sha256(args),
        )

    @property
    def idempotency(self):
        if self._idempotency is None:
            self._idempotency = IdempotencyLedger(self.paths.state_db)
        return self._idempotency

    @property
    def lease(self):
        if self._lease is None:
            self._lease = MutatingLease(self.paths.state_db, lease_seconds=self._lease_seconds)
        return self._lease

    @property
    def audit(self):
        if self._audit is None:
            self._audit = AuditLedger(self.paths.audit_db)
        return self._audit

    async def _validate_provider(self, provider, server, tool, args, principal, call_id):
        pass

    async def _release_owned_resources(self, principal_id):
        pass

    def _update_resource_ownership(self, server, args, principal, result):
        pass

    def _normalize_principal(self, value):
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict) and all(value.get(key) for key in ("authority", "tenant_id", "subject_id", "kind")):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        raise HostError("invalid_principal", "a nonempty trusted principal is required")

    def _mark_result(self, result):
        pass


def _deadline_seconds(deadline_ms: int | float | None) -> float | None:
    if deadline_ms is None:
        return None
    try:
        value = float(deadline_ms)
    except (TypeError, ValueError) as exc:
        raise HostError("invalid_deadline", "deadline_ms must be numeric") from exc
    if value <= 0:
        raise HostError("deadline_exceeded", "local tool deadline already expired")
    # Accept either an absolute Unix epoch in milliseconds or a relative timeout.
    if value > 10_000_000_000:
        value -= time.time() * 1000
    if value <= 0:
        raise HostError("deadline_exceeded", "local tool deadline already expired")
    return value / 1000


async def _drain_mutation(task: asyncio.Task[ToolResult]) -> ToolResult:
    """Wait for a non-cancellable thread-backed mutation to reach certainty."""
    try:
        return await asyncio.shield(task)
    except HostError as exc:
        return ToolResult.text(
            exc.message,
            is_error=True,
            meta={"openagent/error": exc.to_wire()},
        )
    except Exception as exc:  # noqa: BLE001
        return ToolResult.text(
            f"local tool error: {exc}",
            is_error=True,
            meta={"openagent/error": {"code": "tool_error", "message": str(exc)}},
        )
