"""Public adapters from engine MCP pools and authenticated device registries.

Hosts register sources explicitly. Discovery never installs a source or grants
access. An adapter is bound to one exact toolkit/connection generation; it does
not select another destination when that generation disappears.
"""
from __future__ import annotations

from typing import Any

from openagent_core.capabilities import CapabilityCatalog, CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import CapabilityLease, ExecutionContext
from openagent_core.core.execution_origin import TurnExecutionOrigin, current_execution_origin


class PoolCapabilitySource:
    def __init__(self, pool: Any, source_id: str, toolkit: Any, *, effects=None,
                 toolkit_name: str | None = None) -> None:
        self.pool, self.source_id, self.toolkit = pool, source_id, toolkit
        self.toolkit_name = toolkit_name or source_id
        self.effects = effects or {}
        # Deferred in-process tools have not passed through a model's schema
        # preparation. Materialize their real argument schemas before issuing
        # references, so later invocation cannot silently change the contract.
        from .servers.tool_search.adapters import _functions_dict
        for function in _functions_dict(toolkit).values():
            prepare = getattr(function, "process_entrypoint", None)
            if callable(prepare):
                prepare()

    def _check(self) -> None:
        if self.pool.toolkit_by_name(self.toolkit_name) is not self.toolkit:
            raise CapabilityUnavailable("The registered MCP instance was removed or replaced")

    async def inspect(self, context) -> tuple[ToolDefinition, ...]:
        # Schema-only host administration; the catalog authorizes this path
        # twice and does not issue tool references or bind an agent principal.
        from .servers.tool_search.adapters import _functions_dict
        self._check()
        return tuple(ToolDefinition(name, getattr(fn, "description", "") or "",
            getattr(fn, "parameters", None) or {}) for name, fn in _functions_dict(self.toolkit).items())

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        from .servers.tool_search.adapters import _functions_dict, _tool_is_denied, _require_server_allowed
        try:
            self._check()
            _require_server_allowed(self.toolkit_name)
        except (PermissionError, CapabilityUnavailable):
            return ()
        return tuple(ToolDefinition(name, getattr(fn, "description", "") or "",
                                    getattr(fn, "parameters", None) or {}, self.effects.get(name, frozenset()))
                     for name, fn in _functions_dict(self.toolkit).items()
                     if not _tool_is_denied(self.toolkit_name, name))

    async def call_tool(self, name: str, arguments: dict, context: ExecutionContext) -> Any:
        from .servers.tool_search.adapters import _invoke_registered_tool
        self._check()
        return await _invoke_registered_tool(self.pool, self.toolkit_name, name, arguments)


class InteractiveCapabilitySource:
    def __init__(self, origin: TurnExecutionOrigin, server_name: str, source_id: str) -> None:
        self.origin, self.server_name, self.source_id = origin, server_name, source_id

    def _check(self, context: ExecutionContext) -> None:
        current = current_execution_origin()
        if (context.deferred or current is None or current != self.origin
                or current.registry is not self.origin.registry):
            raise CapabilityUnavailable("The exact originating capability connection is unavailable")

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        try:
            self._check(context)
            registry = self.origin.registry
            tools = registry.list_tools(self.origin, self.server_name)
        except (PermissionError, LookupError, RuntimeError):
            # A disconnected/revoked source disappears without suppressing
            # unrelated sources. Calls still recheck the exact registry origin.
            return ()
        definitions = []
        for item in tools:
            full = registry.describe_tool(self.origin, self.server_name, item["name"])
            definitions.append(ToolDefinition(item["name"], full.get("description", ""),
                                              full.get("input_schema", full.get("inputSchema", {}))))
        return tuple(definitions)

    async def call_tool(self, name: str, arguments: dict, context: ExecutionContext) -> Any:
        self._check(context)
        return await self.origin.registry.call_tool(self.origin, self.server_name, name,
                                                    arguments, session_id=context.session_id,
                                                    execution_context=context, source_id=self.source_id)


def register_interactive_capabilities(catalog: CapabilityCatalog, origin: TurnExecutionOrigin,
                                     *, source_namespace: str) -> tuple[CapabilityLease, ...]:
    """Called by authenticated ingress, never from a client-supplied registration.

    The host puts the returned leases in the verified ExecutionContext. Its
    disconnect/revoke handler calls catalog.revoke for these source IDs; registry
    authorization is also rechecked on every discovery and invocation.
    """
    if not source_namespace or source_namespace.startswith(("client:", "server:")):
        raise ValueError("Provide a host-owned capability namespace")
    leases = []
    for item in origin.registry.list_servers(origin):
        # The old transport registry's prefix is decoded here only; no model API
        # or argument ever uses it to select a destination.
        name = str(item["name"]).removeprefix("client:")
        source_id = f"{source_namespace}/{name}"
        lease = CapabilityLease(source_id, origin.client_instance_id,
                                f"{origin.generation}:{origin.auth_epoch}")
        adapter = InteractiveCapabilitySource(origin, name, source_id)
        catalog.register(source_id, adapter, adapter, target_label=origin.device_label, lease=lease)
        leases.append(lease)
    return tuple(leases)


def revoke_interactive_capabilities(catalog: CapabilityCatalog,
                                    leases: tuple[CapabilityLease, ...]) -> None:
    """Release one disconnected origin without revoking a newer registration."""
    for lease in leases:
        catalog.revoke_lease(lease)


class PoolCatalogBinding:
    def __init__(self, pool: Any, catalog: CapabilityCatalog, *, trusted_modules: tuple[str, ...],
                 target_label: str = "Agent workspace", user_sources: frozenset[str] = frozenset(),
                 graph_generation: int | None = None, source_wrapper: Any | None = None) -> None:
        self.pool, self.catalog = pool, catalog
        self.trusted_modules = frozenset(trusted_modules)
        self.target_label = target_label
        self.graph_generation = graph_generation
        self.source_wrapper = source_wrapper
        # This set comes from the host's trusted registry ownership service.
        # Persisted MCP metadata cannot label its own registration as mutable.
        self.user_sources = frozenset(user_sources)
        self.sources: dict[str, PoolCapabilitySource] = {}

    def set_user_sources(self, names) -> None:
        """Apply a trusted ownership update and invalidate affected handles."""
        current = frozenset(names)
        if current and not self.catalog.allow_dynamic:
            raise PermissionError("This host has a fixed capability catalog")
        for name in self.user_sources.symmetric_difference(current):
            if name in self.sources:
                self.catalog.revoke_registration(name, self.graph_generation)
                del self.sources[name]
        self.user_sources = current

    def sync(self) -> None:
        current = {name: toolkit for name, toolkit in self.pool._toolkit_by_name.items()
                   if name != "tool-search"}
        for name, source in tuple(self.sources.items()):
            if current.get(name) is not source.toolkit:
                self.catalog.revoke_registration(name, self.graph_generation)
                del self.sources[name]
        specs = {spec.name: spec for spec in self.pool.specs}
        for name, toolkit in current.items():
            if name in self.sources:
                continue
            effects = {}
            spec = specs.get(name)
            if (name in {"vault", "vault-gate"} and "vault" in self.trusted_modules
                    and getattr(spec, "trusted_module", None) == name):
                from openagent_core.core.vault_recall import vault_tool_semantics
                from .servers.tool_search.adapters import _functions_dict
                for tool in _functions_dict(toolkit):
                    canonical = tool if tool.startswith("vault_") else "vault_" + tool
                    semantics = vault_tool_semantics(canonical)
                    if semantics is None:
                        continue
                    tags = {"vault." + semantics.operation}
                    if semantics.recalls_content and semantics.path_argument:
                        tags.add("vault.recall." + semantics.path_argument)
                    effects[tool] = frozenset(tags)
            source = PoolCapabilitySource(self.pool, name, toolkit, effects=effects)
            registered = self.source_wrapper(name, source, name in self.user_sources) \
                if self.source_wrapper is not None else source
            if name in self.user_sources:
                self.catalog.register_user_source(name, registered, registered, target_label=self.target_label,
                                                  graph_generation=self.graph_generation)
            else:
                self.catalog.register(name, registered, registered, target_label=self.target_label,
                                      trusted_effects=effects, graph_generation=self.graph_generation)
            self.sources[name] = source

    def close(self) -> None:
        for name in self.sources:
            self.catalog.revoke_registration(name, self.graph_generation)
        self.sources.clear()
