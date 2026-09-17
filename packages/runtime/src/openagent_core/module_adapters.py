"""Generic adapters used by independently packaged native modules.

This file contains no feature selection table.  A module distribution supplies
its own stable ID, native source names, aliases and prompt rule IDs.  The
underlying subprocess transport is an implementation detail: these sources are
registered directly by the module graph and are never MCP catalog resources.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .modules import (MODULE_API_VERSION, CapabilityContribution, ModuleConfig,
    ModuleContext, ModuleContribution, ModuleMigration, ServiceBinding)


@dataclass(frozen=True, slots=True)
class NativeCapabilityDescriptor:
    id: str
    version: str
    native_sources: tuple[str, ...]
    source_names: Mapping[str, str]
    prompt_rule_ids: tuple[str, ...] = ()
    additional_prompt_blocks: tuple[Any, ...] = ()
    search_operation: str | None = None
    search_source: str | None = None
    search_result_key: str | None = None
    search_query_argument: str | None = None
    search_accepts_limit: bool = True
    legacy_source_aliases: bool = True
    runtime_environment: Mapping[str, str] | None = None
    requires_modules: frozenset[str] = frozenset()
    optional_integrations: frozenset[str] = frozenset()
    required_services: frozenset[object] = frozenset()
    provided_services: frozenset[object] = frozenset()
    supported_surfaces: frozenset[str] = frozenset({"service", "agent_tools", "host_api", "workers", "event_ingress"})
    api_version: str = MODULE_API_VERSION

    def __post_init__(self) -> None:
        if not self.provided_services:
            object.__setattr__(self, "provided_services", frozenset({self.id + ".service"}))

    def validate(self, config: ModuleConfig) -> None:
        if config.surfaces & {"agent_tools", "host_api", "workers", "event_ingress"} \
                and "service" not in config.surfaces:
            raise ValueError(f"Module {self.id} exposed surfaces require its service surface")
        if self.id == "vault" and not str(config.options.get("vault_path") or "").strip():
            raise ValueError("The vault module requires an explicit vault_path")
        environment = config.options.get("environment", {})
        if not isinstance(environment, Mapping):
            raise TypeError(f"Module {self.id} environment must be a mapping")
        for option in ("workers", "event_ingresses", "indices", "health_checks", "routes"):
            values = config.options.get(option, ())
            if not isinstance(values, (tuple, list)):
                raise TypeError(f"Module {self.id} {option} must be a list or tuple")

    def migrations(self) -> tuple[ModuleMigration, ...]:
        return ()

    async def prepare(self, context: ModuleContext):
        return _NativeCapabilityInstance(self, context)


class _NativeCapabilityInstance:
    def __init__(self, descriptor: NativeCapabilityDescriptor, context: ModuleContext) -> None:
        self.descriptor = descriptor
        self.context = context
        self.pool = None
        self._contribution = ModuleContribution()

    async def start(self) -> ModuleContribution:
        config = self.context.config
        prompt_blocks = ()
        if "agent_tools" in config.surfaces and self.descriptor.prompt_rule_ids:
            from .prompts import rule_blocks
            prompt_blocks = rule_blocks(*self.descriptor.prompt_rule_ids)
        if "agent_tools" in config.surfaces and self.descriptor.additional_prompt_blocks:
            prompt_blocks = (*prompt_blocks, *self.descriptor.additional_prompt_blocks)
        capabilities: list[CapabilityContribution] = []
        active_surfaces = config.surfaces & {"service", "agent_tools", "host_api", "workers", "event_ingress"}
        if active_surfaces and self.descriptor.native_sources:
            from .mcp.catalog import PoolCapabilitySource
            from .mcp.pool import MCPPool

            environment = {str(key): str(value) for key, value in
                           dict(config.options.get("environment", {})).items()}
            environment.update({str(key): str(value) for key, value in
                                dict(self.descriptor.runtime_environment or {}).items()})
            environment["OPENAGENT_ACTIVE_MODULES"] = ",".join(
                sorted(self.context.profile.module_ids)
            )
            db_path = config.options.get("db_path")
            if db_path:
                environment.setdefault("OPENAGENT_DB_PATH", str(db_path))
            vault_path = config.options.get("vault_path")
            if vault_path:
                environment["OPENAGENT_VAULT_PATH"] = str(vault_path)
            # ``builtin`` is the private legacy loader key accepted by MCPPool.
            # It does not register these module-owned sources in the MCP catalog.
            entries = [{"builtin": name, "env": dict(environment)}
                       for name in self.descriptor.native_sources]
            self.pool = MCPPool.from_config(entries, include_defaults=False,
                                            db_path=str(db_path) if db_path else None)
            await self.pool.connect_all()
            for builtin in self.descriptor.native_sources:
                toolkit = self.pool.toolkit_by_name(builtin)
                if toolkit is None:
                    raise RuntimeError(f"Module {self.descriptor.id} failed to start capability source {builtin}")
                canonical = self.descriptor.source_names.get(builtin, builtin)
                effects = _trusted_effects(self.descriptor.id, builtin, toolkit)
                source = PoolCapabilitySource(self.pool, canonical, toolkit,
                                              toolkit_name=builtin, effects=effects)
                if "agent_tools" in config.surfaces:
                    capabilities.append(CapabilityContribution(
                        canonical, source, source,
                        str(config.options.get("target_label") or "Agent workspace"),
                        trusted_effects=effects,
                    ))
                    if self.descriptor.legacy_source_aliases and canonical != builtin:
                        alias = PoolCapabilitySource(self.pool, builtin, toolkit,
                                                     toolkit_name=builtin, effects=effects)
                        capabilities.append(CapabilityContribution(
                            builtin, alias, alias,
                            str(config.options.get("target_label") or "Agent workspace"),
                            trusted_effects=effects,
                        ))
        search_providers = ()
        if self.descriptor.search_operation and "service" in config.surfaces:
            search_providers = (self,)
        self._contribution = ModuleContribution(
            services=(ServiceBinding(self.descriptor.id + ".service", self),)
                if config.surfaces & {"service", "host_api", "workers", "event_ingress"} else (),
            capabilities=tuple(capabilities), prompt_blocks=prompt_blocks,
            search_providers=search_providers,
            routes=tuple(config.options.get("routes", ())) if "host_api" in config.surfaces else (),
            health_checks=tuple(config.options.get("health_checks", ())),
            workers=tuple(config.options.get("workers", ())) if "workers" in config.surfaces else (),
            event_ingresses=tuple(config.options.get("event_ingresses", ()))
                if "event_ingress" in config.surfaces else (),
            indices=tuple(config.options.get("indices", ())) if "service" in config.surfaces else (),
        )
        return self._contribution

    async def reconfigure(self, config: ModuleConfig) -> ModuleContribution:
        if config != self.context.config:
            raise RuntimeError("Module configuration changes use an atomic graph replacement")
        return self._contribution

    async def drain(self) -> None:
        return None

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close_all()
            self.pool = None

    async def inspect_tools(self, context: Any) -> tuple[Any, ...]:
        """Typed host API over the same active module implementation."""
        if self.pool is None:
            return ()
        definitions = []
        from .mcp.catalog import PoolCapabilitySource
        for builtin in self.descriptor.native_sources:
            toolkit = self.pool.toolkit_by_name(builtin)
            if toolkit is not None:
                definitions.extend(await PoolCapabilitySource(
                    self.pool, self.descriptor.source_names.get(builtin, builtin), toolkit,
                    toolkit_name=builtin,
                ).inspect(context))
        return tuple(definitions)

    async def call(self, source_name: str, tool_name: str, arguments: Mapping[str, Any], context: Any) -> Any:
        """Host API with an explicit module-owned source, never a fallback."""
        if self.pool is None:
            raise RuntimeError(f"Module {self.descriptor.id} has no active agent tool surface")
        matches = [builtin for builtin, canonical in self.descriptor.source_names.items()
                   if source_name in {builtin, canonical}]
        if len(matches) != 1:
            raise LookupError(source_name)
        from .mcp.catalog import PoolCapabilitySource
        toolkit = self.pool.toolkit_by_name(matches[0])
        if toolkit is None:
            raise LookupError(source_name)
        return await PoolCapabilitySource(self.pool, source_name, toolkit,
                                          toolkit_name=matches[0]).call_tool(
            tool_name, dict(arguments), context
        )

    @property
    def domain(self) -> str:
        return self.descriptor.id

    async def search(self, context: Any, *, query: str, limit: int = 20,
                     cursor: str | None = None) -> Mapping[str, Any]:
        """Search this module through its own canonical service operation."""
        operation = self.descriptor.search_operation
        if not operation:
            return {"hits": (), "next_cursor": None}
        source = self.descriptor.search_source or next(iter(self.descriptor.source_names.values()))
        arguments: dict[str, Any] = {}
        if self.descriptor.search_query_argument:
            arguments[self.descriptor.search_query_argument] = query
        if self.descriptor.search_accepts_limit:
            arguments["limit"] = limit
        result = await self.call(source, operation, arguments, context)
        if isinstance(result, Mapping):
            key = self.descriptor.search_result_key
            rows = result.get(key, ()) if key else result.get("results", result.get("hits", ()))
        else:
            rows = result
        if not isinstance(rows, (list, tuple)):
            rows = (rows,) if rows else ()
        needle = query.casefold().strip()
        hits = [row for row in rows if not needle or needle in str(row).casefold()]
        return {"hits": tuple(hits[:limit]), "next_cursor": None}


def _trusted_effects(module_id: str, builtin: str, toolkit: Any) -> dict[str, frozenset[str]]:
    if module_id != "vault" or builtin not in {"vault", "vault-gate"}:
        return {}
    from .core.vault_recall import vault_tool_semantics
    from .mcp.servers.tool_search.adapters import _functions_dict

    effects: dict[str, frozenset[str]] = {}
    for tool in _functions_dict(toolkit):
        canonical = tool if tool.startswith("vault_") else "vault_" + tool
        semantics = vault_tool_semantics(canonical)
        if semantics is None:
            continue
        tags = {"vault." + semantics.operation}
        if semantics.recalls_content and semantics.path_argument:
            tags.add("vault.recall." + semantics.path_argument)
        effects[tool] = frozenset(tags)
    return effects


class BuiltinCapabilityDescriptor(NativeCapabilityDescriptor):
    """Deprecated beta compatibility spelling for ``NativeCapabilityDescriptor``."""

    def __init__(self, id: str, version: str, builtins: tuple[str, ...],
                 source_names: Mapping[str, str], **kwargs: Any) -> None:
        super().__init__(id=id, version=version, native_sources=builtins,
                         source_names=source_names, **kwargs)


__all__ = ["NativeCapabilityDescriptor", "BuiltinCapabilityDescriptor"]
