"""External MCP protocol module with fixed, managed and dynamic catalogs."""
from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Mapping

from openagent_core.mcp.catalog import PoolCapabilitySource
from openagent_core.mcp.pool import MCPPool
from openagent_core.modules import (
    MODULE_API_VERSION,
    CapabilityContribution,
    ModuleConfig,
    ModuleContext,
    ModuleContribution,
    ModuleMigration,
    ServiceBinding,
)
from openagent_core.prompts import PromptBlock


MCP_PROMPT = PromptBlock(
    "module.mcp",
    "1",
    """## External MCP servers

MCP is the protocol used to connect external capability servers. Discover and
invoke their tools through the uniform capability catalog and copy every opaque
tool reference exactly. An unavailable or revoked server must not be replaced
with a similarly named destination. Catalog administration, when exposed, may
manage MCP servers only; it cannot install or activate OpenAgent modules.
Changes to a server catalog are visible to the next run, while removals and
revocations prevent new calls immediately.""",
    "openagent-module-mcp",
)


@dataclass(frozen=True, slots=True)
class MCPDescriptor:
    id: str = "mcp"
    version: str = "1.1.0b1"
    api_version: str = MODULE_API_VERSION
    requires_modules: frozenset[str] = frozenset()
    optional_integrations: frozenset[str] = frozenset({"tool-discovery"})
    required_services: frozenset[object] = frozenset()
    provided_services: frozenset[object] = frozenset({"mcp.service"})
    supported_surfaces: frozenset[str] = frozenset({"service", "agent_tools", "host_api"})

    def validate(self, config: ModuleConfig) -> None:
        mode = config.options.get("catalog_mode", "fixed")
        if mode not in {"fixed", "managed", "dynamic"}:
            raise ValueError("mcp.catalog_mode must be fixed, managed or dynamic")
        if config.surfaces & {"agent_tools", "host_api"} and "service" not in config.surfaces:
            raise ValueError("mcp exposed surfaces require the service surface")
        sources = config.options.get("sources", ())
        if not isinstance(sources, (list, tuple)):
            raise TypeError("mcp.sources must be a list or tuple")
        if any(not isinstance(item, Mapping) for item in sources):
            raise TypeError("Every MCP source must be a mapping")
        factory = config.options.get("pool_factory")
        if factory is not None and not callable(factory):
            raise TypeError("mcp.pool_factory must be callable")
        if factory is not None and sources:
            raise ValueError("Configure either mcp.sources or mcp.pool_factory")

    def migrations(self) -> tuple[ModuleMigration, ...]:
        return ()

    def required_services_for(self, config: ModuleConfig) -> frozenset[object]:
        mode = config.options.get("catalog_mode", "fixed")
        return frozenset({"catalog_management"}) if mode in {"managed", "dynamic"} else frozenset()

    async def prepare(self, context: ModuleContext):
        return MCPModule(context)


class MCPModule:
    domain = "mcp"

    def __init__(self, context: ModuleContext) -> None:
        self.context = context
        self.runtime = context.runtime
        self.pool: MCPPool | None = None
        self._contribution = ModuleContribution()
        self._management: Any | None = None

    async def start(self) -> ModuleContribution:
        options = self.context.config.options
        mode = str(options.get("catalog_mode", "fixed"))
        management = (self.context.services.optional("catalog_management")
                      if mode in {"managed", "dynamic"} else None)
        if mode == "dynamic" and management is None:
            raise RuntimeError("Dynamic MCP mode requires a catalog_management service")
        self._management = management
        self.pool = await self._create_pool(options)
        await self.pool.connect_all()
        if management is not None:
            bind = getattr(management, "bind_pool", None)
            if callable(bind):
                result = bind(self.pool)
                if inspect.isawaitable(result):
                    await result
        user_sources: frozenset[str] = frozenset()
        if management is not None:
            value = getattr(management, "user_sources", ())
            value = value() if callable(value) else value
            if inspect.isawaitable(value):
                value = await value
            user_sources = frozenset(value)
        if "agent_tools" in self.context.config.surfaces:
            self.pool.bind_capability_catalog(
                self.runtime.capabilities,
                trusted_modules=(),
                target_label=str(options.get("target_label") or "External MCP server"),
                user_sources=user_sources,
                graph_generation=self.context.profile.generation,
                source_wrapper=options.get("source_wrapper"),
            )
        capabilities: list[CapabilityContribution] = []
        # External sources are registered by PoolCatalogBinding so reloads can
        # atomically replace exact toolkit instances. The manager is a native,
        # in-process capability of this module and is present only in dynamic mode.
        if mode == "dynamic" and "agent_tools" in self.context.config.surfaces:
            toolkit = _manager_toolkit()
            source = PoolCapabilitySource(_SingleToolkitPool(toolkit), "mcp", toolkit,
                                          toolkit_name="mcp-manager")
            capabilities.append(CapabilityContribution(
                "mcp", source, source,
                str(options.get("target_label") or "External MCP catalog"),
            ))
            legacy = PoolCapabilitySource(_SingleToolkitPool(toolkit), "mcp-manager", toolkit,
                                          toolkit_name="mcp-manager")
            capabilities.append(CapabilityContribution(
                "mcp-manager", legacy, legacy,
                str(options.get("target_label") or "External MCP catalog"),
            ))
        services = ()
        if "host_api" in self.context.config.surfaces or "service" in self.context.config.surfaces:
            services = (ServiceBinding("mcp.service", self),)
        self._contribution = ModuleContribution(
            services=services,
            capabilities=tuple(capabilities),
            prompt_blocks=(MCP_PROMPT,) if "agent_tools" in self.context.config.surfaces else (),
            search_providers=(self,),
        )
        return self._contribution

    async def _create_pool(self, options: Mapping[str, Any]) -> MCPPool:
        factory = options.get("pool_factory")
        if factory is not None:
            value = factory(self.context)
            if inspect.isawaitable(value):
                value = await value
            if not isinstance(value, MCPPool):
                raise TypeError("mcp.pool_factory must return MCPPool")
            return value
        sources = [dict(item) for item in options.get("sources", ())]
        return MCPPool.from_config(
            sources,
            include_defaults=False,
            db_path=str(options["db_path"]) if options.get("db_path") else None,
        )

    async def reconfigure(self, config: ModuleConfig) -> ModuleContribution:
        if config != self.context.config:
            raise RuntimeError("MCP reconfiguration uses an atomic graph replacement")
        return self._contribution

    async def drain(self) -> None:
        return None

    async def close(self) -> None:
        if self._management is not None:
            unbind = getattr(self._management, "unbind_pool", None)
            if callable(unbind):
                result = unbind(self.pool)
                if inspect.isawaitable(result):
                    await result
        if self.pool is not None:
            self.pool.unbind_capability_catalog()
            await self.pool.close_all()
            self.pool = None

    async def search(self, context, *, query: str, limit: int = 20, cursor: str | None = None):
        query = query.casefold().strip()
        if not query:
            return {"hits": ()}
        descriptors = await self.runtime.capabilities.discover(context)
        hits = [
            {"source_id": item.source_id, "name": item.name,
             "description": item.description, "tool_ref": item.tool_ref}
            for item in descriptors
            if query in item.source_id.casefold() or query in item.name.casefold()
            or query in item.description.casefold()
        ]
        return {"hits": hits[:limit], "next_cursor": None}


class _SingleToolkitPool:
    """Exact adapter boundary for the native manager toolkit."""

    def __init__(self, toolkit: Any) -> None:
        self.toolkit = toolkit

    def toolkit_by_name(self, name: str):
        return self.toolkit if name == "mcp-manager" else None


def _manager_toolkit():
    from openagent_core.mcp.servers.mcp_manager.server import build_runtime_toolkit
    return build_runtime_toolkit()


descriptor = MCPDescriptor()

__all__ = ["MCPDescriptor", "MCPModule", "descriptor"]
