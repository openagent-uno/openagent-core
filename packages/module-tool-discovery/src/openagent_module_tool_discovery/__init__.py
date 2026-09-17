from __future__ import annotations
from dataclasses import dataclass

from openagent_core.modules import MODULE_API_VERSION,ModuleConfig,ModuleContext,ModuleContribution,ModuleMigration
from openagent_core.prompts import rule_blocks


@dataclass(frozen=True,slots=True)
class ToolDiscoveryDescriptor:
    id:str="tool-discovery";version:str="1.1.0b1";api_version:str=MODULE_API_VERSION
    requires_modules:frozenset[str]=frozenset();optional_integrations:frozenset[str]=frozenset()
    required_services:frozenset[object]=frozenset({"runtime.executor"})
    provided_services:frozenset[object]=frozenset()
    supported_surfaces:frozenset[str]=frozenset({"service","agent_tools"})
    def validate(self,config:ModuleConfig)->None:
        if "agent_tools" not in config.surfaces:raise ValueError("tool-discovery requires its agent_tools surface")
    def migrations(self)->tuple[ModuleMigration,...]:return ()
    async def prepare(self,context:ModuleContext):return ToolDiscoveryModule(context)


class ToolDiscoveryModule:
    def __init__(self,context):self.context=context;self._contribution=ModuleContribution()
    async def start(self):
        executor=self.context.services.require("runtime.executor");agent=getattr(executor,"agent",None)
        if agent is None:raise RuntimeError("tool-discovery requires an engine executor")
        pool=agent.capability_pool
        names={getattr(spec,"name",None) for spec in getattr(pool,"specs",())}
        if "tool-search" not in names:
            from openagent_core.mcp.pool import MCPPool
            agent.set_capability_pool(MCPPool.from_config([{"builtin":"tool-search"}],include_defaults=False))
        self._contribution=ModuleContribution(prompt_blocks=rule_blocks(
            "core.tools","core.managers","core.tool-preference"))
        return self._contribution
    async def reconfigure(self,config):return self._contribution
    async def drain(self):return None
    async def close(self):return None


descriptor=ToolDiscoveryDescriptor()
__all__=["descriptor"]
