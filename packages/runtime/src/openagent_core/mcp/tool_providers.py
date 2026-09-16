"""Compatibility exports for the uniform capability source/executor contract.

Routing is an internal registration concern. Model-facing APIs use the opaque
reference issued by CapabilityCatalog, irrespective of transport or destination.
"""
from openagent_core.capabilities import CapabilitySource as ToolCatalogProvider
from openagent_core.capabilities import ToolExecutor as ToolDispatcher
from .catalog import PoolCapabilitySource, InteractiveCapabilitySource, register_interactive_capabilities, revoke_interactive_capabilities

# Import aliases for code moving from the previous internal provider module.
ServerMCPProvider = PoolCapabilitySource
InteractiveClientMCPProvider = InteractiveCapabilitySource

__all__ = ['ToolCatalogProvider', 'ToolDispatcher', 'PoolCapabilitySource',
           'InteractiveCapabilitySource', 'register_interactive_capabilities', 'revoke_interactive_capabilities',
           'ServerMCPProvider', 'InteractiveClientMCPProvider']
