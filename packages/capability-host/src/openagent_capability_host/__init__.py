"""Explicit capability host. Imports no tools, product or agent engine."""
from .host import CapabilityHost
from .paths import HostPaths
from .config import PluginSpec, PluginConfigStore
from .consent import ConsentState, ConsentStore, CONSENT_VERSION
from .mcp_stdio import MCPStdioServer
