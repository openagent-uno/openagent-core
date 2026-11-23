"""Compatibility imports for the delegated-child tool scope."""

from openagent_core.tool_scope import (
    current_tool_allowlist,
    normalize_family,
    reset_tool_allowlist,
    set_tool_allowlist,
)

__all__ = ["current_tool_allowlist", "normalize_family", "reset_tool_allowlist", "set_tool_allowlist"]
