"""Core runtime: agent loop, server lifecycle, scheduler, config, prompts.

The submodules import lazily — eagerly importing ``Agent`` /
``AgentServer`` at package-init pulls in ``openagent_core.mcp.pool``, which in turn
needs ``openagent_core.core.logging``, which re-enters this ``__init__`` and
deadlocks before ``Agent`` is bound. Subprocess MCPs (scheduler /
workflow-manager) trip the cycle because their import path comes through
``openagent_core.memory.db`` rather than the main process's order. Importing
``openagent_core.core.config.load_config`` lazily lets every caller use the full
``from openagent_core.core.<module> import ...`` form without paying that cost.
"""

from openagent_core.core.config import load_config

__all__ = ["load_config"]
