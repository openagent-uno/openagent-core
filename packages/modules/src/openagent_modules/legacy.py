"""One-release compatibility for pre-descriptor OpenAgent consumers."""
from __future__ import annotations

from typing import Iterable


_SELECTIONS = {
    "vault": ("vault", "vault-gate"),
    "history": ("memory-search",),
    "delegation": ("delegation",),
    "automation": ("scheduler", "workflow-manager", "events-manager"),
    "skills": ("skills", "skill-data"),
    "models": ("model-manager", "budget-manager"),
    "attachments": ("attachments",),
    "logs": ("logs",),
    "ptc": ("ptc",),
}


def module_pool(modules: tuple[str, ...], *, db_path: str, vault_path: str | None = None,
                environment: dict[str, str] | None = None):
    from openagent_core.mcp.pool import MCPPool

    unknown = set(modules) - set(_SELECTIONS)
    if unknown:
        raise ValueError("Unknown reusable modules: " + ", ".join(sorted(unknown)))
    names = ["tool-search"]
    for module in modules:
        names.extend(_SELECTIONS[module])
    env = dict(environment or {})
    if "vault" in modules:
        if not vault_path:
            raise ValueError("The vault module requires an explicit vault directory")
        env["OPENAGENT_VAULT_PATH"] = vault_path
    return MCPPool.from_config(
        [{"builtin": name, "env": dict(env)} for name in dict.fromkeys(names)],
        include_defaults=False,
        db_path=db_path,
    )


def modules_for_catalog(source_names: Iterable[str]) -> frozenset[str]:
    names = frozenset(source_names)
    return frozenset(
        module for module, sources in _SELECTIONS.items() if names & set(sources)
    )


__all__ = ["module_pool", "modules_for_catalog"]
