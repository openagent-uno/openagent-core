"""Deprecated full-profile compatibility package.

Feature implementations live in their individual ``openagent-module-*``
distributions. Import from those packages for new integrations.
"""

__version__ = "1.1.0b1"


def module_assets(name: str):
    """Compatibility redirect to the owning module distribution."""
    if name == "vault":
        from openagent_module_vault import module_assets as owned_assets
        return owned_assets(name)
    raise LookupError(f"Unknown module resources: {name}")
