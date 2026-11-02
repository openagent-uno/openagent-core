"""Normalized operational history, activity, and derived-search support.

The Memory Vault deliberately does not live in this package.  Operational
history is canonical SQLite state; its FTS database is a disposable projection.
"""

__all__ = [
    "OPERATIONAL_SCHEMA_VERSION",
    "OperationalMigrationError",
    "ensure_operational_storage",
    "PhaseTransition",
    "StoragePhaseError",
    "transition_storage_phase",
    "transition_storage_phase_async",
]



def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from importlib import import_module
    module = 'schema' if name in {
        'OPERATIONAL_SCHEMA_VERSION', 'OperationalMigrationError', 'ensure_operational_storage'
    } else 'phase'
    return getattr(import_module(f'{__name__}.{module}'), name)
