"""Run cancellation management."""

from typing import Dict
from contextvars import ContextVar

from openagent_core.core._run_state.cancellation_management.base import BaseRunCancellationManager
from openagent_core.core._run_state.cancellation_management.in_memory_cancellation_manager import InMemoryRunCancellationManager
from openagent_core.core._runner.utils.log import logger

# Global cancellation manager instance
_cancellation_manager: ContextVar[BaseRunCancellationManager | None] = ContextVar("openagent_cancellation_manager", default=None)


def set_cancellation_manager(manager: BaseRunCancellationManager) -> None:
    """Set a custom cancellation manager.

    Args:
        manager: A BaseRunCancellationManager instance or subclass.

    Example:
        ```python
        class MyCustomManager(BaseRunCancellationManager):
            ....

        set_cancellation_manager(MyCustomManager())
        ```
    """
    from openagent_core.runtime import current_runtime
    runtime = current_runtime()
    if runtime is not None:
        runtime.cancellation_manager = manager
    else:
        _cancellation_manager.set(manager)
    logger.info(f"Cancellation manager set to {type(manager).__name__}")


def get_cancellation_manager() -> BaseRunCancellationManager:
    """Get the current cancellation manager instance."""
    from openagent_core.runtime import current_runtime
    runtime = current_runtime()
    if runtime is not None:
        manager = getattr(runtime,"cancellation_manager",None)
        if manager is None:
            manager = InMemoryRunCancellationManager()
            runtime.cancellation_manager = manager
        return manager
    manager = _cancellation_manager.get()
    if manager is None:
        manager = InMemoryRunCancellationManager()
        _cancellation_manager.set(manager)
    return manager


def register_run(run_id: str) -> None:
    """Register a new run for cancellation tracking."""
    get_cancellation_manager().register_run(run_id)


async def aregister_run(run_id: str) -> None:
    """Register a new run for cancellation tracking (async version)."""
    await get_cancellation_manager().aregister_run(run_id)


def cancel_run(run_id: str) -> bool:
    """Cancel a run."""
    return get_cancellation_manager().cancel_run(run_id)


async def acancel_run(run_id: str) -> bool:
    """Cancel a run (async version)."""
    return await get_cancellation_manager().acancel_run(run_id)


def is_cancelled(run_id: str) -> bool:
    """Check if a run is cancelled."""
    return get_cancellation_manager().is_cancelled(run_id)


async def ais_cancelled(run_id: str) -> bool:
    """Check if a run is cancelled (async version)."""
    return await get_cancellation_manager().ais_cancelled(run_id)


def cleanup_run(run_id: str) -> None:
    """Clean up cancellation tracking for a completed run."""
    get_cancellation_manager().cleanup_run(run_id)


async def acleanup_run(run_id: str) -> None:
    """Clean up cancellation tracking for a completed run (async version)."""
    await get_cancellation_manager().acleanup_run(run_id)


def raise_if_cancelled(run_id: str) -> None:
    """Check if a run should be cancelled and raise exception if so."""
    get_cancellation_manager().raise_if_cancelled(run_id)


async def araise_if_cancelled(run_id: str) -> None:
    """Check if a run should be cancelled and raise exception if so (async version)."""
    await get_cancellation_manager().araise_if_cancelled(run_id)


def get_active_runs() -> Dict[str, bool]:
    """Get all currently tracked runs and their cancellation status."""
    return get_cancellation_manager().get_active_runs()


async def aget_active_runs() -> Dict[str, bool]:
    """Get all currently tracked runs and their cancellation status (async version)."""
    return await get_cancellation_manager().aget_active_runs()
