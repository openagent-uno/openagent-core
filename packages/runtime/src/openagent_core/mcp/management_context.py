"""Task-local DB selection, without loading adapters in legacy stdio servers."""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .management_adapters import ManagementToolkit


_binding: ContextVar[tuple[ManagementToolkit, asyncio.Task] | None] = ContextVar(
    "management_toolkit_binding", default=None
)


def _bound(role: str) -> ManagementToolkit | None:
    binding = _binding.get()
    if binding is None:
        return None  # The unchanged standalone stdio handlers own their DB.
    toolkit, task = binding
    if task is not asyncio.current_task() or toolkit.name != role:
        raise RuntimeError("Management database binding does not match this call")
    toolkit._check_owner()
    return toolkit


async def scoped_database(role: str):
    toolkit = _bound(role)
    return await toolkit._database() if toolkit is not None else None


def scoped_path(role: str) -> str | None:
    toolkit = _bound(role)
    return toolkit.db_path if toolkit is not None else None
