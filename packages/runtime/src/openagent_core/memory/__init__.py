"""Optional engine memory API; importing storage helpers does not load the engine."""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .db import MemoryDB

__all__ = ["MemoryDB"]


def __getattr__(name):
    if name == "MemoryDB":
        from .db import MemoryDB
        return MemoryDB
    raise AttributeError(name)
