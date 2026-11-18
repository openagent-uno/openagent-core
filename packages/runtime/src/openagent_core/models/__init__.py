"""Model contracts with optional engine implementations loaded on demand."""
from importlib import import_module

from openagent_core.models.base import BaseModel, ModelResponse, ToolCall

_ENGINE_EXPORTS = {
    "NativeProvider": "native_provider",
    "ModelDispatcher": "dispatcher",
    "BudgetTracker": "budget",
}


def __getattr__(name):
    module = _ENGINE_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f"{__name__}.{module}"), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))

__all__ = [
    "BaseModel", "ModelResponse", "ToolCall",
    "NativeProvider",
    "ModelDispatcher", "BudgetTracker",
]
