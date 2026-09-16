"""Instance-owned registries for reusable engine modules."""
from contextvars import ContextVar
from collections.abc import MutableMapping, MutableSet

_fallback: ContextVar[dict | None] = ContextVar("openagent_unbound_registries",default=None)


def registry(name: str) -> dict:
    from .runtime import current_runtime
    runtime = current_runtime()
    if runtime is not None:
        if not hasattr(runtime,"module_registries"):
            runtime.module_registries = {}
        return runtime.module_registries.setdefault(name,{})
    # Direct module APIs have task-local registries. Embedding hosts bind a
    # Runtime before starting modules so concurrent agents never share these.
    registries = _fallback.get()
    if registries is None:
        registries = {}
        _fallback.set(registries)
    return registries.setdefault(name,{})


class InstanceMapping(MutableMapping):
    """A module-level handle whose contents belong to the bound runtime."""
    def __init__(self,name): self.name=name
    def __getitem__(self,key): return registry(self.name)[key]
    def __setitem__(self,key,value): registry(self.name)[key]=value
    def __delitem__(self,key): del registry(self.name)[key]
    def __iter__(self): return iter(registry(self.name))
    def __len__(self): return len(registry(self.name))


class InstanceSet(MutableSet):
    def __init__(self,name): self.name=name
    def __contains__(self,key): return key in registry(self.name)
    def __iter__(self): return iter(registry(self.name))
    def __len__(self): return len(registry(self.name))
    def add(self,key): registry(self.name)[key]=True
    def discard(self,key): registry(self.name).pop(key,None)
