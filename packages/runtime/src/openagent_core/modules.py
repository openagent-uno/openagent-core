"""Uniform, host-controlled OpenAgent module composition.

The kernel knows how to validate and run descriptors, but it does not contain
feature names or select optional functionality.  Products install descriptors
from independent distributions and build one :class:`RuntimeProfile` per
runtime instance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Iterable, Mapping, Protocol
from importlib.metadata import entry_points


MODULE_API_VERSION = "1"
MODULE_SURFACES = frozenset({"service", "agent_tools", "host_api", "workers", "event_ingress"})


class ModuleResolutionError(ValueError):
    """A profile cannot be resolved without changing runtime state."""


class ModuleReconfigurationError(RuntimeError):
    """A prepared graph could not be activated atomically."""


class ModuleReferenceConflict(ModuleReconfigurationError):
    """Durable resources still refer to modules selected for deactivation."""

    def __init__(self, references: Mapping[str, tuple[str, ...]]) -> None:
        self.references = MappingProxyType({
            str(module_id): tuple(str(value) for value in values)
            for module_id, values in references.items() if values
        })
        detail = "; ".join(
            f"{module_id}: {', '.join(values)}"
            for module_id, values in sorted(self.references.items())
        )
        super().__init__(
            "Active durable resources depend on modules being removed"
            + (f" ({detail})" if detail else "")
        )


@dataclass(frozen=True, slots=True)
class ModuleConfig:
    surfaces: frozenset[str] = frozenset({"service"})
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        surfaces = frozenset(self.surfaces)
        unknown = surfaces - MODULE_SURFACES
        if unknown:
            raise ValueError("Unknown module surfaces: " + ", ".join(sorted(unknown)))
        if not isinstance(self.options, Mapping):
            raise TypeError("Module options must be a mapping")
        object.__setattr__(self, "surfaces", surfaces)
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    generation: int = 0
    modules: Mapping[str, ModuleConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.generation, int) or self.generation < 0:
            raise ValueError("Runtime profile generation must be a non-negative integer")
        normalized: dict[str, ModuleConfig] = {}
        for module_id, config in self.modules.items():
            if not isinstance(module_id, str) or not module_id.strip():
                raise ValueError("Runtime profile module IDs must be non-empty strings")
            if not isinstance(config, ModuleConfig):
                raise TypeError(f"Profile entry {module_id!r} is not a ModuleConfig")
            normalized[module_id] = config
        object.__setattr__(self, "modules", MappingProxyType(normalized))

    @property
    def module_ids(self) -> frozenset[str]:
        return frozenset(self.modules)


@dataclass(frozen=True, slots=True)
class ModuleMigration:
    id: str
    revision: int
    apply: Callable[["ModuleContext"], Awaitable[None]]

    def __post_init__(self) -> None:
        if not self.id or self.revision < 1 or not callable(self.apply):
            raise ValueError("A module migration requires an ID, positive revision and apply callback")


@dataclass(frozen=True, slots=True)
class ServiceBinding:
    key: object
    value: Any


@dataclass(frozen=True, slots=True)
class CapabilityContribution:
    source_id: str
    source: Any
    executor: Any
    target_label: str
    managed: bool = True
    trusted_effects: Mapping[str, frozenset[str]] | frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class ModuleContribution:
    services: tuple[ServiceBinding, ...] = ()
    capabilities: tuple[CapabilityContribution, ...] = ()
    prompt_blocks: tuple[Any, ...] = ()
    routes: tuple[Any, ...] = ()
    health_checks: tuple[Any, ...] = ()
    search_providers: tuple[Any, ...] = ()
    workers: tuple[Any, ...] = ()
    event_ingresses: tuple[Any, ...] = ()
    indices: tuple[Any, ...] = ()


class ServiceRegistry:
    """Typed runtime bindings with explicit ownership and graph overlays."""

    def __init__(self, initial: Mapping[object, Any] | None = None) -> None:
        self._values: dict[object, Any] = dict(initial or {})
        self._owners: dict[object, str] = {key: "host" for key in self._values}
        self._multi: dict[object, list[tuple[str, Any]]] = {}

    def clone(self) -> "ServiceRegistry":
        cloned = ServiceRegistry()
        cloned._values = dict(self._values)
        cloned._owners = dict(self._owners)
        cloned._multi = {key: list(values) for key, values in self._multi.items()}
        return cloned

    def bind(self, key: object, value: Any, *, owner: str = "host", replace: bool = False) -> None:
        if key in self._values and not replace:
            raise ModuleResolutionError(f"Service {key!r} is already provided by {self._owners[key]}")
        self._values[key] = value
        self._owners[key] = owner

    def bind_many(self, key: object, value: Any, *, owner: str) -> None:
        self._multi.setdefault(key, []).append((owner, value))

    def require(self, key: object) -> Any:
        try:
            return self._values[key]
        except KeyError:
            raise ModuleResolutionError(f"Required service is unavailable: {key!r}") from None

    def optional(self, key: object, default: Any = None) -> Any:
        return self._values.get(key, default)

    def all(self, key: object) -> tuple[Any, ...]:
        values = []
        if key in self._values:
            values.append(self._values[key])
        values.extend(value for _owner, value in self._multi.get(key, ()))
        return tuple(values)

    def has(self, key: object) -> bool:
        return key in self._values or bool(self._multi.get(key))

    def owner(self, key: object) -> str | None:
        """Return the single binding owner, or ``None`` for no binding."""
        return self._owners.get(key)

    def unbind(self, key: object, *, owner: str | None = None) -> None:
        """Remove one binding without allowing a module to remove host state."""
        current = self._owners.get(key)
        if current is None:
            return
        if owner is not None and current != owner:
            raise ModuleResolutionError(
                f"Service {key!r} is owned by {current}, not {owner}"
            )
        self._owners.pop(key, None)
        self._values.pop(key, None)

    def remove_owner(self, owner: str) -> None:
        for key in [key for key, current in self._owners.items() if current == owner]:
            self._owners.pop(key, None)
            self._values.pop(key, None)
        for key, values in tuple(self._multi.items()):
            remaining = [entry for entry in values if entry[0] != owner]
            if remaining:
                self._multi[key] = remaining
            else:
                self._multi.pop(key, None)


@dataclass(frozen=True, slots=True)
class ModuleContext:
    runtime: Any
    descriptor: "ModuleDescriptor"
    config: ModuleConfig
    services: ServiceRegistry
    profile: RuntimeProfile


class ModuleInstance(Protocol):
    async def start(self) -> ModuleContribution: ...
    async def reconfigure(self, config: ModuleConfig) -> ModuleContribution: ...
    async def drain(self) -> None: ...
    async def close(self) -> None: ...


class ModuleDescriptor(Protocol):
    id: str
    version: str
    api_version: str
    requires_modules: frozenset[str]
    optional_integrations: frozenset[str]
    required_services: frozenset[object]
    provided_services: frozenset[object]
    supported_surfaces: frozenset[str]

    def validate(self, config: ModuleConfig) -> None: ...
    def required_services_for(self, config: ModuleConfig) -> frozenset[object]: ...
    def migrations(self) -> tuple[ModuleMigration, ...]: ...
    async def prepare(self, context: ModuleContext) -> ModuleInstance: ...


class ModuleReferenceInspector(Protocol):
    async def references_for_removed_modules(
        self,
        removed_modules: frozenset[str],
        *,
        current_profile: RuntimeProfile,
        candidate_profile: RuntimeProfile,
    ) -> Mapping[str, Iterable[str]]: ...


@dataclass(frozen=True, slots=True)
class ResolvedProfile:
    profile: RuntimeProfile
    descriptors: tuple[ModuleDescriptor, ...]


class ModuleCatalog:
    """Installed descriptors. Installation never implies activation."""

    def __init__(self, descriptors: Iterable[ModuleDescriptor] = ()) -> None:
        self._descriptors: dict[str, ModuleDescriptor] = {}
        for descriptor in descriptors:
            self.register(descriptor)

    @classmethod
    def from_entry_points(cls, *, group: str = "openagent.modules") -> "ModuleCatalog":
        """Load installed, verified module descriptors without activating them.

        Products may instead pass descriptors directly for a fully static build.
        Entry-point discovery is deterministic: distributions are loaded in
        entry-point-name order and duplicate module IDs fail closed.
        """
        selected = entry_points().select(group=group)
        descriptors = []
        for entry in sorted(selected, key=lambda item: (item.name, item.value)):
            descriptor = entry.load()
            if callable(descriptor) and not hasattr(descriptor, "id"):
                descriptor = descriptor()
            descriptors.append(descriptor)
        return cls(descriptors)

    def register(self, descriptor: ModuleDescriptor) -> None:
        module_id = getattr(descriptor, "id", "")
        if not isinstance(module_id, str) or not module_id:
            raise ValueError("A module descriptor requires an ID")
        if module_id in self._descriptors:
            raise ValueError(f"Duplicate installed module: {module_id}")
        if getattr(descriptor, "api_version", None) != MODULE_API_VERSION:
            raise ModuleResolutionError(
                f"Module {module_id} targets unsupported API {getattr(descriptor, 'api_version', None)!r}"
            )
        self._descriptors[module_id] = descriptor

    def get(self, module_id: str) -> ModuleDescriptor:
        try:
            return self._descriptors[module_id]
        except KeyError:
            raise ModuleResolutionError(f"Module is configured but not installed: {module_id}") from None

    @property
    def installed(self) -> tuple[str, ...]:
        return tuple(sorted(self._descriptors))

    @property
    def descriptors(self) -> tuple[ModuleDescriptor, ...]:
        return tuple(self._descriptors[module_id] for module_id in sorted(self._descriptors))

    def resolve(self, profile: RuntimeProfile, services: ServiceRegistry) -> ResolvedProfile:
        selected = {module_id: self.get(module_id) for module_id in sorted(profile.modules)}
        graph: dict[str, set[str]] = {}
        providers: dict[object, list[str]] = {}
        for provider_id, descriptor in selected.items():
            for key in descriptor.provided_services:
                providers.setdefault(key, []).append(provider_id)
        for module_id, descriptor in selected.items():
            missing = set(descriptor.requires_modules) - set(selected)
            if missing:
                raise ModuleResolutionError(
                    f"Module {module_id} requires missing modules: {', '.join(sorted(missing))}"
                )
            unsupported = profile.modules[module_id].surfaces - descriptor.supported_surfaces
            if unsupported:
                raise ModuleResolutionError(
                    f"Module {module_id} does not support surfaces: {', '.join(sorted(unsupported))}"
                )
            required_for = getattr(descriptor, "required_services_for", None)
            required_services = (required_for(profile.modules[module_id])
                                 if callable(required_for) else descriptor.required_services)
            missing_services = [key for key in required_services
                                if not services.has(key) and key not in providers]
            if missing_services:
                raise ModuleResolutionError(
                    f"Module {module_id} requires unavailable services: {missing_services!r}"
                )
            descriptor.validate(profile.modules[module_id])
            graph[module_id] = set(descriptor.requires_modules)
            for key in required_services:
                if services.has(key):
                    continue
                candidates = providers.get(key, ())
                if len(candidates) != 1:
                    raise ModuleResolutionError(
                        f"Service {key!r} required by {module_id} has ambiguous providers: {candidates!r}"
                    )
                if candidates[0] != module_id:
                    graph[module_id].add(candidates[0])
        try:
            order = tuple(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise ModuleResolutionError("Module dependency graph contains a cycle") from exc
        return ResolvedProfile(profile, tuple(selected[module_id] for module_id in order))


@dataclass(frozen=True, slots=True)
class ModuleStatus:
    id: str
    version: str
    surfaces: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ReconfigureReceipt:
    previous_generation: int
    generation: int
    activated: tuple[ModuleStatus, ...]
    deactivated: tuple[str, ...]
    mode: str


__all__ = [
    "MODULE_API_VERSION", "MODULE_SURFACES", "CapabilityContribution", "ModuleCatalog",
    "ModuleConfig", "ModuleContext", "ModuleContribution", "ModuleDescriptor", "ModuleInstance",
    "ModuleMigration", "ModuleReconfigurationError", "ModuleReferenceConflict",
    "ModuleReferenceInspector", "ModuleResolutionError", "ModuleStatus",
    "ReconfigureReceipt", "ResolvedProfile", "RuntimeProfile", "ServiceBinding", "ServiceRegistry",
]
